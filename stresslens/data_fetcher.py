"""
StressLens Data Fetcher
Primary source: Screener.in HTML page (scrapes financial tables)
Secondary source: NSE quote API (company name, price)
Shareholding: Screener.in shareholding table
Falls back to hardcoded DHFL historical data ONLY for DHFL symbol.
"""

import requests
import time
import json
import os
import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup


# DHFL hardcoded historical data (the company that crashed from 690 to 30)
DHFL_HISTORICAL = {
    "Q1_FY2018": {
        "quarter": "Q1_FY2018",
        "pledge_pct": 41,
        "profit": 380,
        "cfo": 210,
        "promoter_holding": 39.2,
        "sales": 3200,
        "receivables": 890,
        "cogs": 2100,
        "ppe": 120,
        "current_assets": 18500,
        "total_assets": 95000,
        "depreciation": 35,
        "sga": 310,
        "long_term_debt": 62000,
        "current_liabilities": 15000,
        "total_liabilities": 80000,
        "working_capital": 3500,
        "retained_earnings": 4200,
        "ebit": 520,
        "market_cap": 32000,
        "shares_outstanding": 313,
        "gross_margin": 0.344,
        "roa": 0.004,
    },
    "Q2_FY2018": {
        "quarter": "Q2_FY2018",
        "pledge_pct": 52,
        "profit": 390,
        "cfo": 180,
        "promoter_holding": 39.1,
        "sales": 3400,
        "receivables": 1020,
        "cogs": 2250,
        "ppe": 125,
        "current_assets": 19200,
        "total_assets": 102000,
        "depreciation": 36,
        "sga": 330,
        "long_term_debt": 68000,
        "current_liabilities": 16500,
        "total_liabilities": 87000,
        "working_capital": 2700,
        "retained_earnings": 4500,
        "ebit": 540,
        "market_cap": 34000,
        "shares_outstanding": 313,
        "gross_margin": 0.338,
        "roa": 0.0038,
    },
    "Q3_FY2018": {
        "quarter": "Q3_FY2018",
        "pledge_pct": 58,
        "profit": 410,
        "cfo": 120,
        "promoter_holding": 38.9,
        "sales": 3600,
        "receivables": 1210,
        "cogs": 2400,
        "ppe": 128,
        "current_assets": 20500,
        "total_assets": 110000,
        "depreciation": 37,
        "sga": 345,
        "long_term_debt": 75000,
        "current_liabilities": 18000,
        "total_liabilities": 96000,
        "working_capital": 2500,
        "retained_earnings": 4700,
        "ebit": 560,
        "market_cap": 35000,
        "shares_outstanding": 313,
        "gross_margin": 0.333,
        "roa": 0.0037,
    },
    "Q4_FY2018": {
        "quarter": "Q4_FY2018",
        "pledge_pct": 61,
        "profit": 420,
        "cfo": -40,
        "promoter_holding": 38.7,
        "sales": 3800,
        "receivables": 1450,
        "cogs": 2580,
        "ppe": 130,
        "current_assets": 22000,
        "total_assets": 118000,
        "depreciation": 38,
        "sga": 360,
        "long_term_debt": 82000,
        "current_liabilities": 20000,
        "total_liabilities": 105000,
        "working_capital": 2000,
        "retained_earnings": 4900,
        "ebit": 570,
        "market_cap": 28000,
        "shares_outstanding": 313,
        "gross_margin": 0.321,
        "roa": 0.0036,
    },
    "Q1_FY2019": {
        "quarter": "Q1_FY2019",
        "pledge_pct": 65,
        "profit": 430,
        "cfo": -180,
        "promoter_holding": 38.2,
        "sales": 4000,
        "receivables": 1700,
        "cogs": 2780,
        "ppe": 132,
        "current_assets": 23500,
        "total_assets": 125000,
        "depreciation": 39,
        "sga": 380,
        "long_term_debt": 88000,
        "current_liabilities": 22000,
        "total_liabilities": 113000,
        "working_capital": 1500,
        "retained_earnings": 5000,
        "ebit": 580,
        "market_cap": 22000,
        "shares_outstanding": 313,
        "gross_margin": 0.305,
        "roa": 0.0034,
    },
    "Q2_FY2019": {
        "quarter": "Q2_FY2019",
        "pledge_pct": 71,
        "profit": 440,
        "cfo": -320,
        "promoter_holding": 37.8,
        "sales": 4200,
        "receivables": 2050,
        "cogs": 2980,
        "ppe": 134,
        "current_assets": 25000,
        "total_assets": 132000,
        "depreciation": 40,
        "sga": 400,
        "long_term_debt": 95000,
        "current_liabilities": 24000,
        "total_liabilities": 122000,
        "working_capital": 1000,
        "retained_earnings": 5100,
        "ebit": 590,
        "market_cap": 15000,
        "shares_outstanding": 313,
        "gross_margin": 0.290,
        "roa": 0.0033,
    },
}


def _parse_indian_number(text: str) -> float:
    """Parse Indian-formatted numbers like '19,14,032' or '-1,234.56' or '11%'."""
    if not text or not text.strip():
        return 0.0
    text = text.strip()
    # Remove percentage sign
    text = text.replace("%", "")
    # Remove commas
    text = text.replace(",", "")
    # Remove any non-numeric chars except minus and dot
    text = re.sub(r"[^\d.\-]", "", text)
    try:
        return float(text)
    except ValueError:
        return 0.0


def _get_last_value(row_cells) -> float:
    """Get the last non-empty numeric value from a table row."""
    for cell in reversed(row_cells[1:]):  # skip row label
        text = cell.get_text(strip=True)
        if text and text != "—" and text != "-":
            return _parse_indian_number(text)
    return 0.0


def _get_last_two_values(row_cells) -> tuple:
    """Get the last two non-empty values from a table row (current, previous)."""
    values = []
    for cell in reversed(row_cells[1:]):
        text = cell.get_text(strip=True)
        if text and text != "—" and text != "-":
            values.append(_parse_indian_number(text))
        if len(values) == 2:
            break
    if len(values) == 2:
        return values[0], values[1]  # current, previous
    elif len(values) == 1:
        return values[0], values[0]
    return 0.0, 0.0


# Common name-to-symbol mapping for search
NAME_TO_SYMBOL = {
    "reliance": "RELIANCE", "reliance industries": "RELIANCE", "ril": "RELIANCE",
    "tcs": "TCS", "tata consultancy": "TCS", "tata consultancy services": "TCS",
    "infosys": "INFY", "infy": "INFY",
    "hdfc bank": "HDFCBANK", "hdfcbank": "HDFCBANK", "hdfc": "HDFCBANK",
    "icici bank": "ICICIBANK", "icicibank": "ICICIBANK", "icici": "ICICIBANK",
    "sbi": "SBIN", "sbin": "SBIN", "state bank": "SBIN", "state bank of india": "SBIN",
    "yes bank": "YESBANK", "yesbank": "YESBANK",
    "dhfl": "DHFL", "dewan housing": "DHFL",
    "tata motors": "TATAMOTORS", "tatamotors": "TATAMOTORS",
    "wipro": "WIPRO",
    "itc": "ITC",
    "bharti airtel": "BHARTIARTL", "airtel": "BHARTIARTL", "bhartiartl": "BHARTIARTL",
    "axis bank": "AXISBANK", "axisbank": "AXISBANK", "axis": "AXISBANK",
    "kotak bank": "KOTAKBANK", "kotakbank": "KOTAKBANK", "kotak": "KOTAKBANK",
    "kotak mahindra": "KOTAKBANK", "kotak mahindra bank": "KOTAKBANK",
    "larsen": "LT", "l&t": "LT", "lt": "LT", "larsen and toubro": "LT",
    "bajaj finance": "BAJFINANCE", "bajfinance": "BAJFINANCE",
    "maruti": "MARUTI", "maruti suzuki": "MARUTI",
    "sun pharma": "SUNPHARMA", "sunpharma": "SUNPHARMA",
    "hcl tech": "HCLTECH", "hcltech": "HCLTECH", "hcl technologies": "HCLTECH",
    "power grid": "POWERGRID", "powergrid": "POWERGRID",
    "ntpc": "NTPC",
    "adani enterprises": "ADANIENT", "adanient": "ADANIENT", "adani": "ADANIENT",
    "tech mahindra": "TECHM", "techm": "TECHM",
    "titan": "TITAN",
    "asian paints": "ASIANPAINT", "asianpaint": "ASIANPAINT",
}


def normalize_symbol(query: str) -> str:
    """Convert a company name or symbol query to an NSE symbol."""
    q = query.strip()
    # If it's already all uppercase and no spaces, treat as symbol
    if q == q.upper() and " " not in q:
        return q
    # Try name lookup
    key = q.lower().strip()
    if key in NAME_TO_SYMBOL:
        return NAME_TO_SYMBOL[key]
    # Try removing common suffixes
    for suffix in [" ltd", " limited", " corp", " inc"]:
        if key.endswith(suffix):
            trimmed = key[:-len(suffix)].strip()
            if trimmed in NAME_TO_SYMBOL:
                return NAME_TO_SYMBOL[trimmed]
    # Fallback: uppercase it and hope it's a valid symbol
    return q.upper().replace(" ", "")


class DataFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        })
        self._nse_cookies_set = False

    # --- NSE SESSION --------------------------------------------

    def _init_nse_session(self):
        """Get cookies from NSE. Even a 403 sets the AKA_A2 cookie which is enough."""
        if self._nse_cookies_set:
            return True
        for attempt in range(2):
            try:
                resp = self.session.get("https://www.nseindia.com", timeout=10, allow_redirects=True)
                if self.session.cookies:
                    self.session.headers.update({
                        "Accept": "*/*",
                        "Referer": "https://www.nseindia.com/",
                    })
                    self._nse_cookies_set = True
                    print(f"[DataFetcher] NSE session initialized (status={resp.status_code}, cookies={[c.name for c in self.session.cookies]})")
                    return True
                else:
                    print(f"[DataFetcher] NSE homepage returned {resp.status_code} but no cookies set")
            except Exception as e:
                print(f"[DataFetcher] NSE session init attempt {attempt+1} failed: {type(e).__name__}: {e}")
                time.sleep(1)
        print("[DataFetcher] ERROR: Could not establish NSE session after 2 attempts")
        return False

    def _nse_api_get(self, url: str) -> Optional[requests.Response]:
        """Make an NSE API call with session handling + retry on 401/403."""
        if not self._init_nse_session():
            print(f"[DataFetcher] SKIP {url} -no NSE session")
            return None
        try:
            resp = self.session.get(url, timeout=15)
            if resp.status_code in (401, 403):
                print(f"[DataFetcher] NSE returned {resp.status_code} for {url} -refreshing cookies")
                self._nse_cookies_set = False
                if not self._init_nse_session():
                    return None
                resp = self.session.get(url, timeout=15)
            if resp.status_code == 200:
                return resp
            else:
                print(f"[DataFetcher] NSE returned {resp.status_code} for {url}")
        except Exception as e:
            print(f"[DataFetcher] NSE API error for {url}: {type(e).__name__}: {e}")
        return None

    # --- NSE QUOTE (company name, price) -----------------------

    def fetch_nse_quote(self, symbol: str) -> Optional[Dict]:
        """Fetch company name and basic info from NSE quote API."""
        try:
            url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
            resp = self._nse_api_get(url)
            if resp:
                data = resp.json()
                info = data.get("info", {})
                metadata = data.get("metadata", {})
                company_name = info.get("companyName", metadata.get("companyName", symbol))
                industry = metadata.get("industry", info.get("industry", ""))
                price_info = data.get("priceInfo", {})

                result = {
                    "company_name": company_name,
                    "industry": industry,
                    "last_price": price_info.get("lastPrice", 0),
                    "face_value": data.get("securityInfo", {}).get("faceValue", 10),
                    "issued_size": data.get("securityInfo", {}).get("issuedSize", 0),
                }
                print(f"[DataFetcher] NSE quote OK: {company_name} ({symbol})")
                return result
            else:
                print(f"[DataFetcher] NSE quote failed for {symbol} -API returned no data")
        except Exception as e:
            print(f"[DataFetcher] NSE quote error for {symbol}: {type(e).__name__}: {e}")
        return None

    # --- SCREENER.IN HTML SCRAPER (PRIMARY DATA SOURCE) --------

    def fetch_screener_data(self, symbol: str) -> Optional[Dict]:
        """
        Scrape financial data from Screener.in HTML page.
        Tries standalone first, then consolidated balance sheet.
        """
        urls = [
            f"https://www.screener.in/company/{symbol}/consolidated/",
            f"https://www.screener.in/company/{symbol}/",
        ]
        for url in urls:
            try:
                resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
                if resp.status_code == 200 and len(resp.text) > 5000:
                    variant = "consolidated" if "consolidated" in url else "standalone"
                    print(f"[DataFetcher] Screener.in {variant} loaded for {symbol} ({len(resp.text)} bytes)")
                    result = self._parse_screener_html(resp.text, symbol)
                    if result and result.get("sales", 0) > 0:
                        return result
                    print(f"[DataFetcher] Screener.in {variant} had no usable data, trying next...")
                elif resp.status_code == 404:
                    continue
                else:
                    print(f"[DataFetcher] Screener.in returned {resp.status_code} for {url}")
            except requests.exceptions.Timeout:
                print(f"[DataFetcher] Screener.in TIMEOUT for {url}")
            except requests.exceptions.ConnectionError as e:
                print(f"[DataFetcher] Screener.in CONNECTION ERROR: {e}")
            except Exception as e:
                print(f"[DataFetcher] Screener.in ERROR: {type(e).__name__}: {e}")
        print(f"[DataFetcher] Screener.in: all URLs failed for {symbol}")
        return None

    def _parse_screener_html(self, html: str, symbol: str) -> Optional[Dict]:
        """Parse the Screener.in HTML page and extract financial data."""
        soup = BeautifulSoup(html, "html.parser")
        result = {}

        # -- Company name --
        h1 = soup.find("h1")
        if h1:
            result["company_name"] = h1.get_text(strip=True)

        # -- Top ratios (Market Cap, Current Price, ROE, ROCE) --
        top_ratios = soup.find("ul", id="top-ratios")
        if top_ratios:
            for li in top_ratios.find_all("li"):
                name_el = li.find("span", class_="name")
                val_el = li.find("span", class_="number") or li.find("span", class_="value")
                if name_el and val_el:
                    name = name_el.get_text(strip=True).lower()
                    val = _parse_indian_number(val_el.get_text(strip=True))
                    if "market cap" in name:
                        result["market_cap"] = val
                    elif "current price" in name:
                        result["current_price"] = val
                    elif name == "roe":
                        result["roe"] = val
                    elif "face value" in name:
                        result["face_value"] = val

        # -- Financial tables --
        tables = soup.find_all("table")
        if len(tables) < 7:
            print(f"[DataFetcher] Screener.in: only {len(tables)} tables found for {symbol}, expected 7+")
            return result if result else None

        # Build a lookup: find tables by scanning row labels
        table_map = {}  # "pl", "bs", "cf", "shareholding"
        for i, table in enumerate(tables):
            rows = table.find_all("tr")
            labels = set()
            for row in rows[:15]:
                cells = row.find_all(["th", "td"])
                if cells:
                    labels.add(cells[0].get_text(strip=True).lower().split("\n")[0].strip().rstrip("+").strip())

            if "sales" in labels or "revenue" in labels:
                table_map["pl"] = table
            elif "total assets" in labels and "total liabilities" not in labels:
                table_map["bs"] = table
            elif "total liabilities" in labels:
                table_map["bs"] = table
            elif "borrowings" in labels and "reserves" in labels:
                table_map["bs"] = table
            elif "cash from operating activity" in labels:
                table_map["cf"] = table
            elif "promoters" in labels:
                if "shareholding" not in table_map:  # take the first one (quarterly)
                    table_map["shareholding"] = table

        # Helper to extract a named row's last value from a table
        def get_row_value(table, *row_names) -> float:
            if table is None:
                return 0.0
            for row in table.find_all("tr"):
                cells = row.find_all(["th", "td"])
                if not cells:
                    continue
                label = cells[0].get_text(strip=True).lower().split("\n")[0].strip().rstrip("+").strip()
                for name in row_names:
                    if name in label:
                        return _get_last_value(cells)
            return 0.0

        # -- P&L table --
        pl = table_map.get("pl")
        result["sales"] = get_row_value(pl, "sales", "revenue")
        result["profit"] = get_row_value(pl, "net profit", "profit after tax")
        result["depreciation"] = get_row_value(pl, "depreciation")
        expenses = get_row_value(pl, "expenses")
        operating_profit = get_row_value(pl, "operating profit")
        interest = get_row_value(pl, "interest")
        pbt = get_row_value(pl, "profit before tax")

        # Derive COGS = Expenses (or Sales - Operating Profit if expenses not found)
        if expenses > 0:
            result["cogs"] = expenses
        elif result["sales"] > 0 and operating_profit > 0:
            result["cogs"] = result["sales"] - operating_profit
        else:
            result["cogs"] = result["sales"] * 0.65

        # EBIT = PBT + Interest (or Operating Profit + Other Income)
        if pbt > 0 and interest > 0:
            result["ebit"] = pbt + interest
        elif operating_profit > 0:
            result["ebit"] = operating_profit
        else:
            result["ebit"] = result["profit"] * 1.3 if result["profit"] > 0 else 0

        # SGA estimate (Screener doesn't break this out)
        result["sga"] = result["sales"] * 0.08 if result["sales"] > 0 else 0

        # -- Balance Sheet table --
        bs = table_map.get("bs")
        result["total_assets"] = get_row_value(bs, "total assets")
        result["total_liabilities"] = result["total_assets"]  # Assets = Liabilities + Equity in Indian format
        result["long_term_debt"] = get_row_value(bs, "borrowings", "long term debt")
        result["retained_earnings"] = get_row_value(bs, "reserves")
        result["ppe"] = get_row_value(bs, "fixed assets", "property")
        result["other_assets"] = get_row_value(bs, "other assets")
        result["investments"] = get_row_value(bs, "investments")
        other_liabilities = get_row_value(bs, "other liabilities")
        equity_capital = get_row_value(bs, "equity capital")

        # Current assets/liabilities: derive from what's available
        result["current_assets"] = result["other_assets"]  # "Other Assets" on Screener includes current assets
        result["current_liabilities"] = other_liabilities
        result["working_capital"] = result["current_assets"] - result["current_liabilities"]

        # Total liabilities (debt side) = Borrowings + Other Liabilities
        result["total_liabilities"] = result["long_term_debt"] + other_liabilities
        if result["total_liabilities"] == 0:
            result["total_liabilities"] = result["total_assets"] - result["retained_earnings"] - equity_capital

        # Receivables (use debtor days estimate if not available directly)
        result["receivables"] = result["sales"] * 0.08 if result["sales"] > 0 else 0

        # Shares outstanding
        face_value = result.get("face_value", 10) or 10
        if equity_capital > 0 and face_value > 0:
            result["shares_outstanding"] = equity_capital / face_value
        else:
            result["shares_outstanding"] = 0

        # -- Cash Flow table --
        cf = table_map.get("cf")
        result["cfo"] = get_row_value(cf, "cash from operating", "operating activity")

        # -- Shareholding table --
        sh = table_map.get("shareholding")
        if sh:
            promoter_pct = get_row_value(sh, "promoters")
            if promoter_pct > 0:
                result["promoter_holding"] = promoter_pct
        result.setdefault("promoter_holding", 0)
        result.setdefault("pledge_pct", 0)

        # -- Gross margin --
        if result["sales"] > 0:
            result["gross_margin"] = (result["sales"] - result["cogs"]) / result["sales"]
        else:
            result["gross_margin"] = 0

        # -- ROA --
        if result["total_assets"] > 0:
            result["roa"] = result["profit"] / result["total_assets"]
        else:
            result["roa"] = 0

        # Validate we got meaningful data
        if result.get("sales", 0) == 0 and result.get("total_assets", 0) == 0:
            print(f"[DataFetcher] Screener.in: parsed page but no financial data found for {symbol}")
            return None

        print(f"[DataFetcher] Screener.in parsed OK: sales={result.get('sales')}, profit={result.get('profit')}, assets={result.get('total_assets')}, cfo={result.get('cfo')}")

        # Extract historical multi-year data for trend chart
        result["_historical_quarters"] = self._parse_screener_historical(table_map, result)

        return result

    def _parse_screener_historical(self, table_map: Dict, current: Dict) -> List[Dict]:
        """
        Extract last 8 years of financial data from Screener.in tables.
        Each table has columns like: ['', 'Mar 2014', 'Mar 2015', ..., 'TTM']
        Returns a list of quarterly dicts suitable for scoring.
        """

        def get_row_all_values(table, *row_names):
            """Get all year values for a named row."""
            if table is None:
                return []
            for row in table.find_all("tr"):
                cells = row.find_all(["th", "td"])
                if not cells:
                    continue
                label = cells[0].get_text(strip=True).lower().split("\n")[0].strip().rstrip("+").strip()
                for name in row_names:
                    if name in label:
                        values = []
                        for cell in cells[1:]:
                            txt = cell.get_text(strip=True)
                            if txt and txt != "TTM":
                                values.append(_parse_indian_number(txt))
                            elif txt == "TTM":
                                pass  # skip TTM column
                        return values
            return []

        def get_year_headers(table):
            """Get column headers (year names) from a table."""
            if table is None:
                return []
            header_row = table.find("tr")
            if not header_row:
                return []
            ths = header_row.find_all("th")
            years = []
            for th in ths[1:]:  # skip first empty header
                txt = th.get_text(strip=True)
                if txt and txt != "TTM":
                    years.append(txt)
            return years

        pl = table_map.get("pl")
        bs = table_map.get("bs")
        cf = table_map.get("cf")

        # Get year labels from P&L table
        years = get_year_headers(pl)
        if not years:
            return []

        # Extract all row data
        sales_all = get_row_all_values(pl, "sales", "revenue")
        profit_all = get_row_all_values(pl, "net profit", "profit after tax")
        depreciation_all = get_row_all_values(pl, "depreciation")
        expenses_all = get_row_all_values(pl, "expenses")

        assets_all = get_row_all_values(bs, "total assets")
        borrowings_all = get_row_all_values(bs, "borrowings", "long term debt")
        reserves_all = get_row_all_values(bs, "reserves")
        other_liab_all = get_row_all_values(bs, "other liabilities")
        fixed_assets_all = get_row_all_values(bs, "fixed assets", "property")
        other_assets_all = get_row_all_values(bs, "other assets")
        equity_all = get_row_all_values(bs, "equity capital")

        cfo_all = get_row_all_values(cf, "cash from operating", "operating activity")

        # Take last 8 years
        n = min(len(years), 8)
        quarters = []

        for i in range(-n, 0):
            def safe_get(arr, idx, default=0.0):
                try:
                    return arr[idx]
                except (IndexError, TypeError):
                    return default

            sales = safe_get(sales_all, i)
            profit = safe_get(profit_all, i)
            expenses = safe_get(expenses_all, i)
            dep = safe_get(depreciation_all, i)
            ta = safe_get(assets_all, i)
            borrowings = safe_get(borrowings_all, i)
            reserves = safe_get(reserves_all, i)
            other_liab = safe_get(other_liab_all, i)
            ppe = safe_get(fixed_assets_all, i)
            other_assets = safe_get(other_assets_all, i)
            eq_cap = safe_get(equity_all, i)
            cfo = safe_get(cfo_all, i)

            cogs = expenses if expenses > 0 else (sales * 0.65 if sales > 0 else 0)
            ebit = profit * 1.3 if profit > 0 else 0
            total_liab = borrowings + other_liab
            if total_liab == 0 and ta > 0:
                total_liab = ta - reserves - eq_cap

            year_label = safe_get(years, i, f"Y{i}")

            quarters.append({
                "quarter": year_label,
                "sales": sales,
                "profit": profit,
                "cfo": cfo,
                "cogs": cogs,
                "depreciation": dep,
                "total_assets": ta if ta > 0 else 1,
                "long_term_debt": borrowings,
                "retained_earnings": reserves,
                "ppe": ppe,
                "current_assets": other_assets,
                "current_liabilities": other_liab,
                "total_liabilities": total_liab if total_liab > 0 else 1,
                "working_capital": other_assets - other_liab,
                "ebit": ebit,
                "sga": sales * 0.08,
                "receivables": sales * 0.08,
                "shares_outstanding": eq_cap / (current.get("face_value", 10) or 10) if eq_cap > 0 else 0,
                "market_cap": current.get("market_cap", 0),
                "pledge_pct": current.get("pledge_pct", 0),
                "promoter_holding": current.get("promoter_holding", 0),
                "gross_margin": (sales - cogs) / sales if sales > 0 else 0,
                "roa": profit / ta if ta > 0 else 0,
            })

        if quarters:
            print(f"[DataFetcher] Extracted {len(quarters)} years of historical data: {quarters[0]['quarter']} to {quarters[-1]['quarter']}")

        return quarters

    # --- NSE ANNOUNCEMENTS -------------------------------------

    def fetch_nse_announcements(self, symbol: str) -> List[Dict]:
        """Fetch corporate announcements from NSE."""
        try:
            url = f"https://www.nseindia.com/api/corporate-announcements?index=equities&symbol={symbol}"
            resp = self._nse_api_get(url)
            if resp:
                data = resp.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            print(f"[DataFetcher] NSE announcements error for {symbol}: {type(e).__name__}: {e}")
        return []

    # --- MAIN ENTRY POINT -------------------------------------

    def get_company_data(self, symbol: str) -> Dict:
        """
        Fetch all available data for a company.
        ONLY uses DHFL fallback for DHFL itself.
        For all other companies: Screener.in + NSE, or clear error message.
        """
        symbol = normalize_symbol(symbol)

        # DHFL always uses hardcoded historical data
        if symbol == "DHFL":
            return self._get_dhfl_fallback()

        result = {
            "symbol": symbol,
            "company_name": symbol,
            "data_source": "live",
            "quarters": [],
            "current": {},
            "errors": [],
        }

        # -- Primary: Screener.in HTML scrape --
        print(f"\n[DataFetcher] === Fetching data for {symbol} ===")
        screener_data = self.fetch_screener_data(symbol)

        # -- Secondary: NSE quote for company name --
        nse_quote = self.fetch_nse_quote(symbol)
        if nse_quote:
            result["company_name"] = nse_quote.get("company_name", symbol)
        else:
            result["errors"].append("NSE quote API unavailable")

        if screener_data:
            current = screener_data.copy()
            # Use company name from Screener if NSE didn't provide one
            if result["company_name"] == symbol and "company_name" in screener_data:
                result["company_name"] = screener_data["company_name"]
            current["quarter"] = "Current"
            result["current"] = current

            # Use historical quarters if available for trend chart
            hist = screener_data.get("_historical_quarters", [])
            if hist and len(hist) >= 2:
                result["quarters"] = hist
            else:
                result["quarters"] = [current]

            result["data_source"] = "screener.in"
            return result

        # -- No financial data available --
        result["errors"].append("Screener.in data unavailable")

        if nse_quote:
            # We at least know the company exists on NSE
            result["data_source"] = "nse_partial"
            result["errors"].append("Real financial data unavailable -only basic quote info from NSE")
            current = {
                "quarter": "Current",
                "sales": 0, "profit": 0, "cfo": 0,
                "total_assets": 1, "total_liabilities": 1,
                "current_assets": 0, "current_liabilities": 0,
                "working_capital": 0, "retained_earnings": 0,
                "ebit": 0, "market_cap": 0, "receivables": 0,
                "cogs": 0, "ppe": 0, "depreciation": 0, "sga": 0,
                "long_term_debt": 0, "shares_outstanding": 0,
                "pledge_pct": 0, "promoter_holding": 0,
            }
            result["current"] = current
            result["quarters"] = [current]
            return result

        # Nothing at all
        result["data_source"] = "unavailable"
        result["errors"].append("Real data unavailable -NSE API blocked and Screener.in returned no data")
        print(f"[DataFetcher] ERROR: No data sources returned data for {symbol}")
        return result

    def _get_dhfl_fallback(self) -> Dict:
        """Return hardcoded DHFL data."""
        quarters = list(DHFL_HISTORICAL.values())
        return {
            "symbol": "DHFL",
            "company_name": "Dewan Housing Finance Corporation Ltd",
            "data_source": "historical_fallback",
            "quarters": quarters,
            "current": quarters[-1] if quarters else {},
            "errors": [],
        }

    def get_dhfl_historical(self) -> List[Dict]:
        """Return DHFL historical quarters for validation."""
        return list(DHFL_HISTORICAL.values())


# Singleton
_fetcher = None


def get_fetcher() -> DataFetcher:
    global _fetcher
    if _fetcher is None:
        _fetcher = DataFetcher()
    return _fetcher
