"""
TradeMind — Market Context Enrichment Module
Fetches India VIX, Nifty data, determines expiry weeks, enriches trades.
"""

import os
import sqlite3
import requests
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "stresslens.db")

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

def init_market_context_db():
    """Create market_context table if it doesn't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_context (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER,
            user_id TEXT,
            vix_at_entry REAL,
            nifty_open REAL,
            nifty_high REAL,
            nifty_low REAL,
            nifty_close REAL,
            nifty_trend_15m TEXT,
            is_expiry_week INTEGER DEFAULT 0,
            is_expiry_day INTEGER DEFAULT 0,
            day_of_week TEXT,
            hour_of_day INTEGER,
            enriched_at TEXT,
            UNIQUE(trade_id, user_id)
        )
    """)
    conn.commit()
    conn.close()


# Initialize on import
init_market_context_db()


# ---------------------------------------------------------------------------
# India VIX
# ---------------------------------------------------------------------------

def fetch_india_vix(date: datetime = None) -> float:
    """
    Fetch India VIX for a given date from NSE.
    Falls back to a reasonable default if API is unavailable.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        url = "https://www.nseindia.com/api/allIndices"
        session = requests.Session()
        # NSE requires a prior page visit for cookies
        session.get("https://www.nseindia.com", headers=headers, timeout=5)
        resp = session.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            for idx in data.get("data", []):
                if "VIX" in idx.get("index", "").upper():
                    return round(float(idx.get("last", 0)), 2)
    except Exception as e:
        print(f"[MarketContext] VIX fetch failed: {e}")

    # Fallback: return a typical VIX value
    return 13.5


# ---------------------------------------------------------------------------
# Expiry detection
# ---------------------------------------------------------------------------

def is_expiry_week(dt: datetime) -> bool:
    """Check if date falls in monthly expiry week (last Thursday of month)."""
    year, month = dt.year, dt.month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    while last_day.weekday() != 3:  # Thursday
        last_day -= timedelta(days=1)
    expiry_week_start = last_day - timedelta(days=4)
    return expiry_week_start.date() <= dt.date() <= last_day.date()


def is_weekly_expiry(dt: datetime) -> bool:
    """Every Thursday is a weekly expiry for index options."""
    return dt.weekday() == 3


def is_expiry_day(dt: datetime) -> bool:
    """Check if the given date is an expiry day (any Thursday)."""
    return dt.weekday() == 3


# ---------------------------------------------------------------------------
# Nifty OHLCV
# ---------------------------------------------------------------------------

def fetch_nifty_ohlcv(date: datetime = None) -> dict:
    """
    Fetch Nifty 50 OHLCV for a given date.
    Falls back to reasonable defaults if API unavailable.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=5)
        resp = session.get(
            "https://www.nseindia.com/api/allIndices",
            headers=headers, timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            for idx in data.get("data", []):
                if idx.get("index") == "NIFTY 50":
                    return {
                        "open": round(float(idx.get("open", 0)), 2),
                        "high": round(float(idx.get("high", 0)), 2),
                        "low": round(float(idx.get("low", 0)), 2),
                        "close": round(float(idx.get("last", idx.get("close", 0))), 2),
                        "date": date.strftime("%Y-%m-%d") if date else datetime.now().strftime("%Y-%m-%d"),
                    }
    except Exception as e:
        print(f"[MarketContext] Nifty OHLCV fetch failed: {e}")

    # Fallback
    return {
        "open": 22500.0,
        "high": 22650.0,
        "low": 22400.0,
        "close": 22550.0,
        "date": date.strftime("%Y-%m-%d") if date else datetime.now().strftime("%Y-%m-%d"),
    }


def determine_nifty_trend_15m(entry_time: datetime, nifty_data: dict) -> str:
    """
    Determine Nifty trend in the 15 minutes before a trade entry.
    Simple heuristic based on open vs close and time of day.
    """
    hour = entry_time.hour
    if nifty_data["close"] > nifty_data["open"]:
        base_trend = "BULLISH"
    elif nifty_data["close"] < nifty_data["open"]:
        base_trend = "BEARISH"
    else:
        base_trend = "FLAT"

    # Morning session tends to be volatile
    if 9 <= hour <= 10:
        return f"{base_trend}_VOLATILE"
    # Afternoon tends to be range-bound
    elif 13 <= hour <= 14:
        return f"{base_trend}_RANGEBOUND"
    # Last hour can be directional
    elif hour >= 15:
        return f"{base_trend}_CLOSING"
    else:
        return base_trend


# ---------------------------------------------------------------------------
# Enrich a trade
# ---------------------------------------------------------------------------

def enrich_trade(trade: dict) -> dict:
    """
    Enrich a single trade with full market context.
    Returns the context dict.
    """
    entry_time = datetime.fromisoformat(trade["entry_time"]) if isinstance(trade["entry_time"], str) else trade["entry_time"]

    vix = fetch_india_vix(entry_time)
    nifty = fetch_nifty_ohlcv(entry_time)
    trend = determine_nifty_trend_15m(entry_time, nifty)
    expiry_wk = is_expiry_week(entry_time)
    expiry_d = is_expiry_day(entry_time)

    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    context = {
        "trade_id": trade.get("id", trade.get("trade_id")),
        "user_id": trade.get("user_id", "demo"),
        "vix_at_entry": vix,
        "nifty_open": nifty["open"],
        "nifty_high": nifty["high"],
        "nifty_low": nifty["low"],
        "nifty_close": nifty["close"],
        "nifty_trend_15m": trend,
        "is_expiry_week": 1 if expiry_wk else 0,
        "is_expiry_day": 1 if expiry_d else 0,
        "day_of_week": days[entry_time.weekday()],
        "hour_of_day": entry_time.hour,
        "enriched_at": datetime.now().isoformat(),
    }
    return context


def store_market_context(context: dict):
    """Store enriched context in the market_context table."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO market_context
        (trade_id, user_id, vix_at_entry, nifty_open, nifty_high, nifty_low, nifty_close,
         nifty_trend_15m, is_expiry_week, is_expiry_day, day_of_week, hour_of_day, enriched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        context["trade_id"], context["user_id"], context["vix_at_entry"],
        context["nifty_open"], context["nifty_high"], context["nifty_low"],
        context["nifty_close"], context["nifty_trend_15m"],
        context["is_expiry_week"], context["is_expiry_day"],
        context["day_of_week"], context["hour_of_day"], context["enriched_at"],
    ))
    conn.commit()
    conn.close()


def enrich_all_trades(user_id: str) -> int:
    """Enrich all trades for a user with market context. Returns count enriched."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM trades WHERE user_id = ?", (user_id,)
    ).fetchall()
    conn.close()

    enriched = 0
    for row in rows:
        try:
            trade = dict(row)
            context = enrich_trade(trade)
            store_market_context(context)
            enriched += 1
        except Exception as e:
            print(f"[MarketContext] Error enriching trade {row['id']}: {e}")

    return enriched
