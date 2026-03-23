"""
StressLens Scorer
Implements 5 academic stress scoring models:
1. Beneish M-Score (25 pts)
2. Altman Z-Score (25 pts)
3. Piotroski F-Score (20 pts)
4. Promoter Pledge Trend (20 pts)
5. Cash Flow Divergence (10 pts)
Total: 100 points
"""

from typing import Dict, Optional, List
import math


def safe_div(a, b, default=1.0):
    """Safe division to avoid ZeroDivisionError."""
    try:
        if b == 0 or b is None:
            return default
        return a / b
    except (TypeError, ZeroDivisionError):
        return default


def calculate_beneish_mscore(current: Dict, previous: Optional[Dict] = None) -> Dict:
    """
    Beneish M-Score: detects earnings manipulation.
    Weight: 25 points.
    """
    if not previous:
        previous = current  # Use same data if no previous quarter

    # Current period
    recv_t = current.get("receivables", 0) or 1
    sales_t = current.get("sales", 1) or 1
    cogs_t = current.get("cogs", 0) or 1
    ppe_t = current.get("ppe", 0) or 1
    ca_t = current.get("current_assets", 0) or 1
    ta_t = current.get("total_assets", 1) or 1
    dep_t = current.get("depreciation", 0) or 1
    sga_t = current.get("sga", 0) or 1
    ltd_t = current.get("long_term_debt", 0)
    cl_t = current.get("current_liabilities", 0)
    income_t = current.get("profit", 0)
    cfo_t = current.get("cfo", 0)

    # Previous period
    recv_p = previous.get("receivables", 0) or 1
    sales_p = previous.get("sales", 1) or 1
    cogs_p = previous.get("cogs", 0) or 1
    ppe_p = previous.get("ppe", 0) or 1
    ca_p = previous.get("current_assets", 0) or 1
    ta_p = previous.get("total_assets", 1) or 1
    dep_p = previous.get("depreciation", 0) or 1
    sga_p = previous.get("sga", 0) or 1
    ltd_p = previous.get("long_term_debt", 0)
    cl_p = previous.get("current_liabilities", 0)

    # 8 ratios
    dsri = safe_div(recv_t / sales_t, recv_p / sales_p)
    gmi = safe_div((sales_p - cogs_p) / sales_p, (sales_t - cogs_t) / sales_t)
    aqi_num = 1 - safe_div(ppe_t + ca_t, ta_t)
    aqi_den = 1 - safe_div(ppe_p + ca_p, ta_p)
    aqi = safe_div(aqi_num, aqi_den) if aqi_den != 0 else 1.0
    sgi = safe_div(sales_t, sales_p)
    depi_num = safe_div(dep_p, ppe_p + dep_p)
    depi_den = safe_div(dep_t, ppe_t + dep_t)
    depi = safe_div(depi_num, depi_den)
    sgai = safe_div(sga_t / sales_t, sga_p / sales_p)
    lvgi = safe_div((ltd_t + cl_t) / ta_t, (ltd_p + cl_p) / ta_p)
    tata = safe_div(income_t - cfo_t, ta_t, default=0)

    # M-Score formula
    m_score = (
        -4.84
        + 0.920 * dsri
        + 0.528 * gmi
        + 0.404 * aqi
        + 0.892 * sgi
        + 0.115 * depi
        - 0.172 * sgai
        + 4.679 * tata
        - 0.327 * lvgi
    )

    # Score assignment
    if m_score > -1.78:
        score = 25
        verdict = "MANIPULATION LIKELY"
    elif m_score > -2.22:
        score = 15
        verdict = "GREY ZONE"
    else:
        score = 0
        verdict = "NO MANIPULATION DETECTED"

    return {
        "score": score,
        "max_score": 25,
        "m_score": round(m_score, 4),
        "verdict": verdict,
        "details": {
            "DSRI": round(dsri, 4),
            "GMI": round(gmi, 4),
            "AQI": round(aqi, 4),
            "SGI": round(sgi, 4),
            "DEPI": round(depi, 4),
            "SGAI": round(sgai, 4),
            "LVGI": round(lvgi, 4),
            "TATA": round(tata, 4),
        },
    }


def calculate_altman_zscore(data: Dict) -> Dict:
    """
    Altman Z-Score: predicts bankruptcy probability.
    Weight: 25 points.
    """
    ta = data.get("total_assets", 1) or 1
    tl = data.get("total_liabilities", 1) or 1

    x1 = safe_div(data.get("working_capital", 0), ta, 0)
    x2 = safe_div(data.get("retained_earnings", 0), ta, 0)
    x3 = safe_div(data.get("ebit", 0), ta, 0)
    x4 = safe_div(data.get("market_cap", 0), tl, 0)
    x5 = safe_div(data.get("sales", 0), ta, 0)

    z_score = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 1.0 * x5

    if z_score < 1.81:
        score = 25
        verdict = "DISTRESS ZONE"
    elif z_score < 2.99:
        score = 15
        verdict = "GREY ZONE"
    else:
        score = 0
        verdict = "SAFE ZONE"

    return {
        "score": score,
        "max_score": 25,
        "z_score": round(z_score, 4),
        "verdict": verdict,
        "details": {
            "X1_WC_TA": round(x1, 4),
            "X2_RE_TA": round(x2, 4),
            "X3_EBIT_TA": round(x3, 4),
            "X4_MC_TL": round(x4, 4),
            "X5_Sales_TA": round(x5, 4),
        },
    }


def calculate_piotroski_fscore(current: Dict, previous: Optional[Dict] = None) -> Dict:
    """
    Piotroski F-Score: measures financial strength (9 signals).
    Weight: 20 points. Lower F-Score = higher stress.
    """
    if not previous:
        previous = current

    ta = current.get("total_assets", 1) or 1
    ta_p = previous.get("total_assets", 1) or 1

    signals = {}

    # 1. ROA > 0
    roa = safe_div(current.get("profit", 0), ta, 0)
    signals["ROA_positive"] = 1 if roa > 0 else 0

    # 2. Operating CF > 0
    cfo = current.get("cfo", 0)
    signals["CFO_positive"] = 1 if cfo > 0 else 0

    # 3. ROA increasing
    roa_prev = safe_div(previous.get("profit", 0), ta_p, 0)
    signals["ROA_increasing"] = 1 if roa > roa_prev else 0

    # 4. Accruals: CFO/Assets > ROA
    cfo_ratio = safe_div(cfo, ta, 0)
    signals["Accruals_quality"] = 1 if cfo_ratio > roa else 0

    # 5. Leverage decreasing
    lev_curr = safe_div(current.get("long_term_debt", 0), ta, 0)
    lev_prev = safe_div(previous.get("long_term_debt", 0), ta_p, 0)
    signals["Leverage_decreasing"] = 1 if lev_curr <= lev_prev else 0

    # 6. Current ratio improving
    cr_curr = safe_div(current.get("current_assets", 0), current.get("current_liabilities", 1), 0)
    cr_prev = safe_div(previous.get("current_assets", 0), previous.get("current_liabilities", 1), 0)
    signals["Current_ratio_improving"] = 1 if cr_curr >= cr_prev else 0

    # 7. No new shares issued
    shares_curr = current.get("shares_outstanding", 0)
    shares_prev = previous.get("shares_outstanding", 0)
    signals["No_dilution"] = 1 if shares_curr <= shares_prev else 0

    # 8. Gross margin improving
    gm_curr = current.get("gross_margin", safe_div(current.get("sales", 0) - current.get("cogs", 0), current.get("sales", 1), 0))
    gm_prev = previous.get("gross_margin", safe_div(previous.get("sales", 0) - previous.get("cogs", 0), previous.get("sales", 1), 0))
    signals["Gross_margin_improving"] = 1 if gm_curr >= gm_prev else 0

    # 9. Asset turnover improving
    at_curr = safe_div(current.get("sales", 0), ta, 0)
    at_prev = safe_div(previous.get("sales", 0), ta_p, 0)
    signals["Asset_turnover_improving"] = 1 if at_curr >= at_prev else 0

    f_score = sum(signals.values())
    # Lower F-Score = more stress
    stress_score = round((9 - f_score) / 9 * 20, 2)

    if f_score >= 7:
        verdict = "STRONG"
    elif f_score >= 4:
        verdict = "MODERATE"
    else:
        verdict = "WEAK"

    return {
        "score": stress_score,
        "max_score": 20,
        "f_score": f_score,
        "verdict": verdict,
        "signals": signals,
    }


def calculate_pledge_score(current: Dict, previous: Optional[Dict] = None) -> Dict:
    """
    Promoter Pledge Trend analysis.
    Weight: 20 points.
    """
    pledge_pct = current.get("pledge_pct", 0) or 0
    prev_pledge = previous.get("pledge_pct", 0) if previous else 0

    pledge_score = 0

    # Absolute level
    if pledge_pct > 75:
        pledge_score += 15
    elif pledge_pct > 50:
        pledge_score += 10
    elif pledge_pct > 25:
        pledge_score += 5

    # Quarter-on-quarter change
    quarter_change = pledge_pct - prev_pledge
    if quarter_change > 20:
        pledge_score += 5
    elif quarter_change > 10:
        pledge_score += 3

    # Cap at 20
    pledge_score = min(pledge_score, 20)

    return {
        "score": pledge_score,
        "max_score": 20,
        "current_pct": pledge_pct,
        "previous_pct": prev_pledge,
        "quarter_change": round(quarter_change, 2),
        "trend": "INCREASING" if quarter_change > 0 else ("STABLE" if quarter_change == 0 else "DECREASING"),
    }


def calculate_cashflow_divergence(data: Dict) -> Dict:
    """
    Cash Flow Divergence: profit vs. operating cash flow mismatch.
    Weight: 10 points.
    """
    profit = data.get("profit", 0)
    cfo = data.get("cfo", 0)

    if profit > 0 and cfo < 0:
        score = 10
        verdict = "SEVERE DIVERGENCE"
    elif profit > 0 and cfo < profit * 0.3:
        score = 7
        verdict = "HIGH DIVERGENCE"
    elif profit > 0 and cfo < profit * 0.6:
        score = 4
        verdict = "MODERATE DIVERGENCE"
    else:
        score = 0
        verdict = "HEALTHY"

    return {
        "score": score,
        "max_score": 10,
        "profit": profit,
        "cfo": cfo,
        "verdict": verdict,
        "ratio": round(safe_div(cfo, profit, 0), 4) if profit != 0 else 0,
    }


def calculate_total_stress(current: Dict, previous: Optional[Dict] = None) -> Dict:
    """
    Calculate the total stress score from all 5 components.
    Returns detailed breakdown.
    """
    beneish = calculate_beneish_mscore(current, previous)
    altman = calculate_altman_zscore(current)
    piotroski = calculate_piotroski_fscore(current, previous)
    pledge = calculate_pledge_score(current, previous)
    cashflow = calculate_cashflow_divergence(current)

    total = beneish["score"] + altman["score"] + piotroski["score"] + pledge["score"] + cashflow["score"]
    total = round(min(total, 100))

    if total >= 81:
        risk_level = "CRITICAL"
    elif total >= 61:
        risk_level = "HIGH"
    elif total >= 31:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    return {
        "stress_score": total,
        "risk_level": risk_level,
        "signals": {
            "beneish": beneish,
            "altman": altman,
            "piotroski": piotroski,
            "pledge": pledge,
            "cashflow": cashflow,
        },
    }


def score_historical_quarters(quarters: List[Dict]) -> List[Dict]:
    """Score a list of quarterly data, using previous quarter as baseline."""
    results = []
    for i, quarter in enumerate(quarters):
        previous = quarters[i - 1] if i > 0 else None
        score_result = calculate_total_stress(quarter, previous)
        results.append({
            "quarter": quarter.get("quarter", f"Q{i+1}"),
            "stress_score": score_result["stress_score"],
            "risk_level": score_result["risk_level"],
            "signals": score_result["signals"],
        })
    return results
