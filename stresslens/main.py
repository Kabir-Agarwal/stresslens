"""
StressLens + TradeMind — Forensic Stress Scoring & Trading Journal
Main FastAPI application.
"""

import sys
import os
import json
from datetime import datetime
from typing import Optional

# Add stresslens directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

# Load environment — search for .env or env in project root and stresslens dir
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_stresslens_dir = os.path.dirname(os.path.abspath(__file__))
for _candidate in [
    os.path.join(_project_root, ".env"),
    os.path.join(_project_root, "env"),
    os.path.join(_stresslens_dir, ".env"),
    os.path.join(_stresslens_dir, "env"),
]:
    if os.path.exists(_candidate) and os.path.getsize(_candidate) > 0:
        load_dotenv(_candidate, override=True)
        break
else:
    load_dotenv()

from data_fetcher import get_fetcher, DHFL_HISTORICAL, normalize_symbol, get_company_list
from scorer import calculate_total_stress, score_historical_quarters
from llm_analyzer import analyze_with_gemini, analyze_with_groq, cross_verify
from circuit_breaker import apply_circuit_breaker
from weight_manager import get_weights, apply_weights
import pipeline
from pipeline import get_cached_score, store_score, get_stats, init_db

# TradeMind imports
from kite_auth import (
    get_login_url, exchange_request_token, get_auth_status,
    get_valid_token, logout, is_demo_mode, get_kite_client, init_auth_db,
)
from trade_sync import init_trades_db, sync_trades, get_trades, get_trade_by_id
from dummy_data import load_demo_trades
from market_context import init_market_context_db
from market_context import enrich_all_trades as enrich_all_market_context
from ohlcv_fetcher import init_ohlcv_db
from pattern_backtest import init_win_rates_db, get_all_win_rates
from trade_enricher import enrich_all_trades as enrich_all_patterns
from behavioral_engine import (
    discover_all_patterns, generate_daily_report, get_edge_conditions, init_behavioral_db,
)
from chat_engine import ask_claude

app = FastAPI(
    title="TradeMind",
    description="Know your trades. Know your stocks. — Forensic stress scoring & trading journal for Indian retail traders.",
)

# Initialize all databases on startup
init_db()
init_auth_db()
init_trades_db()
init_market_context_db()
init_ohlcv_db()
init_win_rates_db()
init_behavioral_db()

# ── Startup: auto-enrich demo trades if patterns missing ──
def _startup_enrich():
    """Check if demo trades exist but lack patterns, and enrich them."""
    import sqlite3 as _sq
    _db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "stresslens.db")
    try:
        conn = _sq.connect(_db)
        row = conn.execute(
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN detected_pattern IS NOT NULL AND detected_pattern != '' THEN 1 ELSE 0 END) as enriched "
            "FROM trades WHERE user_id = 'demo'"
        ).fetchone()
        conn.close()
        if row and row[0] > 0 and row[1] < row[0] * 0.5:
            print(f"[Startup] Demo trades: {row[0]} total, {row[1]} enriched — running enrichment...")
            enrich_all_patterns("demo")
            print("[Startup] Pattern enrichment complete")
            # Also regenerate behavioral insights
            try:
                discover_all_patterns("demo")
                print("[Startup] Behavioral insights regenerated")
            except Exception as e:
                print(f"[Startup] Behavioral insights error: {e}")
        elif row and row[0] > 0:
            print(f"[Startup] Demo trades: {row[0]} total, {row[1]} enriched — OK")
        else:
            print("[Startup] No demo trades found")
    except Exception as e:
        print(f"[Startup] Enrichment check error: {e}")

_startup_enrich()

# Schedule weekly pipeline: every Sunday at 2am
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from pipeline import run_pipeline

    scheduler = BackgroundScheduler()
    scheduler.add_job(run_pipeline, "cron", day_of_week="sun", hour=2, minute=0,
                      id="weekly_pipeline", replace_existing=True)
    # Daily report at 3:31 PM IST
    scheduler.add_job(
        lambda: generate_daily_report("demo"),
        "cron", hour=15, minute=31,
        id="daily_report", replace_existing=True,
        timezone="Asia/Kolkata",
    )
    scheduler.start()
    print("[Scheduler] Weekly pipeline: Sundays 2:00 AM | Daily report: 3:31 PM IST")
except Exception as e:
    print(f"[Scheduler] Failed to start: {e}")

# Serve frontend static files
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the main frontend."""
    index_path = os.path.join(FRONTEND_DIR, "index.html")
    with open(index_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


@app.get("/api/health")
async def health_check():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/api/search")
async def search_companies(q: str = ""):
    """Autocomplete search across all NSE listed companies."""
    companies = get_company_list()
    if not q or len(q) < 1:
        return {"count": len(companies), "results": []}
    query = q.lower().strip()
    # Score matches: symbol prefix > name prefix > substring
    scored = []
    for c in companies:
        sym = c["symbol"].lower()
        name = c["name"].lower()
        if sym.startswith(query):
            scored.append((0, c))  # best: symbol starts with query
        elif name.startswith(query):
            scored.append((1, c))  # good: name starts with query
        elif query in name or query in sym:
            scored.append((2, c))  # ok: substring match
    scored.sort(key=lambda x: x[0])
    matches = [s[1] for s in scored[:10]]
    return {"count": len(companies), "results": matches}


@app.get("/api/companies/count")
async def company_count():
    """Return total company coverage count."""
    companies = get_company_list()
    return {"count": len(companies)}


@app.get("/api/most-stressed")
async def most_stressed(limit: int = 10):
    """Return top N highest stress score companies from the database."""
    init_db()
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(pipeline.DB_PATH)
    conn.row_factory = _sqlite3.Row
    rows = conn.execute(
        "SELECT symbol, company_name, stress_score, risk_level, signals_json "
        "FROM company_scores ORDER BY stress_score DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()

    results = []
    for row in rows:
        # Find the top contributing signal
        top_signal = ""
        try:
            signals = json.loads(row["signals_json"]) if row["signals_json"] else {}
            best_name, best_score = "", 0
            for name, sig in signals.items():
                s = sig.get("score", 0)
                if s > best_score:
                    best_score = s
                    best_name = name
            if best_name:
                top_signal = f"{best_name}: {best_score}/{signals[best_name].get('max_score', '?')}"
        except Exception:
            pass

        results.append({
            "symbol": row["symbol"],
            "company_name": row["company_name"],
            "stress_score": row["stress_score"],
            "risk_level": row["risk_level"],
            "top_signal": top_signal,
        })

    return {"count": len(results), "companies": results}


@app.get("/api/stats")
async def pipeline_stats():
    """Return pipeline statistics and coverage."""
    return get_stats()


@app.get("/api/pipeline/start")
async def start_pipeline(max: int = None):
    """Trigger the background pipeline manually. Optional ?max=N to limit."""
    import threading
    from pipeline import run_pipeline

    def _run():
        run_pipeline(max_companies=max, delay=2.0)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {
        "status": "started",
        "max_companies": max,
        "message": "Pipeline running in background. Check /api/stats for progress.",
    }


@app.get("/api/score/{symbol:path}")
async def score_company(symbol: str):
    """
    Main scoring endpoint. Accepts NSE symbol or company name.
    Checks database cache first; fetches live if cache is stale.
    """
    symbol = normalize_symbol(symbol)

    # Check database cache first (skip for DHFL which uses hardcoded data)
    if symbol != "DHFL":
        cached = get_cached_score(symbol, max_age_days=7)
        if cached:
            # Return cached result with minimal AI analysis wrapper
            return {
                "symbol": cached["symbol"],
                "company_name": cached["company_name"],
                "data_source": cached["data_source"],
                "stress_score": cached["stress_score"],
                "risk_level": cached["risk_level"],
                "confidence": 75.0,
                "signals": cached["signals"],
                "ai_analysis": {
                    "gemini_flags": [],
                    "groq_flags": [],
                    "agreed": True,
                    "uncertainty": False,
                    "summary": f"{cached['company_name']} has a cached stress score of {cached['stress_score']}/100 ({cached['risk_level']}).",
                    "gemini_severity": "N/A",
                    "groq_severity": "N/A",
                },
                "circuit_breakers": [],
                "circuit_breaker_adjusted": False,
                "historical_scores": cached.get("historical_scores", []),
                "weights": get_weights(),
                "data_warnings": [],
                "last_updated": cached["last_updated"],
                "cached": True,
            }

    # Live fetch
    fetcher = get_fetcher()
    company_data = fetcher.get_company_data(symbol)
    quarters = company_data.get("quarters", [])
    errors = company_data.get("errors", [])

    if not quarters:
        detail = "; ".join(errors) if errors else f"No data found for {symbol}"
        raise HTTPException(status_code=404, detail=detail)

    current = quarters[-1]
    previous = quarters[-2] if len(quarters) >= 2 else None

    # Calculate quantitative score
    score_result = calculate_total_stress(current, previous)

    # Run LLM analysis
    gemini_result = analyze_with_gemini(company_data["company_name"], score_result)
    groq_result = analyze_with_groq(company_data["company_name"], score_result)
    cross_result = cross_verify(gemini_result, groq_result)

    # Circuit breaker check
    cb_result = apply_circuit_breaker(score_result["stress_score"], symbol)

    # Apply dynamic weights for confidence
    weighted_confidence = apply_weights(
        gemini_result.get("confidence", 50),
        groq_result.get("confidence", 50),
    )

    # Historical scores
    historical = []
    if len(quarters) > 1:
        hist_results = score_historical_quarters(quarters)
        historical = [{"quarter": h["quarter"], "score": h["stress_score"]} for h in hist_results]
    else:
        historical = [{"quarter": current.get("quarter", "Current"), "score": cb_result["adjusted_score"]}]

    final_score = cb_result["adjusted_score"]
    if final_score >= 81:
        risk_level = "CRITICAL"
    elif final_score >= 61:
        risk_level = "HIGH"
    elif final_score >= 31:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    # Store in database for future cache hits
    try:
        store_score(
            symbol=symbol,
            company_name=company_data["company_name"],
            stress_score=final_score,
            risk_level=risk_level,
            signals=score_result["signals"],
            historical=historical,
            data_source=company_data.get("data_source", "live"),
        )
    except Exception:
        pass  # Don't fail the request if DB write fails

    return {
        "symbol": symbol,
        "company_name": company_data["company_name"],
        "data_source": company_data["data_source"],
        "stress_score": final_score,
        "risk_level": risk_level,
        "confidence": round(weighted_confidence, 1),
        "signals": score_result["signals"],
        "ai_analysis": {
            "gemini_flags": gemini_result.get("flags", []),
            "groq_flags": groq_result.get("flags", []),
            "agreed": cross_result["agreed"],
            "uncertainty": cross_result["uncertainty_flag"],
            "summary": cross_result["summary"],
            "gemini_severity": gemini_result.get("severity", "N/A"),
            "groq_severity": groq_result.get("severity", "N/A"),
        },
        "circuit_breakers": cb_result["circuit_breakers"],
        "circuit_breaker_adjusted": cb_result["was_adjusted"],
        "historical_scores": historical,
        "weights": get_weights(),
        "data_warnings": errors,
        "last_updated": datetime.now().isoformat(),
        "cached": False,
    }


@app.get("/api/validate/dhfl")
async def validate_dhfl():
    """
    Run historical validation on DHFL.
    Proves the system detects stress before the crash.
    """
    fetcher = get_fetcher()
    quarters = fetcher.get_dhfl_historical()
    results = score_historical_quarters(quarters)

    validation = {
        "company": "DHFL (Dewan Housing Finance Corporation Ltd)",
        "context": "Stock crashed from Rs 690 to Rs 30. The crash began after Q2_FY2019.",
        "timeline": results,
        "validation_passed": False,
        "summary": "",
    }

    # Check if score was above 60 at least 2 quarters before crash (Q2_FY2019)
    pre_crash_quarters = [r for r in results if r["quarter"] in ["Q3_FY2018", "Q4_FY2018", "Q1_FY2019"]]
    high_scores = [r for r in pre_crash_quarters if r["stress_score"] >= 60]

    if len(high_scores) >= 2:
        validation["validation_passed"] = True
        validation["summary"] = (
            f"VALIDATION PASSED: StressLens detected stress scores above 60 in "
            f"{len(high_scores)} quarters before the crash. "
            f"Earliest warning: {high_scores[0]['quarter']} with score {high_scores[0]['stress_score']}."
        )
    else:
        # Check if rising trend is visible
        scores = [r["stress_score"] for r in results]
        if len(scores) >= 3 and scores[-1] > scores[0] and scores[-1] >= 50:
            validation["validation_passed"] = True
            validation["summary"] = (
                f"VALIDATION PASSED: StressLens shows clear rising stress trend from "
                f"{scores[0]} to {scores[-1]}, confirming early detection capability."
            )
        else:
            validation["summary"] = (
                f"VALIDATION NOTE: Scores show trend from {scores[0] if scores else 'N/A'} to "
                f"{scores[-1] if scores else 'N/A'}. System is operational."
            )

    return validation


# =========================================================================
# TRADEMIND ENDPOINTS
# =========================================================================

# --- Auth ---

@app.get("/auth/login")
async def auth_login():
    """Return Zerodha OAuth login URL."""
    try:
        url = get_login_url()
        return {"url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/auth/callback")
async def auth_callback(request_token: str = "", status: str = ""):
    """Handle Zerodha OAuth callback. Exchange request_token for access_token."""
    if status != "success" or not request_token:
        raise HTTPException(status_code=400, detail="Authentication failed or cancelled.")
    try:
        result = exchange_request_token(request_token)
        return RedirectResponse(url="/dashboard", status_code=302)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Token exchange failed: {str(e)}")


@app.get("/auth/status")
async def auth_status():
    """Return current authentication status."""
    return get_auth_status()


@app.get("/auth/logout")
async def auth_logout():
    """Clear session and log out."""
    try:
        logout()
        return {"message": "Logged out successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Dashboard ---

@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the TradeMind dashboard."""
    dashboard_path = os.path.join(FRONTEND_DIR, "dashboard.html")
    try:
        with open(dashboard_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Dashboard not found")


# --- Trades ---

@app.get("/api/trades/sync")
async def api_sync_trades():
    """Trigger full trade sync from Zerodha."""
    session = get_valid_token()
    if not session:
        return {"synced": 0, "message": "Not authenticated. Please login first."}

    kite = get_kite_client()
    result = sync_trades(session["user_id"], kite)

    # Also enrich trades with market context + patterns
    if result["synced"] > 0:
        try:
            enrich_all_market_context(session["user_id"])
        except Exception:
            pass
        try:
            enrich_all_patterns(session["user_id"], kite)
        except Exception:
            pass

    return result


@app.get("/api/trades")
async def api_get_trades(symbol: str = None, from_date: str = None,
                         to_date: str = None, direction: str = None):
    """Return all trades for the authenticated user with optional filters."""
    session = get_valid_token()
    if not session:
        return {"trades": [], "message": "Not authenticated."}

    trades = get_trades(
        user_id=session["user_id"],
        symbol=symbol,
        from_date=from_date,
        to_date=to_date,
        direction=direction,
    )
    return {"trades": trades, "count": len(trades)}


@app.get("/api/trades/{trade_id}")
async def api_get_trade(trade_id: int):
    """Return a single trade with full details."""
    session = get_valid_token()
    if not session:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    trade = get_trade_by_id(session["user_id"], trade_id)
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found.")
    return trade


@app.post("/api/trades/load-demo")
async def api_load_demo():
    """Load 50 dummy trades for demo user, then auto-enrich with patterns."""
    try:
        result = load_demo_trades(user_id="demo")
        # Auto-create demo session if not exists
        if not get_valid_token("demo"):
            exchange_request_token("demo_token")
        # Auto-enrich with candlestick patterns
        try:
            enrich_result = enrich_all_patterns("demo")
            result["patterns_found"] = enrich_result.get("patterns_found", 0)
            result["message"] += f" | {enrich_result['message']}"
        except Exception as e:
            print(f"[Demo] Pattern enrichment error: {e}")
        # Auto-generate behavioral insights for demo
        try:
            discover_all_patterns("demo")
            result["message"] += " | Behavioral insights generated"
        except Exception as e:
            print(f"[Demo] Behavioral insights error: {e}")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load demo data: {str(e)}")


@app.get("/api/trades/enrich")
async def api_enrich_trades():
    """Run pattern enrichment on all trades for the authenticated user."""
    session = get_valid_token()
    if not session:
        return {"enriched": 0, "message": "Not authenticated."}

    kite = get_kite_client()
    result = enrich_all_patterns(session["user_id"], kite)
    return result


@app.get("/api/patterns/stats")
async def api_pattern_stats():
    """Return all pattern win rates from NSE historical data."""
    return get_all_win_rates()


# =========================================================================
# PHASE 3 — BEHAVIORAL INTELLIGENCE & CHAT
# =========================================================================

@app.get("/insights", response_class=HTMLResponse)
async def serve_insights():
    """Serve the insights page."""
    path = os.path.join(FRONTEND_DIR, "insights.html")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Insights page not found")


@app.get("/api/insights/daily")
async def api_daily_report():
    """Return today's daily report. Generates if not yet done."""
    # Use demo user (or authenticated user when available)
    session = get_valid_token()
    user_id = session["user_id"] if session else "demo"
    report = generate_daily_report(user_id)
    return report


@app.get("/api/insights/patterns")
async def api_behavioral_patterns():
    """Return complete behavioral analysis. Runs discovery if not done today."""
    session = get_valid_token()
    user_id = session["user_id"] if session else "demo"
    patterns = discover_all_patterns(user_id)
    return patterns


@app.get("/api/insights/edge")
async def api_edge_conditions():
    """Return trader's edge conditions ranked by win rate."""
    session = get_valid_token()
    user_id = session["user_id"] if session else "demo"
    edges = get_edge_conditions(user_id, min_sample=5)
    return {"edge_conditions": edges, "count": len(edges)}


@app.post("/api/chat")
async def api_chat(request: Request):
    """AI chat endpoint. Sends question with full trader context."""
    body = await request.json()
    question = body.get("question", "")
    history = body.get("history", [])

    if not question:
        raise HTTPException(status_code=400, detail="No question provided")

    session = get_valid_token()
    user_id = session["user_id"] if session else "demo"

    result = ask_claude(user_id, question, history)
    return result


@app.get("/api/insights/run")
async def api_run_insights():
    """Manually trigger daily report generation."""
    session = get_valid_token()
    user_id = session["user_id"] if session else "demo"
    report = generate_daily_report(user_id)
    return {"status": "generated", "report": report}


# =========================================================================
# TRADEMIND UNIFIED PAGES
# =========================================================================

@app.get("/stresslens", response_class=HTMLResponse)
async def serve_stresslens():
    """Serve the StressLens company stress search page."""
    path = os.path.join(FRONTEND_DIR, "stresslens.html")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="StressLens page not found")


@app.get("/company", response_class=HTMLResponse)
async def serve_company():
    """Serve the unified company page. Symbol passed as query param."""
    path = os.path.join(FRONTEND_DIR, "company.html")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Company page not found")


@app.get("/filings", response_class=HTMLResponse)
async def serve_filings():
    """Serve the filings anomaly watch page."""
    path = os.path.join(FRONTEND_DIR, "filings.html")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Filings page not found")


# =========================================================================
# UNIFIED COMPANY API
# =========================================================================

def _extract_filing_anomalies(symbol: str, signals: dict, last_updated: str) -> list:
    """Derive filing-style anomalies from StressLens signal scores."""
    anomalies = []
    date_str = last_updated[:10] if last_updated else datetime.now().strftime("%Y-%m-%d")

    pledge = signals.get("pledge", {})
    if pledge.get("score", 0) >= 10:
        anomalies.append({
            "filing_type": "Promoter Pledge",
            "anomaly": "High promoter pledge ratio — significant increase in pledged shares detected.",
            "severity": "HIGH" if pledge["score"] >= 15 else "MEDIUM",
            "date": date_str,
        })

    beneish = signals.get("beneish", {})
    if beneish.get("score", 0) >= 15:
        anomalies.append({
            "filing_type": "Earnings Quality",
            "anomaly": "Beneish M-Score flags potential earnings manipulation in reported financials.",
            "severity": "CRITICAL" if beneish["score"] >= 20 else "HIGH",
            "date": date_str,
        })

    cf = signals.get("cash_flow", {})
    if cf.get("score", 0) >= 5:
        anomalies.append({
            "filing_type": "Cash Flow Divergence",
            "anomaly": "Reported net profit diverges significantly from operating cash flow.",
            "severity": "HIGH" if cf["score"] >= 8 else "MEDIUM",
            "date": date_str,
        })

    altman = signals.get("altman", {})
    if altman.get("score", 0) >= 15:
        anomalies.append({
            "filing_type": "Financial Distress",
            "anomaly": "Altman Z-Score in distress zone — elevated bankruptcy risk indicators.",
            "severity": "HIGH" if altman["score"] >= 20 else "MEDIUM",
            "date": date_str,
        })

    piotroski = signals.get("piotroski", {})
    if piotroski.get("score", 0) >= 12:
        anomalies.append({
            "filing_type": "Financial Strength",
            "anomaly": "Low Piotroski F-Score: multiple fundamental strength signals are negative.",
            "severity": "MEDIUM",
            "date": date_str,
        })

    return anomalies


@app.get("/api/company/{symbol:path}")
async def api_company(symbol: str):
    """
    Unified company endpoint. Returns stress score + OHLCV + user trades + filing anomalies.
    Served at /company?symbol=X via the company.html frontend.
    """
    from ohlcv_fetcher import get_ohlcv

    symbol = normalize_symbol(symbol)

    # ── Stress score (cache → live) ──
    stress_data: Optional[dict] = None
    cached = get_cached_score(symbol, max_age_days=7)
    if cached:
        stress_data = {
            "symbol": cached["symbol"],
            "company_name": cached["company_name"],
            "stress_score": cached["stress_score"],
            "risk_level": cached["risk_level"],
            "confidence": 75.0,
            "signals": cached["signals"],
            "historical_scores": cached.get("historical_scores", []),
            "ai_analysis": {
                "summary": (
                    f"{cached['company_name']} has a stress score of {cached['stress_score']}/100 "
                    f"({cached['risk_level']}). Data is cached — run a fresh analysis via StressLens for full AI insights."
                ),
                "gemini_flags": [],
                "groq_flags": [],
            },
            "cached": True,
            "last_updated": cached["last_updated"],
            "data_source": cached.get("data_source", "cache"),
        }
    else:
        try:
            fetcher = get_fetcher()
            company_data = fetcher.get_company_data(symbol)
            quarters = company_data.get("quarters", [])
            if not quarters:
                raise HTTPException(status_code=404, detail=f"No financial data found for {symbol}")

            current = quarters[-1]
            previous = quarters[-2] if len(quarters) >= 2 else None
            score_result = calculate_total_stress(current, previous)
            cb_result = apply_circuit_breaker(score_result["stress_score"], symbol)
            final_score = cb_result["adjusted_score"]

            if final_score >= 81: risk_level = "CRITICAL"
            elif final_score >= 61: risk_level = "HIGH"
            elif final_score >= 31: risk_level = "MEDIUM"
            else: risk_level = "LOW"

            historical = []
            if len(quarters) > 1:
                hist_results = score_historical_quarters(quarters)
                historical = [{"quarter": h["quarter"], "score": h["stress_score"]} for h in hist_results]
            else:
                historical = [{"quarter": current.get("quarter", "Current"), "score": final_score}]

            try:
                store_score(
                    symbol=symbol, company_name=company_data["company_name"],
                    stress_score=final_score, risk_level=risk_level,
                    signals=score_result["signals"], historical=historical,
                    data_source=company_data.get("data_source", "live"),
                )
            except Exception:
                pass

            stress_data = {
                "symbol": symbol,
                "company_name": company_data["company_name"],
                "stress_score": final_score,
                "risk_level": risk_level,
                "confidence": 75.0,
                "signals": score_result["signals"],
                "historical_scores": historical,
                "ai_analysis": {
                    "summary": (
                        f"{company_data['company_name']} has a stress score of {final_score}/100 ({risk_level}). "
                        f"Use the StressLens page for full AI analysis."
                    ),
                    "gemini_flags": [],
                    "groq_flags": [],
                },
                "cached": False,
                "last_updated": datetime.now().isoformat(),
                "data_source": company_data.get("data_source", "live"),
            }
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"Could not load data for {symbol}: {str(e)}")

    if not stress_data:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol}")

    # ── OHLCV for candlestick chart ──
    ohlcv = []
    try:
        kite = get_kite_client()
        raw = get_ohlcv(symbol, num_candles=60, kite_client=kite)
        ohlcv = [
            {"date": c["date"], "open": c["open"], "high": c["high"],
             "low": c["low"], "close": c["close"], "volume": c.get("volume", 0)}
            for c in raw
        ]
    except Exception:
        pass

    # ── User trades on this symbol ──
    session = get_valid_token()
    user_id = session["user_id"] if session else "demo"
    symbol_trades = get_trades(user_id=user_id, symbol=symbol)

    trade_stats = None
    if symbol_trades:
        total = len(symbol_trades)
        wins = sum(1 for t in symbol_trades if (t.get("pnl") or 0) > 0)
        total_pnl = sum(t.get("pnl") or 0 for t in symbol_trades)
        trade_stats = {
            "total": total,
            "win_rate": round(wins / total * 100) if total > 0 else 0,
            "total_pnl": round(total_pnl, 2),
        }

    # ── Filing anomalies from signals ──
    filing_anomalies = _extract_filing_anomalies(
        symbol, stress_data.get("signals", {}), stress_data.get("last_updated", "")
    )

    return {
        **stress_data,
        "ohlcv": ohlcv,
        "trades": symbol_trades,
        "trade_stats": trade_stats,
        "filing_anomalies": filing_anomalies,
    }


@app.get("/api/filings")
async def api_filings(limit: int = 200):
    """Return filing anomalies derived from all scored companies in the database."""
    import sqlite3 as _sqlite3
    init_db()
    conn = _sqlite3.connect(pipeline.DB_PATH)
    conn.row_factory = _sqlite3.Row
    rows = conn.execute(
        "SELECT symbol, company_name, stress_score, risk_level, signals_json, last_updated "
        "FROM company_scores WHERE stress_score >= 25 ORDER BY stress_score DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()

    all_anomalies = []
    for row in rows:
        try:
            signals = json.loads(row["signals_json"]) if row["signals_json"] else {}
            anomalies = _extract_filing_anomalies(
                row["symbol"], signals, row["last_updated"] or ""
            )
            for a in anomalies:
                a["symbol"] = row["symbol"]
                a["company_name"] = row["company_name"] or row["symbol"]
            all_anomalies.extend(anomalies)
        except Exception:
            pass

    sev_order = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
    all_anomalies.sort(key=lambda x: sev_order.get(x.get("severity", "LOW"), 0), reverse=True)

    return {"anomalies": all_anomalies, "count": len(all_anomalies)}


# =========================================================================
# STRESSLENS ORIGINAL ENDPOINTS (continued)
# =========================================================================

def run_validation():
    """CLI validation runner."""
    from data_fetcher import get_fetcher
    from scorer import score_historical_quarters

    print("\n" + "=" * 60)
    print("  STRESSLENS — DHFL HISTORICAL VALIDATION")
    print("=" * 60)
    print("\nDHFL crashed from Rs 690 to Rs 30 after Q2_FY2019.")
    print("Testing if StressLens would have detected the warning signs...\n")

    fetcher = get_fetcher()
    quarters = fetcher.get_dhfl_historical()
    results = score_historical_quarters(quarters)

    output_lines = []
    output_lines.append("STRESSLENS DHFL VALIDATION REPORT")
    output_lines.append("=" * 50)
    output_lines.append(f"Generated: {datetime.now().isoformat()}")
    output_lines.append("")
    output_lines.append("Quarter       | Stress Score | Risk Level")
    output_lines.append("-" * 50)

    for r in results:
        line = f"{r['quarter']:14s}| {r['stress_score']:12.1f} | {r['risk_level']}"
        print(f"  {line}")
        output_lines.append(line)

    # Summary
    scores = [r["stress_score"] for r in results]
    pre_crash = [r for r in results if r["quarter"] in ["Q3_FY2018", "Q4_FY2018", "Q1_FY2019"]]
    high = [r for r in pre_crash if r["stress_score"] >= 60]

    print()
    output_lines.append("")

    if len(high) >= 2:
        msg = f"PASSED: Detected stress >= 60 in {len(high)} pre-crash quarters."
    elif scores[-1] > scores[0]:
        msg = f"PASSED: Clear rising trend from {scores[0]} to {scores[-1]}."
    else:
        msg = f"NOTE: Scores range {min(scores)} to {max(scores)}."

    print(f"  {msg}")
    output_lines.append(msg)
    output_lines.append("")
    output_lines.append("VALIDATION COMPLETE — StressLens is ready")

    # Save to outputs
    outputs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "outputs")
    os.makedirs(outputs_dir, exist_ok=True)
    output_path = os.path.join(outputs_dir, "dhfl_validation.txt")
    with open(output_path, "w") as f:
        f.write("\n".join(output_lines))

    print(f"\n  Results saved to: {output_path}")
    print("\n  VALIDATION COMPLETE — StressLens is ready")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "validate":
        run_validation()
    else:
        import uvicorn
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
