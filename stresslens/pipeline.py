"""
StressLens Background Data Pipeline
Fetches and scores all NSE companies, stores results in SQLite.
"""

import sys
import os
import json
import sqlite3
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_fetcher import get_fetcher, get_company_list, normalize_symbol
from scorer import calculate_total_stress

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "stresslens.db")


def init_db():
    """Create the database and table if they don't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS company_scores (
            symbol TEXT PRIMARY KEY,
            company_name TEXT,
            stress_score INTEGER,
            risk_level TEXT,
            signals_json TEXT,
            historical_json TEXT,
            data_source TEXT,
            last_updated TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            finished_at TEXT,
            total_companies INTEGER,
            scored_companies INTEGER,
            failed_companies INTEGER
        )
    """)
    conn.commit()
    conn.close()


def get_cached_score(symbol: str, max_age_days: int = 7) -> dict:
    """Get cached score from database if it exists and is fresh."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM company_scores WHERE symbol = ?", (symbol,)
    ).fetchone()
    conn.close()

    if not row:
        return None

    last_updated = datetime.fromisoformat(row["last_updated"])
    if datetime.now() - last_updated > timedelta(days=max_age_days):
        return None

    return {
        "symbol": row["symbol"],
        "company_name": row["company_name"],
        "stress_score": row["stress_score"],
        "risk_level": row["risk_level"],
        "signals": json.loads(row["signals_json"]) if row["signals_json"] else {},
        "historical_scores": json.loads(row["historical_json"]) if row["historical_json"] else [],
        "data_source": row["data_source"],
        "last_updated": row["last_updated"],
        "cached": True,
    }


def store_score(symbol: str, company_name: str, stress_score: int,
                risk_level: str, signals: dict, historical: list,
                data_source: str):
    """Store or update a company score in the database."""
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO company_scores
        (symbol, company_name, stress_score, risk_level, signals_json,
         historical_json, data_source, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        symbol, company_name, stress_score, risk_level,
        json.dumps(signals), json.dumps(historical),
        data_source, datetime.now().isoformat(),
    ))
    conn.commit()
    conn.close()


def get_stats() -> dict:
    """Get pipeline statistics."""
    init_db()
    conn = sqlite3.connect(DB_PATH)

    total_scored = conn.execute("SELECT COUNT(*) FROM company_scores").fetchone()[0]
    last_run = conn.execute(
        "SELECT finished_at FROM pipeline_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()

    conn.close()

    companies = get_company_list()
    total = len(companies) if companies else 2272

    return {
        "total_companies": total,
        "scored_companies": total_scored,
        "last_pipeline_run": last_run[0] if last_run else None,
        "coverage_percentage": round(total_scored / total * 100, 1) if total > 0 else 0,
    }


def run_pipeline(max_companies: int = None, delay: float = 2.0):
    """
    Run the full pipeline: fetch and score all NSE companies.
    Skips companies already updated in the last 7 days.
    """
    init_db()
    started_at = datetime.now().isoformat()

    companies = get_company_list()
    if not companies:
        print("[Pipeline] ERROR: No company list available")
        return

    total = len(companies)
    if max_companies:
        companies = companies[:max_companies]
        total = len(companies)

    print(f"[Pipeline] Starting pipeline for {total} companies")
    print(f"[Pipeline] Delay between requests: {delay}s")

    fetcher = get_fetcher()
    scored = 0
    skipped = 0
    failed = 0

    for i, company in enumerate(companies):
        symbol = company["symbol"]
        name = company.get("name", symbol)

        # Check if already cached and fresh
        cached = get_cached_score(symbol, max_age_days=7)
        if cached:
            skipped += 1
            if (i + 1) % 100 == 0:
                print(f"[Pipeline] Progress {i+1}/{total}: {skipped} skipped, {scored} scored, {failed} failed")
            continue

        print(f"[Pipeline] Processing {i+1}/{total}: {symbol} ({name})...")

        try:
            data = fetcher.get_company_data(symbol)
            quarters = data.get("quarters", [])

            if not quarters:
                failed += 1
                continue

            current = quarters[-1]
            previous = quarters[-2] if len(quarters) >= 2 else None
            score_result = calculate_total_stress(current, previous)

            stress_score = score_result["stress_score"]
            if stress_score >= 81:
                risk_level = "CRITICAL"
            elif stress_score >= 61:
                risk_level = "HIGH"
            elif stress_score >= 31:
                risk_level = "MEDIUM"
            else:
                risk_level = "LOW"

            # Build historical
            historical = []
            if len(quarters) > 1:
                from scorer import score_historical_quarters
                hist_results = score_historical_quarters(quarters)
                historical = [{"quarter": h["quarter"], "score": h["stress_score"]} for h in hist_results]

            store_score(
                symbol=symbol,
                company_name=data.get("company_name", name),
                stress_score=stress_score,
                risk_level=risk_level,
                signals=score_result["signals"],
                historical=historical,
                data_source=data.get("data_source", "unknown"),
            )
            scored += 1

        except Exception as e:
            print(f"[Pipeline] ERROR scoring {symbol}: {type(e).__name__}: {e}")
            failed += 1

        # Rate limit
        time.sleep(delay)

    finished_at = datetime.now().isoformat()

    # Record pipeline run
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO pipeline_runs (started_at, finished_at, total_companies, scored_companies, failed_companies)
        VALUES (?, ?, ?, ?, ?)
    """, (started_at, finished_at, total, scored, failed))
    conn.commit()
    conn.close()

    print(f"\n[Pipeline] COMPLETE")
    print(f"[Pipeline] Scored: {scored}, Skipped: {skipped}, Failed: {failed}")
    print(f"[Pipeline] Duration: {started_at} to {finished_at}")


# Run directly: python pipeline.py [max_count]
if __name__ == "__main__":
    max_n = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run_pipeline(max_companies=max_n)
