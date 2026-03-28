"""
TradeMind — Trade Enricher
Fetches OHLCV around each trade entry, runs pattern detection, stamps trades.
"""

import os
import sqlite3
from datetime import datetime, timedelta

from ohlcv_fetcher import get_ohlcv
from pattern_detector import detect_patterns, get_strongest_pattern
from pattern_backtest import get_win_rate

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "stresslens.db")

NUM_CANDLES = 15  # candles to fetch around trade entry


def _ensure_pattern_columns():
    """Add pattern columns to trades table if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Check existing columns
    cursor.execute("PRAGMA table_info(trades)")
    existing = {row[1] for row in cursor.fetchall()}

    new_cols = {
        "detected_pattern": "TEXT DEFAULT ''",
        "pattern_confidence": "INTEGER DEFAULT 0",
        "pattern_direction": "TEXT DEFAULT ''",
        "nse_win_rate": "REAL DEFAULT 0",
        "pattern_sample_count": "INTEGER DEFAULT 0",
    }

    for col, typedef in new_cols.items():
        if col not in existing:
            cursor.execute(f"ALTER TABLE trades ADD COLUMN {col} {typedef}")

    conn.commit()
    conn.close()


# Ensure columns exist on import
_ensure_pattern_columns()


def enrich_single_trade(trade: dict, kite_client=None) -> dict:
    """
    Enrich a single trade with candlestick pattern data.

    Returns dict with pattern info, or empty pattern if none detected.
    """
    symbol = trade.get("symbol", "UNKNOWN")
    entry_price = trade.get("entry_price", 0)
    entry_time_str = trade.get("entry_time", "")

    if not entry_time_str or not entry_price:
        return _empty_pattern()

    try:
        entry_dt = datetime.fromisoformat(entry_time_str)
    except (ValueError, TypeError):
        return _empty_pattern()

    # Fetch OHLCV: 15 candles ending on or before the trade entry date
    to_date = entry_dt.strftime("%Y-%m-%d")
    from_date = (entry_dt - timedelta(days=NUM_CANDLES + 10)).strftime("%Y-%m-%d")

    candles = get_ohlcv(
        symbol=symbol,
        from_date=from_date,
        to_date=to_date,
        interval="day",
        ref_price=entry_price,
        num_candles=NUM_CANDLES,
        kite_client=kite_client,
    )

    if len(candles) < 3:
        return _empty_pattern()

    # Extract OHLC arrays
    opens = [c["open"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    closes = [c["close"] for c in candles]

    # Scan the last 10 candles for patterns (the ones just before entry)
    scan_start = max(0, len(candles) - 10)
    pattern = get_strongest_pattern(opens, highs, lows, closes, scan_range=(scan_start, len(candles)))

    if pattern:
        wr = get_win_rate(pattern["pattern_name"])
        return {
            "detected_pattern": pattern["pattern_name"],
            "pattern_confidence": pattern["confidence"],
            "pattern_direction": pattern["direction"],
            "nse_win_rate": wr["win_rate"],
            "pattern_sample_count": wr["sample_count"],
        }

    return _empty_pattern()


def _empty_pattern():
    """Return empty pattern dict."""
    return {
        "detected_pattern": "",
        "pattern_confidence": 0,
        "pattern_direction": "",
        "nse_win_rate": 0,
        "pattern_sample_count": 0,
    }


def update_trade_pattern(trade_id: int, pattern_data: dict):
    """Write pattern data back to the trades table."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        UPDATE trades SET
            detected_pattern = ?,
            pattern_confidence = ?,
            pattern_direction = ?,
            nse_win_rate = ?,
            pattern_sample_count = ?
        WHERE id = ?
    """, (
        pattern_data["detected_pattern"],
        pattern_data["pattern_confidence"],
        pattern_data["pattern_direction"],
        pattern_data["nse_win_rate"],
        pattern_data["pattern_sample_count"],
        trade_id,
    ))
    conn.commit()
    conn.close()


def enrich_all_trades(user_id: str, kite_client=None) -> dict:
    """
    Enrich all trades for a user with candlestick pattern stamps.
    Returns {enriched: N, patterns_found: N, message: str}.
    """
    _ensure_pattern_columns()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM trades WHERE user_id = ? ORDER BY entry_time ASC",
        (user_id,)
    ).fetchall()
    conn.close()

    enriched = 0
    patterns_found = 0

    for row in rows:
        trade = dict(row)
        try:
            pattern_data = enrich_single_trade(trade, kite_client)
            update_trade_pattern(trade["id"], pattern_data)
            enriched += 1
            if pattern_data["detected_pattern"]:
                patterns_found += 1
        except Exception as e:
            print(f"[Enricher] Error enriching trade {trade['id']}: {e}")

    return {
        "enriched": enriched,
        "patterns_found": patterns_found,
        "message": f"Enriched {enriched} trades. Found {patterns_found} candlestick patterns.",
    }
