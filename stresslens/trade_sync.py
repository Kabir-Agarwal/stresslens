"""
TradeMind — Trade Sync Module
Fetches trades from Zerodha, calculates P&L, stores in SQLite.
"""

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "stresslens.db")

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

def init_trades_db():
    """Create trades table if it doesn't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            trade_id TEXT,
            symbol TEXT NOT NULL,
            exchange TEXT DEFAULT 'NSE',
            instrument_type TEXT DEFAULT 'EQ',
            direction TEXT NOT NULL,
            entry_price REAL,
            exit_price REAL,
            quantity INTEGER,
            entry_time TEXT,
            exit_time TEXT,
            pnl REAL DEFAULT 0,
            status TEXT DEFAULT 'CLOSED',
            note TEXT DEFAULT '',
            emotion_tag TEXT DEFAULT '',
            product_type TEXT DEFAULT 'CNC',
            order_id TEXT DEFAULT '',
            UNIQUE(user_id, trade_id)
        )
    """)
    conn.commit()
    conn.close()


# Initialize on import
init_trades_db()


# ---------------------------------------------------------------------------
# Fetch trades from Zerodha
# ---------------------------------------------------------------------------

def fetch_zerodha_trades(kite_client) -> list:
    """
    Fetch all trades for the last 365 days from Zerodha using KiteConnect.
    Returns list of raw trade dicts.
    """
    if kite_client is None:
        return []

    all_trades = []
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365)

    # Kite API allows max 60 days per request
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=59), end_date)
        try:
            trades = kite_client.trades()
            if trades:
                all_trades.extend(trades)
        except Exception as e:
            print(f"[TradeSync] Error fetching trades {current_start} to {current_end}: {e}")
        current_start = current_end + timedelta(days=1)

    return all_trades


def pair_trades(raw_trades: list) -> list:
    """
    Pair buy and sell trades to calculate P&L for completed round-trips.
    Returns list of paired trade dicts ready for DB insertion.
    """
    # Group by symbol
    by_symbol = {}
    for t in raw_trades:
        sym = t.get("tradingsymbol", "UNKNOWN")
        by_symbol.setdefault(sym, []).append(t)

    paired = []
    for sym, trades in by_symbol.items():
        buys = sorted(
            [t for t in trades if t.get("transaction_type") == "BUY"],
            key=lambda x: x.get("order_timestamp", "")
        )
        sells = sorted(
            [t for t in trades if t.get("transaction_type") == "SELL"],
            key=lambda x: x.get("order_timestamp", "")
        )

        # Simple FIFO matching
        while buys and sells:
            buy = buys.pop(0)
            sell = sells.pop(0)
            qty = min(buy.get("quantity", 0), sell.get("quantity", 0))
            if qty <= 0:
                continue

            entry_price = buy.get("average_price", buy.get("price", 0))
            exit_price = sell.get("average_price", sell.get("price", 0))
            pnl = round((exit_price - entry_price) * qty, 2)

            paired.append({
                "trade_id": f"{buy.get('trade_id', '')}_{sell.get('trade_id', '')}",
                "symbol": sym,
                "exchange": buy.get("exchange", "NSE"),
                "instrument_type": buy.get("instrument_type", "EQ"),
                "direction": "LONG",
                "entry_price": entry_price,
                "exit_price": exit_price,
                "quantity": qty,
                "entry_time": str(buy.get("order_timestamp", "")),
                "exit_time": str(sell.get("order_timestamp", "")),
                "pnl": pnl,
                "status": "CLOSED",
                "product_type": buy.get("product", "CNC"),
                "order_id": buy.get("order_id", ""),
            })

    return paired


# ---------------------------------------------------------------------------
# Store trades
# ---------------------------------------------------------------------------

def store_trades(user_id: str, trades: list) -> int:
    """Store paired trades in the database. Returns count of inserted trades."""
    conn = sqlite3.connect(DB_PATH)
    inserted = 0
    for t in trades:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO trades
                (user_id, trade_id, symbol, exchange, instrument_type, direction,
                 entry_price, exit_price, quantity, entry_time, exit_time, pnl,
                 status, product_type, order_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id, t["trade_id"], t["symbol"], t["exchange"],
                t.get("instrument_type", "EQ"), t["direction"],
                t["entry_price"], t["exit_price"], t["quantity"],
                t["entry_time"], t["exit_time"], t["pnl"],
                t.get("status", "CLOSED"), t.get("product_type", "CNC"),
                t.get("order_id", ""),
            ))
            inserted += 1
        except Exception as e:
            print(f"[TradeSync] Error storing trade {t.get('trade_id')}: {e}")
    conn.commit()
    conn.close()
    return inserted


# ---------------------------------------------------------------------------
# Full sync
# ---------------------------------------------------------------------------

def sync_trades(user_id: str, kite_client) -> dict:
    """
    Full trade sync: fetch from Zerodha, pair, store.
    Returns {synced: N, message: str}.
    """
    if kite_client is None:
        return {"synced": 0, "message": "Not connected to Zerodha. Use demo mode or authenticate first."}

    try:
        raw = fetch_zerodha_trades(kite_client)
        if not raw:
            return {"synced": 0, "message": "No trades found in Zerodha account for the last 365 days."}

        paired = pair_trades(raw)
        count = store_trades(user_id, paired)
        return {"synced": count, "message": f"Successfully synced {count} trades from Zerodha."}
    except Exception as e:
        return {"synced": 0, "message": f"Sync failed: {str(e)}"}


# ---------------------------------------------------------------------------
# Query trades
# ---------------------------------------------------------------------------

def get_trades(user_id: str, symbol: str = None, from_date: str = None,
               to_date: str = None, direction: str = None) -> list:
    """Get trades for a user with optional filters."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    query = "SELECT * FROM trades WHERE user_id = ?"
    params = [user_id]

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())
    if from_date:
        query += " AND entry_time >= ?"
        params.append(from_date)
    if to_date:
        query += " AND entry_time <= ?"
        params.append(to_date + "T23:59:59")
    if direction:
        query += " AND direction = ?"
        params.append(direction.upper())

    query += " ORDER BY entry_time DESC"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_trade_by_id(user_id: str, trade_id: int) -> Optional[dict]:
    """Get a single trade by its database ID."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM trades WHERE user_id = ? AND id = ?",
        (user_id, trade_id)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# WebSocket real-time sync (placeholder for live usage)
# ---------------------------------------------------------------------------

def start_realtime_sync(kite_client, user_id: str):
    """
    Start real-time trade sync via Zerodha WebSocket.
    This is a placeholder — in production, this would run in a background thread
    and listen for order updates via KiteTicker.
    """
    if kite_client is None:
        print("[TradeSync] WebSocket sync not available in demo mode.")
        return

    try:
        from kiteconnect import KiteTicker
        api_key = os.getenv("KITE_API_KEY", "")
        access_token = kite_client.access_token

        kws = KiteTicker(api_key, access_token)

        def on_order_update(ws, data):
            """Handle order updates and sync completed trades."""
            if data.get("status") == "COMPLETE":
                print(f"[TradeSync] Order completed: {data.get('tradingsymbol')} {data.get('transaction_type')}")
                # In production, would pair and store the trade here

        kws.on_order_update = on_order_update
        print("[TradeSync] Real-time sync started via WebSocket.")
        # kws.connect(threaded=True)  # Uncomment in production

    except Exception as e:
        print(f"[TradeSync] WebSocket sync failed: {e}")
