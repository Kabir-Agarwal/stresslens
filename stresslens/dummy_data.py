"""
TradeMind — Dummy Data Generator
50 realistic demo trades on NSE stocks for testing.
"""

import os
import sqlite3
import random
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "stresslens.db")

# Realistic price ranges for popular NSE stocks (as of 2024-2025)
STOCK_PROFILES = {
    "RELIANCE": {"exchange": "NSE", "type": "EQ", "price_range": (2400, 3000), "lot": 1},
    "INFY": {"exchange": "NSE", "type": "EQ", "price_range": (1400, 1900), "lot": 1},
    "TCS": {"exchange": "NSE", "type": "EQ", "price_range": (3500, 4300), "lot": 1},
    "HDFCBANK": {"exchange": "NSE", "type": "EQ", "price_range": (1500, 1800), "lot": 1},
    "BANKNIFTY": {"exchange": "NFO", "type": "FUT", "price_range": (44000, 52000), "lot": 15},
    "NIFTY": {"exchange": "NFO", "type": "FUT", "price_range": (21000, 24500), "lot": 25},
}

EMOTION_TAGS = ["confident", "fearful", "greedy", "revenge", "fomo", "disciplined", "impulsive", "calm", ""]


def _random_datetime(start: datetime, end: datetime) -> datetime:
    """Random datetime between start and end during market hours (9:15 - 15:30)."""
    delta = end - start
    random_days = random.randint(0, delta.days)
    d = start + timedelta(days=random_days)
    # Skip weekends
    while d.weekday() >= 5:
        d += timedelta(days=1)
    hour = random.randint(9, 15)
    if hour == 9:
        minute = random.randint(15, 59)
    elif hour == 15:
        minute = random.randint(0, 30)
    else:
        minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return d.replace(hour=hour, minute=minute, second=second)


def _is_expiry_week(dt: datetime) -> bool:
    """Check if the date falls in an expiry week (last Thursday of month)."""
    # Find last Thursday of the month
    year, month = dt.year, dt.month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    # Walk back to Thursday (weekday 3)
    while last_day.weekday() != 3:
        last_day -= timedelta(days=1)
    # Expiry week = last Thursday minus 4 days to last Thursday
    expiry_week_start = last_day - timedelta(days=4)
    return expiry_week_start.date() <= dt.date() <= last_day.date()


def generate_dummy_trades(user_id: str = "demo") -> list:
    """Generate 50 realistic dummy trades."""
    random.seed(42)  # Reproducible
    now = datetime.now()
    six_months_ago = now - timedelta(days=180)

    trades = []
    trade_counter = 0

    symbols = list(STOCK_PROFILES.keys())

    for i in range(50):
        trade_counter += 1
        sym = random.choice(symbols)
        profile = STOCK_PROFILES[sym]

        low, high = profile["price_range"]
        entry_price = round(random.uniform(low, high), 2)

        # Entry time
        entry_time = _random_datetime(six_months_ago, now)

        # Some trades placed after 2 PM (for behavioral pattern testing)
        if i % 5 == 0:  # ~10 trades after 2 PM
            entry_time = entry_time.replace(hour=random.randint(14, 15), minute=random.randint(0, 29))

        # Exit 1 minute to 3 hours after entry
        exit_delta = timedelta(minutes=random.randint(1, 180))
        exit_time = entry_time + exit_delta
        # Cap at market close
        if exit_time.hour > 15 or (exit_time.hour == 15 and exit_time.minute > 30):
            exit_time = exit_time.replace(hour=15, minute=random.randint(15, 29))

        # Direction
        direction = random.choice(["LONG", "SHORT"])

        # Quantity
        qty = profile["lot"] * random.randint(1, 10)

        # P&L: mix of wins and losses
        # ~55% winning trades, ~45% losing
        if random.random() < 0.55:
            # Winning trade
            pnl_pct = random.uniform(0.002, 0.03)  # 0.2% to 3% gain
        else:
            # Losing trade
            pnl_pct = random.uniform(-0.04, -0.002)  # 0.2% to 4% loss

        if direction == "LONG":
            exit_price = round(entry_price * (1 + pnl_pct), 2)
            pnl = round((exit_price - entry_price) * qty, 2)
        else:
            exit_price = round(entry_price * (1 - pnl_pct), 2)
            pnl = round((entry_price - exit_price) * qty, 2)

        # Revenge trades: after a loss, place oversized trade within 30 min
        emotion = random.choice(EMOTION_TAGS)
        if i > 0 and trades[i - 1]["pnl"] < 0:
            time_diff = (entry_time - datetime.fromisoformat(trades[i - 1]["exit_time"])).total_seconds()
            if abs(time_diff) < 1800:  # within 30 minutes
                qty = qty * 3  # oversized
                pnl = round(pnl * 3, 2)  # recalculate
                emotion = "revenge"

        # Some trades during expiry week
        if _is_expiry_week(entry_time) and sym in ("BANKNIFTY", "NIFTY"):
            emotion = emotion or "expiry_pressure"

        product_type = "MIS" if profile["type"] == "FUT" else random.choice(["CNC", "MIS"])

        trade = {
            "user_id": user_id,
            "trade_id": f"demo_{trade_counter:04d}",
            "symbol": sym,
            "exchange": profile["exchange"],
            "instrument_type": profile["type"],
            "direction": direction,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "quantity": qty,
            "entry_time": entry_time.isoformat(),
            "exit_time": exit_time.isoformat(),
            "pnl": pnl,
            "status": "CLOSED",
            "note": "",
            "emotion_tag": emotion,
            "product_type": product_type,
            "order_id": f"demo_order_{trade_counter:04d}",
        }
        trades.append(trade)

    return trades


def load_demo_trades(user_id: str = "demo") -> dict:
    """Load 50 demo trades into the database. Clears existing demo trades first."""
    from trade_sync import init_trades_db
    init_trades_db()

    conn = sqlite3.connect(DB_PATH)
    # Clear existing demo trades for this user
    conn.execute("DELETE FROM trades WHERE user_id = ?", (user_id,))
    conn.commit()

    trades = generate_dummy_trades(user_id)
    inserted = 0

    for t in trades:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO trades
                (user_id, trade_id, symbol, exchange, instrument_type, direction,
                 entry_price, exit_price, quantity, entry_time, exit_time, pnl,
                 status, note, emotion_tag, product_type, order_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                t["user_id"], t["trade_id"], t["symbol"], t["exchange"],
                t["instrument_type"], t["direction"], t["entry_price"],
                t["exit_price"], t["quantity"], t["entry_time"], t["exit_time"],
                t["pnl"], t["status"], t["note"], t["emotion_tag"],
                t["product_type"], t["order_id"],
            ))
            inserted += 1
        except Exception as e:
            print(f"[DummyData] Error inserting trade {t['trade_id']}: {e}")

    conn.commit()
    conn.close()

    return {"loaded": inserted, "message": f"Demo trades loaded for user '{user_id}'"}


if __name__ == "__main__":
    result = load_demo_trades()
    print(result)
