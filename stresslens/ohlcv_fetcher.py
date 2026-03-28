"""
TradeMind — OHLCV Data Fetcher
Primary: Zerodha Kite historical API. Fallback: realistic dummy OHLCV generator.
Caches all data in SQLite.
"""

import os
import sqlite3
import random
import math
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "stresslens.db")


def init_ohlcv_db():
    """Create ohlcv_cache table if it doesn't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER DEFAULT 0,
            interval TEXT DEFAULT 'day',
            UNIQUE(symbol, date, interval)
        )
    """)
    conn.commit()
    conn.close()


init_ohlcv_db()


# ---------------------------------------------------------------------------
# Dummy OHLCV generator — realistic candles around a reference price
# ---------------------------------------------------------------------------

def generate_dummy_ohlcv(symbol: str, ref_price: float, num_candles: int = 15,
                         end_date: datetime = None, interval: str = "day") -> list:
    """
    Generate realistic dummy OHLCV data centered around a reference price.
    Creates candles that naturally contain candlestick patterns.
    """
    if end_date is None:
        end_date = datetime.now()

    random.seed(hash(f"{symbol}_{ref_price}_{end_date.strftime('%Y%m%d')}") % (2**31))

    candles = []
    price = ref_price * random.uniform(0.95, 1.02)

    # Pre-decide a pattern to embed (ensures every trade gets a pattern)
    embed_pattern = random.choice([
        "hammer", "shooting_star", "bullish_engulfing", "bearish_engulfing",
        "morning_star", "evening_star", "piercing_line", "dark_cloud",
        "bull_flag", "bear_flag",
    ])
    embed_at = num_candles - random.randint(2, 5)  # near the end

    for i in range(num_candles):
        dt = end_date - timedelta(days=(num_candles - 1 - i))
        # Skip weekends
        while dt.weekday() >= 5:
            dt += timedelta(days=1)

        daily_vol = ref_price * random.uniform(0.008, 0.025)
        drift = random.uniform(-0.003, 0.003)

        if i == embed_at:
            candle = _make_pattern_candle(embed_pattern, price, daily_vol, i, candles)
        elif i == embed_at - 1 and embed_pattern in ("bullish_engulfing", "bearish_engulfing",
                                                       "piercing_line", "dark_cloud",
                                                       "morning_star", "evening_star"):
            candle = _make_setup_candle(embed_pattern, price, daily_vol)
        elif i == embed_at - 2 and embed_pattern in ("morning_star", "evening_star"):
            candle = _make_pre_setup_candle(embed_pattern, price, daily_vol)
        elif embed_pattern in ("bull_flag", "bear_flag") and embed_at - 5 <= i < embed_at - 2:
            candle = _make_flag_pole_candle(embed_pattern, price, daily_vol, i - (embed_at - 5))
        elif embed_pattern in ("bull_flag", "bear_flag") and embed_at - 2 <= i < embed_at:
            candle = _make_flag_consol_candle(price, daily_vol * 0.3)
        else:
            # Normal random candle
            o = price + random.uniform(-daily_vol * 0.3, daily_vol * 0.3)
            c = o + random.uniform(-daily_vol, daily_vol) + drift * price
            h = max(o, c) + random.uniform(0, daily_vol * 0.5)
            l = min(o, c) - random.uniform(0, daily_vol * 0.5)
            candle = {"open": o, "high": h, "low": l, "close": c}

        candle["date"] = dt.strftime("%Y-%m-%d")
        candle["symbol"] = symbol
        candle["volume"] = random.randint(100000, 5000000)
        candle["interval"] = interval

        # Round everything
        for k in ("open", "high", "low", "close"):
            candle[k] = round(candle[k], 2)

        # Ensure OHLC validity
        candle["high"] = max(candle["open"], candle["high"], candle["close"], candle["low"])
        candle["low"] = min(candle["open"], candle["low"], candle["close"], candle["high"])

        candles.append(candle)
        price = candle["close"]

    return candles


def _make_pattern_candle(pattern, price, vol, i, prev_candles):
    """Generate a candle that completes the specified pattern."""
    if pattern == "hammer":
        body = vol * 0.2
        o = price + body / 2
        c = price + body
        l = price - vol * 1.5
        h = max(o, c) + vol * 0.05
        return {"open": o, "high": h, "low": l, "close": c}

    elif pattern == "shooting_star":
        body = vol * 0.2
        o = price
        c = price - body
        h = price + vol * 1.5
        l = min(o, c) - vol * 0.05
        return {"open": o, "high": h, "low": l, "close": c}

    elif pattern == "bullish_engulfing":
        prev = prev_candles[-1] if prev_candles else {"open": price, "close": price - vol * 0.3}
        o = min(prev["open"], prev["close"]) - vol * 0.1
        c = max(prev["open"], prev["close"]) + vol * 0.2
        return {"open": o, "high": c + vol * 0.1, "low": o - vol * 0.1, "close": c}

    elif pattern == "bearish_engulfing":
        prev = prev_candles[-1] if prev_candles else {"open": price, "close": price + vol * 0.3}
        o = max(prev["open"], prev["close"]) + vol * 0.1
        c = min(prev["open"], prev["close"]) - vol * 0.2
        return {"open": o, "high": o + vol * 0.1, "low": c - vol * 0.1, "close": c}

    elif pattern in ("morning_star", "evening_star"):
        # Third candle of the star
        if pattern == "morning_star":
            o = price - vol * 0.1
            c = price + vol * 0.8
        else:
            o = price + vol * 0.1
            c = price - vol * 0.8
        h = max(o, c) + vol * 0.15
        l = min(o, c) - vol * 0.15
        return {"open": o, "high": h, "low": l, "close": c}

    elif pattern == "piercing_line":
        prev = prev_candles[-1] if prev_candles else {"open": price, "close": price - vol, "low": price - vol * 1.2}
        prev_low = prev.get("low", min(prev["open"], prev["close"]) - vol * 0.2)
        o = prev_low - vol * 0.1
        midpoint = (prev["open"] + prev["close"]) / 2
        c = midpoint + vol * 0.2
        return {"open": o, "high": c + vol * 0.1, "low": o - vol * 0.05, "close": c}

    elif pattern == "dark_cloud":
        prev = prev_candles[-1] if prev_candles else {"open": price, "close": price + vol, "high": price + vol * 1.2}
        prev_high = prev.get("high", max(prev["open"], prev["close"]) + vol * 0.2)
        o = prev_high + vol * 0.1
        midpoint = (prev["open"] + prev["close"]) / 2
        c = midpoint - vol * 0.2
        return {"open": o, "high": o + vol * 0.05, "low": c - vol * 0.1, "close": c}

    else:
        # bull_flag / bear_flag consolidation candle
        o = price + random.uniform(-vol * 0.1, vol * 0.1)
        c = o + random.uniform(-vol * 0.15, vol * 0.15)
        h = max(o, c) + vol * 0.05
        l = min(o, c) - vol * 0.05
        return {"open": o, "high": h, "low": l, "close": c}


def _make_setup_candle(pattern, price, vol):
    """Candle before the pattern candle (for 2-candle patterns)."""
    if pattern in ("bullish_engulfing", "piercing_line", "morning_star"):
        o = price + vol * 0.3
        c = price - vol * 0.3
    else:
        o = price - vol * 0.3
        c = price + vol * 0.3
    h = max(o, c) + vol * 0.15
    l = min(o, c) - vol * 0.15
    return {"open": o, "high": h, "low": l, "close": c}


def _make_pre_setup_candle(pattern, price, vol):
    """First candle for 3-candle patterns (morning/evening star)."""
    if pattern == "morning_star":
        o = price + vol * 0.8
        c = price
    else:
        o = price - vol * 0.8
        c = price
    h = max(o, c) + vol * 0.1
    l = min(o, c) - vol * 0.1
    return {"open": o, "high": h, "low": l, "close": c}


def _make_flag_pole_candle(pattern, price, vol, pole_idx):
    """Candles forming the pole of a flag pattern."""
    step = vol * 0.6
    if pattern == "bull_flag":
        o = price + step * pole_idx
        c = o + step
    else:
        o = price - step * pole_idx
        c = o - step
    h = max(o, c) + vol * 0.1
    l = min(o, c) - vol * 0.1
    return {"open": o, "high": h, "low": l, "close": c}


def _make_flag_consol_candle(price, vol):
    """Tight consolidation candle for flag pattern."""
    o = price + random.uniform(-vol, vol)
    c = o + random.uniform(-vol, vol)
    h = max(o, c) + vol * 0.3
    l = min(o, c) - vol * 0.3
    return {"open": o, "high": h, "low": l, "close": c}


# ---------------------------------------------------------------------------
# Cache operations
# ---------------------------------------------------------------------------

def cache_ohlcv(candles: list):
    """Store OHLCV candles in the cache."""
    conn = sqlite3.connect(DB_PATH)
    for c in candles:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO ohlcv_cache
                (symbol, date, open, high, low, close, volume, interval)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                c["symbol"], c["date"], c["open"], c["high"], c["low"],
                c["close"], c.get("volume", 0), c.get("interval", "day"),
            ))
        except Exception:
            pass
    conn.commit()
    conn.close()


def get_cached_ohlcv(symbol: str, from_date: str, to_date: str,
                     interval: str = "day") -> list:
    """Retrieve cached OHLCV data."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM ohlcv_cache
        WHERE symbol = ? AND date >= ? AND date <= ? AND interval = ?
        ORDER BY date ASC
    """, (symbol, from_date, to_date, interval)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Main fetcher
# ---------------------------------------------------------------------------

def get_ohlcv(symbol: str, from_date: str = None, to_date: str = None,
              interval: str = "day", ref_price: float = None,
              num_candles: int = 15, kite_client=None) -> list:
    """
    Get OHLCV data for a symbol.
    1. Try cache first
    2. Try Kite API if authenticated
    3. Fall back to dummy generation

    Args:
        symbol: Trading symbol
        from_date/to_date: date strings (YYYY-MM-DD)
        interval: 'day', '15minute', etc.
        ref_price: reference price for dummy generation
        num_candles: number of candles for dummy generation
        kite_client: authenticated KiteConnect instance or None
    """
    if to_date is None:
        to_date = datetime.now().strftime("%Y-%m-%d")
    if from_date is None:
        from_date = (datetime.now() - timedelta(days=num_candles + 5)).strftime("%Y-%m-%d")

    # 1. Check cache
    cached = get_cached_ohlcv(symbol, from_date, to_date, interval)
    if len(cached) >= num_candles:
        return cached[-num_candles:]

    # 2. Try Kite API
    if kite_client is not None:
        try:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d")
            to_dt = datetime.strptime(to_date, "%Y-%m-%d")

            kite_interval = "day"
            if interval == "15minute":
                kite_interval = "15minute"
            elif interval == "5minute":
                kite_interval = "5minute"

            instrument_token = _resolve_instrument(kite_client, symbol)
            if instrument_token:
                data = kite_client.historical_data(
                    instrument_token, from_dt, to_dt, kite_interval
                )
                candles = []
                for d in data:
                    candles.append({
                        "symbol": symbol,
                        "date": d["date"].strftime("%Y-%m-%d") if hasattr(d["date"], "strftime") else str(d["date"])[:10],
                        "open": float(d["open"]),
                        "high": float(d["high"]),
                        "low": float(d["low"]),
                        "close": float(d["close"]),
                        "volume": int(d.get("volume", 0)),
                        "interval": interval,
                    })
                if candles:
                    cache_ohlcv(candles)
                    return candles[-num_candles:]
        except Exception as e:
            print(f"[OHLCV] Kite fetch failed for {symbol}: {e}")

    # 3. Fallback: generate dummy
    if ref_price is None:
        ref_price = 1000.0  # generic fallback

    end_dt = datetime.strptime(to_date, "%Y-%m-%d")
    candles = generate_dummy_ohlcv(symbol, ref_price, num_candles, end_dt, interval)
    cache_ohlcv(candles)
    return candles


def _resolve_instrument(kite_client, symbol: str):
    """Resolve a symbol to an instrument token using Kite API."""
    try:
        instruments = kite_client.ltp(f"NSE:{symbol}")
        key = f"NSE:{symbol}"
        if key in instruments:
            return instruments[key].get("instrument_token")
    except Exception:
        pass
    return None
