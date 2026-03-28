"""
TradeMind — Candlestick Pattern Detector
Pure rule-based detection of 10 patterns using pandas/numpy only. No TA-Lib.
"""

import numpy as np


def _body(o, c):
    return abs(c - o)

def _upper_shadow(o, h, c):
    return h - max(o, c)

def _lower_shadow(o, l, c):
    return min(o, c) - l

def _is_green(o, c):
    return c > o

def _is_red(o, c):
    return c < o

def _is_doji(o, h, l, c):
    body = _body(o, c)
    full_range = h - l
    if full_range == 0:
        return True
    return body / full_range < 0.1


# ---------------------------------------------------------------------------
# Individual pattern detectors — each takes OHLCV arrays and index i
# Returns (detected: bool, confidence: int 0-100)
# ---------------------------------------------------------------------------

def detect_hammer(opens, highs, lows, closes, i):
    """Hammer: lower shadow >= 2x body, upper shadow <= 0.1x range, small body at top."""
    o, h, l, c = opens[i], highs[i], lows[i], closes[i]
    body = _body(o, c)
    lower = _lower_shadow(o, l, c)
    upper = _upper_shadow(o, h, c)
    full_range = h - l

    if full_range == 0 or body == 0:
        return False, 0

    if lower >= 2 * body and upper <= 0.3 * full_range:
        # Stronger if preceded by downtrend
        confidence = 60
        if i >= 2:
            if closes[i-1] < closes[i-2]:
                confidence = 80
        return True, confidence
    return False, 0


def detect_shooting_star(opens, highs, lows, closes, i):
    """Shooting Star: upper shadow >= 2x body, lower shadow <= 0.1x range."""
    o, h, l, c = opens[i], highs[i], lows[i], closes[i]
    body = _body(o, c)
    upper = _upper_shadow(o, h, c)
    lower = _lower_shadow(o, l, c)
    full_range = h - l

    if full_range == 0 or body == 0:
        return False, 0

    if upper >= 2 * body and lower <= 0.3 * full_range:
        confidence = 60
        if i >= 2:
            if closes[i-1] > closes[i-2]:
                confidence = 80
        return True, confidence
    return False, 0


def detect_bullish_engulfing(opens, highs, lows, closes, i):
    """Bullish Engulfing: current green candle body engulfs previous red candle body."""
    if i < 1:
        return False, 0

    o0, c0 = opens[i-1], closes[i-1]
    o1, c1 = opens[i], closes[i]

    if not _is_red(o0, c0) or not _is_green(o1, c1):
        return False, 0

    prev_body_low = min(o0, c0)
    prev_body_high = max(o0, c0)
    curr_body_low = min(o1, c1)
    curr_body_high = max(o1, c1)

    if curr_body_low <= prev_body_low and curr_body_high >= prev_body_high:
        size_ratio = _body(o1, c1) / max(_body(o0, c0), 0.01)
        confidence = min(55 + int(size_ratio * 10), 90)
        return True, confidence
    return False, 0


def detect_bearish_engulfing(opens, highs, lows, closes, i):
    """Bearish Engulfing: current red candle body engulfs previous green candle body."""
    if i < 1:
        return False, 0

    o0, c0 = opens[i-1], closes[i-1]
    o1, c1 = opens[i], closes[i]

    if not _is_green(o0, c0) or not _is_red(o1, c1):
        return False, 0

    prev_body_low = min(o0, c0)
    prev_body_high = max(o0, c0)
    curr_body_low = min(o1, c1)
    curr_body_high = max(o1, c1)

    if curr_body_low <= prev_body_low and curr_body_high >= prev_body_high:
        size_ratio = _body(o1, c1) / max(_body(o0, c0), 0.01)
        confidence = min(55 + int(size_ratio * 10), 90)
        return True, confidence
    return False, 0


def detect_morning_star(opens, highs, lows, closes, i):
    """Morning Star: red candle, small doji/body, green candle closing above midpoint of first."""
    if i < 2:
        return False, 0

    o0, c0 = opens[i-2], closes[i-2]
    o1, h1, l1, c1 = opens[i-1], highs[i-1], lows[i-1], closes[i-1]
    o2, c2 = opens[i], closes[i]

    if not _is_red(o0, c0):
        return False, 0
    if not _is_green(o2, c2):
        return False, 0

    # Middle candle should be small
    body1 = _body(o1, c1)
    body0 = _body(o0, c0)
    if body0 == 0:
        return False, 0
    if body1 > 0.4 * body0:
        return False, 0

    # Third candle closes above midpoint of first
    midpoint = (o0 + c0) / 2
    if c2 > midpoint:
        confidence = 70
        if c2 > o0:
            confidence = 85
        return True, confidence
    return False, 0


def detect_evening_star(opens, highs, lows, closes, i):
    """Evening Star: green candle, small doji/body, red candle closing below midpoint of first."""
    if i < 2:
        return False, 0

    o0, c0 = opens[i-2], closes[i-2]
    o1, h1, l1, c1 = opens[i-1], highs[i-1], lows[i-1], closes[i-1]
    o2, c2 = opens[i], closes[i]

    if not _is_green(o0, c0):
        return False, 0
    if not _is_red(o2, c2):
        return False, 0

    body1 = _body(o1, c1)
    body0 = _body(o0, c0)
    if body0 == 0:
        return False, 0
    if body1 > 0.4 * body0:
        return False, 0

    midpoint = (o0 + c0) / 2
    if c2 < midpoint:
        confidence = 70
        if c2 < o0:
            confidence = 85
        return True, confidence
    return False, 0


def detect_piercing_line(opens, highs, lows, closes, i):
    """Piercing Line: red candle then green opening below low, closing above 50% of red body."""
    if i < 1:
        return False, 0

    o0, c0 = opens[i-1], closes[i-1]
    o1, c1 = opens[i], closes[i]
    l0 = lows[i-1]

    if not _is_red(o0, c0) or not _is_green(o1, c1):
        return False, 0

    if o1 > l0:
        return False, 0

    midpoint = (o0 + c0) / 2
    if c1 > midpoint and c1 < o0:
        penetration = (c1 - c0) / max(_body(o0, c0), 0.01)
        confidence = min(55 + int(penetration * 20), 85)
        return True, confidence
    return False, 0


def detect_dark_cloud_cover(opens, highs, lows, closes, i):
    """Dark Cloud Cover: green candle then red opening above high, closing below 50% of green body."""
    if i < 1:
        return False, 0

    o0, c0 = opens[i-1], closes[i-1]
    o1, c1 = opens[i], closes[i]
    h0 = highs[i-1]

    if not _is_green(o0, c0) or not _is_red(o1, c1):
        return False, 0

    if o1 < h0:
        return False, 0

    midpoint = (o0 + c0) / 2
    if c1 < midpoint and c1 > o0:
        penetration = (c0 - c1) / max(_body(o0, c0), 0.01)
        confidence = min(55 + int(penetration * 20), 85)
        return True, confidence
    return False, 0


def detect_bull_flag(opens, highs, lows, closes, i):
    """Bull Flag: strong upward move (pole) followed by tight consolidation of 3-5 candles."""
    if i < 5:
        return False, 0

    # Check for a strong pole: candles i-5 to i-3 should show strong upward move
    pole_start = closes[i-5]
    pole_end = closes[i-3]
    if pole_start == 0:
        return False, 0
    pole_move = (pole_end - pole_start) / pole_start

    if pole_move < 0.015:  # At least 1.5% move up
        return False, 0

    # Check consolidation: candles i-2 to i should have tight range
    consol_highs = [highs[j] for j in range(i-2, i+1)]
    consol_lows = [lows[j] for j in range(i-2, i+1)]
    consol_range = max(consol_highs) - min(consol_lows)
    pole_range = highs[i-3] - lows[i-5]

    if pole_range == 0:
        return False, 0

    # Consolidation should be tighter than the pole
    if consol_range < 0.6 * pole_range:
        tightness = 1 - (consol_range / pole_range)
        confidence = min(60 + int(tightness * 30), 88)
        return True, confidence
    return False, 0


def detect_bear_flag(opens, highs, lows, closes, i):
    """Bear Flag: strong downward move followed by tight consolidation."""
    if i < 5:
        return False, 0

    pole_start = closes[i-5]
    pole_end = closes[i-3]
    if pole_start == 0:
        return False, 0
    pole_move = (pole_end - pole_start) / pole_start

    if pole_move > -0.015:  # At least 1.5% move down
        return False, 0

    consol_highs = [highs[j] for j in range(i-2, i+1)]
    consol_lows = [lows[j] for j in range(i-2, i+1)]
    consol_range = max(consol_highs) - min(consol_lows)
    pole_range = highs[i-5] - lows[i-3]

    if pole_range == 0:
        return False, 0

    if consol_range < 0.6 * pole_range:
        tightness = 1 - (consol_range / pole_range)
        confidence = min(60 + int(tightness * 30), 88)
        return True, confidence
    return False, 0


# ---------------------------------------------------------------------------
# Master detector — scans OHLCV and returns all detected patterns
# ---------------------------------------------------------------------------

ALL_DETECTORS = [
    ("Hammer", "bullish", detect_hammer),
    ("Shooting Star", "bearish", detect_shooting_star),
    ("Bullish Engulfing", "bullish", detect_bullish_engulfing),
    ("Bearish Engulfing", "bearish", detect_bearish_engulfing),
    ("Morning Star", "bullish", detect_morning_star),
    ("Evening Star", "bearish", detect_evening_star),
    ("Piercing Line", "bullish", detect_piercing_line),
    ("Dark Cloud Cover", "bearish", detect_dark_cloud_cover),
    ("Bull Flag", "bullish", detect_bull_flag),
    ("Bear Flag", "bearish", detect_bear_flag),
]


def detect_patterns(opens, highs, lows, closes, scan_range=None):
    """
    Scan OHLCV arrays for all 10 patterns.

    Args:
        opens, highs, lows, closes: numpy arrays or lists of OHLCV data
        scan_range: tuple (start, end) of indices to scan. Defaults to full range.

    Returns:
        List of dicts: [{pattern_name, direction, confidence, candle_index}, ...]
        sorted by confidence descending.
    """
    n = len(opens)
    if n < 2:
        return []

    opens = np.array(opens, dtype=float)
    highs = np.array(highs, dtype=float)
    lows = np.array(lows, dtype=float)
    closes = np.array(closes, dtype=float)

    start = scan_range[0] if scan_range else 0
    end = scan_range[1] if scan_range else n

    results = []
    for i in range(max(start, 0), min(end, n)):
        for name, direction, detector in ALL_DETECTORS:
            detected, confidence = detector(opens, highs, lows, closes, i)
            if detected:
                results.append({
                    "pattern_name": name,
                    "direction": direction,
                    "confidence": confidence,
                    "candle_index": i,
                })

    # Sort by confidence descending
    results.sort(key=lambda x: x["confidence"], reverse=True)
    return results


def get_strongest_pattern(opens, highs, lows, closes, scan_range=None):
    """Return the single strongest pattern detected, or None."""
    patterns = detect_patterns(opens, highs, lows, closes, scan_range)
    return patterns[0] if patterns else None
