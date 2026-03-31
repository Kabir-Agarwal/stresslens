"""
TradeMind — Behavioral Intelligence Engine
Discovers repeating patterns in a trader's history from raw data.
Zero judgement — only data observations.
"""

import os
import sqlite3
import json
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Optional

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "stresslens.db")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_behavioral_db():
    """Create behavioral_insights table."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS behavioral_insights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            analysis_date TEXT NOT NULL,
            insights_json TEXT,
            generated_at TEXT,
            UNIQUE(user_id, analysis_date)
        )
    """)
    conn.commit()
    conn.close()


init_behavioral_db()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_trades_df(user_id: str) -> List[dict]:
    """Load all trades for a user as list of dicts."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM trades WHERE user_id = ? AND status = 'CLOSED' ORDER BY entry_time ASC",
        (user_id,),
    ).fetchall()
    conn.close()
    trades = []
    for r in rows:
        t = dict(r)
        try:
            t["_entry_dt"] = datetime.fromisoformat(t["entry_time"])
        except Exception:
            t["_entry_dt"] = None
        try:
            t["_exit_dt"] = datetime.fromisoformat(t["exit_time"])
        except Exception:
            t["_exit_dt"] = None
        t["_win"] = (t.get("pnl", 0) or 0) > 0
        trades.append(t)
    return trades


def _get_market_context(user_id: str) -> Dict[int, dict]:
    """Load market context keyed by trade_id."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM market_context WHERE user_id = ?", (user_id,)
    ).fetchall()
    conn.close()
    return {r["trade_id"]: dict(r) for r in rows}


def _fmt_inr(value: float) -> str:
    """Format as Indian rupees."""
    if abs(value) >= 1e7:
        return f"\u20b9{value / 1e7:.2f} Cr"
    if abs(value) >= 1e5:
        return f"\u20b9{value / 1e5:.2f} L"
    return f"\u20b9{value:,.0f}"


def _pct(n: int, total: int) -> float:
    return round(n / total * 100, 1) if total > 0 else 0.0


# ---------------------------------------------------------------------------
# 1. Time Patterns
# ---------------------------------------------------------------------------

def analyze_time_patterns(trades: List[dict]) -> dict:
    """Win rate by hour, day of week, and time segment."""
    by_hour = defaultdict(lambda: {"wins": 0, "total": 0, "pnl": 0.0})
    by_dow = defaultdict(lambda: {"wins": 0, "total": 0, "pnl": 0.0})
    by_segment = defaultdict(lambda: {"wins": 0, "total": 0, "pnl": 0.0})

    for t in trades:
        dt = t.get("_entry_dt")
        if not dt:
            continue
        h = dt.hour
        pnl = t.get("pnl", 0) or 0
        win = t["_win"]

        # Hour
        by_hour[h]["total"] += 1
        by_hour[h]["pnl"] += pnl
        if win:
            by_hour[h]["wins"] += 1

        # Day of week
        dow = dt.strftime("%A")
        by_dow[dow]["total"] += 1
        by_dow[dow]["pnl"] += pnl
        if win:
            by_dow[dow]["wins"] += 1

        # Segment
        if 9 <= h < 11:
            seg = "Morning (9-11)"
        elif 11 <= h < 13:
            seg = "Midday (11-1)"
        else:
            seg = "Afternoon (1-3:30)"
        by_segment[seg]["total"] += 1
        by_segment[seg]["pnl"] += pnl
        if win:
            by_segment[seg]["wins"] += 1

    def _build(d):
        return {
            k: {
                "win_rate": _pct(v["wins"], v["total"]),
                "total_trades": v["total"],
                "avg_pnl": round(v["pnl"] / v["total"], 2) if v["total"] else 0,
                "total_pnl": round(v["pnl"], 2),
            }
            for k, v in sorted(d.items())
        }

    # Identify best and worst
    segments = _build(by_segment)
    best_seg = max(segments.items(), key=lambda x: x[1]["win_rate"]) if segments else (None, {})
    worst_seg = min(segments.items(), key=lambda x: x[1]["win_rate"]) if segments else (None, {})

    findings = []
    if best_seg[0] and best_seg[1].get("total_trades", 0) >= 3:
        findings.append(
            f"Trades placed during {best_seg[0]} have a {best_seg[1]['win_rate']}% win rate "
            f"across {best_seg[1]['total_trades']} trades, averaging {_fmt_inr(best_seg[1]['avg_pnl'])} per trade"
        )
    if worst_seg[0] and worst_seg[0] != best_seg[0] and worst_seg[1].get("total_trades", 0) >= 3:
        findings.append(
            f"Trades placed during {worst_seg[0]} have a {worst_seg[1]['win_rate']}% win rate "
            f"across {worst_seg[1]['total_trades']} trades, averaging {_fmt_inr(worst_seg[1]['avg_pnl'])} per trade"
        )

    return {
        "by_hour": _build(by_hour),
        "by_day_of_week": _build(by_dow),
        "by_segment": segments,
        "findings": findings,
    }


# ---------------------------------------------------------------------------
# 2. Symbol Patterns
# ---------------------------------------------------------------------------

def analyze_symbol_patterns(trades: List[dict]) -> dict:
    """Win rate and P&L per symbol."""
    by_sym = defaultdict(lambda: {"wins": 0, "total": 0, "pnl": 0.0, "sizes": []})

    for t in trades:
        sym = t.get("symbol", "UNKNOWN")
        pnl = t.get("pnl", 0) or 0
        qty = t.get("quantity", 0) or 0
        by_sym[sym]["total"] += 1
        by_sym[sym]["pnl"] += pnl
        by_sym[sym]["sizes"].append(qty)
        if t["_win"]:
            by_sym[sym]["wins"] += 1

    result = {}
    for sym, v in by_sym.items():
        avg_size = sum(v["sizes"]) / len(v["sizes"]) if v["sizes"] else 0
        size_std = (sum((s - avg_size) ** 2 for s in v["sizes"]) / len(v["sizes"])) ** 0.5 if len(v["sizes"]) > 1 else 0
        result[sym] = {
            "win_rate": _pct(v["wins"], v["total"]),
            "total_trades": v["total"],
            "total_pnl": round(v["pnl"], 2),
            "avg_pnl": round(v["pnl"] / v["total"], 2) if v["total"] else 0,
            "avg_position_size": round(avg_size, 1),
            "size_consistency": round(size_std / avg_size * 100, 1) if avg_size > 0 else 0,
        }

    sorted_by_pnl = sorted(result.items(), key=lambda x: x[1]["total_pnl"], reverse=True)
    best = sorted_by_pnl[0] if sorted_by_pnl else (None, {})
    worst = sorted_by_pnl[-1] if sorted_by_pnl else (None, {})

    findings = []
    if best[0] and best[1].get("total_trades", 0) >= 3:
        findings.append(
            f"{best[0]}: {_fmt_inr(best[1]['total_pnl'])} total P&L across "
            f"{best[1]['total_trades']} trades ({best[1]['win_rate']}% win rate)"
        )
    if worst[0] and worst[0] != best[0] and worst[1].get("total_trades", 0) >= 3:
        findings.append(
            f"{worst[0]}: {_fmt_inr(worst[1]['total_pnl'])} total P&L across "
            f"{worst[1]['total_trades']} trades ({worst[1]['win_rate']}% win rate)"
        )

    # Flag inconsistent sizing
    for sym, v in result.items():
        if v["size_consistency"] > 80 and v["total_trades"] >= 3:
            findings.append(
                f"Position sizing on {sym} varies significantly "
                f"({v['size_consistency']:.0f}% coefficient of variation across {v['total_trades']} trades)"
            )

    return {"by_symbol": result, "best": best[0], "worst": worst[0], "findings": findings}


# ---------------------------------------------------------------------------
# 3. Streak Patterns
# ---------------------------------------------------------------------------

def analyze_streak_patterns(trades: List[dict]) -> dict:
    """Win rate after consecutive wins/losses, position sizing changes."""
    if len(trades) < 3:
        return {"findings": ["Insufficient trade history for streak analysis (minimum 3 trades needed)"]}

    after_loss = defaultdict(lambda: {"wins": 0, "total": 0})
    after_win = defaultdict(lambda: {"wins": 0, "total": 0})
    oversized_trades = []

    consecutive_losses = 0
    consecutive_wins = 0
    recent_sizes = []

    for i, t in enumerate(trades):
        pnl = t.get("pnl", 0) or 0
        qty = t.get("quantity", 0) or 0
        avg_size = sum(recent_sizes) / len(recent_sizes) if recent_sizes else qty

        # Check sizing after loss
        if consecutive_losses > 0 and avg_size > 0 and qty > avg_size * 2:
            oversized_trades.append({
                "trade_index": i,
                "symbol": t.get("symbol"),
                "entry_time": t.get("entry_time", ""),
                "size": qty,
                "avg_size": round(avg_size, 1),
                "ratio": round(qty / avg_size, 1),
                "after_consecutive_losses": consecutive_losses,
                "outcome_pnl": round(pnl, 2),
            })

        # Record performance after streaks
        if consecutive_losses > 0:
            key = f"{consecutive_losses}_loss" if consecutive_losses <= 2 else "3+_losses"
            after_loss[key]["total"] += 1
            if t["_win"]:
                after_loss[key]["wins"] += 1

        if consecutive_wins > 0:
            key = f"{consecutive_wins}_win" if consecutive_wins <= 2 else "3+_wins"
            after_win[key]["total"] += 1
            if t["_win"]:
                after_win[key]["wins"] += 1

        # Update streaks
        if pnl > 0:
            consecutive_wins += 1
            consecutive_losses = 0
        elif pnl < 0:
            consecutive_losses += 1
            consecutive_wins = 0

        recent_sizes.append(qty)
        if len(recent_sizes) > 10:
            recent_sizes.pop(0)

    # Compute win rate after losses
    after_loss_stats = {
        k: {"win_rate": _pct(v["wins"], v["total"]), "sample_size": v["total"]}
        for k, v in sorted(after_loss.items())
    }
    after_win_stats = {
        k: {"win_rate": _pct(v["wins"], v["total"]), "sample_size": v["total"]}
        for k, v in sorted(after_win.items())
    }

    # Average sizing after wins vs losses
    sizes_after_win = []
    sizes_after_loss = []
    prev_win = None
    for t in trades:
        qty = t.get("quantity", 0) or 0
        if prev_win is True:
            sizes_after_win.append(qty)
        elif prev_win is False:
            sizes_after_loss.append(qty)
        prev_win = t["_win"]

    avg_size_after_win = round(sum(sizes_after_win) / len(sizes_after_win), 1) if sizes_after_win else 0
    avg_size_after_loss = round(sum(sizes_after_loss) / len(sizes_after_loss), 1) if sizes_after_loss else 0

    findings = []
    for k, v in after_loss_stats.items():
        if v["sample_size"] >= 3:
            findings.append(
                f"After {k.replace('_', ' ')}: {v['win_rate']}% win rate across {v['sample_size']} trades"
            )

    if avg_size_after_win > 0 and avg_size_after_loss > 0:
        ratio = avg_size_after_loss / avg_size_after_win
        if abs(ratio - 1.0) > 0.15:
            direction = "larger" if ratio > 1 else "smaller"
            findings.append(
                f"Average position size after a loss is {abs(ratio - 1) * 100:.0f}% {direction} "
                f"than after a win ({avg_size_after_loss} vs {avg_size_after_win})"
            )

    if oversized_trades:
        findings.append(
            f"{len(oversized_trades)} trades were placed at >2x average size following a loss. "
            f"Average P&L on those trades: {_fmt_inr(sum(t['outcome_pnl'] for t in oversized_trades) / len(oversized_trades))}"
        )

    return {
        "after_loss": after_loss_stats,
        "after_win": after_win_stats,
        "avg_size_after_win": avg_size_after_win,
        "avg_size_after_loss": avg_size_after_loss,
        "oversized_after_loss": oversized_trades,
        "findings": findings,
    }


# ---------------------------------------------------------------------------
# 4. Market Context Patterns
# ---------------------------------------------------------------------------

def analyze_market_context_patterns(trades: List[dict], context_map: Dict[int, dict]) -> dict:
    """Win rate by market conditions: VIX, expiry, gap, instrument type."""
    vix_high = {"wins": 0, "total": 0, "pnl": 0.0}
    vix_low = {"wins": 0, "total": 0, "pnl": 0.0}
    expiry = {"wins": 0, "total": 0, "pnl": 0.0}
    non_expiry = {"wins": 0, "total": 0, "pnl": 0.0}
    by_instrument = defaultdict(lambda: {"wins": 0, "total": 0, "pnl": 0.0})

    for t in trades:
        pnl = t.get("pnl", 0) or 0
        win = t["_win"]
        tid = t.get("id")
        ctx = context_map.get(tid, {})

        # VIX
        vix = ctx.get("vix_at_entry", 0) or 0
        if vix > 15:
            vix_high["total"] += 1
            vix_high["pnl"] += pnl
            if win:
                vix_high["wins"] += 1
        elif vix > 0:
            vix_low["total"] += 1
            vix_low["pnl"] += pnl
            if win:
                vix_low["wins"] += 1

        # Expiry
        if ctx.get("is_expiry_week"):
            expiry["total"] += 1
            expiry["pnl"] += pnl
            if win:
                expiry["wins"] += 1
        elif ctx:
            non_expiry["total"] += 1
            non_expiry["pnl"] += pnl
            if win:
                non_expiry["wins"] += 1

        # Instrument type
        inst = t.get("instrument_type", "EQ")
        by_instrument[inst]["total"] += 1
        by_instrument[inst]["pnl"] += pnl
        if win:
            by_instrument[inst]["wins"] += 1

    def _stat(d):
        return {
            "win_rate": _pct(d["wins"], d["total"]),
            "total_trades": d["total"],
            "avg_pnl": round(d["pnl"] / d["total"], 2) if d["total"] else 0,
        }

    result = {
        "vix_above_15": _stat(vix_high),
        "vix_below_15": _stat(vix_low),
        "expiry_week": _stat(expiry),
        "non_expiry_week": _stat(non_expiry),
        "by_instrument": {k: _stat(v) for k, v in by_instrument.items()},
    }

    findings = []
    if vix_high["total"] >= 3 and vix_low["total"] >= 3:
        diff = _stat(vix_high)["win_rate"] - _stat(vix_low)["win_rate"]
        if abs(diff) > 5:
            better = "above" if diff > 0 else "below"
            findings.append(
                f"Win rate is {abs(diff):.1f}pp higher when VIX is {better} 15 "
                f"({_stat(vix_high)['win_rate']}% vs {_stat(vix_low)['win_rate']}%)"
            )

    if expiry["total"] >= 3 and non_expiry["total"] >= 3:
        diff = _stat(expiry)["win_rate"] - _stat(non_expiry)["win_rate"]
        if abs(diff) > 5:
            better = "expiry" if diff > 0 else "non-expiry"
            findings.append(
                f"Win rate is {abs(diff):.1f}pp higher during {better} weeks "
                f"({_stat(expiry)['win_rate']}% vs {_stat(non_expiry)['win_rate']}%)"
            )

    result["findings"] = findings
    return result


# ---------------------------------------------------------------------------
# 5. Pattern Stamp Patterns (candlestick patterns)
# ---------------------------------------------------------------------------

def analyze_pattern_stamp_patterns(trades: List[dict]) -> dict:
    """Win rate per detected candlestick pattern, compare to NSE baseline."""
    from pattern_backtest import PATTERN_WIN_RATES

    by_pattern = defaultdict(lambda: {"wins": 0, "total": 0, "pnl": 0.0})
    no_pattern = {"wins": 0, "total": 0, "pnl": 0.0}

    for t in trades:
        pat = t.get("detected_pattern", "")
        pnl = t.get("pnl", 0) or 0
        win = t["_win"]

        if pat:
            by_pattern[pat]["total"] += 1
            by_pattern[pat]["pnl"] += pnl
            if win:
                by_pattern[pat]["wins"] += 1
        else:
            no_pattern["total"] += 1
            no_pattern["pnl"] += pnl
            if win:
                no_pattern["wins"] += 1

    result = {}
    edge_patterns = []
    overtrade_patterns = []

    total_trades = len(trades)
    avg_per_pattern = total_trades / max(len(by_pattern), 1)

    for pat, v in by_pattern.items():
        wr = _pct(v["wins"], v["total"])
        nse_data = PATTERN_WIN_RATES.get(pat, {})
        nse_wr = nse_data.get("win_rate", 50.0)

        entry = {
            "your_win_rate": wr,
            "nse_win_rate": nse_wr,
            "edge": round(wr - nse_wr, 1),
            "total_trades": v["total"],
            "avg_pnl": round(v["pnl"] / v["total"], 2) if v["total"] else 0,
        }
        result[pat] = entry

        # Genuine edge: your rate > NSE rate by >5%, min 3 trades
        if wr - nse_wr > 5 and v["total"] >= 3:
            edge_patterns.append({"pattern": pat, **entry})

        # Overtrade: you trade this pattern > 2x average frequency
        if v["total"] > avg_per_pattern * 2 and len(by_pattern) > 2:
            overtrade_patterns.append({"pattern": pat, "count": v["total"], "avg_count": round(avg_per_pattern, 1)})

    findings = []
    for ep in edge_patterns:
        findings.append(
            f"Your win rate on {ep['pattern']} is {ep['your_win_rate']}% vs NSE historical "
            f"{ep['nse_win_rate']}% (+{ep['edge']}pp edge, {ep['total_trades']} trades)"
        )
    for op in overtrade_patterns:
        findings.append(
            f"{op['pattern']} appears in {op['count']} of your trades "
            f"(average per pattern: {op['avg_count']})"
        )

    return {
        "by_pattern": result,
        "edge_patterns": edge_patterns,
        "overtrade_patterns": overtrade_patterns,
        "trades_without_pattern": no_pattern["total"],
        "findings": findings,
    }


# ---------------------------------------------------------------------------
# 6. Master Discovery
# ---------------------------------------------------------------------------

def discover_all_patterns(user_id: str) -> dict:
    """Run all 5 analysis functions and combine into unified insights."""
    trades = _get_trades_df(user_id)
    if not trades:
        return {"error": "No trades found", "findings": []}

    context_map = _get_market_context(user_id)

    time_patterns = analyze_time_patterns(trades)
    symbol_patterns = analyze_symbol_patterns(trades)
    streak_patterns = analyze_streak_patterns(trades)
    market_patterns = analyze_market_context_patterns(trades, context_map)
    pattern_stamps = analyze_pattern_stamp_patterns(trades)

    # Combine all findings
    all_findings = (
        time_patterns.get("findings", [])
        + symbol_patterns.get("findings", [])
        + streak_patterns.get("findings", [])
        + market_patterns.get("findings", [])
        + pattern_stamps.get("findings", [])
    )

    insights = {
        "user_id": user_id,
        "total_trades": len(trades),
        "date_range": {
            "from": trades[0].get("entry_time", ""),
            "to": trades[-1].get("entry_time", ""),
        },
        "time_patterns": time_patterns,
        "symbol_patterns": symbol_patterns,
        "streak_patterns": streak_patterns,
        "market_context_patterns": market_patterns,
        "pattern_stamp_patterns": pattern_stamps,
        "all_findings": all_findings,
    }

    # Store in database
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO behavioral_insights (user_id, analysis_date, insights_json, generated_at)
        VALUES (?, ?, ?, ?)
    """, (user_id, today, json.dumps(insights, default=str), now.isoformat()))
    conn.commit()
    conn.close()

    return insights


# ---------------------------------------------------------------------------
# 7. Daily Report
# ---------------------------------------------------------------------------

def generate_daily_report(user_id: str, target_date: datetime = None) -> dict:
    """
    Generate after-market-hours daily report.
    Compares today vs last 7 days. Pure data, zero judgement.
    """
    if target_date is None:
        target_date = datetime.now()

    today_str = target_date.strftime("%Y-%m-%d")
    week_ago = (target_date - timedelta(days=7)).strftime("%Y-%m-%d")

    trades = _get_trades_df(user_id)
    if not trades:
        return {"error": "No trades found for report generation"}

    # Split trades
    today_trades = [t for t in trades if t.get("entry_time", "").startswith(today_str)]
    week_trades = [t for t in trades if week_ago <= t.get("entry_time", "") <= today_str]
    # Exclude today from "last 7 days" baseline
    baseline_trades = [t for t in week_trades if not t.get("entry_time", "").startswith(today_str)]

    # If no trades today, find the most recent day with trades
    if not today_trades and trades:
        # Use the latest trade date as "today" for demo purposes
        latest = trades[-1].get("entry_time", "")[:10]
        today_trades = [t for t in trades if t.get("entry_time", "").startswith(latest)]
        today_str = latest
        week_ago = (datetime.fromisoformat(latest) - timedelta(days=7)).strftime("%Y-%m-%d")
        baseline_trades = [t for t in trades if week_ago <= t.get("entry_time", "") < latest]

    # Today summary
    today_pnl = sum(t.get("pnl", 0) or 0 for t in today_trades)
    today_wins = sum(1 for t in today_trades if t["_win"])
    today_wr = _pct(today_wins, len(today_trades))

    # Baseline (last 7 days)
    unique_days = len(set(t.get("entry_time", "")[:10] for t in baseline_trades)) or 1
    baseline_pnl = sum(t.get("pnl", 0) or 0 for t in baseline_trades)
    baseline_wins = sum(1 for t in baseline_trades if t["_win"])
    baseline_wr = _pct(baseline_wins, len(baseline_trades))
    baseline_avg_daily_trades = round(len(baseline_trades) / unique_days, 1)
    baseline_avg_daily_pnl = round(baseline_pnl / unique_days, 2)

    today_summary = {
        "date": today_str,
        "trades_count": len(today_trades),
        "pnl": round(today_pnl, 2),
        "pnl_formatted": _fmt_inr(today_pnl),
        "win_rate": today_wr,
        "baseline_avg_daily_trades": baseline_avg_daily_trades,
        "baseline_avg_daily_pnl": round(baseline_avg_daily_pnl, 2),
        "baseline_avg_daily_pnl_formatted": _fmt_inr(baseline_avg_daily_pnl),
        "baseline_win_rate": baseline_wr,
        "baseline_days": unique_days,
    }

    # Pattern continuity: compare time segment distribution, symbol mix
    today_segs = defaultdict(int)
    for t in today_trades:
        dt = t.get("_entry_dt")
        if dt:
            h = dt.hour
            if 9 <= h < 11:
                today_segs["Morning (9-11)"] += 1
            elif 11 <= h < 13:
                today_segs["Midday (11-1)"] += 1
            else:
                today_segs["Afternoon (1-3:30)"] += 1

    baseline_segs = defaultdict(int)
    for t in baseline_trades:
        dt = t.get("_entry_dt")
        if dt:
            h = dt.hour
            if 9 <= h < 11:
                baseline_segs["Morning (9-11)"] += 1
            elif 11 <= h < 13:
                baseline_segs["Midday (11-1)"] += 1
            else:
                baseline_segs["Afternoon (1-3:30)"] += 1

    continuity = []
    for seg in ["Morning (9-11)", "Midday (11-1)", "Afternoon (1-3:30)"]:
        today_count = today_segs.get(seg, 0)
        base_count = baseline_segs.get(seg, 0)
        base_daily = round(base_count / unique_days, 1) if unique_days else 0
        continued = "Yes" if (today_count > 0) == (base_daily > 0.5) else "No"
        continuity.append({
            "pattern": seg,
            "last_7_days": f"{base_count} trades ({base_daily}/day)",
            "today": f"{today_count} trades",
            "continued": continued,
        })

    # Conditions alignment — use full history behavioral insights
    full_insights = None
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT insights_json FROM behavioral_insights WHERE user_id = ? ORDER BY generated_at DESC LIMIT 1",
        (user_id,),
    ).fetchone()
    conn.close()
    if row:
        try:
            full_insights = json.loads(row["insights_json"])
        except Exception:
            pass

    # Build edge conditions from full history
    edge_conditions = _build_edge_conditions(trades, _get_market_context(user_id))

    # Count how many today trades were inside edge conditions
    inside_edge = 0
    outside_edge = 0
    for t in today_trades:
        in_edge = False
        dt = t.get("_entry_dt")
        if dt:
            for ec in edge_conditions:
                if ec["type"] == "time_segment":
                    h = dt.hour
                    if ec["condition"] == "Before 11am" and 9 <= h < 11:
                        in_edge = True
                    elif ec["condition"] == "11am-1pm" and 11 <= h < 13:
                        in_edge = True
                    elif ec["condition"] == "After 1pm" and h >= 13:
                        in_edge = True
                elif ec["type"] == "symbol" and t.get("symbol") == ec["condition"]:
                    in_edge = True
                elif ec["type"] == "direction" and t.get("direction") == ec["condition"]:
                    in_edge = True
        if in_edge:
            inside_edge += 1
        else:
            outside_edge += 1

    conditions_alignment = {
        "inside_edge": inside_edge,
        "outside_edge": outside_edge,
        "alignment_score": round(inside_edge / len(today_trades) * 100) if today_trades else 0,
    }

    # Notable observations (max 5)
    observations = []
    if today_trades:
        # Time clustering
        hours = [t["_entry_dt"].hour for t in today_trades if t.get("_entry_dt")]
        if hours:
            after_1pm = sum(1 for h in hours if h >= 13)
            if after_1pm > len(hours) * 0.6 and len(hours) >= 2:
                afternoon_trades = [t for t in trades if t.get("_entry_dt") and t["_entry_dt"].hour >= 13]
                afternoon_wr = _pct(sum(1 for t in afternoon_trades if t["_win"]), len(afternoon_trades))
                observations.append(
                    f"{after_1pm} of today's {len(today_trades)} trades were placed after 1pm. "
                    f"Your historical win rate after 1pm: {afternoon_wr}%"
                )

        # Symbol concentration
        today_syms = [t.get("symbol") for t in today_trades]
        if today_syms:
            most_common = max(set(today_syms), key=today_syms.count)
            count = today_syms.count(most_common)
            if count > 1:
                sym_trades = [t for t in trades if t.get("symbol") == most_common]
                sym_wr = _pct(sum(1 for t in sym_trades if t["_win"]), len(sym_trades))
                observations.append(
                    f"{count} of today's trades were on {most_common}. "
                    f"Your historical win rate on {most_common}: {sym_wr}% ({len(sym_trades)} total trades)"
                )

        # P&L comparison
        if today_pnl != 0 and baseline_avg_daily_pnl != 0:
            ratio = today_pnl / abs(baseline_avg_daily_pnl) if baseline_avg_daily_pnl else 0
            if abs(ratio) > 2:
                observations.append(
                    f"Today's P&L ({_fmt_inr(today_pnl)}) is {abs(ratio):.1f}x your 7-day daily average ({_fmt_inr(baseline_avg_daily_pnl)})"
                )

        # Direction bias today
        longs = sum(1 for t in today_trades if t.get("direction") == "LONG")
        shorts = len(today_trades) - longs
        if today_trades and (longs == 0 or shorts == 0) and len(today_trades) >= 2:
            dir_name = "LONG" if longs > 0 else "SHORT"
            observations.append(
                f"All {len(today_trades)} trades today were {dir_name}"
            )

        # Win streak or loss streak today
        today_wins_streak = sum(1 for t in today_trades if t["_win"])
        if today_wins_streak == len(today_trades) and len(today_trades) >= 2:
            observations.append(f"All {len(today_trades)} trades today were profitable")
        elif today_wins_streak == 0 and len(today_trades) >= 2:
            observations.append(f"None of today's {len(today_trades)} trades were profitable")

    # ── Always generate observations from full history if we have < 3 ──
    if len(observations) < 3 and len(trades) >= 5:
        # Best time segment from full history
        seg_stats = defaultdict(lambda: {"wins": 0, "total": 0})
        for t in trades:
            dt = t.get("_entry_dt")
            if dt:
                h = dt.hour
                if 9 <= h < 11:
                    seg = "before 11am"
                elif 11 <= h < 13:
                    seg = "between 11am-1pm"
                else:
                    seg = "after 1pm"
                seg_stats[seg]["total"] += 1
                if t["_win"]:
                    seg_stats[seg]["wins"] += 1
        if seg_stats and len(observations) < 5:
            best_seg = max(seg_stats.items(), key=lambda x: _pct(x[1]["wins"], x[1]["total"]))
            worst_seg = min(seg_stats.items(), key=lambda x: _pct(x[1]["wins"], x[1]["total"]))
            best_wr = _pct(best_seg[1]["wins"], best_seg[1]["total"])
            worst_wr = _pct(worst_seg[1]["wins"], worst_seg[1]["total"])
            if best_seg[0] != worst_seg[0]:
                observations.append(
                    f"Across your history, trades placed {best_seg[0]} have a {best_wr}% win rate "
                    f"({best_seg[1]['total']} trades) vs {worst_wr}% {worst_seg[0]} ({worst_seg[1]['total']} trades)"
                )

        # Streak pattern: performance after consecutive losses
        consec_losses = 0
        after_loss_wins = 0
        after_loss_total = 0
        for t in trades:
            if consec_losses >= 2:
                after_loss_total += 1
                if t["_win"]:
                    after_loss_wins += 1
            if t["_win"]:
                consec_losses = 0
            else:
                consec_losses += 1
        if after_loss_total >= 3 and len(observations) < 5:
            after_loss_wr = _pct(after_loss_wins, after_loss_total)
            overall_wr = _pct(sum(1 for t in trades if t["_win"]), len(trades))
            observations.append(
                f"After 2+ consecutive losses, your win rate is {after_loss_wr}% "
                f"({after_loss_total} trades) vs your overall {overall_wr}%"
            )

        # Best and worst symbol
        sym_perf = defaultdict(lambda: {"wins": 0, "total": 0, "pnl": 0.0})
        for t in trades:
            sym = t.get("symbol", "")
            if sym:
                sym_perf[sym]["total"] += 1
                sym_perf[sym]["pnl"] += t.get("pnl", 0) or 0
                if t["_win"]:
                    sym_perf[sym]["wins"] += 1
        qualified = {s: v for s, v in sym_perf.items() if v["total"] >= 3}
        if len(qualified) >= 2 and len(observations) < 5:
            best_sym = max(qualified.items(), key=lambda x: x[1]["pnl"])
            worst_sym = min(qualified.items(), key=lambda x: x[1]["pnl"])
            observations.append(
                f"Your best symbol is {best_sym[0]} ({_fmt_inr(best_sym[1]['pnl'])} across "
                f"{best_sym[1]['total']} trades, {_pct(best_sym[1]['wins'], best_sym[1]['total'])}% win rate). "
                f"Lowest: {worst_sym[0]} ({_fmt_inr(worst_sym[1]['pnl'])}, {worst_sym[1]['total']} trades)"
            )

        # Direction split
        all_longs = [t for t in trades if t.get("direction") == "LONG"]
        all_shorts = [t for t in trades if t.get("direction") == "SHORT"]
        if all_longs and all_shorts and len(observations) < 5:
            long_wr = _pct(sum(1 for t in all_longs if t["_win"]), len(all_longs))
            short_wr = _pct(sum(1 for t in all_shorts if t["_win"]), len(all_shorts))
            if abs(long_wr - short_wr) > 3:
                observations.append(
                    f"LONG trades: {long_wr}% win rate ({len(all_longs)} trades) vs "
                    f"SHORT trades: {short_wr}% win rate ({len(all_shorts)} trades)"
                )

    report = {
        "report_date": today_str,
        "generated_at": datetime.now().isoformat(),
        "today_summary": today_summary,
        "pattern_continuity": continuity,
        "conditions_alignment": conditions_alignment,
        "notable_observations": observations[:5],
        "edge_conditions": edge_conditions[:10],
    }

    return report


# ---------------------------------------------------------------------------
# Edge Conditions Builder
# ---------------------------------------------------------------------------

def _build_edge_conditions(trades: List[dict], context_map: Dict[int, dict]) -> list:
    """
    Build ranked list of conditions where this trader historically performs best.
    Minimum 5 trades to qualify.
    """
    candidates = []

    # Time segments
    for seg_name, hour_range in [("Before 11am", (9, 11)), ("11am-1pm", (11, 13)), ("After 1pm", (13, 16))]:
        seg_trades = [t for t in trades if t.get("_entry_dt") and hour_range[0] <= t["_entry_dt"].hour < hour_range[1]]
        if len(seg_trades) >= 5:
            wr = _pct(sum(1 for t in seg_trades if t["_win"]), len(seg_trades))
            candidates.append({
                "condition": seg_name,
                "type": "time_segment",
                "win_rate": wr,
                "sample_size": len(seg_trades),
            })

    # Symbols
    by_sym = defaultdict(list)
    for t in trades:
        by_sym[t.get("symbol", "")].append(t)
    for sym, sym_trades in by_sym.items():
        if len(sym_trades) >= 5 and sym:
            wr = _pct(sum(1 for t in sym_trades if t["_win"]), len(sym_trades))
            candidates.append({
                "condition": sym,
                "type": "symbol",
                "win_rate": wr,
                "sample_size": len(sym_trades),
            })

    # Direction + symbol combos
    by_combo = defaultdict(list)
    for t in trades:
        combo = f"{t.get('symbol', '')} {t.get('direction', '')}"
        by_combo[combo].append(t)
    for combo, combo_trades in by_combo.items():
        if len(combo_trades) >= 5:
            wr = _pct(sum(1 for t in combo_trades if t["_win"]), len(combo_trades))
            candidates.append({
                "condition": combo,
                "type": "symbol_direction",
                "win_rate": wr,
                "sample_size": len(combo_trades),
            })

    # Days of week
    by_dow = defaultdict(list)
    for t in trades:
        dt = t.get("_entry_dt")
        if dt:
            by_dow[dt.strftime("%A")].append(t)
    for dow, dow_trades in by_dow.items():
        if len(dow_trades) >= 5:
            wr = _pct(sum(1 for t in dow_trades if t["_win"]), len(dow_trades))
            candidates.append({
                "condition": dow,
                "type": "day_of_week",
                "win_rate": wr,
                "sample_size": len(dow_trades),
            })

    # VIX regimes
    vix_high = [t for t in trades if context_map.get(t.get("id"), {}).get("vix_at_entry", 0) > 15]
    vix_low = [t for t in trades if 0 < (context_map.get(t.get("id"), {}).get("vix_at_entry", 0) or 0) <= 15]
    if len(vix_high) >= 5:
        wr = _pct(sum(1 for t in vix_high if t["_win"]), len(vix_high))
        candidates.append({"condition": "VIX above 15", "type": "vix", "win_rate": wr, "sample_size": len(vix_high)})
    if len(vix_low) >= 5:
        wr = _pct(sum(1 for t in vix_low if t["_win"]), len(vix_low))
        candidates.append({"condition": "VIX below 15", "type": "vix", "win_rate": wr, "sample_size": len(vix_low)})

    # Candlestick patterns
    by_pat = defaultdict(list)
    for t in trades:
        pat = t.get("detected_pattern", "")
        if pat:
            by_pat[pat].append(t)
    for pat, pat_trades in by_pat.items():
        if len(pat_trades) >= 5:
            wr = _pct(sum(1 for t in pat_trades if t["_win"]), len(pat_trades))
            candidates.append({
                "condition": f"{pat} pattern",
                "type": "candlestick_pattern",
                "win_rate": wr,
                "sample_size": len(pat_trades),
            })

    # Direction
    for d in ["LONG", "SHORT"]:
        d_trades = [t for t in trades if t.get("direction") == d]
        if len(d_trades) >= 5:
            wr = _pct(sum(1 for t in d_trades if t["_win"]), len(d_trades))
            candidates.append({"condition": d, "type": "direction", "win_rate": wr, "sample_size": len(d_trades)})

    # Sort by win rate descending
    candidates.sort(key=lambda x: x["win_rate"], reverse=True)
    return candidates


def get_edge_conditions(user_id: str, min_sample: int = 5) -> list:
    """Public API: return edge conditions for a user."""
    trades = _get_trades_df(user_id)
    context_map = _get_market_context(user_id)
    edges = _build_edge_conditions(trades, context_map)
    return [e for e in edges if e["sample_size"] >= min_sample]
