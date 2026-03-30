"""
TradeMind — Chat Engine
AI chat interface for traders. Uses Anthropic Claude API with Groq fallback.
Zero judgement — only data-driven observations.
"""

import os
import json
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "stresslens.db")

# Load env
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

SYSTEM_PROMPT = """You are TradeMind, a trading journal AI for Indian retail traders.
You have access to this trader's complete trade history and behavioral analysis.
Answer only from their actual data. Never give generic trading advice.
Never judge their decisions. Only observe and report what the data shows.
If asked about something not in their data, say so directly.
Always cite specific numbers from their history.
Use Indian number format: lakhs and crores not millions.
Keep answers concise and data-driven.
Never use words like: mistake, wrong, poor, bad, emotional, irrational, should.
Instead use: historically, your data shows, in similar conditions, across your trades."""


def _fmt_inr(value: float) -> str:
    if abs(value) >= 1e7:
        return f"₹{value / 1e7:.2f} Cr"
    if abs(value) >= 1e5:
        return f"₹{value / 1e5:.2f} L"
    return f"₹{value:,.0f}"


# ---------------------------------------------------------------------------
# Context Builder
# ---------------------------------------------------------------------------

def build_context(user_id: str) -> str:
    """Build a structured context string from all available trader data."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Trades summary
    trades = conn.execute(
        "SELECT * FROM trades WHERE user_id = ? AND status = 'CLOSED' ORDER BY entry_time DESC",
        (user_id,),
    ).fetchall()

    if not trades:
        conn.close()
        return "No trade data available for this user."

    trades = [dict(r) for r in trades]
    total = len(trades)
    wins = sum(1 for t in trades if (t.get("pnl", 0) or 0) > 0)
    total_pnl = sum(t.get("pnl", 0) or 0 for t in trades)
    win_rate = round(wins / total * 100, 1) if total else 0

    # Symbol breakdown
    by_sym = {}
    for t in trades:
        sym = t.get("symbol", "?")
        by_sym.setdefault(sym, {"count": 0, "pnl": 0, "wins": 0})
        by_sym[sym]["count"] += 1
        by_sym[sym]["pnl"] += t.get("pnl", 0) or 0
        if (t.get("pnl", 0) or 0) > 0:
            by_sym[sym]["wins"] += 1

    sym_lines = []
    for sym, d in sorted(by_sym.items(), key=lambda x: x[1]["pnl"], reverse=True):
        wr = round(d["wins"] / d["count"] * 100, 1) if d["count"] else 0
        sym_lines.append(f"  {sym}: {d['count']} trades, {_fmt_inr(d['pnl'])} P&L, {wr}% win rate")

    # Recent trades (last 10)
    recent_lines = []
    for t in trades[:10]:
        pnl = t.get("pnl", 0) or 0
        recent_lines.append(
            f"  {t.get('entry_time', '?')[:16]} | {t.get('symbol', '?')} {t.get('direction', '?')} | "
            f"{_fmt_inr(pnl)} | qty={t.get('quantity', 0)}"
        )

    # Behavioral insights
    insights_row = conn.execute(
        "SELECT insights_json FROM behavioral_insights WHERE user_id = ? ORDER BY generated_at DESC LIMIT 1",
        (user_id,),
    ).fetchone()

    insights_text = ""
    if insights_row:
        try:
            insights = json.loads(insights_row["insights_json"])
            findings = insights.get("all_findings", [])
            if findings:
                insights_text = "\n\nBEHAVIORAL FINDINGS:\n" + "\n".join(f"- {f}" for f in findings)
        except Exception:
            pass

    # Pattern win rates
    patterns_text = ""
    try:
        from pattern_backtest import PATTERN_WIN_RATES
        pat_trades = [t for t in trades if t.get("detected_pattern")]
        if pat_trades:
            pat_lines = []
            by_pat = {}
            for t in pat_trades:
                p = t["detected_pattern"]
                by_pat.setdefault(p, {"count": 0, "wins": 0})
                by_pat[p]["count"] += 1
                if (t.get("pnl", 0) or 0) > 0:
                    by_pat[p]["wins"] += 1
            for p, d in by_pat.items():
                nse_wr = PATTERN_WIN_RATES.get(p, {}).get("win_rate", "?")
                your_wr = round(d["wins"] / d["count"] * 100, 1) if d["count"] else 0
                pat_lines.append(f"  {p}: your {your_wr}% vs NSE {nse_wr}% ({d['count']} trades)")
            if pat_lines:
                patterns_text = "\n\nCANDLESTICK PATTERN PERFORMANCE:\n" + "\n".join(pat_lines)
    except Exception:
        pass

    conn.close()

    # Date range
    first_trade = trades[-1].get("entry_time", "?")[:10]
    last_trade = trades[0].get("entry_time", "?")[:10]

    context = f"""TRADER PROFILE:
Total trades: {total}
Win rate: {win_rate}%
Total P&L: {_fmt_inr(total_pnl)}
Date range: {first_trade} to {last_trade}
Best trade: {_fmt_inr(max(t.get('pnl', 0) or 0 for t in trades))}
Worst trade: {_fmt_inr(min(t.get('pnl', 0) or 0 for t in trades))}

SYMBOL BREAKDOWN:
{chr(10).join(sym_lines)}

RECENT TRADES (last 10):
{chr(10).join(recent_lines)}
{insights_text}
{patterns_text}"""

    return context


# ---------------------------------------------------------------------------
# AI Chat — Anthropic Claude (primary) / Groq (fallback)
# ---------------------------------------------------------------------------

def ask_claude(user_id: str, question: str, conversation_history: List[Dict] = None) -> dict:
    """
    Send question to Claude API with full trader context.
    Falls back to Groq if Anthropic key unavailable.
    Returns {response: str, cited_data: [], model: str}.
    """
    context = build_context(user_id)

    messages = []
    if conversation_history:
        for msg in conversation_history[-10:]:  # Keep last 10 messages
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role in ("user", "assistant") and content:
                messages.append({"role": role, "content": content})

    messages.append({"role": "user", "content": question})

    # Try Anthropic Claude first
    anthropic_key = os.getenv("ANTHROPIC_KEY", "")
    if anthropic_key:
        try:
            return _call_anthropic(context, messages, anthropic_key)
        except Exception as e:
            print(f"[Chat] Anthropic API failed: {e}")

    # Fallback to Groq
    groq_key = os.getenv("GROQ_KEY", "")
    if groq_key:
        try:
            return _call_groq(context, messages, groq_key)
        except Exception as e:
            print(f"[Chat] Groq API failed: {e}")

    # Fallback to Gemini
    gemini_key = os.getenv("GEMINI_KEY_1", "")
    if gemini_key:
        try:
            return _call_gemini(context, messages, gemini_key)
        except Exception as e:
            print(f"[Chat] Gemini API failed: {e}")

    # No API available — return data-only response
    return _offline_response(user_id, question, context)


def _call_anthropic(context: str, messages: List[Dict], api_key: str) -> dict:
    """Call Anthropic Claude API."""
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    full_system = f"{SYSTEM_PROMPT}\n\nTRADER DATA:\n{context}"

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=full_system,
        messages=messages,
    )

    text = response.content[0].text if response.content else "No response generated."
    return {"response": text, "cited_data": [], "model": "claude-sonnet"}


def _call_groq(context: str, messages: List[Dict], api_key: str) -> dict:
    """Call Groq API as fallback."""
    from groq import Groq

    client = Groq(api_key=api_key)
    full_messages = [{"role": "system", "content": f"{SYSTEM_PROMPT}\n\nTRADER DATA:\n{context}"}]
    full_messages.extend(messages)

    response = client.chat.completions.create(
        model="llama3-8b-8192",
        messages=full_messages,
        max_tokens=1024,
        temperature=0.3,
    )

    text = response.choices[0].message.content if response.choices else "No response generated."
    return {"response": text, "cited_data": [], "model": "groq-llama3"}


def _call_gemini(context: str, messages: List[Dict], api_key: str) -> dict:
    """Call Gemini API as second fallback."""
    import google.generativeai as genai

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")

    prompt = f"{SYSTEM_PROMPT}\n\nTRADER DATA:\n{context}\n\nUser question: {messages[-1]['content']}"
    response = model.generate_content(prompt)

    text = response.text if response else "No response generated."
    return {"response": text, "cited_data": [], "model": "gemini-flash"}


def _offline_response(user_id: str, question: str, context: str) -> dict:
    """Generate a data-only response when no API keys are available."""
    q = question.lower()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    trades = conn.execute(
        "SELECT * FROM trades WHERE user_id = ? AND status = 'CLOSED'", (user_id,)
    ).fetchall()
    conn.close()

    if not trades:
        return {
            "response": "No trade data available. Load demo data or sync your Zerodha account first.",
            "cited_data": [],
            "model": "offline",
        }

    trades = [dict(r) for r in trades]
    total = len(trades)
    wins = sum(1 for t in trades if (t.get("pnl", 0) or 0) > 0)
    total_pnl = sum(t.get("pnl", 0) or 0 for t in trades)
    win_rate = round(wins / total * 100, 1)

    # Try to answer common questions from data
    if "win rate" in q or "win%" in q:
        response = f"Your overall win rate is {win_rate}% across {total} trades."
    elif "best" in q and ("symbol" in q or "stock" in q):
        by_sym = {}
        for t in trades:
            sym = t.get("symbol", "?")
            by_sym.setdefault(sym, 0)
            by_sym[sym] += t.get("pnl", 0) or 0
        best = max(by_sym.items(), key=lambda x: x[1])
        response = f"Your best performing symbol is {best[0]} with {_fmt_inr(best[1])} total P&L."
    elif "worst" in q and ("symbol" in q or "stock" in q):
        by_sym = {}
        for t in trades:
            sym = t.get("symbol", "?")
            by_sym.setdefault(sym, 0)
            by_sym[sym] += t.get("pnl", 0) or 0
        worst = min(by_sym.items(), key=lambda x: x[1])
        response = f"Your lowest performing symbol is {worst[0]} with {_fmt_inr(worst[1])} total P&L."
    elif "total" in q and "pnl" in q or "total" in q and "profit" in q:
        response = f"Your total P&L across {total} trades is {_fmt_inr(total_pnl)}."
    elif "how many" in q and "trade" in q:
        response = f"You have {total} completed trades in your history."
    else:
        response = (
            f"Your trading history: {total} trades, {win_rate}% win rate, "
            f"{_fmt_inr(total_pnl)} total P&L. "
            f"For detailed AI analysis, add an ANTHROPIC_KEY or GROQ_KEY to your env file."
        )

    return {"response": response, "cited_data": [], "model": "offline"}
