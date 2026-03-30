"""
StressLens LLM Analyzer
Uses Gemini and Groq for AI-powered forensic analysis.
Rotates between multiple API keys. Cross-verifies results.
"""

import os
import json
import time
from typing import Dict, Optional
from dotenv import load_dotenv


def _load_env():
    """
    Load .env / env file. Tries multiple paths and filenames.
    Falls back to manually parsing the file if load_dotenv fails.
    """
    stresslens_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(stresslens_dir)

    candidates = [
        os.path.join(project_root, ".env"),
        os.path.join(project_root, "env"),
        os.path.join(stresslens_dir, ".env"),
        os.path.join(stresslens_dir, "env"),
    ]

    for path in candidates:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            print(f"[LLM] Found env file: {path} ({os.path.getsize(path)} bytes)")

            # Method 1: python-dotenv
            load_dotenv(path, override=True)

            # Verify it worked
            if os.getenv("GEMINI_KEY_1"):
                print(f"[LLM] load_dotenv() loaded keys successfully")
                return True

            # Method 2: manual parse as fallback
            print(f"[LLM] load_dotenv() did not set keys, trying manual parse...")
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if "=" in line and not line.startswith("#"):
                            key, _, val = line.partition("=")
                            os.environ[key.strip()] = val.strip()
            except Exception as e:
                print(f"[LLM] Manual parse failed: {e}")
                continue

            if os.getenv("GEMINI_KEY_1"):
                print(f"[LLM] Manual parse loaded keys successfully")
                return True

    print(f"[LLM] WARNING: No env file with API keys found")
    print(f"[LLM] Searched: {candidates}")
    print(f"[LLM] Create a .env file with GEMINI_KEY_1, GEMINI_KEY_2, GEMINI_KEY_3, GROQ_KEY")
    return False


_load_env()

# Load keys
GEMINI_KEYS = [
    k.strip() for k in [
        os.getenv("GEMINI_KEY_1", ""),
        os.getenv("GEMINI_KEY_2", ""),
        os.getenv("GEMINI_KEY_3", ""),
    ] if k.strip()
]

GROQ_KEY = (os.getenv("GROQ_KEY", "") or "").strip()
ANTHROPIC_KEY = (os.getenv("ANTHROPIC_KEY", "") or "").strip()

# Print key status
for kn in ["GEMINI_KEY_1", "GEMINI_KEY_2", "GEMINI_KEY_3", "GROQ_KEY", "ANTHROPIC_KEY"]:
    kv = os.getenv(kn, "")
    if kv and kv.strip():
        print(f"[LLM] OK {kn} = {kv[:8]}...{kv[-4:]}")
    else:
        print(f"[LLM] MISSING {kn}")

if GEMINI_KEYS:
    print(f"[LLM] {len(GEMINI_KEYS)} Gemini key(s) ready")
else:
    print("[LLM] No Gemini keys -- will use rule-based fallback")

if GROQ_KEY:
    print("[LLM] Groq key ready")
else:
    print("[LLM] No Groq key -- will use rule-based fallback")

if ANTHROPIC_KEY:
    print("[LLM] Anthropic key ready (used by TradeMind chat)")
else:
    print("[LLM] No Anthropic key -- TradeMind chat will use Groq/Gemini fallback")


# Gemini model: gemini-2.0-flash-lite (free tier, separate quota from flash)
GEMINI_MODEL = "gemini-2.0-flash-lite"
# Groq model: llama-3.1-8b-instant (llama3-8b-8192 was decommissioned)
GROQ_MODEL = "llama-3.1-8b-instant"

ANALYSIS_PROMPT = """You are a forensic financial analyst specializing in Indian markets. Analyze this company's financial data and identify stress signals. Return ONLY a JSON object with these exact fields: {{"stress_detected": true/false, "confidence": 0-100, "flags": [list of specific concerns], "summary": "two paragraph plain english explanation", "severity": "LOW/MEDIUM/HIGH/CRITICAL"}}

Company: {company_name}
Financial Summary:
{financial_summary}

Return ONLY valid JSON. No markdown, no explanation outside JSON."""

_gemini_key_index = 0


def _get_next_gemini_key() -> Optional[str]:
    global _gemini_key_index, GEMINI_KEYS
    if not GEMINI_KEYS:
        # Retry loading in case keys were added after startup
        GEMINI_KEYS.extend([
            k.strip() for k in [
                os.getenv("GEMINI_KEY_1", ""),
                os.getenv("GEMINI_KEY_2", ""),
                os.getenv("GEMINI_KEY_3", ""),
            ] if k.strip()
        ])
    if not GEMINI_KEYS:
        return None
    key = GEMINI_KEYS[_gemini_key_index % len(GEMINI_KEYS)]
    _gemini_key_index += 1
    return key


def _build_financial_summary(score_data: Dict) -> str:
    """Build a text summary from score data for LLM consumption."""
    lines = []
    signals = score_data.get("signals", {})

    lines.append(f"Total Stress Score: {score_data.get('stress_score', 'N/A')}/100")
    lines.append(f"Risk Level: {score_data.get('risk_level', 'N/A')}")
    lines.append("")

    if "beneish" in signals:
        b = signals["beneish"]
        lines.append(f"Beneish M-Score: {b.get('m_score', 'N/A')} ({b.get('verdict', '')})")
        lines.append(f"  Score contribution: {b.get('score', 0)}/25")

    if "altman" in signals:
        a = signals["altman"]
        lines.append(f"Altman Z-Score: {a.get('z_score', 'N/A')} ({a.get('verdict', '')})")
        lines.append(f"  Score contribution: {a.get('score', 0)}/25")

    if "piotroski" in signals:
        p = signals["piotroski"]
        lines.append(f"Piotroski F-Score: {p.get('f_score', 'N/A')}/9 ({p.get('verdict', '')})")
        lines.append(f"  Score contribution: {p.get('score', 0)}/20")

    if "pledge" in signals:
        pl = signals["pledge"]
        lines.append(f"Promoter Pledge: {pl.get('current_pct', 0)}% (change: {pl.get('quarter_change', 0)}%)")
        lines.append(f"  Score contribution: {pl.get('score', 0)}/20")

    if "cashflow" in signals:
        cf = signals["cashflow"]
        lines.append(f"Cash Flow: Profit={cf.get('profit', 0)}, CFO={cf.get('cfo', 0)} ({cf.get('verdict', '')})")
        lines.append(f"  Score contribution: {cf.get('score', 0)}/10")

    return "\n".join(lines)


def _parse_llm_response(text: str) -> Dict:
    """Parse LLM response, extracting JSON from potential markdown."""
    text = text.strip()
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass
    return {
        "stress_detected": False,
        "confidence": 0,
        "flags": ["LLM response parsing failed"],
        "summary": "Unable to parse AI analysis response.",
        "severity": "LOW",
    }


def analyze_with_gemini(company_name: str, score_data: Dict) -> Dict:
    """
    Analyze using Google Gemini.
    Rotates through all keys on quota errors.
    """
    if not GEMINI_KEYS:
        return _fallback_analysis(company_name, score_data, "Gemini")

    financial_summary = _build_financial_summary(score_data)
    prompt = ANALYSIS_PROMPT.format(
        company_name=company_name,
        financial_summary=financial_summary,
    )

    # Try each key (rotate on quota errors)
    tried = 0
    while tried < len(GEMINI_KEYS):
        key = _get_next_gemini_key()
        if not key:
            break
        tried += 1
        try:
            import google.generativeai as genai
            genai.configure(api_key=key)
            model = genai.GenerativeModel(GEMINI_MODEL)
            response = model.generate_content(prompt)
            result = _parse_llm_response(response.text)
            result["model"] = GEMINI_MODEL
            print(f"[LLM] Gemini analysis OK (key {tried}/{len(GEMINI_KEYS)})")
            return result
        except Exception as e:
            err = str(e).lower()
            if "quota" in err or "exhausted" in err or "429" in err:
                print(f"[LLM] Gemini key {tried} quota exhausted, trying next...")
                continue
            else:
                print(f"[LLM] Gemini error: {str(e)[:150]}")
                break

    print(f"[LLM] All Gemini keys exhausted or failed, using fallback")
    return _fallback_analysis(company_name, score_data, "Gemini")


def analyze_with_groq(company_name: str, score_data: Dict) -> Dict:
    """Analyze using Groq (llama-3.1-8b-instant)."""
    global GROQ_KEY
    if not GROQ_KEY:
        GROQ_KEY = (os.getenv("GROQ_KEY", "") or "").strip()
    if not GROQ_KEY:
        return _fallback_analysis(company_name, score_data, "Groq")

    try:
        from groq import Groq

        client = Groq(api_key=GROQ_KEY)
        financial_summary = _build_financial_summary(score_data)
        prompt = ANALYSIS_PROMPT.format(
            company_name=company_name,
            financial_summary=financial_summary,
        )

        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=1024,
        )

        result = _parse_llm_response(response.choices[0].message.content)
        result["model"] = GROQ_MODEL
        print(f"[LLM] Groq analysis OK")
        return result

    except Exception as e:
        print(f"[LLM] Groq error: {str(e)[:150]}")
        return _fallback_analysis(company_name, score_data, "Groq")


def _fallback_analysis(company_name: str, score_data: Dict, model_name: str) -> Dict:
    """Rule-based fallback when LLM APIs are unavailable."""
    stress = score_data.get("stress_score", 0)
    risk = score_data.get("risk_level", "LOW")
    signals = score_data.get("signals", {})

    flags = []
    if signals.get("beneish", {}).get("score", 0) >= 15:
        flags.append("Beneish M-Score indicates possible earnings manipulation")
    if signals.get("altman", {}).get("score", 0) >= 15:
        flags.append("Altman Z-Score signals financial distress risk")
    if signals.get("piotroski", {}).get("f_score", 9) <= 3:
        flags.append("Weak Piotroski F-Score indicates deteriorating fundamentals")
    if signals.get("pledge", {}).get("current_pct", 0) > 50:
        flags.append(f"High promoter pledge at {signals['pledge']['current_pct']}%")
    if signals.get("cashflow", {}).get("score", 0) >= 7:
        flags.append("Significant cash flow divergence from reported profits")

    if not flags:
        flags.append("No major stress signals detected from quantitative analysis")

    severity = risk
    stress_detected = stress >= 40

    summary = (
        f"{company_name} shows a stress score of {stress}/100, placing it in the {risk} risk category. "
        f"The quantitative models have identified {len(flags)} concern(s) that warrant attention.\n\n"
        f"Key findings include: {'; '.join(flags)}. "
        f"Investors should monitor these indicators closely and cross-reference with "
        f"qualitative factors such as management commentary, industry trends, and regulatory developments."
    )

    return {
        "stress_detected": stress_detected,
        "confidence": min(stress + 20, 95) if stress_detected else max(80 - stress, 30),
        "flags": flags,
        "summary": summary,
        "severity": severity,
        "model": f"{model_name}_fallback",
    }


def cross_verify(gemini_result: Dict, groq_result: Dict) -> Dict:
    """Cross-verify results from both LLMs."""
    gem_stress = gemini_result.get("stress_detected", False)
    groq_stress = groq_result.get("stress_detected", False)
    gem_conf = gemini_result.get("confidence", 50)
    groq_conf = groq_result.get("confidence", 50)

    agreed = gem_stress == groq_stress

    if agreed:
        final_confidence = min((gem_conf + groq_conf) / 2 + 10, 100)
        uncertainty = False
    else:
        final_confidence = max(min(gem_conf, groq_conf) - 10, 0)
        uncertainty = True

    gem_flags = gemini_result.get("flags", [])
    groq_flags = groq_result.get("flags", [])
    combined = list(set(gem_flags + groq_flags))

    severity_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
    gem_sev = severity_order.get(gemini_result.get("severity", "LOW"), 0)
    groq_sev = severity_order.get(groq_result.get("severity", "LOW"), 0)
    final_severity = gemini_result.get("severity", "LOW") if gem_sev >= groq_sev else groq_result.get("severity", "LOW")

    gem_summary = gemini_result.get("summary", "")
    groq_summary = groq_result.get("summary", "")
    summary = gem_summary if len(gem_summary) >= len(groq_summary) else groq_summary

    return {
        "agreed": agreed,
        "final_confidence": round(final_confidence, 1),
        "combined_flags": combined,
        "uncertainty_flag": uncertainty,
        "final_severity": final_severity,
        "summary": summary,
        "gemini_confidence": gem_conf,
        "groq_confidence": groq_conf,
        "gemini_model": gemini_result.get("model", "unknown"),
        "groq_model": groq_result.get("model", "unknown"),
    }
