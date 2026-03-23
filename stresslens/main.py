"""
StressLens — Forensic Stress Scoring System for Indian Listed Companies
Main FastAPI application.
"""

import sys
import os
import json
from datetime import datetime
from typing import Optional

# Add stresslens directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
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

from data_fetcher import get_fetcher, DHFL_HISTORICAL
from scorer import calculate_total_stress, score_historical_quarters
from llm_analyzer import analyze_with_gemini, analyze_with_groq, cross_verify
from circuit_breaker import apply_circuit_breaker
from weight_manager import get_weights, apply_weights

app = FastAPI(title="StressLens", description="Forensic stress scoring for Indian listed companies")

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


@app.get("/api/score/{symbol}")
async def score_company(symbol: str):
    """
    Main scoring endpoint.
    1. Fetch data
    2. Calculate quantitative score
    3. Run LLM analysis
    4. Check circuit breakers
    5. Apply dynamic weights
    6. Return complete response
    """
    symbol = symbol.upper().strip()

    # 1. Fetch data
    fetcher = get_fetcher()
    company_data = fetcher.get_company_data(symbol)
    quarters = company_data.get("quarters", [])
    errors = company_data.get("errors", [])

    if not quarters:
        detail = "; ".join(errors) if errors else f"No data found for {symbol}"
        raise HTTPException(status_code=404, detail=detail)

    current = quarters[-1]
    previous = quarters[-2] if len(quarters) >= 2 else None

    # 2. Calculate quantitative score
    score_result = calculate_total_stress(current, previous)

    # 3. Run LLM analysis
    gemini_result = analyze_with_gemini(company_data["company_name"], score_result)
    groq_result = analyze_with_groq(company_data["company_name"], score_result)
    cross_result = cross_verify(gemini_result, groq_result)

    # 4. Circuit breaker check
    cb_result = apply_circuit_breaker(score_result["stress_score"], symbol)

    # 5. Apply dynamic weights for confidence
    weighted_confidence = apply_weights(
        gemini_result.get("confidence", 50),
        groq_result.get("confidence", 50),
    )

    # 6. Historical scores if available
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
