"""
StressLens Circuit Breaker
Checks NSE announcements for critical trigger events that override scores.
Uses tight keyword patterns to avoid false positives on routine filings.
"""

import re
from typing import Dict, List, Optional
from data_fetcher import get_fetcher


# Each pattern requires ALL keywords to appear in the announcement subject/description.
# Using multi-word phrases to avoid matching routine compliance text.
CRITICAL_PATTERNS = [
    {"keywords": ["auditor", "resign"],        "label": "Auditor resignation detected"},
    {"keywords": ["auditor", "stepped down"],  "label": "Auditor resignation detected"},
    {"keywords": ["sebi order"],               "label": "SEBI order/action detected"},
    {"keywords": ["sebi", "penalty"],          "label": "SEBI penalty detected"},
    {"keywords": ["sebi", "show cause"],       "label": "SEBI show cause notice detected"},
    {"keywords": ["sebi", "adjudicat"],        "label": "SEBI adjudication detected"},
    {"keywords": ["alleged fraud"],            "label": "Fraud allegation detected"},
    {"keywords": ["fraud", "investigation"],   "label": "Fraud investigation detected"},
]

HIGH_PATTERNS = [
    {"keywords": ["cfo", "resign"],            "label": "CFO resignation detected"},
    {"keywords": ["ceo", "resign"],            "label": "CEO resignation detected"},
    {"keywords": ["chief financial", "resign"],"label": "CFO resignation detected"},
    {"keywords": ["chief executive", "resign"],"label": "CEO resignation detected"},
    {"keywords": ["credit rating", "downgrad"],"label": "Credit rating downgrade detected"},
    {"keywords": ["rating", "downgrad"],       "label": "Credit rating downgrade detected"},
    {"keywords": ["pledge", "increas"],        "label": "Pledge increase detected"},
    {"keywords": ["pledge", "invoca"],         "label": "Pledge invocation detected"},
    {"keywords": ["loan default"],             "label": "Loan default detected"},
    {"keywords": ["default", "repayment"],     "label": "Repayment default detected"},
    {"keywords": ["default", "obligation"],    "label": "Payment default detected"},
]


def check_announcements(symbol: str) -> Dict:
    """
    Check NSE announcements for circuit breaker triggers.
    Each unique alert label only appears once (deduplication).
    Only checks the subject field to avoid false positives from boilerplate body text.
    """
    fetcher = get_fetcher()
    announcements = fetcher.fetch_nse_announcements(symbol)

    seen_labels = set()
    triggers = []
    max_severity = None

    for ann in announcements:
        # Only use subject/desc for matching, NOT the full attachment text
        subject = str(ann.get("desc", "")).lower()

        for pattern in CRITICAL_PATTERNS:
            label = pattern["label"]
            if label not in seen_labels and all(kw in subject for kw in pattern["keywords"]):
                seen_labels.add(label)
                triggers.append({
                    "severity": "CRITICAL",
                    "label": label,
                    "source": ann.get("desc", "")[:100],
                })
                max_severity = "CRITICAL"

        for pattern in HIGH_PATTERNS:
            label = pattern["label"]
            if label not in seen_labels and all(kw in subject for kw in pattern["keywords"]):
                seen_labels.add(label)
                triggers.append({
                    "severity": "HIGH",
                    "label": label,
                    "source": ann.get("desc", "")[:100],
                })
                if max_severity != "CRITICAL":
                    max_severity = "HIGH"

    return {
        "triggers": triggers,
        "max_severity": max_severity,
        "trigger_count": len(triggers),
    }


def apply_circuit_breaker(score: int, symbol: str) -> Dict:
    """
    Apply circuit breaker logic to the stress score.
    CRITICAL triggers: minimum score 85
    HIGH triggers: add 15 to score
    No triggers: score unchanged
    """
    check_result = check_announcements(symbol)
    original_score = score
    adjusted_score = score

    if check_result["max_severity"] == "CRITICAL":
        adjusted_score = max(score, 85)
    elif check_result["max_severity"] == "HIGH":
        adjusted_score = min(score + 15, 100)

    return {
        "original_score": original_score,
        "adjusted_score": adjusted_score,
        "circuit_breakers": check_result["triggers"],
        "max_severity": check_result["max_severity"],
        "was_adjusted": adjusted_score != original_score,
    }
