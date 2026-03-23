"""
StressLens Weight Manager
Dynamically adjusts weights between Gemini and Groq based on accuracy.
Persists weights to data/weights.json.
"""

import json
import os
from typing import Dict

WEIGHTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "weights.json")

DEFAULT_WEIGHTS = {"gemini": 0.5, "groq": 0.5}


def get_weights() -> Dict[str, float]:
    """Load weights from file, or return defaults."""
    try:
        if os.path.exists(WEIGHTS_FILE):
            with open(WEIGHTS_FILE, "r") as f:
                weights = json.load(f)
                if "gemini" in weights and "groq" in weights:
                    return weights
    except Exception:
        pass
    # Initialize file with defaults
    save_weights(DEFAULT_WEIGHTS)
    return DEFAULT_WEIGHTS.copy()


def save_weights(weights: Dict[str, float]):
    """Save weights to file."""
    os.makedirs(os.path.dirname(WEIGHTS_FILE), exist_ok=True)
    with open(WEIGHTS_FILE, "w") as f:
        json.dump(weights, f, indent=2)


def update_weights(gemini_correct: bool, groq_correct: bool):
    """
    Update weights based on which model was correct.
    Shift by max 0.05 per update.
    Keep both between 0.2 and 0.8.
    Normalize to sum to 1.0.
    """
    weights = get_weights()

    shift = 0.05

    if gemini_correct and not groq_correct:
        weights["gemini"] = min(weights["gemini"] + shift, 0.8)
        weights["groq"] = max(weights["groq"] - shift, 0.2)
    elif groq_correct and not gemini_correct:
        weights["groq"] = min(weights["groq"] + shift, 0.8)
        weights["gemini"] = max(weights["gemini"] - shift, 0.2)
    # If both correct or both wrong, no change

    # Normalize
    total = weights["gemini"] + weights["groq"]
    weights["gemini"] = round(weights["gemini"] / total, 4)
    weights["groq"] = round(weights["groq"] / total, 4)

    save_weights(weights)
    return weights


def apply_weights(gemini_confidence: float, groq_confidence: float) -> float:
    """Apply dynamic weights to get weighted confidence score."""
    weights = get_weights()
    return round(
        gemini_confidence * weights["gemini"] + groq_confidence * weights["groq"],
        2,
    )
