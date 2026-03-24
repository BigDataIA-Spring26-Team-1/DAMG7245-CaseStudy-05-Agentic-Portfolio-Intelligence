from __future__ import annotations
from typing import Dict
from app.scoring.utils import clamp, safe_div

def compute_hhi(function_counts: Dict[str, int]) -> float:
    total = sum(function_counts.values())
    if total <= 0:
        return 0.0
    shares = [v / total for v in function_counts.values()]
    return sum(s * s for s in shares)

def compute_talent_concentration(
    function_counts: Dict[str, int],
    min_sample_size: int = 25,
    hhi_mild: float = 0.40,
    hhi_severe: float = 0.70,
    penalty_mild: float = 0.92,
    penalty_severe: float = 0.80,
) -> dict:
    sample_size = sum(function_counts.values())
    min_met = sample_size >= int(min_sample_size)

    hhi = compute_hhi(function_counts)
    top1 = max(function_counts.values()) if sample_size > 0 else 0
    top1_ratio = safe_div(top1, sample_size, 0.0)

    penalty_factor = 1.0
    if min_met:
        if hhi >= hhi_severe:
            penalty_factor = float(penalty_severe)
        elif hhi >= hhi_mild:
            penalty_factor = float(penalty_mild)

    return {
        "sample_size": int(sample_size),
        "min_sample_met": bool(min_met),
        "hhi_value": float(hhi),
        "top1_ratio": float(clamp(top1_ratio, 0.0, 1.0)),
        "penalty_factor": float(clamp(penalty_factor, 0.0, 1.0)),
        "function_counts": dict(function_counts),
    }