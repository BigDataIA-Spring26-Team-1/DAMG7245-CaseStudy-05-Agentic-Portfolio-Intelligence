from __future__ import annotations
from typing import Dict
from app.scoring.utils import weighted_mean, coefficient_of_variation, clamp

def compute_vr(
    dimension_scores: Dict[str, float],
    weights: Dict[str, float],
    cv_penalty_strength: float = 8.0,
) -> float:
    """
    - default dimension score = 50 if missing
    - weighted mean + imbalance penalty via CV
    """
    dims = list(weights.keys())
    scores = [float(dimension_scores.get(d, 50.0)) for d in dims]
    w = [float(weights.get(d, 0.0)) for d in dims]

    base = weighted_mean(scores, w) if sum(w) > 0 else 50.0
    cv = coefficient_of_variation(scores, [1.0] * len(scores))
    penalty = clamp(cv * float(cv_penalty_strength), 0.0, 15.0)

    return clamp(base - penalty, 0.0, 100.0)