from __future__ import annotations
from app.scoring.utils import clamp

def compute_org_air(
    vr: float,
    hr: float,
    synergy: float,
    alpha: float = 0.6,
    beta: float = 0.3,
) -> float:
    score = (alpha * float(vr) + (1.0 - alpha) * float(hr)) + beta * float(synergy)
    return clamp(score, 0.0, 100.0)