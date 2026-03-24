from __future__ import annotations
from app.scoring.utils import clamp

def compute_position_factor(market_share: float, leadership_index: float) -> float:
    """
    Spec: bounded [-1, 1]
    Phase-1: average then clamp.
    """
    raw = 0.5 * float(market_share) + 0.5 * float(leadership_index)
    return clamp(raw, -1.0, 1.0)