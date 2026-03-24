from __future__ import annotations
from app.scoring.utils import clamp

def compute_hr(hr_base: float, position_factor: float) -> float:
    """
    PDF spec: HR = HR_base * (1 + 0.15 * PositionFactor)
    """
    hr = float(hr_base) * (1.0 + 0.15 * float(position_factor))
    return clamp(hr, 0.0, 100.0)