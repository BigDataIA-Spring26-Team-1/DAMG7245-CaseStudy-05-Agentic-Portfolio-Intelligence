from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CompositeResult:
    composite_score: float
    score_band: str


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _assign_score_band(score: float) -> str:
    """
    Score bands defined in CS3 checklist:
    0-20   Nascent
    21-40  Developing
    41-60  Progressing
    61-80  Advanced
    81-100 Leading
    """
    if score <= 20:
        return "Nascent"
    if score <= 40:
        return "Developing"
    if score <= 60:
        return "Progressing"
    if score <= 80:
        return "Advanced"
    return "Leading"


def compute_composite(
    *,
    vr_score: float,
    hr_score: Optional[float] = None,
    synergy_score: Optional[float] = None,
    alpha: float = 0.60,
    beta: float = 0.12,
    # Legacy parameters kept for backward compatibility.
    synergy_bonus: Optional[float] = None,
    penalty_factor: Optional[float] = None,
) -> CompositeResult:
    """
    Preferred formula:
      Org-AI-R = (1 - beta) * (alpha * VR + (1 - alpha) * HR) + beta * Synergy

    Legacy fallback (used only if HR/Synergy are not provided):
      Org-AI-R = (VR + synergy_bonus) * penalty_factor
    """
    vr = _clamp(float(vr_score), 0.0, 100.0)

    if hr_score is not None and synergy_score is not None:
        hr = _clamp(float(hr_score), 0.0, 100.0)
        syn = _clamp(float(synergy_score), 0.0, 100.0)
        a = _clamp(float(alpha), 0.0, 1.0)
        b = _clamp(float(beta), 0.0, 1.0)
        composite = (1.0 - b) * (a * vr + (1.0 - a) * hr) + b * syn
    else:
        sb = float(synergy_bonus or 0.0)
        pf = _clamp(float(1.0 if penalty_factor is None else penalty_factor), 0.0, 1.0)
        composite = (vr + sb) * pf

    composite = _clamp(composite, 0.0, 100.0)
    band = _assign_score_band(composite)
    return CompositeResult(
        composite_score=round(composite, 2),
        score_band=band,
    )

