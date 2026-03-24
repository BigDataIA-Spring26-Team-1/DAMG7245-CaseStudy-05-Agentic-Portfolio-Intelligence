from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class DimensionInput:
    dimension: str
    raw_score: float          # 0-100
    confidence: float         # 0-1
    evidence_count: int       # >=0


@dataclass(frozen=True)
class VRResult:
    vr_score: float                          # 0-100
    sector_name: str
    version: str
    dimension_breakdown: List[Dict[str, float]]  # explainability


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def fetch_dimension_inputs(cur, assessment_id: str) -> List[DimensionInput]:
    cur.execute(
        """
        SELECT dimension, score, confidence, evidence_count
        FROM dimension_scores
        WHERE assessment_id = %s
        """,
        (assessment_id,),
    )
    rows = cur.fetchall() or []
    out: List[DimensionInput] = []
    for dim, score, conf, ev in rows:
        out.append(
            DimensionInput(
                dimension=str(dim),
                raw_score=float(score) if score is not None else 0.0,
                confidence=float(conf) if conf is not None else 0.8,
                evidence_count=int(ev) if ev is not None else 0,
            )
        )
    return out


def compute_vr_score(
    dimension_inputs: List[DimensionInput],
    sector_weights: Dict[str, float],
    *,
    confidence_floor: float = 0.20,
) -> Tuple[float, List[Dict[str, float]]]:
    """
    VR = weighted average of dimension scores, where each dimension contribution is:
      contribution_i = raw_score_i * sector_weight_i * conf_i
    Then normalize by sum(sector_weight_i * conf_i) so final VR stays 0-100.

    confidence_floor prevents a dimension with confidence=0 from removing itself entirely.
    """
    numerator = 0.0
    denom = 0.0
    breakdown: List[Dict[str, float]] = []

    for d in dimension_inputs:
        w = float(sector_weights.get(d.dimension, 0.0))
        c = clamp(float(d.confidence), 0.0, 1.0)
        c_eff = max(c, confidence_floor)

        weighted_conf = w * c_eff
        weighted_score = clamp(d.raw_score, 0.0, 100.0) * weighted_conf

        numerator += weighted_score
        denom += weighted_conf

        breakdown.append(
            {
                "dimension": d.dimension,
                "raw_score": clamp(d.raw_score, 0.0, 100.0),
                "confidence": c,
                "confidence_used": c_eff,
                "sector_weight": w,
                "weighted_conf": weighted_conf,
                "weighted_score": weighted_score,
                "evidence_count": float(d.evidence_count),
            }
        )

    if denom <= 0.0:
        return 0.0, breakdown

    vr = numerator / denom
    vr = clamp(vr, 0.0, 100.0)
    return vr, breakdown
