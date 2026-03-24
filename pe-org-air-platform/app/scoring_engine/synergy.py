from __future__ import annotations
 
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
 
 
@dataclass(frozen=True)
class SynergyRule:
    dim_a: str
    dim_b: str
    synergy_type: str      # "positive" or "negative"
    threshold: float       # activation threshold
    magnitude: float       # points (+/-)
 
 
@dataclass(frozen=True)
class SynergyHit:
    dim_a: str
    dim_b: str
    synergy_type: str
    threshold: float
    magnitude: float
    activated: bool
    reason: str
 
 
@dataclass(frozen=True)
class SynergyResult:
    synergy_bonus: float                 # capped total bonus/drag
    cap: float
    hits: List[SynergyHit]
 
 
@dataclass(frozen=True)
class FormulaSynergyResult:
    synergy_score: float
    alignment: float
    timing_factor: float
    base_term: float
 
 
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))
 
 
def load_synergy_rules(cur, version: str = "v1.0") -> List[SynergyRule]:
    cur.execute(
        """
        SELECT dimension_a, dimension_b, synergy_type, threshold, magnitude
        FROM synergy_config
        WHERE version = %s
        """,
        (version,),
    )
    rows = cur.fetchall() or []
    rules: List[SynergyRule] = []
    for a, b, t, thr, mag in rows:
        rules.append(
            SynergyRule(
                dim_a=str(a),
                dim_b=str(b),
                synergy_type=str(t),
                threshold=float(thr),
                magnitude=float(mag),
            )
        )
    return rules
 
 
def compute_synergy(
    scores_by_dim: Dict[str, float],
    rules: List[SynergyRule],
    *,
    cap_abs: float = 15.0,
) -> SynergyResult:
    """
    Positive synergy: if BOTH dims >= threshold, add magnitude (+).
    Negative synergy (execution drag): if dim_a is strong but dim_b is weak, add magnitude (-).
      Rule interpretation for negative:
        - If dim_a >= threshold AND dim_b < threshold => apply drag (negative magnitude)
    Total synergy is capped to ±cap_abs.
    """
    hits: List[SynergyHit] = []
    total = 0.0
 
    for r in rules:
        a = float(scores_by_dim.get(r.dim_a, 0.0))
        b = float(scores_by_dim.get(r.dim_b, 0.0))
 
        if r.synergy_type == "positive":
            activated = (a >= r.threshold) and (b >= r.threshold)
            if activated:
                total += r.magnitude
                reason = f"both >= {r.threshold}"
            else:
                reason = f"needs both >= {r.threshold} (a={a:.1f}, b={b:.1f})"
 
        elif r.synergy_type == "negative":
            # execution risk drag: dim_a high, dim_b low
            activated = (a >= r.threshold) and (b < r.threshold)
            if activated:
                total += r.magnitude  # magnitude should be negative in config
                reason = f"a >= {r.threshold} and b < {r.threshold}"
            else:
                reason = f"needs a >= {r.threshold} & b < {r.threshold} (a={a:.1f}, b={b:.1f})"
 
        else:
            activated = False
            reason = f"unknown synergy_type={r.synergy_type}"
 
        hits.append(
            SynergyHit(
                dim_a=r.dim_a,
                dim_b=r.dim_b,
                synergy_type=r.synergy_type,
                threshold=r.threshold,
                magnitude=r.magnitude,
                activated=activated,
                reason=reason,
            )
        )
 
    capped = clamp(total, -cap_abs, cap_abs)
 
    return SynergyResult(
        synergy_bonus=capped,
        cap=cap_abs,
        hits=hits,
    )
 
 
def compute_formula_synergy(
    *,
    vr_score: float,
    hr_score: float,
    alignment: Optional[float] = None,
    timing_factor: float = 1.0,
) -> FormulaSynergyResult:
    """
    Formula-based synergy from the CS3 specification:
      Synergy = (VR * HR / 100) * Alignment * TimingFactor
    where TimingFactor is bounded to [0.8, 1.2].
    """
    vr = clamp(float(vr_score), 0.0, 100.0)
    hr = clamp(float(hr_score), 0.0, 100.0)
    align = 1.0 - abs(vr - hr) / 100.0 if alignment is None else float(alignment)
    align = clamp(align, 0.0, 1.0)
    timing = clamp(float(timing_factor), 0.8, 1.2)
    base_term = (vr * hr) / 100.0
    synergy = clamp(base_term * align * timing, 0.0, 100.0)
    return FormulaSynergyResult(
        synergy_score=synergy,
        alignment=align,
        timing_factor=timing,
        base_term=base_term,
    )
 
 