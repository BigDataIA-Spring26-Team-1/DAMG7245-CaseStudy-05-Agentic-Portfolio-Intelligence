from __future__ import annotations
 
from dataclasses import dataclass
from typing import Dict
 
 
DIMENSIONS = [
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent_skills",
    "leadership_vision",
    "use_case_portfolio",
    "culture_change",
]
 
 
@dataclass(frozen=True)
class SourceProfile:
    reliability: float
    dim_weights: Dict[str, float]
 
 
def _profile(reliability: float, **weights: float) -> SourceProfile:
    return SourceProfile(reliability=reliability, dim_weights=weights)
 
 
SOURCE_PROFILES: dict[str, SourceProfile] = {
    # CS2 4-signal categories
    "technology_hiring": _profile(
        0.85,
        data_infrastructure=0.10,
        technology_stack=0.20,
        talent_skills=0.70,
        culture_change=0.10,
    ),
    "innovation_activity": _profile(
        0.80,
        data_infrastructure=0.20,
        technology_stack=0.50,
        use_case_portfolio=0.30,
    ),
    "digital_presence": _profile(
        0.70,
        data_infrastructure=0.60,
        technology_stack=0.40,
    ),
    "leadership_signals": _profile(
        0.80,
        ai_governance=0.25,
        leadership_vision=0.60,
        culture_change=0.15,
    ),
    # SEC section mapping
    "sec_item_1": _profile(
        0.90,
        technology_stack=0.30,
        use_case_portfolio=0.70,
    ),
    "sec_item_1a": _profile(
        0.90,
        data_infrastructure=0.20,
        ai_governance=0.80,
    ),
    "sec_item_7": _profile(
        0.90,
        data_infrastructure=0.20,
        leadership_vision=0.50,
        use_case_portfolio=0.30,
    ),
    # CS3 additional collectors
    "glassdoor_reviews": _profile(
        0.75,
        talent_skills=0.10,
        leadership_vision=0.10,
        culture_change=0.80,
    ),
    "board_composition": _profile(
        0.90,
        ai_governance=0.70,
        leadership_vision=0.30,
    ),
    # Backward-compatible aliases used by existing collectors
    "jobs": _profile(
        0.85,
        data_infrastructure=0.10,
        technology_stack=0.20,
        talent_skills=0.70,
        culture_change=0.10,
    ),
    "patents": _profile(
        0.80,
        data_infrastructure=0.20,
        technology_stack=0.50,
        use_case_portfolio=0.30,
    ),
    "tech": _profile(
        0.70,
        data_infrastructure=0.60,
        technology_stack=0.40,
    ),
    "news": _profile(
        0.80,
        ai_governance=0.25,
        leadership_vision=0.60,
        culture_change=0.15,
    ),
    "10k": _profile(
        0.90,
        data_infrastructure=0.20,
        ai_governance=0.20,
        technology_stack=0.15,
        leadership_vision=0.20,
        use_case_portfolio=0.25,
    ),
}
 
 
def normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    s = float(sum(max(0.0, v) for v in weights.values()))
    if s <= 0:
        return {k: 0.0 for k in weights}
    return {k: float(max(0.0, v)) / s for k, v in weights.items()}
 
 