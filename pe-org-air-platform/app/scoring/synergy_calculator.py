from __future__ import annotations
from typing import Dict

def compute_synergy(dim_scores: Dict[str, float], threshold: float = 60.0) -> float:
    """
    Phase-1: simple synergy rules
    Phase-4: load rules from synergy_config
    """
    bonus = 0.0
    if dim_scores.get("technology_stack", 0.0) >= threshold and dim_scores.get("data_infrastructure", 0.0) >= threshold:
        bonus += 3.0
    if dim_scores.get("leadership_vision", 0.0) >= threshold and dim_scores.get("use_case_portfolio", 0.0) >= threshold:
        bonus += 2.5
    if dim_scores.get("ai_governance", 0.0) >= threshold and dim_scores.get("culture_change", 0.0) >= threshold:
        bonus += 3.0
    if dim_scores.get("talent_skills", 0.0) >= threshold and dim_scores.get("use_case_portfolio", 0.0) >= threshold:
        bonus += 2.5
    return float(bonus)