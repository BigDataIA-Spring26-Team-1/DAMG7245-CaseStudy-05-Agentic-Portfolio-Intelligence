from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class DimensionBreakdown(BaseModel):
    dimension: str
    raw_score: float
    sector_weight: float
    weighted_score: float
    confidence: float
    evidence_count: int


class SynergyDetail(BaseModel):
    dim_a: str
    dim_b: str
    type: str
    threshold: float
    magnitude: float
    activated: bool
    reason: str


class TalentPenaltyDetail(BaseModel):
    sample_size: int
    min_sample_met: bool
    hhi_value: float
    penalty_factor: float
    function_counts: Dict[str, int]


class SEMResult(BaseModel):
    lower: Optional[float] = None
    upper: Optional[float] = None
    standard_error: Optional[float] = None
    method_used: Optional[str] = None
    fit: Optional[Dict[str, Any]] = None


class OrgAIRScoreOut(BaseModel):
    company_id: str
    assessment_id: Optional[str] = None
    scoring_run_id: Optional[str] = None

    vr_score: float
    synergy_bonus: float
    talent_penalty: float

    sem_lower: Optional[float] = None
    sem_upper: Optional[float] = None

    composite_score: float
    score_band: str

    dimension_breakdown: List[DimensionBreakdown] = Field(default_factory=list)
    synergy_hits: List[SynergyDetail] = Field(default_factory=list)
    talent_penalty_detail: Optional[TalentPenaltyDetail] = None
    sem: Optional[SEMResult] = None

    scored_at: Optional[datetime] = None
