from __future__ import annotations
from datetime import datetime
from enum import StrEnum
from uuid import UUID
from pydantic import BaseModel, Field, model_validator

class DimensionName(StrEnum):
    data_infrastructure = "data_infrastructure"
    ai_governance = "ai_governance"
    technology_stack = "technology_stack"
    talent_skills = "talent_skills"
    leadership_vision = "leadership_vision"
    use_case_portfolio = "use_case_portfolio"
    culture_change = "culture_change"

DEFAULT_DIMENSION_WEIGHTS: dict[DimensionName, float] = {
    DimensionName.data_infrastructure: 0.25,
    DimensionName.ai_governance: 0.20,
    DimensionName.technology_stack: 0.15,
    DimensionName.talent_skills: 0.15,
    DimensionName.leadership_vision: 0.10,
    DimensionName.use_case_portfolio: 0.10,
    DimensionName.culture_change: 0.05,
}


class DimensionScoreCreate(BaseModel):
    assessment_id: UUID
    dimension: DimensionName
    score: float = Field(ge=0, le=100)
    weight: float | None = Field(default=None, ge=0, le=1)
    confidence: float = Field(default=0.8, ge=0, le=1)
    evidence_count: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def apply_default_weight(self) -> DimensionScoreCreate:
        if self.weight is None:
            self.weight = DEFAULT_DIMENSION_WEIGHTS[self.dimension]
        return self

class DimensionScoreUpdate(BaseModel):
    score: float | None = Field(default=None, ge=0, le=100)
    weight: float | None = Field(default=None, ge=0, le=1)
    confidence: float | None = Field(default=None, ge=0, le=1)
    evidence_count: int | None = Field(default=None, ge=0)

class DimensionScoreOut(BaseModel):
    id: UUID
    assessment_id: UUID
    dimension: DimensionName
    score: float
    weight: float | None = None
    confidence: float
    evidence_count: int
    created_at: datetime | None = None
