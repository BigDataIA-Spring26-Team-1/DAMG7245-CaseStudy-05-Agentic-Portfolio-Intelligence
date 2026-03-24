from __future__ import annotations
from datetime import date, datetime, timezone
from enum import StrEnum
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, model_validator

class AssessmentType(StrEnum):
    screening = "screening"
    due_diligence = "due_diligence"
    quarterly = "quarterly"
    exit_prep = "exit_prep"
    cs3_auto = "cs3_auto"

class AssessmentStatus(StrEnum):
    draft = "draft"
    in_progress = "in_progress"
    submitted = "submitted"
    approved = "approved"
    superseded = "superseded"

class AssessmentCreate(BaseModel):
    model_config = ConfigDict(json_schema_extra={"examples": [{"company_id": None, "assessment_type": "screening", "assessment_date": "2026-02-03", "primary_assessor": "string", "secondary_assessor": "string", "vr_score": 100, "confidence_lower": 100, "confidence_upper": 100}]})
    company_id: UUID
    assessment_type: AssessmentType
    assessment_date: date = Field(default_factory=lambda: datetime.now(timezone.utc).date())
    primary_assessor: str | None = None
    secondary_assessor: str | None = None
    vr_score: float | None = Field(default=None, ge=0, le=100)
    confidence_lower: float | None = Field(default=None, ge=0, le=100)
    confidence_upper: float | None = Field(default=None, ge=0, le=100)

    @model_validator(mode="after")
    def validate_confidence_bounds(self):
        if (self.confidence_lower is not None and self.confidence_upper is not None and self.confidence_lower > self.confidence_upper):
            raise ValueError("confidence_lower must be <= confidence_upper")
        return self

class AssessmentUpdate(BaseModel):
    status: AssessmentStatus | None = None
    primary_assessor: str | None = None
    secondary_assessor: str | None = None
    vr_score: float | None = Field(default=None, ge=0, le=100)
    confidence_lower: float | None = Field(default=None, ge=0, le=100)
    confidence_upper: float | None = Field(default=None, ge=0, le=100)

    @model_validator(mode="after")
    def validate_confidence_bounds(self):
        if (self.confidence_lower is not None and self.confidence_upper is not None and self.confidence_lower > self.confidence_upper):
            raise ValueError("confidence_lower must be <= confidence_upper")
        return self

class AssessmentStatusUpdate(BaseModel):
    status: AssessmentStatus

class AssessmentOut(BaseModel):
    id: UUID
    company_id: UUID
    assessment_type: AssessmentType
    assessment_date: date
    status: AssessmentStatus
    primary_assessor: str | None
    secondary_assessor: str | None
    vr_score: float | None = None
    confidence_lower: float | None = None
    confidence_upper: float | None = None
    created_at: datetime | None = None
