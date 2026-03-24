from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CompanyCreate(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "Acme Corp",
                    "ticker": "ACME",
                    "position_factor": 0.0,
                }
            ]
        }
    )
    name: str = Field(..., min_length=1, max_length=255)
    ticker: str | None = Field(default=None, min_length=1, max_length=10)
    industry_id: UUID | None = None
    position_factor: float = Field(default=0.0, ge=-1.0, le=1.0)

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str | None):
        if v is None:
            return v
        if v != v.upper():
            raise ValueError("ticker must be uppercase")
        return v


class CompanyUpdate(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "string",
                    "ticker": "string",
                    "industry_id": None,
                    "position_factor": 0.0,
                }
            ]
        }
    )
    name: str | None = Field(default=None, min_length=1, max_length=255)
    ticker: str | None = Field(default=None, min_length=1, max_length=10)
    industry_id: UUID | None = None
    position_factor: float | None = Field(default=None, ge=-1.0, le=1.0)

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str | None):
        if v is None:
            return v
        if v != v.upper():
            raise ValueError("ticker must be uppercase")
        return v


class CompanyOut(BaseModel):
    id: UUID
    name: str
    ticker: str | None
    industry_id: UUID | None
    position_factor: float
    is_deleted: bool
    created_at: datetime
    updated_at: datetime | None


class IndustryOut(BaseModel):
    id: UUID
    name: str
    sector: str
    hr_base: float | None = None
    created_at: datetime | None = None
