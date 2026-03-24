from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

from app.services.integration.scoring_client import ScoringClient


class Dimension(str, Enum):
    DATA_INFRASTRUCTURE = "data_infrastructure"
    AI_GOVERNANCE = "ai_governance"
    TECHNOLOGY_STACK = "technology_stack"
    TALENT = "talent"
    LEADERSHIP = "leadership"
    USE_CASE_PORTFOLIO = "use_case_portfolio"
    CULTURE = "culture"


class ScoreLevel(int, Enum):
    LEVEL_5 = 5
    LEVEL_4 = 4
    LEVEL_3 = 3
    LEVEL_2 = 2
    LEVEL_1 = 1

    @property
    def name_label(self) -> str:
        labels = {
            5: "Excellent",
            4: "Good",
            3: "Adequate",
            2: "Developing",
            1: "Nascent",
        }
        return labels[self.value]

    @property
    def score_range(self) -> Tuple[int, int]:
        ranges = {
            5: (80, 100),
            4: (60, 79),
            3: (40, 59),
            2: (20, 39),
            1: (0, 19),
        }
        return ranges[self.value]


@dataclass(frozen=True)
class DimensionScore:
    dimension: Dimension
    score: float
    level: ScoreLevel
    confidence_interval: Tuple[float, float]
    evidence_count: int
    last_updated: str


@dataclass(frozen=True)
class RubricCriteria:
    dimension: Dimension
    level: ScoreLevel
    criteria_text: str
    keywords: List[str]
    quantitative_thresholds: Dict[str, float]


@dataclass(frozen=True)
class CompanyAssessment:
    company_id: str
    assessment_date: str
    vr_score: float
    hr_score: Optional[float]
    synergy_score: float
    org_air_score: float
    confidence_interval: Tuple[float, float]
    dimension_scores: Dict[Dimension, DimensionScore]
    talent_concentration: float
    position_factor: float


class CS3Client:
    def __init__(self, scoring_client: Optional[ScoringClient] = None) -> None:
        self.scoring_client = scoring_client or ScoringClient()

    def get_assessment(self, company_id: str) -> CompanyAssessment:
        payload = self.scoring_client.get_assessment(company_id)
        dimension_scores: Dict[Dimension, DimensionScore] = {}

        for raw_dimension, value in payload.get("dimension_scores", {}).items():
            dimension = Dimension(raw_dimension)
            confidence_interval = tuple(value.get("confidence_interval", (0.0, 0.0)))
            dimension_scores[dimension] = DimensionScore(
                dimension=dimension,
                score=float(value.get("score", 0.0)),
                level=ScoreLevel(int(value.get("level", 1))),
                confidence_interval=(float(confidence_interval[0]), float(confidence_interval[1])),
                evidence_count=int(value.get("evidence_count", 0)),
                last_updated=str(payload.get("scored_at") or payload.get("assessment_date") or ""),
            )

        interval = tuple(payload.get("confidence_interval", (0.0, 0.0)))
        return CompanyAssessment(
            company_id=str(payload.get("company_id")),
            assessment_date=str(payload.get("assessment_date") or ""),
            vr_score=float(payload.get("vr_score", 0.0) or 0.0),
            hr_score=float(payload["hr_score"]) if isinstance(payload.get("hr_score"), (int, float)) else None,
            synergy_score=float(payload.get("synergy_score", 0.0) or 0.0),
            org_air_score=float(payload.get("org_air_score", 0.0) or 0.0),
            confidence_interval=(float(interval[0]), float(interval[1])),
            dimension_scores=dimension_scores,
            talent_concentration=float(payload.get("talent_concentration", 0.0) or 0.0),
            position_factor=float(payload.get("position_factor", 0.0) or 0.0),
        )

    def get_dimension_score(self, company_id: str, dimension: Dimension) -> DimensionScore:
        payload = self.scoring_client.get_dimension_score(company_id, dimension.value)
        interval = tuple(payload.get("confidence_interval", (0.0, 0.0)))
        return DimensionScore(
            dimension=dimension,
            score=float(payload.get("raw_score", 0.0) or 0.0),
            level=ScoreLevel(int(payload.get("level", 1))),
            confidence_interval=(float(interval[0]), float(interval[1])),
            evidence_count=int(payload.get("evidence_count", 0) or 0),
            last_updated=str(payload.get("scored_at") or ""),
        )

    def get_rubric(
        self,
        dimension: Dimension,
        level: Optional[ScoreLevel] = None,
    ) -> List[RubricCriteria]:
        payload = self.scoring_client.get_rubric(dimension.value, level=int(level.value) if level else None)
        return [
            RubricCriteria(
                dimension=dimension,
                level=ScoreLevel(int(item["level"])),
                criteria_text=str(item["criteria_text"]),
                keywords=list(item.get("keywords", [])),
                quantitative_thresholds=dict(item.get("quantitative_thresholds", {})),
            )
            for item in payload
        ]

    def close(self) -> None:
        return None
