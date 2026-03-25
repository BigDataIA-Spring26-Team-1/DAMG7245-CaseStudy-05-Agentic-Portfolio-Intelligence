from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional

import structlog

from app.services.integration.cs1_client import CS1Client
from app.services.integration.cs3_client import CS3Client

logger = structlog.get_logger()


@dataclass
class AssessmentSnapshot:
    company_id: str
    timestamp: datetime
    org_air: Decimal
    vr_score: Decimal
    hr_score: Decimal
    synergy_score: Decimal
    dimension_scores: Dict[str, Decimal]
    confidence_interval: tuple
    evidence_count: int
    assessor_id: str
    assessment_type: str  # screening | limited | full


@dataclass
class AssessmentTrend:
    company_id: str
    current_org_air: float
    entry_org_air: float
    delta_since_entry: float
    delta_30d: Optional[float]
    delta_90d: Optional[float]
    trend_direction: str
    snapshot_count: int


class AssessmentHistoryService:
    """
    Tracks historical assessments for trend analysis.
    Storage can begin in-memory and later be replaced with CS1-backed persistence.
    """

    def __init__(self, cs1_client: CS1Client, cs3_client: CS3Client) -> None:
        self.cs1 = cs1_client
        self.cs3 = cs3_client
        self._cache: Dict[str, List[AssessmentSnapshot]] = {}

    async def record_assessment(
        self,
        company_id: str,
        assessor_id: str,
        assessment_type: str = "full",
    ) -> AssessmentSnapshot:
        assessment = await self.cs3.get_assessment(company_id)

        snapshot = AssessmentSnapshot(
            company_id=company_id,
            timestamp=datetime.utcnow(),
            org_air=Decimal(str(assessment.org_air_score)),
            vr_score=Decimal(str(assessment.vr_score)),
            hr_score=Decimal(str(assessment.hr_score)),
            synergy_score=Decimal(str(assessment.synergy_score)),
            dimension_scores={
                getattr(dim, "value", str(dim)): Decimal(str(getattr(score, "score", score)))
                for dim, score in assessment.dimension_scores.items()
            },
            confidence_interval=assessment.confidence_interval,
            evidence_count=assessment.evidence_count,
            assessor_id=assessor_id,
            assessment_type=assessment_type,
        )

        await self._store_snapshot(snapshot)

        self._cache.setdefault(company_id, []).append(snapshot)

        logger.info(
            "assessment_recorded",
            company_id=company_id,
            org_air=float(snapshot.org_air),
            assessment_type=assessment_type,
        )
        return snapshot

    async def _store_snapshot(self, snapshot: AssessmentSnapshot) -> None:
        """
        Replace with real CS1/Snowflake persistence later.
        """
        logger.info(
            "assessment_snapshot_stored_placeholder",
            company_id=snapshot.company_id,
            timestamp=snapshot.timestamp.isoformat(),
        )

    async def get_history(self, company_id: str, days: int = 365) -> List[AssessmentSnapshot]:
        if company_id in self._cache:
            cutoff = datetime.utcnow() - timedelta(days=days)
            return [s for s in self._cache[company_id] if s.timestamp >= cutoff]

        return []

    async def calculate_trend(self, company_id: str) -> AssessmentTrend:
        history = await self.get_history(company_id, days=365)

        if not history:
            current = await self.cs3.get_assessment(company_id)
            return AssessmentTrend(
                company_id=company_id,
                current_org_air=float(current.org_air_score),
                entry_org_air=float(current.org_air_score),
                delta_since_entry=0.0,
                delta_30d=None,
                delta_90d=None,
                trend_direction="stable",
                snapshot_count=0,
            )

        history.sort(key=lambda s: s.timestamp)

        current = float(history[-1].org_air)
        entry = float(history[0].org_air)

        now = datetime.utcnow()
        delta_30d = None
        delta_90d = None

        for snapshot in reversed(history):
            age_days = (now - snapshot.timestamp).days
            if age_days >= 30 and delta_30d is None:
                delta_30d = current - float(snapshot.org_air)
            if age_days >= 90 and delta_90d is None:
                delta_90d = current - float(snapshot.org_air)
                break

        delta = current - entry
        if delta > 5:
            direction = "improving"
        elif delta < -5:
            direction = "declining"
        else:
            direction = "stable"

        return AssessmentTrend(
            company_id=company_id,
            current_org_air=current,
            entry_org_air=entry,
            delta_since_entry=round(delta, 1),
            delta_30d=round(delta_30d, 1) if delta_30d is not None else None,
            delta_90d=round(delta_90d, 1) if delta_90d is not None else None,
            trend_direction=direction,
            snapshot_count=len(history),
        )


def create_history_service(cs1: CS1Client, cs3: CS3Client) -> AssessmentHistoryService:
    return AssessmentHistoryService(cs1, cs3)