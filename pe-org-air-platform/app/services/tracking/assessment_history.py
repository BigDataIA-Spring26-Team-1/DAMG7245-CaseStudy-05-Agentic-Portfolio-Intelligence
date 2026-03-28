from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import uuid4

from app.logging_utils import get_logger

from app.services import snowflake as snowflake_service
from app.services.integration.cs1_client import CS1Client
from app.services.integration.cs3_client import CS3Client

logger = get_logger(__name__)


def _quantize(value: Any) -> Decimal:
    return Decimal(str(value if value is not None else 0.0))


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _normalize_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


async def _maybe_call(func, *args, **kwargs):
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    return await asyncio.to_thread(func, *args, **kwargs)


def _parse_decimal_mapping(value: Any) -> Dict[str, Decimal]:
    if isinstance(value, dict):
        raw_mapping = value
    elif isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            parsed = {}
        raw_mapping = parsed if isinstance(parsed, dict) else {}
    else:
        raw_mapping = {}

    return {
        str(key): _quantize(item)
        for key, item in raw_mapping.items()
    }


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
    Tracks historical assessments for trend analysis using the platform store.
    """

    def __init__(self, cs1_client: CS1Client, cs3_client: CS3Client) -> None:
        self.cs1 = cs1_client
        self.cs3 = cs3_client
        self._cache: Dict[str, List[AssessmentSnapshot]] = {}
        self._table_ready = False

    def _ensure_history_table(self, cur: Any) -> None:
        if self._table_ready:
            return

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS assessment_history_snapshots (
                id STRING PRIMARY KEY,
                company_id STRING NOT NULL,
                snapshot_timestamp TIMESTAMP_NTZ NOT NULL,
                org_air NUMBER(5,2) NOT NULL,
                vr_score NUMBER(5,2),
                hr_score NUMBER(5,2),
                synergy_score NUMBER(5,2),
                dimension_scores_json VARIANT,
                confidence_lower NUMBER(5,2),
                confidence_upper NUMBER(5,2),
                evidence_count INT DEFAULT 0,
                assessor_id STRING,
                assessment_type STRING DEFAULT 'full',
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )
        self._table_ready = True

    def _row_to_snapshot(self, row: tuple[Any, ...]) -> AssessmentSnapshot:
        confidence_interval = (
            float(row[7]) if row[7] is not None else 0.0,
            float(row[8]) if row[8] is not None else 0.0,
        )
        return AssessmentSnapshot(
            company_id=str(row[0]),
            timestamp=_normalize_timestamp(row[1]),
            org_air=_quantize(row[2]),
            vr_score=_quantize(row[3]),
            hr_score=_quantize(row[4]),
            synergy_score=_quantize(row[5]),
            dimension_scores=_parse_decimal_mapping(row[6]),
            confidence_interval=confidence_interval,
            evidence_count=int(row[9] or 0),
            assessor_id=str(row[10] or ""),
            assessment_type=str(row[11] or "full"),
        )

    async def record_assessment(
        self,
        company_id: str,
        assessor_id: str,
        assessment_type: str = "full",
    ) -> AssessmentSnapshot:
        await _maybe_call(self.cs1.get_company, company_id)
        assessment = await _maybe_call(self.cs3.get_assessment, company_id)

        snapshot = AssessmentSnapshot(
            company_id=company_id,
            timestamp=_utc_now(),
            org_air=_quantize(assessment.org_air_score),
            vr_score=_quantize(assessment.vr_score),
            hr_score=_quantize(assessment.hr_score),
            synergy_score=_quantize(assessment.synergy_score),
            dimension_scores={
                getattr(dim, "value", str(dim)): _quantize(getattr(score, "score", score))
                for dim, score in assessment.dimension_scores.items()
            },
            confidence_interval=assessment.confidence_interval,
            evidence_count=int(getattr(assessment, "evidence_count", 0) or 0),
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
        payload = {
            key: float(value)
            for key, value in snapshot.dimension_scores.items()
        }
        conn = snowflake_service.get_snowflake_connection()
        cur = conn.cursor()
        try:
            self._ensure_history_table(cur)
            cur.execute(
                """
                INSERT INTO assessment_history_snapshots (
                    id,
                    company_id,
                    snapshot_timestamp,
                    org_air,
                    vr_score,
                    hr_score,
                    synergy_score,
                    dimension_scores_json,
                    confidence_lower,
                    confidence_upper,
                    evidence_count,
                    assessor_id,
                    assessment_type
                )
                SELECT
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    PARSE_JSON(%s),
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                """,
                (
                    str(uuid4()),
                    snapshot.company_id,
                    snapshot.timestamp,
                    float(snapshot.org_air),
                    float(snapshot.vr_score),
                    float(snapshot.hr_score),
                    float(snapshot.synergy_score),
                    json.dumps(payload),
                    float(snapshot.confidence_interval[0]) if snapshot.confidence_interval else None,
                    float(snapshot.confidence_interval[1]) if snapshot.confidence_interval else None,
                    snapshot.evidence_count,
                    snapshot.assessor_id,
                    snapshot.assessment_type,
                ),
            )
            try:
                conn.commit()
            except Exception:
                pass
        finally:
            cur.close()
            conn.close()

    async def get_history(self, company_id: str, days: int = 365) -> List[AssessmentSnapshot]:
        cutoff = _utc_now() - timedelta(days=days)
        conn = snowflake_service.get_snowflake_connection()
        cur = conn.cursor()
        try:
            self._ensure_history_table(cur)
            cur.execute(
                """
                SELECT
                    company_id,
                    snapshot_timestamp,
                    org_air,
                    vr_score,
                    hr_score,
                    synergy_score,
                    dimension_scores_json,
                    confidence_lower,
                    confidence_upper,
                    evidence_count,
                    assessor_id,
                    assessment_type
                FROM assessment_history_snapshots
                WHERE company_id = %s
                  AND snapshot_timestamp >= %s
                ORDER BY snapshot_timestamp ASC
                """,
                (company_id, cutoff),
            )
            rows = cur.fetchall()
        finally:
            cur.close()
            conn.close()

        history = [self._row_to_snapshot(row) for row in rows]
        self._cache[company_id] = history
        return history

    async def calculate_trend(self, company_id: str) -> AssessmentTrend:
        history = await self.get_history(company_id, days=365)

        if not history:
            current = await _maybe_call(self.cs3.get_assessment, company_id)
            current_score = float(current.org_air_score)
            return AssessmentTrend(
                company_id=company_id,
                current_org_air=current_score,
                entry_org_air=current_score,
                delta_since_entry=0.0,
                delta_30d=None,
                delta_90d=None,
                trend_direction="stable",
                snapshot_count=0,
            )

        current = float(history[-1].org_air)
        entry = float(history[0].org_air)

        now = _utc_now()
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
