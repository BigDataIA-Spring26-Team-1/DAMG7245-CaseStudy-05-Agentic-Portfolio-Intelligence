from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.services.tracking.assessment_history import AssessmentHistoryService


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _build_assessment(score: float = 72.5):
    return SimpleNamespace(
        org_air_score=score,
        vr_score=68.0,
        hr_score=75.0,
        synergy_score=70.0,
        confidence_interval=(66.0, 79.0),
        evidence_count=12,
        dimension_scores={
            "data_infrastructure": SimpleNamespace(score=70.0),
            "talent": SimpleNamespace(score=62.0),
        },
    )


@pytest.mark.anyio
async def test_record_assessment_persists_snapshot(fake_sf):
    cs1 = Mock()
    cs1.get_company.return_value = {"id": "company-1"}
    cs3 = Mock()
    cs3.get_assessment.return_value = _build_assessment()
    service = AssessmentHistoryService(cs1, cs3)

    snapshot = await service.record_assessment("company-1", "professor", "full")

    assert float(snapshot.org_air) == 72.5
    assert fake_sf.queries
    assert "CREATE TABLE IF NOT EXISTS assessment_history_snapshots" in fake_sf.queries[0][0]
    sql, params = fake_sf.queries[1]
    assert "INSERT INTO assessment_history_snapshots" in sql
    assert params[1] == "company-1"
    assert params[11] == "professor"


@pytest.mark.anyio
async def test_get_history_reads_persisted_snapshots(fake_sf):
    cs1 = Mock()
    cs3 = Mock()
    service = AssessmentHistoryService(cs1, cs3)
    fake_sf._all = [
        (
            "company-1",
            _utc_now() - timedelta(days=10),
            61.0,
            55.0,
            63.0,
            58.0,
            {"data_infrastructure": 60.0},
            56.0,
            66.0,
            4,
            "analyst",
            "limited",
        )
    ]

    history = await service.get_history("company-1", days=30)

    assert len(history) == 1
    assert "CREATE TABLE IF NOT EXISTS assessment_history_snapshots" in fake_sf.queries[0][0]
    assert "FROM assessment_history_snapshots" in fake_sf.queries[1][0]
    assert history[0].company_id == "company-1"
    assert float(history[0].dimension_scores["data_infrastructure"]) == 60.0
    assert history[0].assessment_type == "limited"


@pytest.mark.anyio
async def test_calculate_trend_uses_history_when_available(fake_sf):
    cs1 = Mock()
    cs3 = Mock()
    service = AssessmentHistoryService(cs1, cs3)
    now = _utc_now()
    fake_sf._all = [
        (
            "company-1",
            now - timedelta(days=120),
            50.0,
            45.0,
            55.0,
            48.0,
            {"data_infrastructure": 49.0},
            45.0,
            55.0,
            4,
            "analyst",
            "screening",
        ),
        (
            "company-1",
            now - timedelta(days=45),
            58.0,
            53.0,
            60.0,
            54.0,
            {"data_infrastructure": 57.0},
            53.0,
            63.0,
            5,
            "analyst",
            "limited",
        ),
        (
            "company-1",
            now - timedelta(days=2),
            68.0,
            64.0,
            70.0,
            66.0,
            {"data_infrastructure": 67.0},
            63.0,
            73.0,
            8,
            "analyst",
            "full",
        ),
    ]

    trend = await service.calculate_trend("company-1")

    assert trend.entry_org_air == 50.0
    assert trend.current_org_air == 68.0
    assert trend.delta_since_entry == 18.0
    assert trend.delta_30d == 10.0
    assert trend.delta_90d == 18.0
    assert trend.trend_direction == "improving"
