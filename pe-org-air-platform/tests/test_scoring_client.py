from __future__ import annotations

import json
from datetime import datetime, timezone

from app.services.integration.company_client import CompanyClient
from app.services.integration.scoring_client import ScoringClient


COMPANY_ID = "550e8400-e29b-41d4-a716-446655440001"


def test_company_client_get_company_returns_normalized_payload(fake_sf):
    fake_sf._one = (
        COMPANY_ID,
        "Acme Holdings",
        "ACME",
        None,
        0.35,
        False,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        None,
    )

    out = CompanyClient().get_company(COMPANY_ID)

    assert out["id"] == COMPANY_ID
    assert out["name"] == "Acme Holdings"
    assert out["ticker"] == "ACME"
    assert out["position_factor"] == 0.35


def test_scoring_client_get_dimension_context_maps_aliases(fake_sf):
    fake_sf._one = (
        COMPANY_ID,
        "assessment-1",
        "run-1",
        68.0,
        3.0,
        0.95,
        60.0,
        74.0,
        70.0,
        "good",
        json.dumps(
            {
                "vr": {
                    "dimension_breakdown": [
                        {
                            "dimension": "leadership_vision",
                            "raw_score": 74.0,
                            "weighted_score": 18.5,
                            "sector_weight": 0.25,
                            "confidence_used": 0.82,
                            "evidence_count": 5,
                        }
                    ]
                }
            }
        ),
        datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    out = ScoringClient().get_dimension_context(COMPANY_ID, "leadership")

    assert out["dimension"] == "leadership"
    assert out["score_dimension"] == "leadership_vision"
    assert out["raw_score"] == 74.0
    assert out["level"] == 4
    assert out["level_name"] == "Good"
    assert out["confidence_interval"][0] < out["raw_score"] < out["confidence_interval"][1]
    assert out["score_band"] == "good"


def test_scoring_client_exposes_rubric_and_assessment(fake_sf):
    fake_sf._one = (
        COMPANY_ID,
        "assessment-2",
        "run-2",
        76.0,
        4.0,
        0.5,
        69.0,
        82.0,
        79.5,
        "green",
        json.dumps(
            {
                "vr": {
                    "dimension_breakdown": [
                        {
                            "dimension": "data_infrastructure",
                            "raw_score": 78.0,
                            "weighted_score": 19.5,
                            "sector_weight": 0.25,
                            "confidence_used": 0.9,
                            "evidence_count": 8,
                        }
                    ]
                },
                "hr": {"score": 63.0},
                "talent_penalty": {"hhi_value": 0.22},
                "position_factor": 0.35,
            }
        ),
        datetime(2026, 1, 3, tzinfo=timezone.utc),
    )

    client = ScoringClient()
    rubric = client.get_rubric("data_infrastructure", level=4)
    assessment = client.get_assessment(COMPANY_ID)

    assert rubric
    assert rubric[0]["level"] == 4
    assert rubric[0]["keywords"]
    assert assessment["org_air_score"] == 79.5
    assert assessment["hr_score"] == 63.0
    assert assessment["dimension_scores"]["data_infrastructure"]["level"] == 4
    assert assessment["dimension_scores"]["data_infrastructure"]["confidence_interval"][0] < 78.0
