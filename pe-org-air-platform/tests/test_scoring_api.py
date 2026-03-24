from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

from app.routers import scoring


COMPANY_ID = "550e8400-e29b-41d4-a716-446655440001"
OTHER_COMPANY_ID = "550e8400-e29b-41d4-a716-446655440002"


def test_scoring_compute_endpoint_invokes_runner_and_clears_cache(client, monkeypatch):
    calls: dict[str, object] = {}
    invalidations: dict[str, list[str]] = {"keys": [], "patterns": []}

    def fake_run(cmd, capture_output, text):
        calls["cmd"] = cmd
        calls["capture_output"] = capture_output
        calls["text"] = text
        return SimpleNamespace(returncode=0, stdout="run_id: run-123\n", stderr="")

    monkeypatch.setattr(scoring.subprocess, "run", fake_run)
    monkeypatch.setattr(scoring, "cache_delete", lambda key: invalidations["keys"].append(key))
    monkeypatch.setattr(scoring, "cache_delete_pattern", lambda pattern: invalidations["patterns"].append(pattern))

    resp = client.post(f"/api/v1/scoring/compute/{COMPANY_ID}?version=v2.1")

    assert resp.status_code == 200
    assert resp.json() == {"status": "submitted", "run_id": "run-123"}
    assert calls["cmd"] == [
        sys.executable,
        str(scoring.RUNNER),
        "--company-id",
        COMPANY_ID,
        "--version",
        "v2.1",
    ]
    assert calls["capture_output"] is True
    assert calls["text"] is True
    assert invalidations["keys"] == [f"scoring:results:company:{COMPANY_ID}"]
    assert invalidations["patterns"] == ["scoring:results:list:*"]


def test_scoring_compute_endpoint_surfaces_runner_errors(client, monkeypatch):
    monkeypatch.setattr(
        scoring.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout="", stderr="runner exploded"),
    )

    resp = client.post(f"/api/v1/scoring/compute/{COMPANY_ID}")

    assert resp.status_code == 500
    assert "runner exploded" in resp.json()["detail"]


def test_scoring_results_endpoint_returns_structured_payload(client, fake_sf):
    breakdown = {
        "vr": {
            "dimension_breakdown": [
                {
                    "dimension": "leadership_vision",
                    "raw_score": 77.0,
                    "sector_weight": 0.25,
                    "weighted_score": 19.25,
                    "confidence_used": 0.92,
                    "evidence_count": 4,
                }
            ]
        },
        "synergy": {
            "hits": [
                {
                    "dim_a": "leadership_vision",
                    "dim_b": "technology_stack",
                    "type": "positive",
                    "threshold": 60.0,
                    "magnitude": 5.0,
                    "activated": True,
                    "reason": "Aligned roadmap and platform readiness",
                }
            ]
        },
        "talent_penalty": {
            "sample_size": 7,
            "min_sample_met": True,
            "hhi_value": 0.35,
            "penalty_factor": 0.92,
            "function_counts": {"engineering": 4, "analytics": 3},
        },
        "sem": {
            "lower": 64.1,
            "upper": 78.4,
            "standard_error": 3.2,
            "method_used": "bootstrap",
            "fit": {"r2": 0.81},
        },
    }
    fake_sf._one = (
        COMPANY_ID,
        "assessment-1",
        "run-1",
        68.5,
        4.0,
        0.92,
        64.1,
        78.4,
        71.58,
        "good",
        json.dumps(breakdown),
        datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc),
    )

    resp = client.get(f"/api/v1/scoring/results/{COMPANY_ID}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["company_id"] == COMPANY_ID
    assert body["assessment_id"] == "assessment-1"
    assert body["composite_score"] == 71.58
    assert body["dimension_breakdown"] == [
        {
            "dimension": "leadership_vision",
            "raw_score": 77.0,
            "sector_weight": 0.25,
            "weighted_score": 19.25,
            "confidence": 0.92,
            "evidence_count": 4,
        }
    ]
    assert body["synergy_hits"][0]["activated"] is True
    assert body["talent_penalty_detail"]["function_counts"]["engineering"] == 4
    assert body["sem"]["method_used"] == "bootstrap"


def test_scoring_results_endpoint_returns_404_when_missing(client, fake_sf):
    fake_sf._one = None

    resp = client.get(f"/api/v1/scoring/results/{COMPANY_ID}")

    assert resp.status_code == 404
    assert resp.json()["detail"] == "No scores found for company"


def test_scoring_results_list_endpoint_returns_multiple_companies(client, fake_sf):
    fake_sf._all = [
        (
            COMPANY_ID,
            "assessment-1",
            "run-1",
            68.5,
            4.0,
            0.92,
            64.1,
            78.4,
            71.58,
            "good",
            json.dumps({"vr": {"dimension_breakdown": []}}),
            datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc),
        ),
        (
            OTHER_COMPANY_ID,
            "assessment-2",
            "run-2",
            61.0,
            2.5,
            0.96,
            57.0,
            69.0,
            62.54,
            "developing",
            json.dumps({"vr": {"dimension_breakdown": []}}),
            datetime(2026, 1, 3, 8, 0, tzinfo=timezone.utc),
        ),
    ]

    resp = client.get("/api/v1/scoring/results?limit=2")

    assert resp.status_code == 200
    body = resp.json()
    assert [row["company_id"] for row in body] == [COMPANY_ID, OTHER_COMPANY_ID]
    assert body[0]["score_band"] == "good"
    assert body[1]["composite_score"] == 62.54
