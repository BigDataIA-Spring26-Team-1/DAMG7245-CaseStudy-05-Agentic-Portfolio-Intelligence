from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from app.pipelines.glassdoor_collector import GlassdoorCultureCollector, GlassdoorReview

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures"


def test_fetch_reviews_reads_local_file_when_api_not_configured():
    collector = GlassdoorCultureCollector(rapidapi_key="", data_root=FIXTURE_ROOT)
    out = collector.fetch_reviews("NVDA", limit=10)
    assert len(out) == 1
    assert out[0].review_id == "r1"
    assert out[0].rating == 4.5
    assert out[0].is_current_employee is True


def test_parse_reviews_payload_handles_nested_rapidapi_shape():
    collector = GlassdoorCultureCollector(rapidapi_key="dummy")
    payload = {
        "status": "success",
        "data": {
            "reviews": [
                {
                    "reviewId": "abc-1",
                    "overallRating": "8/10",
                    "headline": "Learning every day",
                    "prosText": "Strong data culture",
                    "consText": "Long meetings",
                    "adviceToManagement": "Reduce process overhead",
                    "employmentStatus": "Current Employee",
                    "jobTitle": "Data Scientist",
                    "createdAt": "2025-07-10T12:30:00Z",
                }
            ]
        },
    }
    out = collector._parse_reviews_payload(payload=payload, ticker="NVDA")
    assert len(out) == 1
    assert out[0].review_id == "abc-1"
    assert out[0].rating == 4.0
    assert out[0].is_current_employee is True
    assert out[0].review_date == datetime(2025, 7, 10, 12, 30, tzinfo=timezone.utc)


def test_extract_company_id_prefers_matching_ticker():
    collector = GlassdoorCultureCollector(rapidapi_key="dummy")
    payload = {
        "data": [
            {"companyId": "101", "ticker": "WMT", "name": "Walmart"},
            {"companyId": "202", "ticker": "NVDA", "name": "NVIDIA"},
        ]
    }
    assert collector._extract_company_id(payload=payload, ticker="NVDA") == "202"


def test_company_id_map_from_env(monkeypatch):
    monkeypatch.setenv("GLASSDOOR_COMPANY_ID_MAP", '{"NVDA":"40772","jpm":"12345"}')
    collector = GlassdoorCultureCollector(rapidapi_key="dummy")
    assert collector._configured_company_id("NVDA") == "40772"
    assert collector._configured_company_id("jpm") == "12345"


def test_company_id_map_from_file():
    collector = GlassdoorCultureCollector(rapidapi_key="dummy", data_root=FIXTURE_ROOT)
    assert collector._configured_company_id("WMT") == "999"
    assert collector._configured_company_id("GE") == "777"


def test_minimal_mode_skips_discovery_and_query(monkeypatch):
    monkeypatch.setenv("GLASSDOOR_DISABLE_DISCOVERY_FALLBACK", "true")
    collector = GlassdoorCultureCollector(rapidapi_key="dummy")
    collector.company_id_map = {"NVDA": "40772"}

    calls = {"resolve": 0, "query": 0, "by_id": 0}

    def fake_resolve(*args, **kwargs):
        calls["resolve"] += 1
        return None

    def fake_query(*args, **kwargs):
        calls["query"] += 1
        return []

    def fake_by_id(*args, **kwargs):
        calls["by_id"] += 1
        return []

    monkeypatch.setattr(collector, "_resolve_company_id", fake_resolve)
    monkeypatch.setattr(collector, "_fetch_reviews_by_query", fake_query)
    monkeypatch.setattr(collector, "_fetch_reviews_by_company_id", fake_by_id)

    out = collector._fetch_reviews_from_rapidapi("NVDA", limit=1)
    assert out == []
    assert calls["resolve"] == 0
    assert calls["query"] == 0
    assert calls["by_id"] == 1


def test_reviews_company_id_param_is_used_first(monkeypatch):
    monkeypatch.setenv("GLASSDOOR_REVIEWS_COMPANY_ID_PARAM", "companyId")
    monkeypatch.setenv("GLASSDOOR_DISABLE_DISCOVERY_FALLBACK", "true")
    collector = GlassdoorCultureCollector(rapidapi_key="dummy")

    captured_params = []

    def fake_safe_get_json(*args, **kwargs):
        captured_params.append(dict(kwargs.get("params") or {}))
        return {"data": {"reviews": []}}

    monkeypatch.setattr(collector, "_safe_get_json", fake_safe_get_json)
    out = collector._fetch_reviews_by_company_id(client=object(), company_id="40772", ticker="NVDA", limit=1)
    assert out == []
    assert captured_params
    assert "companyId" in captured_params[0]


def test_analyze_reviews_returns_defaults_for_empty_input():
    collector = GlassdoorCultureCollector(rapidapi_key="dummy")
    sig = collector.analyze_reviews(company_id="cid-1", ticker="NVDA", reviews=[])
    assert float(sig.overall_score) == 50.0
    assert sig.review_count == 0
    assert float(sig.confidence) == 0.30


def test_analyze_reviews_scores_keywords_and_confidence():
    collector = GlassdoorCultureCollector(rapidapi_key="dummy")
    reviews = [
        GlassdoorReview(
            review_id="r1",
            rating=4.5,
            title="Innovative and data-driven team",
            pros="Great AI and machine learning culture with agile workflows",
            cons="Sometimes fast-paced",
            advice_to_management="Keep investing in automation and analytics",
            is_current_employee=True,
            job_title="ML Engineer",
            review_date=datetime.now(timezone.utc),
        ),
        GlassdoorReview(
            review_id="r2",
            rating=3.5,
            title="Traditional org",
            pros="Strong business fundamentals",
            cons="Bureaucratic and slow to change",
            advice_to_management=None,
            is_current_employee=False,
            job_title="Analyst",
            review_date=datetime.now(timezone.utc),
        ),
    ]

    sig = collector.analyze_reviews(company_id="cid-2", ticker="WMT", reviews=reviews)
    assert sig.review_count == 2
    assert 0.0 <= float(sig.innovation_score) <= 100.0
    assert 0.0 <= float(sig.data_driven_score) <= 100.0
    assert 0.0 <= float(sig.ai_awareness_score) <= 100.0
    assert 0.0 <= float(sig.change_readiness_score) <= 100.0
    assert 0.0 <= float(sig.overall_score) <= 100.0
    assert 0.40 <= float(sig.confidence) <= 0.95
    assert sig.positive_keywords_found
