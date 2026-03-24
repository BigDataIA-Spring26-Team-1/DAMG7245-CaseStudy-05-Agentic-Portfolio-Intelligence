from __future__ import annotations

from types import SimpleNamespace

from app.services.justification.generator import JustificationGenerator


def test_justify_route_returns_generator_payload(client, monkeypatch):
    expected = {"company_id": "company-1", "dimension": "leadership", "score": 71.5}

    class _Generator:
        def generate(self, **kwargs):
            assert kwargs["company_id"] == "company-1"
            assert kwargs["dimension"] == "leadership"
            return expected

    monkeypatch.setattr("app.routers.justifications.get_generator", lambda: _Generator())

    resp = client.post(
        "/api/v1/justify/",
        json={"company_id": "company-1", "dimension": "leadership", "top_k": 3},
    )

    assert resp.status_code == 200
    assert resp.json() == expected


def test_justify_route_returns_400_on_validation_errors(client, monkeypatch):
    class _Generator:
        def generate(self, **kwargs):
            raise ValueError("Unsupported dimension: finance")

    monkeypatch.setattr("app.routers.justifications.get_generator", lambda: _Generator())

    resp = client.post(
        "/api/v1/justify/",
        json={"company_id": "company-1", "dimension": "finance"},
    )

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Unsupported dimension: finance"


def test_justification_generator_uses_score_context_and_retrieved_evidence():
    generator = object.__new__(JustificationGenerator)
    retrieved_hits = [
        SimpleNamespace(
            id="chunk-1",
            text="Executive leadership owns the AI roadmap and board oversight is visible.",
            score=0.88,
            metadata={
                "source_type": "sec_filing",
                "confidence": 0.91,
                "source_url": "https://example.com/doc-1",
                "title": "Annual Report",
                "published_at": "2025-01-01",
                "chunk_index": 2,
            },
        ),
        SimpleNamespace(
            id="chunk-2",
            text="Strategy sponsorship and leadership commitment are reflected in the transformation plan.",
            score=0.72,
            metadata={
                "source_type": "board_composition",
                "confidence": 0.84,
                "source_url": "https://example.com/doc-2",
                "title": "Board Summary",
                "published_at": "2025-02-02",
                "chunk_index": 0,
            },
        ),
    ]

    class _Retriever:
        def __init__(self):
            self.kwargs = {}

        def search(self, **kwargs):
            self.kwargs = kwargs
            return retrieved_hits

    retriever = _Retriever()
    generator.retriever = retriever
    generator.scoring_client = SimpleNamespace(
        get_dimension_context=lambda company_id, dimension: {
            "company_id": company_id,
            "dimension": dimension,
            "score_dimension": "leadership_vision",
            "raw_score": 72.5,
            "weighted_score": 18.1,
            "sector_weight": 0.25,
            "confidence": 0.83,
            "evidence_count": 6,
            "level": 4,
            "level_name": "Good",
            "overall_score": 68.4,
            "score_band": "green",
            "scored_at": "2026-01-01T00:00:00Z",
        }
    )

    out = generator.generate(
        company_id="company-1",
        dimension="leadership",
        question="Why does this leadership score make sense?",
        top_k=2,
        min_confidence=0.6,
    )

    assert out["score"] == 72.5
    assert out["level"] == 4
    assert out["level_name"] == "Good"
    assert out["generation_mode"] == "cs3_context_grounded"
    assert out["evidence_count"] == 2
    assert out["supporting_evidence"][0]["matched_keywords"]
    assert out["query_used"].startswith("Why does this leadership score make sense?")
    assert retriever.kwargs["company_id"] == "company-1"
    assert retriever.kwargs["dimension"] == "leadership"
    assert retriever.kwargs["min_confidence"] == 0.6


def test_justification_generator_falls_back_when_score_context_and_hits_are_missing():
    generator = object.__new__(JustificationGenerator)
    generator.retriever = SimpleNamespace(search=lambda **kwargs: [])
    generator.scoring_client = SimpleNamespace(get_dimension_context=lambda company_id, dimension: {})

    out = generator.generate(company_id="company-1", dimension="culture", top_k=3)

    assert out["score"] == 20.0
    assert out["level"] == 2
    assert out["evidence_strength"] == "weak"
    assert out["supporting_evidence"] == []
    assert out["gaps_identified"]
