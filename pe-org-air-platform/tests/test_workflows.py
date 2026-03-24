from __future__ import annotations

from types import SimpleNamespace

from app.services.workflows.analyst_notes import AnalystNotesCollector
from app.services.workflows.ic_prep import ICPrepWorkflow


def test_ic_prep_workflow_builds_packet_from_dimension_justifications():
    workflow = object.__new__(ICPrepWorkflow)
    workflow.company_client = SimpleNamespace(
        get_company=lambda company_id: {"id": company_id, "name": "Acme Holdings"}
    )
    workflow.scoring_client = SimpleNamespace(
        get_latest_scores=lambda company_id: {
            "overall_score": 67.2,
            "score_band": "green",
            "breakdown": {
                "vr": {
                    "dimension_breakdown": [
                        {"dimension": "leadership_vision"},
                        {"dimension": "talent_skills"},
                    ]
                }
            },
        }
    )

    def fake_generate(*, company_id, dimension, question, top_k, min_confidence):
        payloads = {
            "leadership": {
                "score": 74.0,
                "level": 4,
                "level_name": "Good",
                "evidence_strength": "strong",
                "generated_summary": "Leadership is aligned behind the transformation roadmap.",
                "supporting_evidence": [
                    {
                        "evidence_id": "chunk-1",
                        "source_type": "sec_filing",
                        "source_url": "https://example.com/leadership",
                        "title": "Annual Report",
                        "confidence": 0.91,
                        "relevance_score": 0.88,
                        "matched_keywords": ["strategy", "roadmap"],
                        "content": "Executive sponsorship is explicit in the roadmap.",
                    }
                ],
                "gaps_identified": [],
            },
            "talent": {
                "score": 43.0,
                "level": 3,
                "level_name": "Adequate",
                "evidence_strength": "weak",
                "generated_summary": "Talent depth remains uneven across critical AI roles.",
                "supporting_evidence": [
                    {
                        "evidence_id": "chunk-2",
                        "source_type": "jobs",
                        "source_url": "https://example.com/talent",
                        "title": "Hiring Feed",
                        "confidence": 0.58,
                        "relevance_score": 0.62,
                        "matched_keywords": ["hiring"],
                        "content": "The company continues hiring for several ML engineering roles.",
                    }
                ],
                "gaps_identified": ["No strong evidence of 'retention risk' for next-level readiness"],
            },
        }
        return payloads[dimension]

    workflow.generator = SimpleNamespace(generate=fake_generate)

    out = workflow.build_packet(
        company_id="company-1",
        dimensions=["leadership", "talent"],
        top_k=3,
    )

    assert out["company_profile"]["name"] == "Acme Holdings"
    assert out["overall_score"] == 67.2
    assert out["overall_level"] == 4
    assert out["overall_level_name"] == "Good"
    assert len(out["dimensions"]) == 2
    assert any("Leadership" in item for item in out["strengths"])
    assert any("Talent" in item for item in out["risks"])
    assert out["key_gaps"] == ["No strong evidence of 'retention risk' for next-level readiness"]
    assert out["diligence_questions"]
    assert out["recommendation"] == "PROCEED WITH CAUTION - Moderate readiness, but diligence gaps remain"
    assert out["total_evidence_count"] == 2
    assert out["avg_evidence_strength"] == "moderate"
    assert out["generated_at"]


def test_analyst_notes_collector_builds_dimension_note():
    collector = object.__new__(AnalystNotesCollector)
    collector.company_client = SimpleNamespace(
        get_company=lambda company_id: {"id": company_id, "name": "Acme Holdings"}
    )
    collector.generator = SimpleNamespace(
        generate=lambda **kwargs: {
            "company_id": kwargs["company_id"],
            "dimension": kwargs["dimension"],
            "score": 82.0,
            "level_name": "Excellent",
            "evidence_strength": "strong",
            "generated_summary": "The stack is modern, cloud-native, and well-governed.",
            "supporting_evidence": [
                {
                    "evidence_id": "chunk-9",
                    "title": "Platform Overview",
                    "source_type": "sec_filing",
                    "source_url": "https://example.com/stack",
                    "confidence": 0.93,
                    "relevance_score": 0.87,
                    "matched_keywords": ["cloud", "api"],
                    "content": "Cloud platform usage and deployment automation are both mature.",
                }
            ],
            "gaps_identified": [],
        }
    )

    out = collector.collect_note(company_id="company-1", dimension="technology_stack", top_k=2)

    assert out["company_name"] == "Acme Holdings"
    assert out["dimension"] == "technology_stack"
    assert out["note_title"].startswith("Acme Holdings - Technology Stack")
    assert out["confidence_label"] == "high"
    assert out["evidence_snapshot"][0]["evidence_id"] == "chunk-9"


def test_analyst_notes_collector_submission_methods_index_notes():
    captured = []

    collector = object.__new__(AnalystNotesCollector)
    collector.vector_store = SimpleNamespace(
        upsert=lambda chunks: captured.extend(chunks) or len(chunks)
    )

    interview_id = collector.submit_interview(
        company_id="company-1",
        interviewee="Jane Doe",
        interviewee_title="CTO",
        transcript="Leadership described a governed data platform and clear operating cadence.",
        assessor="Analyst A",
        dimensions_discussed=["leadership", "data infrastructure"],
    )
    dd_id = collector.submit_dd_finding(
        company_id="company-1",
        title="Model governance gap",
        finding="Policy ownership is unclear and monitoring remains manual.",
        dimension="ai governance",
        severity="high",
        assessor="Analyst B",
    )
    dataroom_id = collector.submit_data_room_summary(
        company_id="company-1",
        document_name="AI Strategy Deck",
        summary="The deck shows funded platform investments and named executive sponsors.",
        dimension="leadership",
        assessor="Analyst C",
    )

    assert interview_id.startswith("interview_")
    assert dd_id.startswith("dd_")
    assert dataroom_id.startswith("dataroom_")
    assert len(captured) == 3
    assert captured[0].metadata["source_type"] == "interview_transcript"
    assert captured[0].metadata["dimension"] == "leadership"
    assert captured[1].metadata["severity"] == "high"
    assert captured[2].metadata["document_name"] == "AI Strategy Deck"
