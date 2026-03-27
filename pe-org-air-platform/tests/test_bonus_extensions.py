from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from app.services.extensions.investment_tracker import InvestmentTrackerService
from app.services.extensions.mem0_memory import Mem0SemanticMemoryService
from app.services.extensions.report_generators import ICMemoGenerator, LPLetterGenerator


def test_mem0_memory_service_stores_and_recalls_records(monkeypatch):
    service = Mem0SemanticMemoryService(storage_path=Path("ignored.json"))
    saved_records = []

    monkeypatch.setattr(service, "_load_records", lambda: list(saved_records))
    monkeypatch.setattr(service, "_save_records", lambda records: saved_records[:] == records or saved_records.extend(records[len(saved_records):]))

    created = service.remember(
        title="NVIDIA governance note",
        content="Governance controls are improving and talent investments remain active.",
        company_id="NVDA",
        fund_id="growth_fund_v",
        category="due_diligence",
    )
    results = service.recall(
        query="governance talent",
        company_id="NVDA",
        fund_id="growth_fund_v",
        top_k=3,
    )

    assert created["memory_id"].startswith("mem_")
    assert results
    assert results[0]["company_id"] == "NVDA"
    assert results[0]["similarity"] > 0


def test_investment_tracker_computes_roi_summary(monkeypatch):
    tracker = InvestmentTrackerService(storage_path=Path("ignored.json"))
    saved_records = []

    monkeypatch.setattr(tracker, "_load_records", lambda: list(saved_records))
    monkeypatch.setattr(tracker, "_save_records", lambda records: saved_records.clear() or saved_records.extend(records))

    tracker.add_investment(
        fund_id="growth_fund_v",
        company_id="NVDA",
        program_name="AI Platform",
        thesis="Scale governed AI workflows",
        invested_amount_mm=10.0,
        current_value_mm=13.5,
        realized_value_mm=1.0,
        expected_value_mm=15.0,
    )

    summary = tracker.summarize(fund_id="growth_fund_v")

    assert summary["investment_count"] == 1
    assert summary["invested_amount_mm"] == 10.0
    assert summary["total_value_mm"] == 14.5
    assert summary["roi_pct"] == 45.0


def test_ic_memo_generator_returns_artifact_payload_without_disk_writes(monkeypatch):
    from app.services.extensions import report_generators as module

    memory = SimpleNamespace(
        recall=lambda **kwargs: [
            {"title": "Prior diligence", "summary": "Leadership and governance are improving.", "similarity": 0.82}
        ]
    )
    tracker = SimpleNamespace(
        list_investments=lambda **kwargs: [
            {"program_name": "Workflow redesign", "invested_amount_mm": 5.0, "current_value_mm": 6.5, "status": "active"}
        ]
    )
    workflow = SimpleNamespace(
        build_packet=lambda company_id: {
            "company_profile": {"name": "NVIDIA", "ticker": "NVDA", "industry_id": "technology"},
            "recommendation": "PROCEED",
            "overall_score": 78.4,
            "overall_level_name": "Good",
            "total_evidence_count": 12,
            "strengths": ["Leadership sponsorship is explicit"],
            "key_gaps": ["Governance controls need tighter monitoring"],
            "risks": ["Execution risk on enterprise rollout"],
            "diligence_questions": ["How quickly can operating teams adopt governed copilots?"],
            "dimensions": [
                {
                    "dimension": "leadership",
                    "score": 82.0,
                    "level_name": "Excellent",
                    "evidence_strength": "strong",
                    "summary": "Leadership has a funded roadmap.",
                }
            ],
        }
    )
    captured = {}

    monkeypatch.setattr(module, "_artifact_root", lambda: Path("results/bonus/documents"))
    monkeypatch.setattr(
        module.Path,
        "write_text",
        lambda self, text, encoding="utf-8": captured.setdefault("markdown", text) or len(text),
    )
    monkeypatch.setattr(
        module,
        "write_docx",
        lambda path, title, paragraphs, subject="": captured.setdefault(
            "docx",
            {"path": str(path), "title": title, "paragraphs": list(paragraphs), "subject": subject},
        )
        or Path(path),
    )

    generator = ICMemoGenerator(workflow=workflow, memory_service=memory, investment_tracker=tracker)
    payload = generator.generate("NVDA", fund_id="growth_fund_v")

    assert payload["title"] == "IC Memo - NVIDIA"
    assert "Semantic Memory Recall" in payload["preview_markdown"]
    assert captured["docx"]["title"] == "IC Memo - NVIDIA"
    assert captured["docx"]["subject"] == "Investment Committee Memo"


def test_lp_letter_generator_returns_artifact_payload_without_disk_writes(monkeypatch):
    from app.services.extensions import report_generators as module

    memory = SimpleNamespace(
        recall=lambda **kwargs: [
            {"title": "Fund update note", "summary": "Portfolio AI readiness improved.", "similarity": 0.75}
        ]
    )
    tracker = SimpleNamespace(
        summarize=lambda **kwargs: {
            "investment_count": 1,
            "invested_amount_mm": 8.0,
            "total_value_mm": 10.0,
            "roi_pct": 25.0,
            "projected_roi_pct": 50.0,
        }
    )
    captured = {}

    async def fake_get_portfolio_view(fund_id: str):
        return [
            SimpleNamespace(
                company_id="NVDA",
                ticker="NVDA",
                name="NVIDIA",
                sector="technology",
                org_air=82.0,
                delta_since_entry=14.0,
                enterprise_value_mm=120000.0,
            ),
            SimpleNamespace(
                company_id="JPM",
                ticker="JPM",
                name="JPMorgan",
                sector="financial_services",
                org_air=68.0,
                delta_since_entry=7.0,
                enterprise_value_mm=65000.0,
            ),
        ]

    monkeypatch.setattr(module, "_artifact_root", lambda: Path("results/bonus/documents"))
    monkeypatch.setattr(
        module.Path,
        "write_text",
        lambda self, text, encoding="utf-8": captured.setdefault("markdown", text) or len(text),
    )
    monkeypatch.setattr(
        module,
        "write_docx",
        lambda path, title, paragraphs, subject="": captured.setdefault(
            "docx",
            {"path": str(path), "title": title, "paragraphs": list(paragraphs), "subject": subject},
        )
        or Path(path),
    )
    monkeypatch.setattr(
        module,
        "portfolio_data_service",
        SimpleNamespace(get_portfolio_view=fake_get_portfolio_view),
    )
    monkeypatch.setattr(
        module,
        "fund_air_calculator",
        SimpleNamespace(
            calculate_fund_metrics=lambda **kwargs: SimpleNamespace(
                fund_id="growth_fund_v",
                fund_air=76.5,
                company_count=2,
                avg_delta_since_entry=10.5,
            )
        ),
    )

    generator = LPLetterGenerator(memory_service=memory, investment_tracker=tracker)
    payload = generator.generate_sync("growth_fund_v")

    assert payload["title"] == "LP Letter - growth_fund_v"
    assert "Portfolio Highlights" in payload["preview_markdown"]
    assert captured["docx"]["title"] == "LP Letter - growth_fund_v"
    assert captured["docx"]["subject"] == "LP Update Letter"

