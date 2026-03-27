from __future__ import annotations

import importlib
import json
import sys
import types
from dataclasses import dataclass
from enum import Enum
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


class MockAssessment:
    def __init__(self):
        self.org_air_score = 72.5
        self.vr_score = 68.0
        self.hr_score = 75.0
        self.synergy_score = 70.0
        self.position_factor = 0.35
        self.confidence_interval = (66.0, 79.0)
        self.evidence_count = 12
        self.dimension_scores = {
            "data_infrastructure": SimpleNamespace(score=70.0, level=4, evidence_count=5),
            "talent": SimpleNamespace(score=62.0, level=4, evidence_count=3),
        }


def _load_tools_module(monkeypatch):
    structlog_module = types.ModuleType("structlog")

    class _Logger:
        def info(self, *args, **kwargs):
            return None

        def warning(self, *args, **kwargs):
            return None

        def error(self, *args, **kwargs):
            return None

    structlog_module.get_logger = lambda: _Logger()
    monkeypatch.setitem(sys.modules, "structlog", structlog_module)

    cs2_module = types.ModuleType("app.services.integration.cs2_client")
    cs2_module.CS2Client = type("CS2Client", (), {})
    monkeypatch.setitem(sys.modules, "app.services.integration.cs2_client", cs2_module)

    cs1_module = types.ModuleType("app.services.integration.cs1_client")
    cs1_module.CS1Client = type("CS1Client", (), {})
    monkeypatch.setitem(sys.modules, "app.services.integration.cs1_client", cs1_module)

    cs3_module = types.ModuleType("app.services.integration.cs3_client")

    class Dimension(str, Enum):
        DATA_INFRASTRUCTURE = "data_infrastructure"
        TALENT = "talent"
        LEADERSHIP = "leadership"
        TECHNOLOGY_STACK = "technology_stack"
        AI_GOVERNANCE = "ai_governance"
        USE_CASE_PORTFOLIO = "use_case_portfolio"
        CULTURE = "culture"

    class ScoreLevel(int, Enum):
        LEVEL_1 = 1
        LEVEL_2 = 2
        LEVEL_3 = 3
        LEVEL_4 = 4
        LEVEL_5 = 5

    cs3_module.CS3Client = type("CS3Client", (), {})
    cs3_module.Dimension = Dimension
    cs3_module.ScoreLevel = ScoreLevel
    monkeypatch.setitem(sys.modules, "app.services.integration.cs3_client", cs3_module)

    justification_module = types.ModuleType("app.services.justification.generator")
    justification_module.JustificationGenerator = type("JustificationGenerator", (), {})
    monkeypatch.setitem(sys.modules, "app.services.justification.generator", justification_module)

    dimension_mapper_module = types.ModuleType("app.services.retrieval.dimension_mapper")
    dimension_mapper_module.DimensionMapper = type("DimensionMapper", (), {})
    monkeypatch.setitem(sys.modules, "app.services.retrieval.dimension_mapper", dimension_mapper_module)

    portfolio_module = types.ModuleType("app.services.integration.portfolio_data_service")

    @dataclass
    class PortfolioCompanyView:
        company_id: str
        ticker: str
        name: str
        sector: str
        org_air: float
        vr_score: float
        hr_score: float
        synergy_score: float
        dimension_scores: dict[str, float]
        confidence_interval: tuple[float, float]
        entry_org_air: float
        delta_since_entry: float
        evidence_count: int
        enterprise_value_mm: float
        enterprise_value_source: str

    class _PortfolioDataService:
        async def get_portfolio_view(self, fund_id: str):
            return []

    portfolio_module.PortfolioCompanyView = PortfolioCompanyView
    portfolio_module.portfolio_data_service = _PortfolioDataService()
    monkeypatch.setitem(sys.modules, "app.services.integration.portfolio_data_service", portfolio_module)

    fund_air_module = types.ModuleType("app.services.analytics.fund_air")
    fund_air_module.fund_air_calculator = SimpleNamespace(
        calculate_fund_metrics=lambda **kwargs: None
    )
    monkeypatch.setitem(sys.modules, "app.services.analytics.fund_air", fund_air_module)

    sys.modules.pop("app.mcp.tools", None)
    import app.mcp.tools as tools_module

    return importlib.reload(tools_module), PortfolioCompanyView


@pytest.mark.anyio
async def test_calculate_org_air_calls_cs3(monkeypatch):
    tools, _ = _load_tools_module(monkeypatch)
    mock_client = Mock()
    mock_client.get_assessment.return_value = MockAssessment()

    monkeypatch.setattr(tools, "get_cs3_client", lambda: mock_client)

    result = await tools.calculate_org_air_score({"company_id": "NVDA"})
    payload = json.loads(result)

    mock_client.get_assessment.assert_called_once_with("NVDA")
    assert payload["company_id"] == "NVDA"
    assert payload["org_air"] == 72.5


@pytest.mark.anyio
async def test_no_hardcoded_data_when_cs3_fails(monkeypatch):
    tools, _ = _load_tools_module(monkeypatch)
    mock_client = Mock()
    mock_client.get_assessment.side_effect = ConnectionError("CS3 not running")

    monkeypatch.setattr(tools, "get_cs3_client", lambda: mock_client)

    with pytest.raises(ConnectionError):
        await tools.calculate_org_air_score({"company_id": "NVDA"})


@pytest.mark.anyio
async def test_get_portfolio_summary_uses_portfolio_service(monkeypatch):
    tools, PortfolioCompanyView = _load_tools_module(monkeypatch)

    mock_portfolio = [
        PortfolioCompanyView(
            company_id="1",
            ticker="NVDA",
            name="NVIDIA",
            sector="technology",
            org_air=80.0,
            vr_score=78.0,
            hr_score=82.0,
            synergy_score=76.0,
            dimension_scores={"data_infrastructure": 81.0},
            confidence_interval=(75.0, 85.0),
            entry_org_air=60.0,
            delta_since_entry=20.0,
            evidence_count=10,
            enterprise_value_mm=135000.0,
            enterprise_value_source="portfolio_holdings_table",
        )
    ]

    metrics = SimpleNamespace(
        fund_id="growth_fund_v",
        fund_air=80.0,
        company_count=1,
        quartile_distribution={"q4": 1},
        sector_hhi=1.0,
        avg_delta_since_entry=20.0,
        total_ev_mm=135000.0,
        ai_leaders_count=1,
        ai_laggards_count=0,
    )

    monkeypatch.setattr(
        tools.portfolio_data_service,
        "get_portfolio_view",
        AsyncMock(return_value=mock_portfolio),
    )
    monkeypatch.setitem(
        sys.modules,
        "app.services.analytics.fund_air",
        types.SimpleNamespace(
            fund_air_calculator=SimpleNamespace(
                calculate_fund_metrics=lambda **kwargs: metrics
            )
        ),
    )

    result = await tools.get_portfolio_summary({"fund_id": "growth_fund_v"})
    payload = json.loads(result)

    tools.portfolio_data_service.get_portfolio_view.assert_awaited_once_with("growth_fund_v")
    assert payload["fund_id"] == "growth_fund_v"
    assert payload["company_count"] == 1
    assert payload["fund_air"] == 80.0
    assert payload["total_ev_mm"] == 135000.0
    assert payload["companies"][0]["enterprise_value_mm"] == 135000.0


@pytest.mark.anyio
async def test_project_ebitda_impact_uses_company_and_assessment_context(monkeypatch):
    tools, _ = _load_tools_module(monkeypatch)
    mock_cs1 = Mock()
    mock_cs1.get_company.return_value = SimpleNamespace(
        company_id="NVDA",
        ticker="NVDA",
        sector=SimpleNamespace(value="technology"),
    )
    mock_cs3 = Mock()
    mock_cs3.get_assessment.return_value = MockAssessment()

    monkeypatch.setattr(tools, "get_cs1_client", lambda: mock_cs1)
    monkeypatch.setattr(tools, "get_cs3_client", lambda: mock_cs3)

    result = await tools.project_ebitda_impact(
        {
            "company_id": "NVDA",
            "entry_score": 60.0,
            "target_score": 78.0,
            "h_r_score": 75.0,
        }
    )
    payload = json.loads(result)

    mock_cs1.get_company.assert_called_once_with("NVDA")
    mock_cs3.get_assessment.assert_called_once_with("NVDA")
    assert payload["sector"] == "technology"
    assert payload["inputs_used"]["position_factor"] == 0.35
    assert payload["delta_air"] == 18.0
    assert payload["scenario_values"]["base"] > 0


@pytest.mark.anyio
async def test_run_gap_analysis_returns_initiatives_and_investment_estimates(monkeypatch):
    tools, _ = _load_tools_module(monkeypatch)
    mock_cs1 = Mock()
    mock_cs1.get_company.return_value = SimpleNamespace(
        company_id="NVDA",
        ticker="NVDA",
        sector=SimpleNamespace(value="technology"),
    )
    mock_cs3 = Mock()
    assessment = MockAssessment()
    assessment.dimension_scores = {
        "data_infrastructure": SimpleNamespace(score=52.0, level=3, evidence_count=2),
        "technology_stack": SimpleNamespace(score=48.0, level=3, evidence_count=1),
        "talent": SimpleNamespace(score=58.0, level=3, evidence_count=3),
    }
    mock_cs3.get_assessment.return_value = assessment
    mock_cs3.get_rubric.return_value = [
        SimpleNamespace(criteria_text="Reach repeatable operating maturity with stronger controls.")
    ]

    monkeypatch.setattr(tools, "get_cs1_client", lambda: mock_cs1)
    monkeypatch.setattr(tools, "get_cs3_client", lambda: mock_cs3)

    result = await tools.run_gap_analysis(
        {
            "company_id": "NVDA",
            "target_org_air": 80.0,
        }
    )
    payload = json.loads(result)

    assert payload["company_id"] == "NVDA"
    assert payload["priority_dimensions"]
    assert payload["initiatives"]
    assert payload["investment_estimate_mm"]["base"] > 0
    assert payload["projected_ebitda_pct"] > 0


@pytest.mark.anyio
async def test_remember_company_memory_returns_saved_record(monkeypatch):
    tools, _ = _load_tools_module(monkeypatch)
    monkeypatch.setattr(
        tools,
        "remember_company_memory_entry",
        lambda **kwargs: {"memory_id": "mem_123", **kwargs},
    )

    result = await tools.remember_company_memory(
        {
            "title": "NVDA note",
            "content": "Governance is improving",
            "company_id": "NVDA",
            "fund_id": "growth_fund_v",
            "category": "due_diligence",
        }
    )
    payload = json.loads(result)

    assert payload["memory_id"] == "mem_123"
    assert payload["company_id"] == "NVDA"


@pytest.mark.anyio
async def test_generate_ic_memo_returns_artifact_payload(monkeypatch):
    tools, _ = _load_tools_module(monkeypatch)
    monkeypatch.setattr(
        tools,
        "generate_ic_memo_artifact",
        lambda company_id, fund_id=None: {
            "title": f"IC Memo - {company_id}",
            "docx_path": "results/bonus/documents/ic_memo_nvda.docx",
            "markdown_path": "results/bonus/documents/ic_memo_nvda.md",
            "preview_markdown": "# IC Memo - NVDA",
            "metadata": {"fund_id": fund_id},
        },
    )

    result = await tools.generate_ic_memo({"company_id": "NVDA", "fund_id": "growth_fund_v"})
    payload = json.loads(result)

    assert payload["title"] == "IC Memo - NVDA"
    assert payload["metadata"]["fund_id"] == "growth_fund_v"


@pytest.mark.anyio
async def test_get_investment_tracker_summary_includes_summary_and_records(monkeypatch):
    tools, _ = _load_tools_module(monkeypatch)
    monkeypatch.setattr(
        tools,
        "get_investment_summary",
        lambda fund_id: {"fund_id": fund_id, "investment_count": 1, "roi_pct": 25.0},
    )
    monkeypatch.setattr(
        tools,
        "list_investments",
        lambda fund_id=None, company_id=None: [{"company_id": "NVDA", "program_name": "AI Platform"}],
    )
    monkeypatch.setattr(
        tools,
        "list_memories",
        lambda fund_id=None, company_id=None, limit=10: [{"title": "Fund note"}],
    )

    result = await tools.get_investment_tracker_summary({"fund_id": "growth_fund_v"})
    payload = json.loads(result)

    assert payload["fund_id"] == "growth_fund_v"
    assert payload["investment_count"] == 1
    assert payload["investments"][0]["company_id"] == "NVDA"
