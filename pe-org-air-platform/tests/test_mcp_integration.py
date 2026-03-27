from __future__ import annotations

import importlib
import json
import sys
import types
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


class MockAssessment:
    def __init__(self):
        self.org_air_score = 72.5
        self.vr_score = 68.0
        self.hr_score = 75.0
        self.synergy_score = 70.0
        self.confidence_interval = (66.0, 79.0)
        self.evidence_count = 12
        self.dimension_scores = {
            "data_infrastructure": SimpleNamespace(score=70.0),
            "talent": SimpleNamespace(score=62.0),
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

    cs3_module = types.ModuleType("app.services.integration.cs3_client")
    cs3_module.CS3Client = type("CS3Client", (), {})
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
        )
    ]

    metrics = SimpleNamespace(
        fund_id="growth_fund_v",
        fund_air=80.0,
        company_count=1,
        quartile_distribution={"q4": 1},
        sector_hhi=1.0,
        avg_delta_since_entry=20.0,
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
