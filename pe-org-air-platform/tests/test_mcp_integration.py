from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

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
            SimpleNamespace(value="data_infrastructure"): SimpleNamespace(score=70.0),
            SimpleNamespace(value="talent"): SimpleNamespace(score=62.0),
        }


@pytest.mark.asyncio
async def test_calculate_org_air_calls_cs3():
    from app.mcp import tools

    with patch.object(tools.cs3_client, "get_assessment", new_callable=AsyncMock) as mock:
        mock.return_value = MockAssessment()

        result = await tools.calculate_org_air_score({"company_id": "NVDA"})
        payload = json.loads(result)

        mock.assert_called_once_with("NVDA")
        assert payload["company_id"] == "NVDA"
        assert payload["org_air"] == 72.5


@pytest.mark.asyncio
async def test_no_hardcoded_data_when_cs3_fails():
    from app.mcp import tools

    with patch.object(tools.cs3_client, "get_assessment", new_callable=AsyncMock) as mock:
        mock.side_effect = ConnectionError("CS3 not running")

        with pytest.raises(ConnectionError):
            await tools.calculate_org_air_score({"company_id": "NVDA"})


@pytest.mark.asyncio
async def test_get_portfolio_summary_uses_portfolio_service():
    from app.mcp import tools
    from app.services.integration.portfolio_data_service import PortfolioCompanyView

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

    with patch.object(
        tools.portfolio_data_service,
        "get_portfolio_view",
        new_callable=AsyncMock,
    ) as mock:
        mock.return_value = mock_portfolio

        result = await tools.get_portfolio_summary({"fund_id": "growth_fund_v"})
        payload = json.loads(result)

        mock.assert_called_once_with("growth_fund_v")
        assert payload["fund_id"] == "growth_fund_v"
        assert payload["company_count"] == 1
        assert payload["fund_air"] == 80.0