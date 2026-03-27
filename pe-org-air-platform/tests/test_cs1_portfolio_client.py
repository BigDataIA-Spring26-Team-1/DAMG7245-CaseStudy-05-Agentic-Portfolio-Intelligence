from __future__ import annotations

import json

from app.config import settings
from app.services.integration.cs1_client import CS1Client, Company, Sector


def _sample_company(company_id: str, ticker: str, sector: Sector, percentile: float) -> Company:
    return Company(
        company_id=company_id,
        ticker=ticker,
        name=f"{ticker} Corp",
        sector=sector,
        sub_sector=None,
        market_cap_percentile=percentile,
        revenue_millions=None,
        employee_count=None,
        fiscal_year_end=None,
    )


def test_get_portfolio_holdings_reads_explicit_env_holdings(monkeypatch):
    client = CS1Client()
    company = _sample_company("company-1", "NVDA", Sector.TECHNOLOGY, 0.95)

    monkeypatch.setattr(client, "_get_persisted_holdings", lambda portfolio_id: [])
    monkeypatch.setattr(
        client,
        "_company_lookup_maps",
        lambda: (
            {company.company_id: company},
            {company.ticker: company},
        ),
    )
    monkeypatch.setenv(
        "CS1_PORTFOLIOS_JSON",
        json.dumps(
            {
                "growth_fund_v": {
                    "name": "Growth Fund V",
                    "fund_vintage": 2024,
                    "holdings": [
                        {
                            "ticker": "NVDA",
                            "enterprise_value_mm": 125000.0,
                            "entry_org_air": 58.0,
                            "entry_date": "2024-01-15",
                        }
                    ],
                }
            }
        ),
    )

    holdings = client.get_portfolio_holdings("growth_fund_v")

    assert len(holdings) == 1
    assert holdings[0].company.company_id == "company-1"
    assert holdings[0].enterprise_value_mm == 125000.0
    assert holdings[0].enterprise_value_source == "env_config"
    assert holdings[0].entry_org_air == 58.0
    assert holdings[0].entry_date == "2024-01-15"


def test_get_portfolio_holdings_uses_configured_ticker_fallback(monkeypatch):
    client = CS1Client()
    company_a = _sample_company("company-1", "NVDA", Sector.TECHNOLOGY, 0.95)
    company_b = _sample_company("company-2", "JPM", Sector.FINANCIAL_SERVICES, 0.88)

    monkeypatch.setattr(client, "_get_persisted_holdings", lambda portfolio_id: [])
    monkeypatch.setattr(client, "_configured_holdings", lambda portfolio_id: [])
    monkeypatch.setattr(
        client,
        "_company_lookup_maps",
        lambda: (
            {
                company_a.company_id: company_a,
                company_b.company_id: company_b,
            },
            {
                company_a.ticker: company_a,
                company_b.ticker: company_b,
            },
        ),
    )
    monkeypatch.setattr(settings, "results_portfolio_tickers", "NVDA,JPM")

    holdings = client.get_portfolio_holdings("growth_fund_v")

    assert len(holdings) == 2
    assert {holding.company.ticker for holding in holdings} == {"NVDA", "JPM"}
    assert all(holding.enterprise_value_mm > 0 for holding in holdings)
    assert all(holding.enterprise_value_source == "configured_ticker_fallback" for holding in holdings)
