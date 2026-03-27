from __future__ import annotations

import json
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from app.services.integration.company_client import CompanyClient
from app.services.snowflake import get_snowflake_connection
from app.services.observability.metrics import track_cs_client
from app.services.value_creation import estimate_enterprise_value_mm
from app.config import settings


class Sector(str, Enum):
    TECHNOLOGY = "technology"
    FINANCIAL_SERVICES = "financial_services"
    HEALTHCARE = "healthcare"
    MANUFACTURING = "manufacturing"
    RETAIL = "retail"
    BUSINESS_SERVICES = "business_services"
    CONSUMER = "consumer"
    INDUSTRIALS = "industrials"
    SERVICES = "services"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Company:
    company_id: str
    ticker: Optional[str]
    name: str
    sector: Sector
    sub_sector: Optional[str]
    market_cap_percentile: float
    revenue_millions: Optional[float] = None
    employee_count: Optional[int] = None
    fiscal_year_end: Optional[str] = None
    portfolio_entry_date: Optional[str] = None


@dataclass(frozen=True)
class Portfolio:
    portfolio_id: str
    name: str
    company_ids: List[str]
    fund_vintage: Optional[int] = None


@dataclass(frozen=True)
class PortfolioHolding:
    portfolio_id: str
    company: Company
    enterprise_value_mm: float
    enterprise_value_source: str
    entry_org_air: Optional[float] = None
    entry_date: Optional[str] = None
    fund_vintage: Optional[int] = None


_SECTOR_ALIASES: Dict[str, Sector] = {
    "technology": Sector.TECHNOLOGY,
    "financial": Sector.FINANCIAL_SERVICES,
    "financial_services": Sector.FINANCIAL_SERVICES,
    "healthcare": Sector.HEALTHCARE,
    "manufacturing": Sector.MANUFACTURING,
    "retail": Sector.RETAIL,
    "business_services": Sector.BUSINESS_SERVICES,
    "services": Sector.SERVICES,
    "consumer": Sector.CONSUMER,
    "industrials": Sector.INDUSTRIALS,
}


class CS1Client:
    """
    Spec-aligned CS1 company service client.

    The repository currently stores only a subset of the PDF's target company
    schema, so unavailable fields remain optional rather than fabricated.
    """

    def __init__(self) -> None:
        self.company_client = CompanyClient()

    def _normalize_sector(self, raw_sector: Optional[str]) -> Sector:
        key = (raw_sector or "").strip().lower().replace(" ", "_")
        return _SECTOR_ALIASES.get(key, Sector.UNKNOWN)

    def _row_to_company(self, row: tuple) -> Company:
        position_factor = float(row[4]) if row[4] is not None else 0.0
        market_cap_percentile = max(0.0, min(1.0, round((position_factor + 1.0) / 2.0, 4)))
        return Company(
            company_id=str(row[0]),
            ticker=str(row[2]) if row[2] is not None else None,
            name=str(row[1]),
            sector=self._normalize_sector(row[5] if len(row) > 5 else None),
            sub_sector=str(row[6]) if len(row) > 6 and row[6] is not None else None,
            market_cap_percentile=market_cap_percentile,
            revenue_millions=None,
            employee_count=None,
            fiscal_year_end=None,
        )

    def _list_all_companies(self) -> List[Company]:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT c.id, c.name, c.ticker, c.industry_id, c.position_factor, i.sector, i.name
                FROM companies c
                LEFT JOIN industries i ON c.industry_id = i.id
                WHERE c.is_deleted = FALSE
                ORDER BY c.created_at DESC
                """
            )
            return [self._row_to_company(row) for row in cur.fetchall()]
        finally:
            cur.close()
            conn.close()

    def _company_lookup_maps(self) -> tuple[Dict[str, Company], Dict[str, Company]]:
        companies = self._list_all_companies()
        by_id = {company.company_id: company for company in companies}
        by_ticker = {
            (company.ticker or "").upper(): company
            for company in companies
            if company.ticker
        }
        return by_id, by_ticker

    def _row_to_holding(self, row: tuple[Any, ...]) -> PortfolioHolding:
        company = Company(
            company_id=str(row[6]),
            ticker=str(row[8]) if row[8] is not None else None,
            name=str(row[7]),
            sector=self._normalize_sector(row[11] if len(row) > 11 else None),
            sub_sector=str(row[12]) if len(row) > 12 and row[12] is not None else None,
            market_cap_percentile=max(0.0, min(1.0, round((float(row[10] or 0.0) + 1.0) / 2.0, 4))),
            revenue_millions=None,
            employee_count=None,
            fiscal_year_end=None,
            portfolio_entry_date=str(row[4]) if row[4] is not None else None,
        )
        enterprise_value = float(row[3]) if row[3] is not None else estimate_enterprise_value_mm(
            sector=company.sector.value,
            market_cap_percentile=company.market_cap_percentile,
            position_factor=float(row[10] or 0.0),
        )
        return PortfolioHolding(
            portfolio_id=str(row[0]),
            company=company,
            enterprise_value_mm=enterprise_value,
            enterprise_value_source="portfolio_holdings_table" if row[3] is not None else "estimated_from_position_factor",
            entry_org_air=float(row[5]) if row[5] is not None else None,
            entry_date=str(row[4]) if row[4] is not None else None,
            fund_vintage=int(row[2]) if row[2] is not None else None,
        )

    def _get_persisted_holdings(self, portfolio_id: str) -> List[PortfolioHolding]:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                    p.id,
                    p.name,
                    p.fund_vintage,
                    h.enterprise_value_mm,
                    h.entry_date,
                    h.entry_org_air,
                    c.id,
                    c.name,
                    c.ticker,
                    c.industry_id,
                    c.position_factor,
                    i.sector,
                    i.name
                FROM portfolios p
                JOIN portfolio_holdings h
                  ON h.portfolio_id = p.id
                 AND h.is_active = TRUE
                JOIN companies c
                  ON c.id = h.company_id
                 AND c.is_deleted = FALSE
                LEFT JOIN industries i
                  ON i.id = c.industry_id
                WHERE p.id = %s
                ORDER BY COALESCE(h.enterprise_value_mm, 0) DESC, c.created_at DESC
                """,
                (portfolio_id,),
            )
            rows = cur.fetchall()
        except Exception:
            return []
        finally:
            cur.close()
            conn.close()

        return [self._row_to_holding(row) for row in rows]

    @track_cs_client("cs1", "get_company")
    def get_company(self, identifier: str) -> Company:
        if not identifier or not identifier.strip():
            raise ValueError("identifier is required")

        normalized = identifier.strip()
        like_value = f"%{normalized}%"
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT c.id, c.name, c.ticker, c.industry_id, c.position_factor, i.sector, i.name
                FROM companies c
                LEFT JOIN industries i ON c.industry_id = i.id
                WHERE c.is_deleted = FALSE
                  AND (
                    c.id = %s
                    OR c.ticker = %s
                    OR UPPER(COALESCE(c.name, '')) LIKE UPPER(%s)
                  )
                ORDER BY c.created_at DESC
                LIMIT 1
                """,
                (normalized, normalized.upper(), like_value),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError("Company not found")
            return self._row_to_company(row)
        finally:
            cur.close()
            conn.close()

    @track_cs_client("cs1", "list_companies")
    def list_companies(
        self,
        sector: Optional[Sector] = None,
        min_revenue: Optional[float] = None,
    ) -> List[Company]:
        companies = self._list_all_companies()
        if sector:
            companies = [company for company in companies if company.sector == sector]
        if min_revenue is not None:
            companies = [
                company
                for company in companies
                if (company.revenue_millions or 0.0) >= float(min_revenue)
            ]
        return companies

    def _configured_portfolios(self) -> Dict[str, Portfolio]:
        raw = os.getenv("CS1_PORTFOLIOS_JSON", "").strip()
        if not raw:
            return {}

        try:
            payload = json.loads(raw)
        except Exception:
            return {}

        if not isinstance(payload, dict):
            return {}

        portfolios: Dict[str, Portfolio] = {}
        for portfolio_id, value in payload.items():
            if not isinstance(value, dict):
                continue
            portfolios[str(portfolio_id)] = Portfolio(
                portfolio_id=str(portfolio_id),
                name=str(value.get("name", portfolio_id)),
                company_ids=[str(cid) for cid in value.get("company_ids", [])],
                fund_vintage=int(value["fund_vintage"]) if value.get("fund_vintage") is not None else None,
            )
        return portfolios

    def _configured_holdings(self, portfolio_id: str) -> List[PortfolioHolding]:
        raw = os.getenv("CS1_PORTFOLIOS_JSON", "").strip()
        if not raw:
            return []
        try:
            payload = json.loads(raw)
        except Exception:
            return []
        if not isinstance(payload, dict):
            return []

        portfolio = payload.get(portfolio_id)
        if not isinstance(portfolio, dict):
            return []

        by_id, by_ticker = self._company_lookup_maps()
        holdings: list[PortfolioHolding] = []
        fund_vintage = int(portfolio["fund_vintage"]) if portfolio.get("fund_vintage") is not None else None

        raw_holdings = portfolio.get("holdings")
        if isinstance(raw_holdings, list):
            for item in raw_holdings:
                if not isinstance(item, dict):
                    continue
                company = None
                company_id = str(item.get("company_id") or "").strip()
                ticker = str(item.get("ticker") or "").strip().upper()
                if company_id:
                    company = by_id.get(company_id)
                elif ticker:
                    company = by_ticker.get(ticker)
                if company is None:
                    continue

                ev = item.get("enterprise_value_mm")
                enterprise_value_mm = float(ev) if ev is not None else estimate_enterprise_value_mm(
                    sector=company.sector.value,
                    market_cap_percentile=company.market_cap_percentile,
                    position_factor=(company.market_cap_percentile * 2.0) - 1.0,
                )
                holdings.append(
                    PortfolioHolding(
                        portfolio_id=portfolio_id,
                        company=company,
                        enterprise_value_mm=enterprise_value_mm,
                        enterprise_value_source="env_config" if ev is not None else "estimated_from_position_factor",
                        entry_org_air=float(item["entry_org_air"]) if item.get("entry_org_air") is not None else None,
                        entry_date=str(item["entry_date"]) if item.get("entry_date") is not None else None,
                        fund_vintage=fund_vintage,
                    )
                )
            return holdings

        company_ids = [str(cid) for cid in portfolio.get("company_ids", []) if str(cid).strip()]
        for company_id in company_ids:
            company = by_id.get(company_id)
            if company is None:
                continue
            holdings.append(
                PortfolioHolding(
                    portfolio_id=portfolio_id,
                    company=company,
                    enterprise_value_mm=estimate_enterprise_value_mm(
                        sector=company.sector.value,
                        market_cap_percentile=company.market_cap_percentile,
                        position_factor=(company.market_cap_percentile * 2.0) - 1.0,
                    ),
                    enterprise_value_source="estimated_from_position_factor",
                    fund_vintage=fund_vintage,
                )
            )
        return holdings

    def _default_configured_holdings(self, portfolio_id: str) -> List[PortfolioHolding]:
        configured_tickers = [
            ticker.strip().upper()
            for ticker in str(settings.results_portfolio_tickers or "").split(",")
            if ticker.strip()
        ]
        if not configured_tickers:
            return []

        _, by_ticker = self._company_lookup_maps()
        holdings: list[PortfolioHolding] = []
        for ticker in configured_tickers:
            company = by_ticker.get(ticker)
            if company is None:
                continue
            holdings.append(
                PortfolioHolding(
                    portfolio_id=portfolio_id,
                    company=company,
                    enterprise_value_mm=estimate_enterprise_value_mm(
                        sector=company.sector.value,
                        market_cap_percentile=company.market_cap_percentile,
                        position_factor=(company.market_cap_percentile * 2.0) - 1.0,
                    ),
                    enterprise_value_source="configured_ticker_fallback",
                )
            )
        return holdings

    @track_cs_client("cs1", "get_portfolio_holdings")
    def get_portfolio_holdings(self, portfolio_id: str) -> List[PortfolioHolding]:
        holdings = self._get_persisted_holdings(portfolio_id)
        if holdings:
            return holdings

        holdings = self._configured_holdings(portfolio_id)
        if holdings:
            return holdings

        return self._default_configured_holdings(portfolio_id)

    @track_cs_client("cs1", "get_portfolio_companies")
    def get_portfolio_companies(self, portfolio_id: str) -> List[Company]:
        return [holding.company for holding in self.get_portfolio_holdings(portfolio_id)]

    def get_portfolio_enterprise_values(self, portfolio_id: str) -> Dict[str, float]:
        return {
            holding.company.company_id: float(holding.enterprise_value_mm)
            for holding in self.get_portfolio_holdings(portfolio_id)
        }

    def close(self) -> None:
        return None
