from __future__ import annotations

import json
import os
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

from app.services.integration.company_client import CompanyClient
from app.services.snowflake import get_snowflake_connection


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


@dataclass(frozen=True)
class Portfolio:
    portfolio_id: str
    name: str
    company_ids: List[str]
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

    def get_portfolio_companies(self, portfolio_id: str) -> List[Company]:
        configured = self._configured_portfolios()
        if portfolio_id in configured:
            target_ids = set(configured[portfolio_id].company_ids)
            return [company for company in self._list_all_companies() if company.company_id in target_ids]

        # Fallback: treat the current repository universe as a single implicit portfolio.
        return self._list_all_companies()

    def close(self) -> None:
        return None
