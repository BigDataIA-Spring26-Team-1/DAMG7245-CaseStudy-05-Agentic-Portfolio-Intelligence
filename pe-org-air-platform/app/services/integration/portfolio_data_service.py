from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Dict, List

from app.logging_utils import get_logger

from app.services import snowflake as snowflake_service
from app.services.integration.cs1_client import CS1Client
from app.services.integration.cs2_client import CS2Client
from app.services.integration.cs3_client import CS3Client

if TYPE_CHECKING:
    from app.services.integration.cs4_client import CS4Client

logger = get_logger(__name__)


async def _run_sync(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


def _enum_value(value):
    if isinstance(value, Enum):
        return value.value
    return value


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
    dimension_scores: Dict[str, float]
    confidence_interval: tuple
    entry_org_air: float
    delta_since_entry: float
    evidence_count: int
    enterprise_value_mm: float
    enterprise_value_source: str


class PortfolioDataService:
    def __init__(self) -> None:
        self.cs1 = CS1Client()
        self.cs2 = CS2Client()
        self.cs3 = CS3Client()
        self._cs4: CS4Client | None = None

    @property
    def cs4(self) -> CS4Client:
        if self._cs4 is None:
            from app.services.integration.cs4_client import CS4Client

            self._cs4 = CS4Client()
        return self._cs4

    async def get_portfolio_view(self, fund_id: str) -> List[PortfolioCompanyView]:
        logger.info("portfolio_view_requested", fund_id=fund_id)
        holdings = await _run_sync(self.cs1.get_portfolio_holdings, fund_id)
        portfolio: List[PortfolioCompanyView] = []

        for holding in holdings:
            company = holding.company
            company_id = str(getattr(company, "company_id", "") or getattr(company, "id", "") or "")
            ticker = str(getattr(company, "ticker", "") or company_id)
            if not company_id:
                logger.error("portfolio_company_missing_identifier", ticker=ticker)
                continue

            try:
                assessment = await _run_sync(self.cs3.get_assessment, company_id)
                evidence = await _run_sync(self.cs2.get_evidence, company_id)
                entry_score = float(holding.entry_org_air) if holding.entry_org_air is not None else await self._get_entry_score(
                    company_id,
                    assessment.org_air_score,
                )

                portfolio.append(
                    PortfolioCompanyView(
                        company_id=company_id,
                        ticker=ticker,
                        name=str(getattr(company, "name", ticker)),
                        sector=str(_enum_value(getattr(company, "sector", "unknown")) or "unknown"),
                        org_air=float(assessment.org_air_score),
                        vr_score=float(assessment.vr_score),
                        hr_score=float(assessment.hr_score),
                        synergy_score=float(assessment.synergy_score),
                        dimension_scores={
                            str(getattr(dim, "value", dim)): float(getattr(score, "score", score))
                            for dim, score in assessment.dimension_scores.items()
                        },
                        confidence_interval=assessment.confidence_interval,
                        entry_org_air=entry_score,
                        delta_since_entry=float(assessment.org_air_score) - entry_score,
                        evidence_count=len(evidence),
                        enterprise_value_mm=float(holding.enterprise_value_mm),
                        enterprise_value_source=str(holding.enterprise_value_source),
                    )
                )
            except Exception as exc:
                logger.error(
                    "portfolio_company_failed",
                    company_id=company_id,
                    ticker=ticker,
                    error=str(exc),
                )

        logger.info("portfolio_view_built", fund_id=fund_id, companies=len(portfolio))
        return portfolio

    async def _get_entry_score(self, company_id: str, current_score: float) -> float:
        logger.info("portfolio_entry_score_lookup", company_id=company_id)
        conn = snowflake_service.get_snowflake_connection()
        cur = conn.cursor()
        try:
            try:
                cur.execute(
                    """
                    SELECT org_air
                    FROM assessment_history_snapshots
                    WHERE company_id = %s
                    ORDER BY snapshot_timestamp ASC
                    LIMIT 1
                    """,
                    (company_id,),
                )
                row = cur.fetchone()
                if row is None or row[0] is None:
                    return float(current_score)
                return float(row[0])
            except Exception as exc:
                logger.warning(
                    "portfolio_entry_score_unavailable_falling_back",
                    company_id=company_id,
                    error=str(exc),
                )
                return float(current_score)
        finally:
            cur.close()
            conn.close()

    async def get_company_detail(self, company_id: str) -> PortfolioCompanyView:
        portfolio = await self.get_portfolio_view("default")
        for company in portfolio:
            if company.company_id == company_id or company.ticker == company_id:
                return company
        raise ValueError(f"Company {company_id} not found in portfolio")

    async def get_portfolio_enterprise_values(self, fund_id: str) -> dict[str, float]:
        portfolio = await self.get_portfolio_view(fund_id)
        return {
            company.company_id: float(company.enterprise_value_mm)
            for company in portfolio
        }

    async def get_company_justifications(self, company_id: str, dimensions: list[str]) -> dict[str, dict]:
        results: dict[str, dict] = {}
        for dim in dimensions:
            payload = await self.cs4.generate_justification(company_id, dim)
            if not payload:
                raise ValueError(f"Empty CS4 justification returned for {company_id} / {dim}")
            results[dim] = payload
        return results


portfolio_data_service = PortfolioDataService()
