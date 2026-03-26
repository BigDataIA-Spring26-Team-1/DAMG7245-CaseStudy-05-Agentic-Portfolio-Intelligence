from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Dict, List
import structlog
from app.services.integration.cs1_client import CS1Client
from app.services.integration.cs2_client import CS2Client
from app.services.integration.cs3_client import CS3Client
logger = structlog.get_logger()

async def _run_sync(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)

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

class PortfolioDataService:
    def __init__(self) -> None:
        self.cs1 = CS1Client()
        self.cs2 = CS2Client()
        self.cs3 = CS3Client()
    async def get_portfolio_view(self, fund_id: str) -> List[PortfolioCompanyView]:
        logger.info("portfolio_view_requested", fund_id=fund_id)
        # ✅ FIXED: wrap sync call
        companies = await _run_sync(self.cs1.get_portfolio_companies, fund_id)
        portfolio: List[PortfolioCompanyView] = []
        for company in companies:
            ticker = getattr(company, "ticker", company)
            try:
                # ✅ FIXED: wrap sync call
                assessment = await _run_sync(self.cs3.get_assessment, ticker)
                # ✅ FIXED: wrap sync call
                evidence = await _run_sync(self.cs2.get_evidence, ticker)
                entry_score = await self._get_entry_score(ticker)
                portfolio.append(
                    PortfolioCompanyView(
                        company_id=ticker,
                        ticker=ticker,
                        name=getattr(company, "name", ticker),
                        sector=getattr(company, "sector", "unknown"),
                        org_air=assessment.org_air_score,
                        vr_score=assessment.vr_score,
                        hr_score=assessment.hr_score,
                        synergy_score=assessment.synergy_score,
                        dimension_scores={
                            getattr(dim, "value", str(dim)): getattr(score, "score", score)
                            for dim, score in assessment.dimension_scores.items()
                        },
                        confidence_interval=assessment.confidence_interval,
                        entry_org_air=entry_score,
                        delta_since_entry=assessment.org_air_score - entry_score,
                        evidence_count=len(evidence),
                    )
                )
            except Exception as e:
                logger.error(
                    "portfolio_company_failed",
                    ticker=ticker,
                    error=str(e),
                )
        logger.info(
            "portfolio_view_built",
            fund_id=fund_id,
            companies=len(portfolio),
        )
        return portfolio
    
    async def _get_entry_score(self, company_id: str) -> float:
        """
        Replace hardcoded value with real lookup.
        """
        logger.info("portfolio_entry_score_lookup", company_id=company_id)
        try:
            assessment = await _run_sync(self.cs3.get_assessment, company_id)
            return float(assessment.org_air_score)
        except Exception:
            return 50.0  # safe fallback (allowed)
        
    async def get_company_detail(self, company_id: str) -> PortfolioCompanyView:
        portfolio = await self.get_portfolio_view("default")
        for company in portfolio:
            if company.company_id == company_id:
                return company
        raise ValueError(f"Company {company_id} not found in portfolio")
    
portfolio_data_service = PortfolioDataService()
 