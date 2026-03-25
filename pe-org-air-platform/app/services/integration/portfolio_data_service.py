from __future__ import annotations
 
from dataclasses import dataclass
from typing import Dict, List, Tuple
import structlog
 
from app.services.integration.cs1_client import CS1Client
from app.services.integration.cs2_client import CS2Client
from app.services.integration.cs3_client import CS3Client
 
logger = structlog.get_logger()
 
 
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
    confidence_interval: Tuple[float, float]
    entry_org_air: float
    delta_since_entry: float
    evidence_count: int
 
 
class PortfolioDataService:
    """
    Unified integration layer for CS1-CS4-derived views.
    This should be the main source used by CS5 dashboard + MCP portfolio summary.
    """
 
    def __init__(
        self,
        cs1_client: CS1Client | None = None,
        cs2_client: CS2Client | None = None,
        cs3_client: CS3Client | None = None,
    ) -> None:
        self.cs1 = cs1_client or CS1Client()
        self.cs2 = cs2_client or CS2Client()
        self.cs3 = cs3_client or CS3Client()
 
        logger.info("portfolio_data_service_initialized")
 
    async def get_portfolio_view(self, fund_id: str) -> List[PortfolioCompanyView]:
        companies = await self.cs1.get_portfolio_companies(fund_id)
 
        views: List[PortfolioCompanyView] = []
 
        for company in companies:
            assessment = await self.cs3.get_assessment(company.ticker)
            evidence = await self.cs2.get_evidence(company.ticker)
            entry_score = await self._get_entry_score(company.company_id)
 
            dimension_scores = {}
            for dim, score_obj in assessment.dimension_scores.items():
                dim_name = getattr(dim, "value", str(dim))
                score_value = getattr(score_obj, "score", score_obj)
                dimension_scores[dim_name] = float(score_value)
 
            views.append(
                PortfolioCompanyView(
                    company_id=company.company_id,
                    ticker=company.ticker,
                    name=company.name,
                    sector=getattr(company.sector, "value", str(company.sector)),
                    org_air=float(assessment.org_air_score),
                    vr_score=float(assessment.vr_score),
                    hr_score=float(assessment.hr_score),
                    synergy_score=float(assessment.synergy_score),
                    dimension_scores=dimension_scores,
                    confidence_interval=tuple(assessment.confidence_interval),
                    entry_org_air=float(entry_score),
                    delta_since_entry=float(assessment.org_air_score) - float(entry_score),
                    evidence_count=len(evidence),
                )
            )
 
        logger.info("portfolio_view_loaded", fund_id=fund_id, company_count=len(views))
        return views
 
    async def _get_entry_score(self, company_id: str) -> float:
        """
        Replace this with real CS1 portfolio-entry retrieval once available.
        Keep the method separate so it is easy to swap from placeholder to live lookup.
        """
        logger.info("portfolio_entry_score_lookup", company_id=company_id)
        return 45.0
 
 
portfolio_data_service = PortfolioDataService()