from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from app.logging_utils import get_logger

from app.services.integration.portfolio_data_service import PortfolioCompanyView

logger = get_logger(__name__)

SECTOR_BENCHMARKS = {
    "technology": {"q1": 75, "q2": 65, "q3": 55, "q4": 45},
    "healthcare": {"q1": 70, "q2": 58, "q3": 48, "q4": 38},
    "financial_services": {"q1": 72, "q2": 60, "q3": 50, "q4": 40},
    "manufacturing": {"q1": 68, "q2": 55, "q3": 45, "q4": 35},
    "retail": {"q1": 65, "q2": 52, "q3": 42, "q4": 32},
    "energy": {"q1": 60, "q2": 48, "q3": 38, "q4": 28},
}


@dataclass
class FundMetrics:
    fund_id: str
    fund_air: float
    company_count: int
    quartile_distribution: Dict[int, int]
    sector_hhi: float
    avg_delta_since_entry: float
    total_ev_mm: float
    ai_leaders_count: int
    ai_laggards_count: int


class FundAIRCalculator:
    def calculate_fund_metrics(
        self,
        fund_id: str,
        companies: List[PortfolioCompanyView],
        enterprise_values: Dict[str, float],
    ) -> FundMetrics:
        if not companies:
            raise ValueError("Cannot calculate Fund-AI-R for empty portfolio")

        missing_enterprise_values = [
            c.company_id
            for c in companies
            if c.company_id not in enterprise_values
        ]
        if missing_enterprise_values:
            raise ValueError(
                "Missing enterprise values for portfolio companies: "
                + ", ".join(sorted(missing_enterprise_values))
            )

        total_ev = sum(float(enterprise_values[c.company_id]) for c in companies)
        weighted_sum = sum(
            float(enterprise_values[c.company_id]) * c.org_air for c in companies
        )
        fund_air = weighted_sum / total_ev if total_ev > 0 else 0.0

        quartile_dist = {1: 0, 2: 0, 3: 0, 4: 0}
        for c in companies:
            quartile_dist[self._get_quartile(c.org_air, c.sector)] += 1

        sector_ev: Dict[str, float] = {}
        for c in companies:
            ev = float(enterprise_values[c.company_id])
            sector_ev[c.sector] = sector_ev.get(c.sector, 0.0) + ev

        hhi = sum((ev / total_ev) ** 2 for ev in sector_ev.values()) if total_ev > 0 else 0.0

        metrics = FundMetrics(
            fund_id=fund_id,
            fund_air=round(fund_air, 1),
            company_count=len(companies),
            quartile_distribution=quartile_dist,
            sector_hhi=round(hhi, 4),
            avg_delta_since_entry=round(
                sum(c.delta_since_entry for c in companies) / len(companies), 1
            ),
            total_ev_mm=round(total_ev, 1),
            ai_leaders_count=sum(1 for c in companies if c.org_air >= 70),
            ai_laggards_count=sum(1 for c in companies if c.org_air < 50),
        )

        logger.info(
            "fund_air_calculated",
            fund_id=fund_id,
            fund_air=metrics.fund_air,
            company_count=metrics.company_count,
        )
        return metrics

    def _get_quartile(self, score: float, sector: str) -> int:
        benchmarks = SECTOR_BENCHMARKS.get(sector, SECTOR_BENCHMARKS["technology"])
        if score >= benchmarks["q1"]:
            return 1
        if score >= benchmarks["q2"]:
            return 2
        if score >= benchmarks["q3"]:
            return 3
        return 4


fund_air_calculator = FundAIRCalculator()
