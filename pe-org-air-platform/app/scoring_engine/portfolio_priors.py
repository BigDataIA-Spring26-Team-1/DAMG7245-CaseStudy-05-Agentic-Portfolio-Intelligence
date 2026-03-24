from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PortfolioPrior:
    vr_target: float
    pf_target: float
    tc_target: float
    market_cap_percentile: float


# CS3 5-company calibration priors from the case-study portfolio table.
PORTFOLIO_PRIORS: dict[str, PortfolioPrior] = {
    "NVDA": PortfolioPrior(vr_target=95.0, pf_target=0.90, tc_target=0.12, market_cap_percentile=0.95),
    "JPM": PortfolioPrior(vr_target=70.0, pf_target=0.50, tc_target=0.18, market_cap_percentile=0.75),
    "WMT": PortfolioPrior(vr_target=55.0, pf_target=0.30, tc_target=0.20, market_cap_percentile=0.65),
    "GE": PortfolioPrior(vr_target=40.0, pf_target=0.00, tc_target=0.25, market_cap_percentile=0.50),
    "DG": PortfolioPrior(vr_target=25.0, pf_target=-0.30, tc_target=0.30, market_cap_percentile=0.35),
}
