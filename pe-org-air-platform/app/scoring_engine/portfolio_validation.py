from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


# Deterministic baseline score bands derived from the current CS3 formula stack.
# These bands are intentionally wide enough to tolerate normal data variance while
# still enforcing the intended ordering and magnitude for the five-company portfolio.
EXPECTED_PORTFOLIO_SCORE_RANGES: dict[str, tuple[float, float]] = {
    "NVDA": (84.0, 94.0),
    "JPM": (66.0, 76.0),
    "WMT": (55.0, 66.0),
    "GE": (45.0, 55.0),
    "DG": (34.0, 45.0),
}


@dataclass(frozen=True)
class PortfolioRangeCheck:
    ticker: str
    score: float | None
    lower_bound: float
    upper_bound: float
    in_range: bool


def validate_portfolio_score_ranges(
    scores_by_ticker: Mapping[str, float],
    *,
    expected_ranges: Mapping[str, tuple[float, float]] = EXPECTED_PORTFOLIO_SCORE_RANGES,
) -> dict[str, PortfolioRangeCheck]:
    checks: dict[str, PortfolioRangeCheck] = {}
    normalized = {str(k).upper(): float(v) for k, v in scores_by_ticker.items()}

    for ticker, (lower, upper) in expected_ranges.items():
        score = normalized.get(ticker)
        in_range = score is not None and lower <= score <= upper
        checks[ticker] = PortfolioRangeCheck(
            ticker=ticker,
            score=score,
            lower_bound=float(lower),
            upper_bound=float(upper),
            in_range=bool(in_range),
        )

    return checks


def all_portfolio_scores_in_range(checks: Mapping[str, PortfolioRangeCheck]) -> bool:
    return bool(checks) and all(c.in_range for c in checks.values())
