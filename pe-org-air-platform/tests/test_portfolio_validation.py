from __future__ import annotations

from app.scoring_engine.composite import compute_composite
from app.scoring_engine.portfolio_priors import PORTFOLIO_PRIORS
from app.scoring_engine.portfolio_validation import (
    EXPECTED_PORTFOLIO_SCORE_RANGES,
    all_portfolio_scores_in_range,
    validate_portfolio_score_ranges,
)
from app.scoring_engine.synergy import compute_formula_synergy


def _expected_composite_for_prior(vr_target: float, pf_target: float, hr_base: float = 75.0) -> float:
    hr_score = hr_base * (1.0 + 0.15 * pf_target)
    synergy_score = compute_formula_synergy(vr_score=vr_target, hr_score=hr_score, timing_factor=1.0).synergy_score
    return compute_composite(vr_score=vr_target, hr_score=hr_score, synergy_score=synergy_score, alpha=0.60, beta=0.12).composite_score


def test_portfolio_baseline_scores_fall_in_expected_ranges():
    scores = {
        ticker: _expected_composite_for_prior(vr_target=prior.vr_target, pf_target=prior.pf_target)
        for ticker, prior in PORTFOLIO_PRIORS.items()
    }

    checks = validate_portfolio_score_ranges(scores)
    assert all_portfolio_scores_in_range(checks)


def test_portfolio_range_validator_flags_out_of_range_scores():
    scores = {ticker: (low + high) / 2.0 for ticker, (low, high) in EXPECTED_PORTFOLIO_SCORE_RANGES.items()}
    scores["NVDA"] = 60.0

    checks = validate_portfolio_score_ranges(scores)
    assert checks["NVDA"].in_range is False
    assert all_portfolio_scores_in_range(checks) is False
