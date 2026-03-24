from __future__ import annotations
 
from decimal import Decimal
 
from app.scoring_engine.composite import compute_composite
from app.scoring_engine.evidence_mapper import (
    EvidenceItem,
    EvidenceMapper,
    EvidenceScore,
    SignalSource,
    map_evidence_to_dimensions,
)
from app.scoring_engine.position_factor import PositionFactorCalculator
from app.scoring_engine.rubric_scorer import score_dimensions
from app.scoring_engine.synergy import compute_formula_synergy
from app.scoring_engine.talent_concentration import (
    JobAnalysis,
    TalentConcentrationCalculator,
    talent_risk_adjustment,
)
 
 
def test_evidence_mapper_returns_all_dimensions_and_defaults_to_50():
    mapper = EvidenceMapper()
    out = mapper.map_evidence_to_dimensions([])
    assert len(out) == 7
    assert all(v.score == Decimal("50.00") for v in out.values())
 
 
def test_evidence_mapper_confidence_does_not_drop_with_more_sources():
    mapper = EvidenceMapper()
    one = [
        EvidenceScore(
            source=SignalSource.TECHNOLOGY_HIRING,
            raw_score=Decimal("70"),
            confidence=Decimal("0.70"),
            evidence_count=4,
            metadata={},
        )
    ]
    two = one + [
        EvidenceScore(
            source=SignalSource.INNOVATION_ACTIVITY,
            raw_score=Decimal("75"),
            confidence=Decimal("0.85"),
            evidence_count=5,
            metadata={},
        )
    ]
    r1 = mapper.get_coverage_report(one)
    r2 = mapper.get_coverage_report(two)
    assert r2["technology_stack"]["confidence"] >= r1["technology_stack"]["confidence"]
 
 
def test_position_factor_formula_known_value():
    pf = PositionFactorCalculator.calculate_position_factor(
        vr_score=65.0,
        sector="technology",
        market_cap_percentile=1.0,
    )
    assert float(pf) == 0.4
 
 
def test_talent_concentration_and_risk_adjustment():
    ja = JobAnalysis(
        total_ai_jobs=10,
        senior_ai_jobs=4,
        mid_ai_jobs=4,
        entry_ai_jobs=2,
        unique_skills={"python", "mlops", "spark"},
    )
    tc = TalentConcentrationCalculator.calculate_tc(
        ja,
        glassdoor_individual_mentions=2,
        glassdoor_review_count=10,
    )
    adj = talent_risk_adjustment(float(tc))
    assert Decimal("0.0") <= tc <= Decimal("1.0")
    assert Decimal("0.0") <= adj <= Decimal("1.0")
 
 
def test_formula_synergy_is_bounded_and_uses_timing_band():
    syn = compute_formula_synergy(vr_score=90.0, hr_score=80.0, alignment=0.9, timing_factor=1.5)
    assert 0.0 <= syn.synergy_score <= 100.0
    assert syn.timing_factor == 1.2
 
 
def test_composite_full_formula_mode():
    res = compute_composite(
        vr_score=70.0,
        hr_score=60.0,
        synergy_score=50.0,
        alpha=0.60,
        beta=0.12,
    )
    expected = (1 - 0.12) * (0.60 * 70.0 + 0.40 * 60.0) + 0.12 * 50.0
    assert abs(res.composite_score - round(expected, 2)) < 1e-9
 
 
def test_score_dimensions_wrapper_returns_all_7_dimensions():
    items = [
        EvidenceItem(
            source="document_chunk",
            evidence_type="sec_item_7",
            text="strategy roadmap executive innovation and use case production",
            url=None,
        )
    ]
    mapped = map_evidence_to_dimensions(items)
    out = score_dimensions(mapped)
    assert len(out) == 7
 
 