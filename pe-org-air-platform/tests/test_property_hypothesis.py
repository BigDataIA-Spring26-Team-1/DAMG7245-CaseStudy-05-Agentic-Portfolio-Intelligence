from __future__ import annotations
 
import pytest

pytest.importorskip("hypothesis")

from hypothesis import HealthCheck, given, settings, strategies as st
 
settings.register_profile(
    "cs3",
    max_examples=500,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("cs3")
 
from app.scoring_engine.evidence_mapper import EvidenceMapper, EvidenceScore, SignalSource
from app.scoring_engine.talent_concentration import talent_risk_adjustment
from app.scoring_engine.vr_model import DimensionInput, compute_vr_score
 
 
DIMS = [
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent_skills",
    "leadership_vision",
    "use_case_portfolio",
    "culture_change",
]
 
 
@given(
    st.lists(st.floats(min_value=0.0, max_value=100.0), min_size=7, max_size=7),
    st.lists(st.floats(min_value=0.0, max_value=1.0), min_size=7, max_size=7),
)
def test_vr_always_bounded(scores, confidences):
    dims = [DimensionInput(d, s, c, 1) for d, s, c in zip(DIMS, scores, confidences)]
    weights = {d: 1.0 / len(DIMS) for d in DIMS}
    vr, _ = compute_vr_score(dims, weights)
    assert 0.0 <= vr <= 100.0
 
 
@given(
    st.lists(st.floats(min_value=0.0, max_value=95.0), min_size=7, max_size=7),
    st.floats(min_value=0.0, max_value=5.0),
)
def test_vr_monotonic_when_all_dimensions_improve(base_scores, delta):
    weights = {d: 1.0 / len(DIMS) for d in DIMS}
    base = [DimensionInput(d, s, 0.9, 1) for d, s in zip(DIMS, base_scores)]
    uplift = [DimensionInput(d, min(100.0, s + delta), 0.9, 1) for d, s in zip(DIMS, base_scores)]
    vr_a, _ = compute_vr_score(base, weights)
    vr_b, _ = compute_vr_score(uplift, weights)
    assert vr_b >= vr_a
 
 
@given(st.floats(min_value=0.0, max_value=1.0), st.floats(min_value=0.0, max_value=1.0))
def test_talent_risk_adjustment_monotonic(tc_a, tc_b):
    a = float(talent_risk_adjustment(tc_a))
    b = float(talent_risk_adjustment(tc_b))
    if tc_a <= tc_b:
        assert a >= b
    else:
        assert b >= a
 
 
@given(
    st.floats(min_value=0.0, max_value=100.0),
    st.floats(min_value=0.0, max_value=1.0),
    st.integers(min_value=1, max_value=25),
)
def test_mapper_returns_all_dimensions(score, confidence, evidence_count):
    mapper = EvidenceMapper()
    evidence = [
        EvidenceScore(
            source=SignalSource.TECHNOLOGY_HIRING,
            raw_score=str(score),
            confidence=str(confidence),
            evidence_count=evidence_count,
            metadata={},
        )
    ]
    out = mapper.map_evidence_to_dimensions(evidence)
    assert len(out) == 7
    assert all(0 <= float(v.score) <= 100 for v in out.values())
 
 
