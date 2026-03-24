from app.scoring_engine.vr_model import DimensionInput, compute_vr_score


def test_vr_in_range():
    dims = [
        DimensionInput("data_infrastructure", 70, 0.8, 5),
        DimensionInput("ai_governance", 90, 0.9, 10),
        DimensionInput("technology_stack", 90, 0.9, 10),
        DimensionInput("talent_skills", 100, 0.9, 10),
        DimensionInput("leadership_vision", 90, 0.9, 10),
        DimensionInput("use_case_portfolio", 90, 0.9, 10),
        DimensionInput("culture_change", 90, 0.9, 10),
    ]
    weights = {d.dimension: 1 / 7 for d in dims}
    vr, _ = compute_vr_score(dims, weights)
    assert 0.0 <= vr <= 100.0


def test_vr_handles_zero_weights():
    dims = [DimensionInput("data_infrastructure", 70, 0.8, 5)]
    vr, _ = compute_vr_score(dims, {"data_infrastructure": 0.0})
    assert vr == 0.0
