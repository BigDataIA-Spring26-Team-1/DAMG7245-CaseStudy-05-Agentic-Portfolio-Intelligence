from app.scoring_engine.hr_baselines import apply_hr_adjustment_to_talent


def test_hr_adjustment_only_applies_to_talent():
    assert apply_hr_adjustment_to_talent(dimension="ai_governance", raw_score=50, hr_factor=1.5) == 50
    assert apply_hr_adjustment_to_talent(dimension="talent_skills", raw_score=50, hr_factor=1.5) == 75.0


def test_hr_adjustment_caps_to_100():
    assert apply_hr_adjustment_to_talent(dimension="talent_skills", raw_score=90, hr_factor=2.0) == 100.0
