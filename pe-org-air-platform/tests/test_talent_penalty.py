from app.scoring_engine.talent_penalty import compute_hhi


def test_hhi_range():
    hhi, _ = compute_hhi(["a", "a", "b", "b"])
    assert 0.0 <= hhi <= 1.0


def test_hhi_high_when_concentrated():
    hhi, counts = compute_hhi(["data_engineering"] * 9 + ["other"])
    assert hhi > 0.7
    assert counts["data_engineering"] == 9
