from app.scoring_engine.synergy import SynergyRule, compute_synergy


def test_synergy_cap():
    scores = {
        "a": 100,
        "b": 100,
        "c": 100,
        "d": 100,
    }
    rules = [
        SynergyRule("a", "b", "positive", 60, 10),
        SynergyRule("c", "d", "positive", 60, 10),
    ]
    res = compute_synergy(scores, rules, cap_abs=15.0)
    assert res.synergy_bonus == 15.0


def test_synergy_activation_positive():
    scores = {"x": 70, "y": 80}
    rules = [SynergyRule("x", "y", "positive", 60, 3)]
    res = compute_synergy(scores, rules, cap_abs=15.0)
    assert res.synergy_bonus == 3.0
    assert any(h.activated for h in res.hits)


def test_synergy_negative_rule_activation():
    scores = {"leadership_vision": 75, "use_case_portfolio": 40}
    rules = [SynergyRule("leadership_vision", "use_case_portfolio", "negative", 60, -3)]
    res = compute_synergy(scores, rules, cap_abs=15.0)
    assert res.synergy_bonus == -3.0
    assert res.hits[0].activated is True


def test_synergy_unknown_type_is_ignored():
    scores = {"a": 90, "b": 90}
    rules = [SynergyRule("a", "b", "unexpected", 60, 5)]
    res = compute_synergy(scores, rules, cap_abs=15.0)
    assert res.synergy_bonus == 0.0
    assert "unknown" in res.hits[0].reason
