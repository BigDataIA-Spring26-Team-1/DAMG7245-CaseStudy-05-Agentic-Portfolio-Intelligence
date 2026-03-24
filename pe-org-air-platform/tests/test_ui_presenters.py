from app.ui_presenters import (
    compact_recommendation,
    display_evidence_count,
    extract_orgair_score,
    humanize_source_type,
    sanitize_generated_summary,
)


def test_extract_orgair_score_prefers_orgair_then_composite() -> None:
    assert extract_orgair_score({"org_air_score": 88.02, "composite_score": 77.0}) == 88.02
    assert extract_orgair_score({"composite_score": 43.29}) == 43.29
    assert extract_orgair_score({}) == 0.0


def test_sanitize_generated_summary_replaces_company_id() -> None:
    text = "abc-123 is assessed at 90.0/100 for Data Infrastructure."
    out = sanitize_generated_summary(
        text,
        company_name="Dollar General Corporation",
        company_id="abc-123",
        ticker="DG",
    )
    assert "abc-123" not in out
    assert "Dollar General Corporation" in out


def test_compact_recommendation_uses_short_decision_label() -> None:
    assert compact_recommendation("FURTHER DILIGENCE - Material capability gaps or weak evidence") == "FURTHER DILIGENCE"
    assert compact_recommendation("PROCEED") == "PROCEED"


def test_display_evidence_count_falls_back_to_score_context() -> None:
    justification = {
        "evidence_count": 0,
        "score_context": {"evidence_count": 117},
    }
    ic_packet = {"total_evidence_count": 0}
    dimension_score = {"evidence_count": 99}
    assert display_evidence_count(justification, ic_packet, dimension_score) == 117


def test_display_evidence_count_uses_dimension_score_last() -> None:
    assert display_evidence_count({}, {}, {"evidence_count": 42}) == 42


def test_humanize_source_type_for_sec_items() -> None:
    assert humanize_source_type("sec_10k_item_1") == "SEC 10K ITEM 1"
    assert humanize_source_type("sec_10k_item_1a") == "SEC 10K ITEM 1A"
