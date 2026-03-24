from __future__ import annotations

from app.scoring_engine.evidence_mapper import EvidenceItem, _infer_signal_bucket
from app.scoring_engine.mapping_config import SOURCE_PROFILES


def test_infer_signal_bucket_supports_all_nine_sources():
    expected = {
        "technology_hiring": "technology_hiring",
        "innovation_activity": "innovation_activity",
        "digital_presence": "digital_presence",
        "leadership_signals": "leadership_signals",
        "sec_item_1": "sec_item_1",
        "sec_item_1a": "sec_item_1a",
        "sec_item_7": "sec_item_7",
        "glassdoor_reviews": "glassdoor_reviews",
        "board_composition": "board_composition",
    }
    for evidence_type, bucket in expected.items():
        item = EvidenceItem(source="x", evidence_type=evidence_type, text="sample")
        assert _infer_signal_bucket(item) == bucket


def test_infer_signal_bucket_maps_aliases_to_canonical_sources():
    aliases = {
        "jobs": "technology_hiring",
        "patents": "innovation_activity",
        "tech": "digital_presence",
        "news": "leadership_signals",
    }
    for evidence_type, bucket in aliases.items():
        item = EvidenceItem(source="x", evidence_type=evidence_type, text="sample")
        assert _infer_signal_bucket(item) == bucket


def test_source_profiles_exist_for_all_nine_sources():
    required_sources = [
        "technology_hiring",
        "innovation_activity",
        "digital_presence",
        "leadership_signals",
        "sec_item_1",
        "sec_item_1a",
        "sec_item_7",
        "glassdoor_reviews",
        "board_composition",
    ]
    for src in required_sources:
        assert src in SOURCE_PROFILES

