from __future__ import annotations
 
from typing import Dict, Optional
 
from app.scoring_engine.evidence_mapper import DIMENSION_KEYWORDS
from app.scoring_engine.mapping_config import DIMENSIONS as SCORING_DIMENSIONS
from app.scoring_engine.mapping_config import SOURCE_PROFILES
 
 
DIMENSIONS = set(SCORING_DIMENSIONS)
DEFAULT_DIMENSION = "technology_stack"
 
 
SOURCE_ALIASES = {
    "technology_hiring": "technology_hiring",
    "jobs": "technology_hiring",
    "job_posting": "technology_hiring",
    "job": "technology_hiring",
    "hiring": "technology_hiring",
    "innovation_activity": "innovation_activity",
    "patents": "innovation_activity",
    "patent": "innovation_activity",
    "digital_presence": "digital_presence",
    "tech": "digital_presence",
    "leadership_signals": "leadership_signals",
    "news": "leadership_signals",
    "sec_item_1": "sec_item_1",
    "sec_item_1a": "sec_item_1a",
    "sec_item_7": "sec_item_7",
    "glassdoor_reviews": "glassdoor_reviews",
    "glassdoor": "glassdoor_reviews",
    "board_composition": "board_composition",
    "board": "board_composition",
    "sec_filing": "sec_filing",
    "10k": "sec_filing",
    "10_q": "sec_filing",
}

INTERNAL_TO_PUBLIC_DIMENSION = {
    "talent_skills": "talent",
    "leadership_vision": "leadership",
    "culture_change": "culture",
}
 
 
def _normalize(value: str) -> str:
    return (value or "").strip().lower().replace("-", "_").replace(" ", "_")
 
 
def _canonical_signal_key(raw: str) -> Optional[str]:
    key = _normalize(raw)
    if not key:
        return None
 
    if "item_1a" in key or "item1a" in key:
        return "sec_item_1a"
    if "item_7" in key or "item7" in key:
        return "sec_item_7"
    if "item_1" in key or "item1" in key:
        return "sec_item_1"
 
    return SOURCE_ALIASES.get(key)
 
 
def _primary_dimension_for_signal(signal_key: str) -> Optional[str]:
    prof = SOURCE_PROFILES.get(signal_key)
    if not prof:
        return None
 
    weights: Dict[str, float] = {
        dim: float(weight)
        for dim, weight in prof.dim_weights.items()
        if dim in DIMENSIONS
    }
    if not weights:
        return None
    return max(weights.items(), key=lambda kv: kv[1])[0]
 
 
def _keyword_dimension(text: str) -> Optional[str]:
    normalized = (text or "").lower()
    if not normalized:
        return None
 
    best_dim: Optional[str] = None
    best_hits = 0
    for dim, keywords in DIMENSION_KEYWORDS.items():
        if dim not in DIMENSIONS:
            continue
        hits = sum(1 for kw in keywords if kw in normalized)
        if hits > best_hits:
            best_dim = dim
            best_hits = hits
    return best_dim if best_hits > 0 else None
 
 
def map_dimension(source_type: str, signal_category: Optional[str] = None, chunk_text: Optional[str] = None) -> str:
    signal_key = _canonical_signal_key(signal_category or "") or _canonical_signal_key(source_type)
 
    if signal_key in {"sec_item_1", "sec_item_1a", "sec_item_7"}:
        mapped = _primary_dimension_for_signal(signal_key)
        if mapped:
            return mapped
 
    if signal_key and signal_key != "sec_filing":
        mapped = _primary_dimension_for_signal(signal_key)
        if mapped:
            return mapped
 
    inferred = _keyword_dimension(chunk_text or "")
    if inferred:
        return inferred
 
    if signal_key == "sec_filing":
        return "leadership_vision"

    return DEFAULT_DIMENSION


class DimensionMapper:
    """
    Spec-aligned dimension mapper facade.

    The underlying scoring engine uses internal dimension labels like
    `talent_skills`; this class exposes the assignment-facing labels by default.
    """

    def _render_dimension(self, dimension: str, public_names: bool = True) -> str:
        if not public_names:
            return dimension
        return INTERNAL_TO_PUBLIC_DIMENSION.get(dimension, dimension)

    def get_signal_key(self, signal_category: Optional[str], source_type: Optional[str] = None) -> Optional[str]:
        return _canonical_signal_key(signal_category or "") or _canonical_signal_key(source_type or "")

    def get_dimension_weights(
        self,
        signal_category: Optional[str],
        source_type: Optional[str] = None,
        public_names: bool = True,
    ) -> Dict[str, float]:
        signal_key = self.get_signal_key(signal_category, source_type)
        if not signal_key:
            return {self._render_dimension(DEFAULT_DIMENSION, public_names=public_names): 1.0}

        profile = SOURCE_PROFILES.get(signal_key)
        if not profile:
            return {self._render_dimension(DEFAULT_DIMENSION, public_names=public_names): 1.0}

        weights = {
            self._render_dimension(dim, public_names=public_names): float(weight)
            for dim, weight in profile.dim_weights.items()
            if dim in DIMENSIONS and float(weight) > 0.0
        }
        if not weights:
            return {self._render_dimension(DEFAULT_DIMENSION, public_names=public_names): 1.0}
        return weights

    def get_primary_dimension(
        self,
        signal_category: Optional[str],
        source_type: Optional[str] = None,
        public_names: bool = True,
    ) -> str:
        weights = self.get_dimension_weights(
            signal_category=signal_category,
            source_type=source_type,
            public_names=public_names,
        )
        return max(weights.items(), key=lambda kv: kv[1])[0]

    def get_all_dimensions_for_evidence(
        self,
        signal_category: Optional[str],
        source_type: Optional[str] = None,
        min_weight: float = 0.1,
        public_names: bool = True,
    ) -> Dict[str, float]:
        weights = self.get_dimension_weights(
            signal_category=signal_category,
            source_type=source_type,
            public_names=public_names,
        )
        return {
            dimension: weight
            for dimension, weight in weights.items()
            if float(weight) >= float(min_weight)
        }
