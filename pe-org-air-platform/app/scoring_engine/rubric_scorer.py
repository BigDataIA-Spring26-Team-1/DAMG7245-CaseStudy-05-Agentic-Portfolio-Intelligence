from __future__ import annotations
 
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
import re
from typing import Dict, List
 
from app.scoring_engine.evidence_mapper import (
    DimensionFeature,
    MappedEvidence,
    build_source_payloads,
    map_sources_to_dimension_features,
)
from app.scoring_engine.mapping_config import DIMENSIONS
 
 
class ScoreLevel(Enum):
    LEVEL_5 = (80, 100, "Excellent")
    LEVEL_4 = (60, 79, "Good")
    LEVEL_3 = (40, 59, "Adequate")
    LEVEL_2 = (20, 39, "Developing")
    LEVEL_1 = (0, 19, "Nascent")
 
    @property
    def min_score(self) -> int:
        return self.value[0]
 
    @property
    def max_score(self) -> int:
        return self.value[1]
 
 
@dataclass(frozen=True)
class RubricCriteria:
    level: ScoreLevel
    keywords: List[str]
    min_keyword_matches: int
    quantitative_threshold: float
 
 
@dataclass(frozen=True)
class RubricResult:
    dimension: str
    level: ScoreLevel
    score: Decimal
    matched_keywords: List[str]
    keyword_match_count: int
    confidence: Decimal
    rationale: str
 
 
@dataclass(frozen=True)
class DimensionScoreResult:
    dimension: str
    score: float
    confidence: float
    evidence_count: int
    top_keywords: List[str]
    reasons: List[str]
 
 
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))
 
 
def _mk_levels(
    l5: RubricCriteria,
    l4: RubricCriteria,
    l3: RubricCriteria,
    l2: RubricCriteria,
    l1: RubricCriteria,
) -> Dict[ScoreLevel, RubricCriteria]:
    return {
        ScoreLevel.LEVEL_5: l5,
        ScoreLevel.LEVEL_4: l4,
        ScoreLevel.LEVEL_3: l3,
        ScoreLevel.LEVEL_2: l2,
        ScoreLevel.LEVEL_1: l1,
    }
 
 
DIMENSION_RUBRICS: Dict[str, Dict[ScoreLevel, RubricCriteria]] = {
    "talent": _mk_levels(
        RubricCriteria(ScoreLevel.LEVEL_5, ["ml platform", "ai research", "large team", "principal ml", "staff ml"], 3, 0.40),
        RubricCriteria(ScoreLevel.LEVEL_4, ["data science team", "ml engineers", "active hiring", "retention"], 2, 0.25),
        RubricCriteria(ScoreLevel.LEVEL_3, ["data scientist", "growing team", "capability"], 2, 0.10),
        RubricCriteria(ScoreLevel.LEVEL_2, ["junior", "contractor", "turnover"], 1, 0.03),
        RubricCriteria(ScoreLevel.LEVEL_1, ["vendor only", "no data scientist"], 1, 0.00),
    ),
    "data_infrastructure": _mk_levels(
        RubricCriteria(ScoreLevel.LEVEL_5, ["snowflake", "databricks", "real-time", "api-first"], 2, 0.90),
        RubricCriteria(ScoreLevel.LEVEL_4, ["aws", "azure", "warehouse", "etl"], 2, 0.70),
        RubricCriteria(ScoreLevel.LEVEL_3, ["migration", "hybrid", "modernizing"], 1, 0.50),
        RubricCriteria(ScoreLevel.LEVEL_2, ["legacy", "silos", "on-prem"], 1, 0.30),
        RubricCriteria(ScoreLevel.LEVEL_1, ["manual", "spreadsheet", "mainframe"], 1, 0.00),
    ),
    "ai_governance": _mk_levels(
        RubricCriteria(ScoreLevel.LEVEL_5, ["caio", "cdo", "board committee", "model risk"], 2, 0.85),
        RubricCriteria(ScoreLevel.LEVEL_4, ["vp data", "ai policy", "risk framework"], 2, 0.65),
        RubricCriteria(ScoreLevel.LEVEL_3, ["director", "guidelines", "it governance"], 1, 0.45),
        RubricCriteria(ScoreLevel.LEVEL_2, ["informal", "ad-hoc", "no policy"], 1, 0.20),
        RubricCriteria(ScoreLevel.LEVEL_1, ["no oversight", "unmanaged"], 1, 0.00),
    ),
    "technology_stack": _mk_levels(
        RubricCriteria(ScoreLevel.LEVEL_5, ["mlops", "feature store", "model registry", "sagemaker"], 2, 0.80),
        RubricCriteria(ScoreLevel.LEVEL_4, ["mlflow", "kubeflow", "databricks ml"], 2, 0.60),
        RubricCriteria(ScoreLevel.LEVEL_3, ["jupyter", "notebooks", "manual deploy"], 1, 0.40),
        RubricCriteria(ScoreLevel.LEVEL_2, ["excel", "tableau", "no ml"], 1, 0.20),
        RubricCriteria(ScoreLevel.LEVEL_1, ["no tools", "manual"], 1, 0.00),
    ),
    "leadership": _mk_levels(
        RubricCriteria(ScoreLevel.LEVEL_5, ["ceo ai", "board committee", "ai strategy"], 2, 0.80),
        RubricCriteria(ScoreLevel.LEVEL_4, ["cto ai", "strategic priority", "executive"], 2, 0.60),
        RubricCriteria(ScoreLevel.LEVEL_3, ["vp sponsor", "department initiative"], 1, 0.40),
        RubricCriteria(ScoreLevel.LEVEL_2, ["it led", "limited awareness"], 1, 0.20),
        RubricCriteria(ScoreLevel.LEVEL_1, ["no sponsor", "not discussed"], 1, 0.00),
    ),
    "use_case_portfolio": _mk_levels(
        RubricCriteria(ScoreLevel.LEVEL_5, ["production ai", "ai product", "3x roi"], 2, 0.80),
        RubricCriteria(ScoreLevel.LEVEL_4, ["production", "measured roi", "scaling"], 2, 0.60),
        RubricCriteria(ScoreLevel.LEVEL_3, ["pilot", "early production"], 1, 0.40),
        RubricCriteria(ScoreLevel.LEVEL_2, ["poc", "proof of concept"], 1, 0.20),
        RubricCriteria(ScoreLevel.LEVEL_1, ["exploring", "no use cases"], 1, 0.00),
    ),
    "culture": _mk_levels(
        RubricCriteria(ScoreLevel.LEVEL_5, ["innovative", "data-driven", "fail-fast"], 2, 0.75),
        RubricCriteria(ScoreLevel.LEVEL_4, ["experimental", "learning culture"], 1, 0.55),
        RubricCriteria(ScoreLevel.LEVEL_3, ["open to change", "some resistance"], 1, 0.40),
        RubricCriteria(ScoreLevel.LEVEL_2, ["bureaucratic", "resistant", "slow"], 1, 0.20),
        RubricCriteria(ScoreLevel.LEVEL_1, ["hostile", "siloed", "no data culture"], 1, 0.00),
    ),
}
 
 
FEATURE_TO_RUBRIC_DIM = {
    "talent_skills": "talent",
    "leadership_vision": "leadership",
    "culture_change": "culture",
}
 
 
DIMENSION_METRIC_KEY = {
    "talent": "ai_job_ratio",
    "data_infrastructure": "data_quality_ratio",
    "ai_governance": "governance_maturity",
    "technology_stack": "ml_platform_adoption",
    "leadership": "executive_ai_sponsorship",
    "use_case_portfolio": "production_use_case_ratio",
    "culture": "culture_index",
}
 
 
def _find_matches(text: str, keywords: List[str]) -> List[str]:
    matches: List[str] = []
    for kw in keywords:
        if re.search(r"\b" + re.escape(kw.lower()) + r"\b", text):
            matches.append(kw)
    return matches
 
 
def _interpolate(level: ScoreLevel, keyword_hits: int, needed: int) -> Decimal:
    lo, hi = level.min_score, level.max_score
    ratio = 1.0 if needed <= 0 else min(1.0, keyword_hits / needed)
    score = lo + (hi - lo) * ratio
    return Decimal(str(round(score, 2)))
 
 
class RubricScorer:
    def __init__(self) -> None:
        self.rubrics = DIMENSION_RUBRICS
 
    def score_dimension(
        self,
        dimension: str,
        evidence_text: str,
        quantitative_metrics: Dict[str, float],
    ) -> RubricResult:
        dim = FEATURE_TO_RUBRIC_DIM.get(dimension, dimension)
        text = (evidence_text or "").lower()
        rubric = self.rubrics.get(dim, {})
        metric_key = DIMENSION_METRIC_KEY.get(dim, "")
        metric_val = float(quantitative_metrics.get(metric_key, 0.0))
 
        for level in [
            ScoreLevel.LEVEL_5,
            ScoreLevel.LEVEL_4,
            ScoreLevel.LEVEL_3,
            ScoreLevel.LEVEL_2,
            ScoreLevel.LEVEL_1,
        ]:
            criteria = rubric.get(level)
            if not criteria:
                continue
            matches = _find_matches(text, criteria.keywords)
            if len(matches) >= criteria.min_keyword_matches and metric_val >= criteria.quantitative_threshold:
                score = _interpolate(level, len(matches), criteria.min_keyword_matches + 2)
                conf = Decimal(str(round(min(0.95, 0.50 + 0.08 * len(matches) + 0.30 * metric_val), 3)))
                return RubricResult(
                    dimension=dim,
                    level=level,
                    score=score,
                    matched_keywords=matches,
                    keyword_match_count=len(matches),
                    confidence=conf,
                    rationale=f"{dim}: level={level.name}, metric={metric_key}:{metric_val:.3f}",
                )
 
        return RubricResult(
            dimension=dim,
            level=ScoreLevel.LEVEL_1,
            score=Decimal("10.00"),
            matched_keywords=[],
            keyword_match_count=0,
            confidence=Decimal("0.40"),
            rationale=f"{dim}: no rubric level met",
        )
 
    def score_all_dimensions(
        self,
        evidence_by_dimension: Dict[str, str],
        metrics_by_dimension: Dict[str, Dict[str, float]],
    ) -> Dict[str, RubricResult]:
        out: Dict[str, RubricResult] = {}
        for dim in self.rubrics.keys():
            out[dim] = self.score_dimension(
                dimension=dim,
                evidence_text=evidence_by_dimension.get(dim, ""),
                quantitative_metrics=metrics_by_dimension.get(dim, {}),
            )
        return out
 
 
def _default_result(dim: str) -> DimensionScoreResult:
    return DimensionScoreResult(
        dimension=dim,
        score=50.0,
        confidence=0.50,
        evidence_count=0,
        top_keywords=[],
        reasons=["No evidence found -> default score=50"],
    )
 
 
def _build_quant_metrics(feature_dim: str, f: DimensionFeature) -> Dict[str, float]:
    dim = FEATURE_TO_RUBRIC_DIM.get(feature_dim, feature_dim)
    key = DIMENSION_METRIC_KEY.get(dim, "")
    if not key:
        return {}
 
    # Scale deterministic feature signals into [0,1] rubric metric proxies.
    ws_norm = clamp(f.weighted_signal / 30.0, 0.0, 1.0)
    ev_norm = clamp(f.evidence_count / 40.0, 0.0, 1.0)
    rel_norm = clamp(f.reliability_weighted, 0.0, 1.0)
    metric_val = clamp(0.45 * ws_norm + 0.35 * ev_norm + 0.20 * rel_norm, 0.0, 1.0)
    return {key: metric_val}
 
 
RUBRIC_FALLBACK_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "data_infrastructure": {"low": 4.0, "mid": 10.0, "high": 18.0, "very_high": 28.0},
    "ai_governance": {"low": 3.0, "mid": 9.0, "high": 16.0, "very_high": 26.0},
    "technology_stack": {"low": 4.0, "mid": 11.0, "high": 19.0, "very_high": 30.0},
    "talent_skills": {"low": 5.0, "mid": 12.0, "high": 20.0, "very_high": 32.0},
    "leadership_vision": {"low": 3.0, "mid": 9.0, "high": 16.0, "very_high": 26.0},
    "use_case_portfolio": {"low": 4.0, "mid": 10.0, "high": 18.0, "very_high": 28.0},
    "culture_change": {"low": 3.0, "mid": 8.0, "high": 15.0, "very_high": 24.0},
}
 
 
def _fallback_threshold_score(feature_dim: str, f: DimensionFeature) -> tuple[float, str]:
    t = RUBRIC_FALLBACK_THRESHOLDS.get(feature_dim)
    if not t:
        return 50.0, "fallback default=50"
 
    s = float(f.weighted_signal)
    if s < t["low"]:
        return 25.0, f"fallback weighted_signal={s:.2f} < low({t['low']})"
    if s < t["mid"]:
        return 50.0, f"fallback weighted_signal={s:.2f} < mid({t['mid']})"
    if s < t["high"]:
        return 75.0, f"fallback weighted_signal={s:.2f} < high({t['high']})"
    if s < t["very_high"]:
        return 90.0, f"fallback weighted_signal={s:.2f} < very_high({t['very_high']})"
    return 100.0, f"fallback weighted_signal={s:.2f} >= very_high({t['very_high']})"
 
 
def score_dimension_features(features: Dict[str, DimensionFeature]) -> List[DimensionScoreResult]:
    scorer = RubricScorer()
    results: List[DimensionScoreResult] = []
 
    for feature_dim in DIMENSIONS:
        f = features.get(feature_dim)
        if not f:
            results.append(_default_result(feature_dim))
            continue
 
        evidence_text = " ".join(f.top_keywords)
        quant_metrics = _build_quant_metrics(feature_dim, f)
        rr = scorer.score_dimension(feature_dim, evidence_text, quant_metrics)
        score = clamp(float(rr.score), 0.0, 100.0)
        reasons = [rr.rationale]
 
        # When rubric keyword evidence is sparse, fall back to calibrated weighted-signal thresholds.
        if rr.keyword_match_count == 0:
            score, fallback_reason = _fallback_threshold_score(feature_dim, f)
            reasons.append(fallback_reason)
 
        results.append(
            DimensionScoreResult(
                dimension=f.dimension,
                score=score,
                confidence=clamp(float(rr.confidence), 0.0, 1.0),
                evidence_count=int(f.evidence_count),
                top_keywords=list(f.top_keywords),
                reasons=reasons,
            )
        )
 
    return results
 
 
def score_dimensions(mapped: List[MappedEvidence]) -> List[DimensionScoreResult]:
    """
    Backward-compatible wrapper used by older scripts.
    """
    payloads = build_source_payloads(mapped)
    features = map_sources_to_dimension_features(payloads)
    return score_dimension_features(features)
 
 