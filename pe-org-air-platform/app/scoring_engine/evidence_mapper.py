from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List

from app.scoring_engine.mapping_config import SOURCE_PROFILES, normalize_weights


DIMENSIONS = [
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent_skills",
    "leadership_vision",
    "use_case_portfolio",
    "culture_change",
]


class SignalSource(str, Enum):
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"
    SEC_ITEM_1 = "sec_item_1"
    SEC_ITEM_1A = "sec_item_1a"
    SEC_ITEM_7 = "sec_item_7"
    GLASSDOOR_REVIEWS = "glassdoor_reviews"
    BOARD_COMPOSITION = "board_composition"


@dataclass(frozen=True)
class DimensionMapping:
    source: SignalSource
    primary_dimension: str
    primary_weight: Decimal
    secondary_mappings: Dict[str, Decimal]
    reliability: Decimal = Decimal("0.80")


@dataclass(frozen=True)
class AggregatedDimensionScore:
    dimension: str
    score: Decimal
    contributing_sources: List[SignalSource]
    total_weight: Decimal
    confidence: Decimal


@dataclass(frozen=True)
class EvidenceScore:
    source: SignalSource
    raw_score: Decimal
    confidence: Decimal
    evidence_count: int
    metadata: Dict[str, Any]


# CS2/CS3 signal -> 7-dimension weighted mapping table
SIGNAL_TO_DIMENSION_MAP: Dict[SignalSource, DimensionMapping] = {
    SignalSource.TECHNOLOGY_HIRING: DimensionMapping(
        source=SignalSource.TECHNOLOGY_HIRING,
        primary_dimension="talent_skills",
        primary_weight=Decimal("0.70"),
        secondary_mappings={
            "technology_stack": Decimal("0.20"),
            "ai_governance": Decimal("0.10"),
            "culture_change": Decimal("0.10"),
        },
        reliability=Decimal("0.85"),
    ),
    SignalSource.INNOVATION_ACTIVITY: DimensionMapping(
        source=SignalSource.INNOVATION_ACTIVITY,
        primary_dimension="technology_stack",
        primary_weight=Decimal("0.50"),
        secondary_mappings={
            "use_case_portfolio": Decimal("0.30"),
            "data_infrastructure": Decimal("0.20"),
        },
        reliability=Decimal("0.80"),
    ),
    SignalSource.DIGITAL_PRESENCE: DimensionMapping(
        source=SignalSource.DIGITAL_PRESENCE,
        primary_dimension="data_infrastructure",
        primary_weight=Decimal("0.60"),
        secondary_mappings={
            "technology_stack": Decimal("0.40"),
        },
        reliability=Decimal("0.70"),
    ),
    SignalSource.LEADERSHIP_SIGNALS: DimensionMapping(
        source=SignalSource.LEADERSHIP_SIGNALS,
        primary_dimension="leadership_vision",
        primary_weight=Decimal("0.60"),
        secondary_mappings={
            "ai_governance": Decimal("0.25"),
            "culture_change": Decimal("0.15"),
        },
        reliability=Decimal("0.80"),
    ),
    SignalSource.SEC_ITEM_1: DimensionMapping(
        source=SignalSource.SEC_ITEM_1,
        primary_dimension="use_case_portfolio",
        primary_weight=Decimal("0.70"),
        secondary_mappings={
            "technology_stack": Decimal("0.30"),
        },
        reliability=Decimal("0.90"),
    ),
    SignalSource.SEC_ITEM_1A: DimensionMapping(
        source=SignalSource.SEC_ITEM_1A,
        primary_dimension="ai_governance",
        primary_weight=Decimal("0.80"),
        secondary_mappings={
            "data_infrastructure": Decimal("0.20"),
        },
        reliability=Decimal("0.90"),
    ),
    SignalSource.SEC_ITEM_7: DimensionMapping(
        source=SignalSource.SEC_ITEM_7,
        primary_dimension="leadership_vision",
        primary_weight=Decimal("0.50"),
        secondary_mappings={
            "use_case_portfolio": Decimal("0.30"),
            "data_infrastructure": Decimal("0.20"),
        },
        reliability=Decimal("0.90"),
    ),
    SignalSource.GLASSDOOR_REVIEWS: DimensionMapping(
        source=SignalSource.GLASSDOOR_REVIEWS,
        primary_dimension="culture_change",
        primary_weight=Decimal("0.80"),
        secondary_mappings={
            "talent_skills": Decimal("0.10"),
            "leadership_vision": Decimal("0.10"),
        },
        reliability=Decimal("0.75"),
    ),
    SignalSource.BOARD_COMPOSITION: DimensionMapping(
        source=SignalSource.BOARD_COMPOSITION,
        primary_dimension="ai_governance",
        primary_weight=Decimal("0.70"),
        secondary_mappings={
            "leadership_vision": Decimal("0.30"),
        },
        reliability=Decimal("0.90"),
    ),
}


class EvidenceMapper:
    """Maps CS2/CS3 evidence scores into 7 aggregated dimension scores."""

    def __init__(self) -> None:
        self.mappings = SIGNAL_TO_DIMENSION_MAP

    def map_evidence_to_dimensions(
        self,
        evidence_scores: List[EvidenceScore],
    ) -> Dict[str, AggregatedDimensionScore]:
        dimension_sums: Dict[str, Decimal] = {d: Decimal("0") for d in DIMENSIONS}
        dimension_weights: Dict[str, Decimal] = {d: Decimal("0") for d in DIMENSIONS}
        dimension_sources: Dict[str, List[SignalSource]] = {d: [] for d in DIMENSIONS}
        confidence_num: Dict[str, Decimal] = {d: Decimal("0") for d in DIMENSIONS}
        confidence_den: Dict[str, Decimal] = {d: Decimal("0") for d in DIMENSIONS}

        for ev in evidence_scores:
            src = ev.source if isinstance(ev.source, SignalSource) else SignalSource(str(ev.source))
            mapping = self.mappings.get(src)
            if not mapping:
                continue

            ev_conf = Decimal(str(ev.confidence))
            raw = Decimal(str(ev.raw_score))
            rel = Decimal(str(mapping.reliability))
            effective_score = raw * ev_conf * rel

            def _add(dim: str, weight: Decimal) -> None:
                dimension_sums[dim] += effective_score * weight
                dimension_weights[dim] += weight * ev_conf * rel
                confidence_num[dim] += ev_conf * rel * weight
                confidence_den[dim] += weight
                dimension_sources[dim].append(src)

            _add(mapping.primary_dimension, Decimal(str(mapping.primary_weight)))
            for dim, weight in mapping.secondary_mappings.items():
                _add(dim, Decimal(str(weight)))

        out: Dict[str, AggregatedDimensionScore] = {}
        for dim in DIMENSIONS:
            if dimension_weights[dim] > 0:
                score = (dimension_sums[dim] / dimension_weights[dim]).quantize(Decimal("0.01"))
                score = max(Decimal("0"), min(Decimal("100"), score))
                conf = (
                    (confidence_num[dim] / confidence_den[dim]).quantize(Decimal("0.001"))
                    if confidence_den[dim] > 0
                    else Decimal("0.500")
                )
            else:
                score = Decimal("50.00")
                conf = Decimal("0.500")

            out[dim] = AggregatedDimensionScore(
                dimension=dim,
                score=score,
                contributing_sources=dimension_sources[dim],
                total_weight=dimension_weights[dim].quantize(Decimal("0.0001")),
                confidence=max(Decimal("0"), min(Decimal("1"), conf)),
            )

        return out

    def get_coverage_report(self, evidence_scores: List[EvidenceScore]) -> Dict[str, Dict[str, float | bool | int]]:
        mapped = self.map_evidence_to_dimensions(evidence_scores)
        report: Dict[str, Dict[str, float | bool | int]] = {}
        for dim in DIMENSIONS:
            res = mapped[dim]
            unique_sources = len(set(res.contributing_sources))
            report[dim] = {
                "has_evidence": unique_sources > 0,
                "source_count": unique_sources,
                "total_weight": float(res.total_weight),
                "confidence": float(res.confidence),
            }
        return report


@dataclass(frozen=True)
class EvidenceItem:
    source: str                # "document_chunk" | "external_signal" | "company_summary"
    evidence_type: str         # e.g., "10-K", "jobs", "news"
    text: str
    url: str | None = None
    published_at: str | None = None


@dataclass(frozen=True)
class MappedEvidence:
    dimension: str
    matched_keywords: List[str]
    item: EvidenceItem


# Simple keyword taxonomy (deterministic + explainable)
DIMENSION_KEYWORDS: Dict[str, List[str]] = {
    "data_infrastructure": [
        "data lake", "data warehouse", "etl", "pipeline", "spark", "snowflake", "databricks",
        "governance of data", "data quality", "master data", "metadata", "lineage",
    ],
    "ai_governance": [
        "model risk", "responsible ai", "ai governance", "policy", "compliance", "privacy",
        "security", "bias", "audit", "controls", "risk management",
    ],
    "technology_stack": [
        "cloud", "aws", "azure", "gcp", "kubernetes", "mlops", "api", "microservice",
        "vector database", "llm", "cortex", "bedrock", "sagemaker",
    ],
    "talent_skills": [
        "data scientist", "machine learning engineer", "ml engineer", "data engineer",
        "ai engineer", "mlops", "analytics", "python", "sql",
    ],
    "leadership_vision": [
        "strategy", "roadmap", "executive", "ceo", "cio", "chief data", "chief ai",
        "investment", "transformation", "innovation",
    ],
    "use_case_portfolio": [
        "use case", "pilot", "production", "deployment", "predictive", "forecast",
        "recommendation", "fraud", "optimization", "automation", "genai",
    ],
    "culture_change": [
        "training", "change management", "culture", "adoption", "upskilling", "reskilling",
        "agile", "cross-functional", "center of excellence", "coe",
    ],
}


def _normalize(text: str) -> str:
    return (text or "").lower()


def map_evidence_to_dimensions(items: List[EvidenceItem]) -> List[MappedEvidence]:
    mapped: List[MappedEvidence] = []

    for item in items:
        t = _normalize(item.text)

        for dim, keywords in DIMENSION_KEYWORDS.items():
            hits = [kw for kw in keywords if kw in t]
            if hits:
                mapped.append(
                    MappedEvidence(
                        dimension=dim,
                        matched_keywords=hits[:8],  # cap for readability
                        item=item,
                    )
                )
    return mapped


@dataclass(frozen=True)
class DimensionFeature:
    dimension: str
    weighted_signal: float
    evidence_count: int
    reliability_weighted: float
    top_keywords: List[str]


def _infer_signal_bucket(item: EvidenceItem) -> str:
    """
    Normalize different evidence types into spec buckets.
    """
    t = (item.evidence_type or "").strip().lower()
    normalized = t.replace("-", "_").replace(" ", "_")

    # Explicit 9-source mapping from the scoring design.
    explicit = {
        "technology_hiring": "technology_hiring",
        "jobs": "technology_hiring",
        "innovation_activity": "innovation_activity",
        "patents": "innovation_activity",
        "digital_presence": "digital_presence",
        "tech": "digital_presence",
        "leadership_signals": "leadership_signals",
        "news": "leadership_signals",
        "sec_item_1": "sec_item_1",
        "sec_item_1a": "sec_item_1a",
        "sec_item_7": "sec_item_7",
        "glassdoor_reviews": "glassdoor_reviews",
        "board_composition": "board_composition",
        "10k": "10k",
    }
    if normalized in explicit:
        return explicit[normalized]

    # Heuristic fallback for noisy historical labels.
    if "item 1a" in t:
        return "sec_item_1a"
    if "item 7" in t and "item 7a" not in t:
        return "sec_item_7"
    if "item 1" in t and "item 1a" not in t:
        return "sec_item_1"
    if "glassdoor" in t and "review" in t:
        return "glassdoor_reviews"
    if "board" in t and ("composition" in t or "proxy" in t):
        return "board_composition"
    if "10-k" in t or "10k" in t or "10 q" in t or "10-q" in t:
        return "10k"
    if "job" in t or "hiring" in t:
        return "technology_hiring"
    if "patent" in t or "innovation" in t:
        return "innovation_activity"
    if "tech" in t or "stack" in t or "digital" in t:
        return "digital_presence"
    return "leadership_signals"  # safe default bucket


def build_source_payloads(mapped: List[MappedEvidence]) -> Dict[str, dict]:
    """
    Converts raw mapped evidence into per-source payloads:
    { source: {count: int, keywords: {kw: freq}} }
    """
    payloads: Dict[str, dict] = {}
    for m in mapped:
        src = _infer_signal_bucket(m.item)
        if src not in payloads:
            payloads[src] = {"count": 0, "keywords": {}}
        payloads[src]["count"] += 1
        for kw in m.matched_keywords:
            payloads[src]["keywords"][kw] = payloads[src]["keywords"].get(kw, 0) + 1
    return payloads


def map_sources_to_dimension_features(source_payloads: Dict[str, dict]) -> Dict[str, DimensionFeature]:
    """
    Weighted mapping matrix + reliability.
    Returns features per dimension that rubric scorer will consume.
    """
    acc: Dict[str, Dict[str, Any]] = {
        d: {"weighted_signal": 0.0, "evidence_count": 0, "reliability_weighted": 0.0, "keywords": {}}
        for d in DIMENSIONS
    }
    for source_name, payload in source_payloads.items():
        prof = SOURCE_PROFILES.get(source_name)
        if not prof:
            continue
        w = normalize_weights(prof.dim_weights)
        count = int(payload.get("count", 0))
        kws = payload.get("keywords", {}) or {}
        for dim, dim_w in w.items():
            contrib = float(count) * float(dim_w) * float(prof.reliability)
            acc[dim]["weighted_signal"] += contrib
            acc[dim]["evidence_count"] += count
            acc[dim]["reliability_weighted"] += float(dim_w) * float(prof.reliability)
            for k, v in kws.items():
                acc[dim]["keywords"][k] = acc[dim]["keywords"].get(k, 0) + int(v)
    out: Dict[str, DimensionFeature] = {}
    for dim in DIMENSIONS:
        topk = sorted(acc[dim]["keywords"].items(), key=lambda x: x[1], reverse=True)[:5]
        out[dim] = DimensionFeature(
            dimension=dim,
            weighted_signal=float(acc[dim]["weighted_signal"]),
            evidence_count=int(acc[dim]["evidence_count"]),
            reliability_weighted=float(acc[dim]["reliability_weighted"]),
            top_keywords=[k for k, _ in topk],
        )
    return out
