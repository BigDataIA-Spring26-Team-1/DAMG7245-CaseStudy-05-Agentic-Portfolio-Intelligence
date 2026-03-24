from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple
import re


@dataclass(frozen=True)
class TalentPenaltyConfig:
    hhi_threshold_mild: float
    hhi_threshold_severe: float
    penalty_factor_mild: float
    penalty_factor_severe: float
    min_sample_size: int
    version: str


@dataclass(frozen=True)
class TalentPenaltyResult:
    hhi_value: float
    penalty_factor: float       # 0.0–1.0 multiplier
    penalty_points: float       # optional points drag (we’ll use multiplier later)
    sample_size: int
    min_sample_met: bool
    function_counts: Dict[str, int]


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


FUNCTION_KEYWORDS: list[tuple[str, list[str]]] = [
    ("data_engineering", ["data engineer", "etl", "pipeline", "spark", "dbt"]),
    ("ml_engineering", ["ml engineer", "machine learning engineer", "mlops", "model deployment"]),
    ("data_science", ["data scientist", "research scientist", "applied scientist"]),
    ("analytics", ["data analyst", "business analyst", "analytics", "bi analyst"]),
    ("ai_research", ["ai researcher", "research", "nlp", "computer vision", "deep learning"]),
    ("software_engineering", ["software engineer", "backend", "platform engineer"]),
]


def load_talent_penalty_config(cur, version: str = "v1.0") -> TalentPenaltyConfig:
    cur.execute(
        """
        SELECT hhi_threshold_mild, hhi_threshold_severe,
               penalty_factor_mild, penalty_factor_severe,
               min_sample_size, version
        FROM talent_penalty_config
        WHERE version = %s
        LIMIT 1
        """,
        (version,),
    )
    row = cur.fetchone()
    if not row:
        # safe defaults
        return TalentPenaltyConfig(
            hhi_threshold_mild=0.40,
            hhi_threshold_severe=0.70,
            penalty_factor_mild=0.95,
            penalty_factor_severe=0.85,
            min_sample_size=15,
            version=version,
        )

    return TalentPenaltyConfig(
        hhi_threshold_mild=float(row[0]),
        hhi_threshold_severe=float(row[1]),
        penalty_factor_mild=float(row[2]),
        penalty_factor_severe=float(row[3]),
        min_sample_size=int(row[4]),
        version=str(row[5]),
    )


def _classify_job_function(text: str) -> str:
    t = (text or "").lower()
    for func, kws in FUNCTION_KEYWORDS:
        for kw in kws:
            if kw in t:
                return func
    return "other"


def _extract_function_from_metadata(metadata) -> str | None:
    # Snowflake VARIANT comes back as dict-like in python connector
    if not metadata:
        return None
    if isinstance(metadata, dict):
        for k in ["function", "job_function", "category", "team"]:
            if k in metadata and metadata[k]:
                return str(metadata[k]).lower()
    return None


def fetch_job_functions(cur, company_id: str, window_days: int = 365) -> List[str]:
    """
    Pull job signals and extract a function category.
    Uses metadata if present; else classifies from title + content_text.
    """
    cur.execute(
        """
        SELECT title, content_text, metadata
        FROM external_signals
        WHERE company_id = %s
          AND signal_type = 'jobs'
          AND collected_at >= DATEADD(day, -%s, CURRENT_TIMESTAMP())
        """,
        (company_id, window_days),
    )
    rows = cur.fetchall() or []
    functions: List[str] = []

    for title, content_text, metadata in rows:
        meta_func = _extract_function_from_metadata(metadata)
        if meta_func:
            functions.append(meta_func)
            continue

        text = f"{title or ''} {content_text or ''}"
        functions.append(_classify_job_function(text))

    return functions


def compute_hhi(functions: List[str]) -> Tuple[float, Dict[str, int]]:
    counts: Dict[str, int] = {}
    for f in functions:
        counts[f] = counts.get(f, 0) + 1

    n = len(functions)
    if n == 0:
        return 0.0, counts

    hhi = 0.0
    for _, c in counts.items():
        share = c / n
        hhi += share * share

    return float(hhi), counts


def compute_talent_penalty(
    cur,
    *,
    company_id: str,
    version: str = "v1.0",
    window_days: int = 365,
) -> TalentPenaltyResult:
    cfg = load_talent_penalty_config(cur, version=version)
    functions = fetch_job_functions(cur, company_id, window_days=window_days)
    sample_size = len(functions)

    # Edge case: not enough samples -> no penalty
    if sample_size < cfg.min_sample_size:
        hhi, counts = compute_hhi(functions)
        return TalentPenaltyResult(
            hhi_value=hhi,
            penalty_factor=1.0,
            penalty_points=0.0,
            sample_size=sample_size,
            min_sample_met=False,
            function_counts=counts,
        )

    hhi, counts = compute_hhi(functions)

    # Determine penalty factor
    if hhi >= cfg.hhi_threshold_severe:
        factor = cfg.penalty_factor_severe
    elif hhi >= cfg.hhi_threshold_mild:
        factor = cfg.penalty_factor_mild
    else:
        factor = 1.0

    factor = clamp(float(factor), 0.0, 1.0)

    # penalty_points optional; keep for explainability if you later switch to additive
    penalty_points = 0.0

    return TalentPenaltyResult(
        hhi_value=hhi,
        penalty_factor=factor,
        penalty_points=penalty_points,
        sample_size=sample_size,
        min_sample_met=True,
        function_counts=counts,
    )
