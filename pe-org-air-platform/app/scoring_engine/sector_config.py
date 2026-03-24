from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class SectorProfile:
    sector_name: str
    weights: Dict[str, float]          # dimension -> weight (should sum to ~1)
    hr_baseline_value: Optional[float] # stored but used later (HR baseline)


def normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    s = sum(max(0.0, float(v)) for v in weights.values())
    if s <= 0:
        # fall back to uniform weights if bad config
        n = max(1, len(weights))
        return {k: 1.0 / n for k in weights.keys()}
    return {k: float(v) / s for k, v in weights.items()}


def get_company_sector(cur, company_id: str) -> str:
    """
    Resolve sector via companies -> industries.sector.
    Falls back to 'Services' if missing.
    """
    cur.execute(
        """
        SELECT i.sector
        FROM companies c
        LEFT JOIN industries i ON c.industry_id = i.id
        WHERE c.id = %s
        LIMIT 1
        """,
        (company_id,),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return "Services"
    return str(row[0])


def load_sector_profile(cur, sector_name: str, version: str = "v1.0") -> SectorProfile:
    """
    Load weights + hr baseline from sector_baselines table (seeded earlier).
    Expects one row per (sector, dimension, version).
    """
    cur.execute(
        """
        SELECT dimension, weight, hr_baseline_value
        FROM sector_baselines
        WHERE sector_name = %s AND version = %s
        """,
        (sector_name, version),
    )
    rows = cur.fetchall() or []

    weights: Dict[str, float] = {}
    hr_base: Optional[float] = None

    for dim, w, hr in rows:
        if dim is None:
            continue
        weights[str(dim)] = float(w) if w is not None else 0.0
        if hr_base is None and hr is not None:
            hr_base = float(hr)

    if not weights:
        # defensive fallback: uniform across typical 7 dimensions
        default_dims = [
            "data_infrastructure",
            "ai_governance",
            "technology_stack",
            "talent_skills",
            "leadership_vision",
            "use_case_portfolio",
            "culture_change",
        ]
        weights = {d: 1.0 / len(default_dims) for d in default_dims}

    weights = normalize_weights(weights)
    return SectorProfile(sector_name=sector_name, weights=weights, hr_baseline_value=hr_base)
