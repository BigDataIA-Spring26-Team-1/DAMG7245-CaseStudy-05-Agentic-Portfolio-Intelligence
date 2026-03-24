from __future__ import annotations 
import sys
from pathlib import Path
from uuid import uuid4
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from app.services.snowflake import get_snowflake_connection
DIMENSIONS = [
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent_skills",
    "leadership_vision",
    "use_case_portfolio",
    "culture_change",
]
# Must match industries.sector values in your schema seed MERGE
SECTORS = ["Industrials", "Healthcare", "Services", "Consumer", "Financial"]
VERSION = "v1.0"
SECTOR_WEIGHTS = {
    "Industrials": {
        "data_infrastructure": 0.16, "ai_governance": 0.10, "technology_stack": 0.16,
        "talent_skills": 0.12, "leadership_vision": 0.14, "use_case_portfolio": 0.18, "culture_change": 0.14,
    },
    "Healthcare": {
        "data_infrastructure": 0.14, "ai_governance": 0.18, "technology_stack": 0.12,
        "talent_skills": 0.12, "leadership_vision": 0.12, "use_case_portfolio": 0.14, "culture_change": 0.18,
    },
    "Services": {
        "data_infrastructure": 0.15, "ai_governance": 0.12, "technology_stack": 0.13,
        "talent_skills": 0.16, "leadership_vision": 0.14, "use_case_portfolio": 0.16, "culture_change": 0.14,
    },
    "Consumer": {
        "data_infrastructure": 0.14, "ai_governance": 0.10, "technology_stack": 0.14,
        "talent_skills": 0.12, "leadership_vision": 0.14, "use_case_portfolio": 0.20, "culture_change": 0.16,
    },
    "Financial": {
        "data_infrastructure": 0.16, "ai_governance": 0.20, "technology_stack": 0.14,
        "talent_skills": 0.12, "leadership_vision": 0.12, "use_case_portfolio": 0.12, "culture_change": 0.14,
    },
}
HR_BASELINES = {
    "Industrials": 72.0,
    "Healthcare": 78.0,
    "Services": 75.0,
    "Consumer": 70.0,
    "Financial": 80.0,
}
SYNERGY_RULES = [
    ("data_infrastructure", "technology_stack", "positive", 60.0, 3.0),
    ("ai_governance", "culture_change", "positive", 60.0, 3.0),
    ("leadership_vision", "use_case_portfolio", "positive", 60.0, 2.5),
    ("talent_skills", "use_case_portfolio", "positive", 60.0, 2.5),
    ("use_case_portfolio", "ai_governance", "negative", 60.0, -3.0),
]
TALENT_PENALTY = {
    "hhi_threshold_mild": 0.40,
    "hhi_threshold_severe": 0.70,
    "penalty_factor_mild": 0.95,
    "penalty_factor_severe": 0.85,
    "min_sample_size": 15,
}
def _normalize(weights: dict[str, float]) -> dict[str, float]:
    s = sum(max(0.0, float(v)) for v in weights.values())
    if s <= 0:
        return {d: 1.0 / len(DIMENSIONS) for d in DIMENSIONS}
    return {k: float(v) / s for k, v in weights.items()}
def seed_sector_baselines(cur) -> int:
    upserts = 0
    for sector in SECTORS:
        weights = _normalize(SECTOR_WEIGHTS[sector])
        hr_base = float(HR_BASELINES.get(sector, 75.0))
        for dim in DIMENSIONS:
            stmt = """
            MERGE INTO sector_baselines t
            USING (SELECT %s AS sector_name, %s AS dimension, %s AS version) s
               ON t.sector_name = s.sector_name AND t.dimension = s.dimension AND t.version = s.version
            WHEN MATCHED THEN
              UPDATE SET weight = %s, hr_baseline_value = %s
            WHEN NOT MATCHED THEN
              INSERT (id, sector_name, dimension, weight, hr_baseline_value, version)
              VALUES (%s, %s, %s, %s, %s, %s)
            """
            new_id = str(uuid4())
            cur.execute(
                stmt,
                (sector, dim, VERSION, weights[dim], hr_base, new_id, sector, dim, weights[dim], hr_base, VERSION),
            )
            upserts += 1
    return upserts
def seed_synergy_config(cur) -> int:
    upserts = 0
    for dim_a, dim_b, s_type, threshold, magnitude in SYNERGY_RULES:
        stmt = """
        MERGE INTO synergy_config t
        USING (SELECT %s AS dimension_a, %s AS dimension_b, %s AS synergy_type, %s AS version) s
           ON t.dimension_a = s.dimension_a AND t.dimension_b = s.dimension_b
          AND t.synergy_type = s.synergy_type AND t.version = s.version
        WHEN MATCHED THEN
          UPDATE SET threshold = %s, magnitude = %s
        WHEN NOT MATCHED THEN
          INSERT (id, dimension_a, dimension_b, synergy_type, threshold, magnitude, version)
          VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        new_id = str(uuid4())
        cur.execute(
            stmt,
            (dim_a, dim_b, s_type, VERSION, threshold, magnitude,
             new_id, dim_a, dim_b, s_type, threshold, magnitude, VERSION),
        )
        upserts += 1
    return upserts
def seed_talent_penalty_config(cur) -> int:
    stmt = """
    MERGE INTO talent_penalty_config t
    USING (SELECT %s AS version) s
       ON t.version = s.version
    WHEN MATCHED THEN
      UPDATE SET
        hhi_threshold_mild = %s,
        hhi_threshold_severe = %s,
        penalty_factor_mild = %s,
        penalty_factor_severe = %s,
        min_sample_size = %s
    WHEN NOT MATCHED THEN
      INSERT (id, hhi_threshold_mild, hhi_threshold_severe, penalty_factor_mild, penalty_factor_severe, min_sample_size, version)
      VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    new_id = str(uuid4())
    cur.execute(
        stmt,
        (VERSION,
         TALENT_PENALTY["hhi_threshold_mild"],
         TALENT_PENALTY["hhi_threshold_severe"],
         TALENT_PENALTY["penalty_factor_mild"],
         TALENT_PENALTY["penalty_factor_severe"],
         TALENT_PENALTY["min_sample_size"],
         new_id,
         TALENT_PENALTY["hhi_threshold_mild"],
         TALENT_PENALTY["hhi_threshold_severe"],
         TALENT_PENALTY["penalty_factor_mild"],
         TALENT_PENALTY["penalty_factor_severe"],
         TALENT_PENALTY["min_sample_size"],
         VERSION),
    )
    return 1
def main() -> int:
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        a = seed_sector_baselines(cur)
        b = seed_synergy_config(cur)
        c = seed_talent_penalty_config(cur)
        conn.commit()
        print(f"✅ Seeded sector_baselines: {a} rows")
        print(f"✅ Seeded synergy_config: {b} rows")
        print(f"✅ Seeded talent_penalty_config: {c} row")
        print(f"✅ Config version: {VERSION}")
        return 0
    finally:
        cur.close()
        conn.close()
if __name__ == "__main__":
    raise SystemExit(main())
 
