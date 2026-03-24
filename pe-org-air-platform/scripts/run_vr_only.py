from __future__ import annotations
import argparse
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from app.services.snowflake import get_snowflake_connection
from app.scoring_engine.sector_config import get_company_sector, load_sector_profile
from app.scoring_engine.vr_model import fetch_dimension_inputs, compute_vr_score
from app.scoring_engine.hr_baselines import compute_hr_factor, apply_hr_adjustment_to_talent
from app.scoring_engine.synergy import load_synergy_rules, compute_synergy
from app.scoring_engine.talent_penalty import compute_talent_penalty

def get_latest_assessment_id(cur, company_id: str) -> str:
    cur.execute(
        """
        SELECT id
        FROM assessments
        WHERE company_id = %s
        ORDER BY assessment_date DESC, created_at DESC
        LIMIT 1
        """,
        (company_id,),
    )
    row = cur.fetchone()
    if not row:
        raise SystemExit(f"No assessments found for company_id={company_id}")
    return str(row[0])
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--company-id", required=True)
    parser.add_argument("--version", default="v1.0")
    args = parser.parse_args()
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        assessment_id = get_latest_assessment_id(cur, args.company_id)
        sector = get_company_sector(cur, args.company_id)
        profile = load_sector_profile(cur, sector, version=args.version)
        dims = fetch_dimension_inputs(cur, assessment_id)
                # ---- HR Baseline Adjustment (A2) ----
        hr = compute_hr_factor(cur, company_id=args.company_id, sector_name=sector, version=args.version)
        # Apply only to talent_skills
        adjusted_dims = []
        for d in dims:
            adjusted_score = apply_hr_adjustment_to_talent(
                dimension=d.dimension,
                raw_score=d.raw_score,
                hr_factor=hr.hr_factor,
            )
            adjusted_dims.append(
                type(d)(  # DimensionInput
                    dimension=d.dimension,
                    raw_score=adjusted_score,
                    confidence=d.confidence,
                    evidence_count=d.evidence_count,
                )
            )

        
        scores_by_dim = {d.dimension: d.raw_score for d in adjusted_dims}

        rules = load_synergy_rules(cur, version=args.version)
        syn = compute_synergy(scores_by_dim, rules, cap_abs=15.0)

        print("\n---- Synergy ----")
        print(f"synergy_bonus: {syn.synergy_bonus:.2f} (cap=±{syn.cap:.1f})")
        for h in syn.hits:
            if h.activated:
                print(f"✅ {h.dim_a} x {h.dim_b} [{h.synergy_type}] {h.magnitude:+.2f} ({h.reason})")

        pen = compute_talent_penalty(cur, company_id=args.company_id, version=args.version)

        vr, breakdown = compute_vr_score(adjusted_dims, profile.weights)
        print("\n==== VR RESULT ====")
        print(f"company_id:    {args.company_id}")
        print(f"assessment_id: {assessment_id}")
        print(f"sector:        {sector}")
        print(f"version:       {args.version}")
        print(f"VR (0-100):    {vr:.2f}")
        print("\n---- HR Baseline (Talent Adjustment) ----")
        print(f"baseline_value:   {hr.baseline_value:.2f}")
        print(f"jobs_signal_cnt:  {hr.jobs_signal_count}")
        print(f"hr_factor:        {hr.hr_factor:.3f}")
        print(f"method:           {hr.method}")
        print(f"window_days:      {hr.window_days}")
        print(f"rules_loaded: {len(rules)}")
        print("\n---- Dimension Breakdown ----")
        print("\n---- Talent Concentration Penalty (HHI) ----")
        print(f"sample_size:     {pen.sample_size} (min_met={pen.min_sample_met})")
        print(f"hhi_value:       {pen.hhi_value:.3f}")
        print(f"penalty_factor:  {pen.penalty_factor:.3f}")
        print(f"function_counts: {pen.function_counts}")
        # Keep it readable in terminal:
        for b, d in zip(breakdown, dims):
            print(
                f"{d.dimension:18s} raw={b['raw_score']:6.2f} "
                f"w={b['sector_weight']:.3f} conf={b['confidence']:.2f} "
                f"used={b['confidence_used']:.2f} contrib={b['weighted_score']:.2f}"
            )
        return 0
    finally:
        cur.close()
        conn.close()
if __name__ == "__main__":
    raise SystemExit(main())
