from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from uuid import uuid4

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.snowflake import get_snowflake_connection
from app.scoring_engine.sem_confidence import DIMENSIONS, compute_sem_confidence_intervals


def _latest_assessment_id(cur, company_id: str) -> str:
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
        raise SystemExit(f"No assessment found for company_id={company_id}")
    return str(row[0])


def _fetch_dimension_vector(cur, assessment_id: str) -> list[float]:
    cur.execute(
        """
        SELECT dimension, score
        FROM dimension_scores
        WHERE assessment_id = %s
        """,
        (assessment_id,),
    )
    rows = cur.fetchall() or []
    m = {str(d): float(s) for d, s in rows}
    return [float(m.get(dim, 0.0)) for dim in DIMENSIONS]


def _fetch_targets_for_run(cur, run_id: str) -> list[tuple[str, str, float]]:
    """
    Return list of (org_air_score_id, company_id, composite_score) for the run.
    """
    cur.execute(
        """
        SELECT id, company_id, composite_score
        FROM org_air_scores
        WHERE scoring_run_id = %s
        ORDER BY scored_at ASC
        """,
        (run_id,),
    )
    out = []
    for sid, cid, comp in cur.fetchall() or []:
        out.append((str(sid), str(cid), float(comp)))
    if not out:
        raise SystemExit(f"No org_air_scores found for run_id={run_id}")
    return out


def _audit(cur, run_id: str, company_id: str, step: str, input_obj: dict, output_obj: dict) -> None:
    cur.execute(
        """
        INSERT INTO scoring_audit_log (id, scoring_run_id, company_id, step_name, input_json, output_json)
        SELECT
          %s,
          %s,
          %s,
          %s,
          PARSE_JSON(%s),
          PARSE_JSON(%s)
        """,
        (
            str(uuid4()),
            run_id,
            company_id,
            step,
            json.dumps(input_obj),
            json.dumps(output_obj),
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--bootstrap-samples", type=int, default=400)
    args = parser.parse_args()

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        targets = _fetch_targets_for_run(cur, args.run_id)

        X_rows = []
        y_vals = []
        company_ids = []
        score_ids = []

        for score_id, company_id, composite in targets:
            assessment_id = _latest_assessment_id(cur, company_id)
            vec = _fetch_dimension_vector(cur, assessment_id)

            score_ids.append(score_id)
            company_ids.append(company_id)
            X_rows.append(vec)
            y_vals.append(composite)

        X = np.array(X_rows, dtype=float)
        y = np.array(y_vals, dtype=float)

        results, fit = compute_sem_confidence_intervals(
            X=X,
            y=y,
            bootstrap_samples=args.bootstrap_samples,
        )

        # Update org_air_scores and log audit row
        for i in range(len(results)):
            r = results[i]
            cur.execute(
                """
                UPDATE org_air_scores
                SET sem_lower = %s,
                    sem_upper = %s
                WHERE id = %s
                """,
                (r.lower, r.upper, score_ids[i]),
            )

            _audit(
                cur,
                args.run_id,
                company_ids[i],
                "sem_confidence",
                {"bootstrap_samples": args.bootstrap_samples},
                {
                    "sem_lower": r.lower,
                    "sem_upper": r.upper,
                    "standard_error": r.standard_error,
                    "method_used": r.method_used,
                    "model_fit_index": r.model_fit_index,
                    "global_fit": fit,
                },
            )

        conn.commit()
        print("✅ SEM confidence intervals applied")
        print(f"run_id: {args.run_id}")
        print(f"fit: {fit}")
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
