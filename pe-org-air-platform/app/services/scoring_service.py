from __future__ import annotations
 
from typing import List, Optional
from uuid import uuid4
 
from app.services.snowflake import get_snowflake_connection
from app.scoring_engine.composite import compute_composite
from scripts.run_scoring_engine import score_one_company, insert_scoring_run, update_scoring_run_status
 
 
def compute_for_companies(
    company_ids: List[str],
    version: str = "v1.0",
    model_version: str = "cs3-scoring-v2",
) -> str:
    """
    Executes scoring pipeline for one or multiple companies.
    Returns run_id.
    """
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    run_id = None
 
    try:
        run_id = insert_scoring_run(
            cur,
            companies_scored=company_ids,
            model_version=model_version,
            params={"version": version, "batch": len(company_ids) > 1},
        )
 
        for cid in company_ids:
            score_one_company(
                cur,
                company_id=cid,
                version=version,
                run_id=run_id,
            )
 
        update_scoring_run_status(cur, run_id, "success")
        conn.commit()
        return run_id
 
    except Exception:
        if run_id:
            try:
                update_scoring_run_status(cur, run_id, "failed")
                conn.commit()
            except Exception:
                pass
        raise
 
    finally:
        cur.close()
        conn.close()
 
 