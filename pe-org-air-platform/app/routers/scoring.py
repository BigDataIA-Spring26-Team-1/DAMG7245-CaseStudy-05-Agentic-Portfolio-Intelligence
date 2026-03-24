from __future__ import annotations
 
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
 
from fastapi import APIRouter, HTTPException, Query
 
from app.config import settings
from app.models.scoring import OrgAIRScoreOut, DimensionBreakdown, SynergyDetail, TalentPenaltyDetail, SEMResult
from app.services.redis_cache import cache_delete, cache_delete_pattern, cache_get_json, cache_set_json
from app.services.snowflake import get_snowflake_connection
 
router = APIRouter(prefix="/api/v1/scoring", tags=["scoring"])
 
ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run_scoring_engine.py"
 
 
def _company_result_cache_key(company_id: str) -> str:
    return f"scoring:results:company:{company_id}"
 
 
def _results_list_cache_key(limit: int) -> str:
    return f"scoring:results:list:limit:{limit}"
 
 
def _parse_breakdown(row_variant: Any) -> Dict[str, Any]:
    if row_variant is None:
        return {}
    # snowflake connector returns dict-like for VARIANT in many cases
    if isinstance(row_variant, (dict, list)):
        return row_variant if isinstance(row_variant, dict) else {"_": row_variant}
    # fallback: try JSON string
    try:
        return json.loads(row_variant)
    except Exception:
        return {}
 
 
def _latest_score_for_company(cur, company_id: str) -> Optional[dict]:
    cur.execute(
        """
        SELECT
          company_id,
          assessment_id,
          scoring_run_id,
          vr_score,
          synergy_bonus,
          talent_penalty,
          sem_lower,
          sem_upper,
          composite_score,
          score_band,
          dimension_breakdown_json,
          scored_at
        FROM org_air_scores
        WHERE company_id = %s
        ORDER BY scored_at DESC, created_at DESC
        LIMIT 1
        """,
        (company_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "company_id": str(row[0]),
        "assessment_id": str(row[1]) if row[1] else None,
        "scoring_run_id": str(row[2]) if row[2] else None,
        "vr_score": float(row[3] or 0),
        "synergy_bonus": float(row[4] or 0),
        "talent_penalty": float(row[5] or 0),
        "sem_lower": float(row[6]) if row[6] is not None else None,
        "sem_upper": float(row[7]) if row[7] is not None else None,
        "composite_score": float(row[8] or 0),
        "score_band": str(row[9] or ""),
        "breakdown": _parse_breakdown(row[10]),
        "scored_at": row[11],
    }
 
 
def _to_out(payload: dict) -> OrgAIRScoreOut:
    bd = payload.get("breakdown", {}) or {}
 
    dim_breakdown: List[DimensionBreakdown] = []
    vr = bd.get("vr", {}) or {}
    for item in (vr.get("dimension_breakdown") or []):
        if not isinstance(item, dict):
            continue
        dimension = item.get("dimension") or item.get("dim")
        if not dimension:
            continue
        # item keys come from your vr_model breakdown
        dim_breakdown.append(
            DimensionBreakdown(
                dimension=str(dimension),
                raw_score=float(item.get("raw_score", 0.0)),
                sector_weight=float(item.get("sector_weight", item.get("weight", 0.0))),
                weighted_score=float(item.get("weighted_score", item.get("weighted_contribution", 0.0))),
                confidence=float(item.get("confidence_used", item.get("confidence", 0.0)) or 0.0),
                evidence_count=int(item.get("evidence_count", 0)),
            )
        )
 
    synergy_hits: List[SynergyDetail] = []
    syn = bd.get("synergy", {}) or {}
    for h in (syn.get("hits") or []):
        synergy_hits.append(
            SynergyDetail(
                dim_a=h.get("dim_a"),
                dim_b=h.get("dim_b"),
                type=h.get("type"),
                threshold=float(h.get("threshold", 0.0)),
                magnitude=float(h.get("magnitude", 0.0)),
                activated=bool(h.get("activated", False)),
                reason=h.get("reason", ""),
            )
        )
 
    tp = bd.get("talent_penalty", {}) or {}
    tp_detail = None
    if tp:
        tp_detail = TalentPenaltyDetail(
            sample_size=int(tp.get("sample_size", 0)),
            min_sample_met=bool(tp.get("min_sample_met", False)),
            hhi_value=float(tp.get("hhi_value", 0.0)),
            penalty_factor=float(tp.get("penalty_factor", 1.0)),
            function_counts=dict(tp.get("function_counts", {}) or {}),
        )
 
    sem_bd = bd.get("sem")
    sem_out = None
    if isinstance(sem_bd, dict):
        sem_out = SEMResult(
            lower=sem_bd.get("lower"),
            upper=sem_bd.get("upper"),
            standard_error=sem_bd.get("standard_error"),
            method_used=sem_bd.get("method_used"),
            fit=sem_bd.get("fit"),
        )
 
    return OrgAIRScoreOut(
        company_id=payload["company_id"],
        assessment_id=payload.get("assessment_id"),
        scoring_run_id=payload.get("scoring_run_id"),
        vr_score=payload["vr_score"],
        synergy_bonus=payload["synergy_bonus"],
        talent_penalty=payload["talent_penalty"],
        sem_lower=payload.get("sem_lower"),
        sem_upper=payload.get("sem_upper"),
        composite_score=payload["composite_score"],
        score_band=payload["score_band"],
        dimension_breakdown=dim_breakdown,
        synergy_hits=synergy_hits,
        talent_penalty_detail=tp_detail,
        sem=sem_out,
        scored_at=payload.get("scored_at"),
    )
 
 
@router.post("/compute/{company_id}")
def compute_company(company_id: str, version: str = Query(default="v1.0")) -> Dict[str, str]:
    """
    Triggers the scoring pipeline for a company by invoking scripts/run_scoring_engine.py.
    Returns the scoring_run_id printed by the script.
    """
    if not RUNNER.exists():
        raise HTTPException(status_code=500, detail=f"Scoring runner not found at {RUNNER}")
 
    cmd = [sys.executable, str(RUNNER), "--company-id", company_id, "--version", version]
    proc = subprocess.run(cmd, capture_output=True, text=True)
 
    if proc.returncode != 0:
        raise HTTPException(status_code=500, detail=f"Scoring failed: {proc.stderr or proc.stdout}")
 
    # Extract run_id from stdout lines: "run_id: <uuid>"
    run_id = None
    for line in proc.stdout.splitlines():
        if line.lower().startswith("run_id:"):
            run_id = line.split(":", 1)[1].strip()
            break
 
    cache_delete(_company_result_cache_key(company_id))
    cache_delete_pattern("scoring:results:list:*")
    return {"status": "submitted", "run_id": run_id or ""}
 
 
@router.get("/results/{company_id}", response_model=OrgAIRScoreOut)
def get_latest_company_result(company_id: str) -> OrgAIRScoreOut:
    cache_key = _company_result_cache_key(company_id)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return OrgAIRScoreOut(**cached)
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        rec = _latest_score_for_company(cur, company_id)
        if not rec:
            raise HTTPException(status_code=404, detail="No scores found for company")
        out = _to_out(rec)
        cache_set_json(cache_key, out.model_dump(mode="json"), settings.redis_ttl_seconds)
        return out
    finally:
        cur.close()
        conn.close()
 
 
@router.get("/results", response_model=List[OrgAIRScoreOut])
def get_latest_results_all(limit: int = Query(default=50, ge=1, le=200)) -> List[OrgAIRScoreOut]:
    cache_key = _results_list_cache_key(limit)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return [OrgAIRScoreOut(**x) for x in cached]
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH latest AS (
              SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY scored_at DESC, created_at DESC) AS rn
              FROM org_air_scores
            )
            SELECT
              company_id, assessment_id, scoring_run_id,
              vr_score, synergy_bonus, talent_penalty,
              sem_lower, sem_upper,
              composite_score, score_band,
              dimension_breakdown_json, scored_at
            FROM latest
            WHERE rn = 1
            ORDER BY composite_score DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
        out: List[OrgAIRScoreOut] = []
        for r in rows:
            rec = {
                "company_id": str(r[0]),
                "assessment_id": str(r[1]) if r[1] else None,
                "scoring_run_id": str(r[2]) if r[2] else None,
                "vr_score": float(r[3] or 0),
                "synergy_bonus": float(r[4] or 0),
                "talent_penalty": float(r[5] or 0),
                "sem_lower": float(r[6]) if r[6] is not None else None,
                "sem_upper": float(r[7]) if r[7] is not None else None,
                "composite_score": float(r[8] or 0),
                "score_band": str(r[9] or ""),
                "breakdown": _parse_breakdown(r[10]),
                "scored_at": r[11],
            }
            out.append(_to_out(rec))
        cache_set_json(
            cache_key,
            [x.model_dump(mode="json") for x in out],
            settings.redis_ttl_seconds,
        )
        return out
    finally:
        cur.close()
        conn.close()
 