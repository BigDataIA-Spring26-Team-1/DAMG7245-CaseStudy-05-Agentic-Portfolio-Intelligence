from fastapi import APIRouter, HTTPException, Query, status
from uuid import UUID, uuid4
 
from app.config import settings
from app.models.assessment import (
    AssessmentCreate,
    AssessmentOut,
    AssessmentStatusUpdate,
    AssessmentStatus,
)
from app.models.dimension import (
    DimensionScoreCreate,
    DimensionScoreOut,
)
from app.models.pagination import Page
from app.services.snowflake import get_snowflake_connection
from app.services.redis_cache import cache_delete, cache_delete_pattern, cache_get_json, cache_set_json
 
router = APIRouter(tags=["assessments"])
 
ALLOWED_STATUS_TRANSITIONS: dict[AssessmentStatus, set[AssessmentStatus]] = {
    AssessmentStatus.draft: {AssessmentStatus.in_progress, AssessmentStatus.submitted, AssessmentStatus.superseded},
    AssessmentStatus.in_progress: {AssessmentStatus.submitted, AssessmentStatus.superseded},
    AssessmentStatus.submitted: {AssessmentStatus.approved, AssessmentStatus.superseded},
    AssessmentStatus.approved: {AssessmentStatus.superseded},
    AssessmentStatus.superseded: set(),
}
 
 
def _assessments_list_cache_key(page: int, page_size: int, company_id: UUID | None) -> str:
    cid = str(company_id) if company_id else "all"
    return f"assessments:list:company:{cid}:page:{page}:size:{page_size}"
 
 
def _assessment_scores_cache_key(assessment_id: UUID, page: int, page_size: int) -> str:
    return f"assessments:scores:{assessment_id}:page:{page}:size:{page_size}"
 
 
def _row_to_assessment_out(row: tuple) -> AssessmentOut:
    return AssessmentOut(
        id=UUID(row[0]),
        company_id=UUID(row[1]),
        assessment_type=row[2],
        assessment_date=row[3],
        status=row[4],
        primary_assessor=row[5],
        secondary_assessor=row[6],
        vr_score=float(row[7]) if row[7] is not None else None,
        confidence_lower=float(row[8]) if row[8] is not None else None,
        confidence_upper=float(row[9]) if row[9] is not None else None,
        created_at=row[10],
    )
 
 
@router.post("/assessments", response_model=AssessmentOut, status_code=status.HTTP_201_CREATED)
def create_assessment(assessment: AssessmentCreate):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        # Check if company exists
        cur.execute("SELECT id FROM companies WHERE id = %s", (str(assessment.company_id),))
        if not cur.fetchone():
            raise HTTPException(status_code=400, detail="Invalid company_id")
 
        new_id = str(uuid4())
        cur.execute(
            """
            INSERT INTO assessments (
                id, company_id, assessment_type, assessment_date,
                status, primary_assessor, secondary_assessor,
                vr_score, confidence_lower, confidence_upper
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                new_id,
                str(assessment.company_id),
                assessment.assessment_type.value,
                assessment.assessment_date,
                AssessmentStatus.draft.value,
                assessment.primary_assessor,
                assessment.secondary_assessor,
                assessment.vr_score,
                assessment.confidence_lower,
                assessment.confidence_upper,
            ),
        )
 
        cur.execute(
            """
            SELECT id, company_id, assessment_type, assessment_date, status,
                   primary_assessor, secondary_assessor, vr_score,
                   confidence_lower, confidence_upper, created_at
            FROM assessments
            WHERE id = %s
            """,
            (new_id,),
        )
        row = cur.fetchone()
        out = _row_to_assessment_out(row)
        cache_set_json(
            f"assessment:{out.id}",
            out.model_dump(mode="json"),
            settings.redis_ttl_assessment_seconds,
        )
        cache_delete_pattern("assessments:list:*")
        return out
    finally:
        cur.close()
        conn.close()
 
 
@router.get("/assessments", response_model=Page[AssessmentOut])
def list_assessments(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    company_id: UUID | None = None,
):
    cache_key = _assessments_list_cache_key(page, page_size, company_id)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return Page[AssessmentOut](**cached)
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        query_params = []
        where_clause = ""
        if company_id:
            where_clause = "WHERE company_id = %s"
            query_params.append(str(company_id))
 
        # Count total
        cur.execute(f"SELECT COUNT(*) FROM assessments {where_clause}", tuple(query_params))
        total = cur.fetchone()[0]
 
        # Fetch items
        limit = page_size
        offset = (page - 1) * page_size
        query_params.extend([limit, offset])
       
        sql = f"""
            SELECT id, company_id, assessment_type, assessment_date, status,
                   primary_assessor, secondary_assessor, vr_score,
                   confidence_lower, confidence_upper, created_at
            FROM assessments
            {where_clause}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        cur.execute(sql, tuple(query_params))
        rows = cur.fetchall()
 
        items = []
        for row in rows:
            items.append(_row_to_assessment_out(row))
 
        page_out = Page[AssessmentOut].create(items=items, total=total, page=page, page_size=page_size)
        cache_set_json(
            cache_key,
            page_out.model_dump(mode="json"),
            settings.redis_ttl_seconds,
        )
        return page_out
 
    finally:
        cur.close()
        conn.close()
 
 
@router.get("/assessments/{id}", response_model=AssessmentOut)
def get_assessment(id: UUID):
    cache_key = f"assessment:{id}"
    cached = cache_get_json(cache_key)
    if cached is not None:
        return AssessmentOut(**cached)
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, company_id, assessment_type, assessment_date, status,
                   primary_assessor, secondary_assessor, vr_score,
                   confidence_lower, confidence_upper, created_at
            FROM assessments WHERE id = %s
            """,
            (str(id),),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Assessment not found")
 
        assessment = _row_to_assessment_out(row)
        cache_set_json(cache_key, assessment.model_dump(mode="json"), ttl_seconds=settings.redis_ttl_assessment_seconds)
        return assessment
    finally:
        cur.close()
        conn.close()
 
 
@router.patch("/assessments/{id}/status", response_model=AssessmentOut)
def update_assessment_status(id: UUID, status_update: AssessmentStatusUpdate):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        # Check existence
        cur.execute("SELECT status FROM assessments WHERE id = %s", (str(id),))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Assessment not found")
        current_status = AssessmentStatus(row[0])
        target_status = status_update.status
 
        if target_status != current_status and target_status not in ALLOWED_STATUS_TRANSITIONS[current_status]:
            allowed = ", ".join(sorted(s.value for s in ALLOWED_STATUS_TRANSITIONS[current_status])) or "none"
            raise HTTPException(
                status_code=400,
                detail=f"Invalid status transition from '{current_status.value}' to '{target_status.value}'. "
                f"Allowed next statuses: {allowed}",
            )
 
        if target_status != current_status:
            cur.execute(
                "UPDATE assessments SET status = %s WHERE id = %s",
                (target_status.value, str(id)),
            )
       
        # Invalidate cache
        cache_delete(f"assessment:{id}")
        cache_delete_pattern("assessments:list:*")
       
        # Return updated (fetching fresh)
        return get_assessment(id)
    finally:
        cur.close()
        conn.close()
 
 
@router.get("/assessments/{id}/scores", response_model=Page[DimensionScoreOut])
def get_dimension_scores(
    id: UUID,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    cache_key = _assessment_scores_cache_key(id, page, page_size)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return Page[DimensionScoreOut](**cached)
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        # Check assessment exists
        cur.execute("SELECT id FROM assessments WHERE id = %s", (str(id),))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Assessment not found")
 
        # Count
        cur.execute("SELECT COUNT(*) FROM dimension_scores WHERE assessment_id = %s", (str(id),))
        total = cur.fetchone()[0]
 
        # Fetch
        limit = page_size
        offset = (page - 1) * page_size
        cur.execute(
            """
            SELECT id, assessment_id, dimension, score, weight, confidence, evidence_count, created_at
            FROM dimension_scores
            WHERE assessment_id = %s
            ORDER BY created_at ASC
            LIMIT %s OFFSET %s
            """,
            (str(id), limit, offset)
        )
        rows = cur.fetchall()
 
        items = []
        for row in rows:
            items.append(
                DimensionScoreOut(
                    id=UUID(row[0]),
                    assessment_id=UUID(row[1]),
                    dimension=row[2],
                    score=float(row[3]),
                    weight=float(row[4]) if row[4] is not None else None,
                    confidence=float(row[5]),
                    evidence_count=int(row[6]),
                    created_at=row[7]
                )
            )
 
        page_out = Page[DimensionScoreOut].create(items=items, total=total, page=page, page_size=page_size)
        cache_set_json(
            cache_key,
            page_out.model_dump(mode="json"),
            settings.redis_ttl_seconds,
        )
        return page_out
 
    finally:
        cur.close()
        conn.close()
 
 
@router.post("/assessments/{id}/scores", response_model=DimensionScoreOut, status_code=status.HTTP_201_CREATED)
def upsert_dimension_score(id: UUID, score_in: DimensionScoreCreate):
    # Ensure URL id matches body id
    if score_in.assessment_id != id:
        # In a real app we might override it or raise error.
        # For simplicity, we'll enforce consistency or use the path ID.
        score_in.assessment_id = id
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        # Check assessment exists
        cur.execute("SELECT id FROM assessments WHERE id = %s", (str(id),))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Assessment not found")
 
        new_id = str(uuid4())
       
        # Merge logic (Upsert): If (assessment_id, dimension) exists, update; else insert.
        # Snowflake MERGE is best, but standard SQL here:
        cur.execute(
            """
            MERGE INTO dimension_scores t
            USING (SELECT %s AS aid, %s AS dim) s
            ON t.assessment_id = s.aid AND t.dimension = s.dim
            WHEN MATCHED THEN
                UPDATE SET score = %s, weight = %s, confidence = %s, evidence_count = %s
            WHEN NOT MATCHED THEN
                INSERT (id, assessment_id, dimension, score, weight, confidence, evidence_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(score_in.assessment_id), score_in.dimension.value,
                # Update params
                score_in.score, score_in.weight, score_in.confidence, score_in.evidence_count,
                # Insert params
                new_id, str(score_in.assessment_id), score_in.dimension.value,
                score_in.score, score_in.weight, score_in.confidence, score_in.evidence_count
            )
        )
       
        # Retrieve the record to return correct ID/timestamps
        cur.execute(
            """
            SELECT id, assessment_id, dimension, score, weight, confidence, evidence_count, created_at
            FROM dimension_scores
            WHERE assessment_id = %s AND dimension = %s
            """,
            (str(id), score_in.dimension.value)
        )
        row = cur.fetchone()
 
        out = DimensionScoreOut(
            id=UUID(row[0]),
            assessment_id=UUID(row[1]),
            dimension=row[2],
            score=float(row[3]),
            weight=float(row[4]) if row[4] is not None else None,
            confidence=float(row[5]),
            evidence_count=int(row[6]),
            created_at=row[7]
        )
        cache_delete_pattern(f"assessments:scores:{id}:*")
        return out
 
    finally:
        cur.close()
        conn.close()
 
 