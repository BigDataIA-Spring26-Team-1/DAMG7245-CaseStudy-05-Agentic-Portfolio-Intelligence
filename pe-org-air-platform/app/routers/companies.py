from __future__ import annotations
 
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4
 
from fastapi import APIRouter, HTTPException, Query, Response
 
from app.config import settings
from app.models.company import CompanyCreate, CompanyOut, CompanyUpdate, IndustryOut
from app.models.pagination import Page
from app.services.redis_cache import cache_delete, cache_delete_pattern, cache_get_json, cache_set_json
from app.services.snowflake import get_snowflake_connection
 
router = APIRouter(prefix="/companies")
 
 
def _is_unique_constraint_violation(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "unique" in msg and ("constraint" in msg or "key" in msg)
 
 
def _ticker_conflict_detail(ticker: str | None) -> str:
    if ticker:
        return f"Ticker already exists: {ticker}"
    return "Company ticker already exists"
 
 
def _row_to_company_out(row: tuple[Any, ...]) -> CompanyOut:
    # Order must match SELECT columns
    return CompanyOut(
        id=row[0],
        name=row[1],
        ticker=row[2],
        industry_id=row[3],
        position_factor=float(row[4]) if row[4] is not None else 0.0,
        is_deleted=bool(row[5]),
        created_at=row[6],
        updated_at=row[7],
    )
 
 
def _companies_list_cache_key(page: int, page_size: int, query: str | None = None) -> str:
    normalized = (query or "").strip().lower()
    return f"companies:list:page:{page}:size:{page_size}:q:{normalized}"
 
 
@router.post("", response_model=CompanyOut, status_code=201)
def create_company(payload: CompanyCreate) -> CompanyOut:
    company_id = str(uuid4())
    now = datetime.now(timezone.utc)
    industry_id = str(payload.industry_id) if payload.industry_id is not None else None
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        # Optional industry existence check (recommended)
        if industry_id:
            cur.execute(
                "SELECT 1 FROM industries WHERE id = %s",
                (industry_id,),
            )
            if cur.fetchone() is None:
                raise HTTPException(status_code=400, detail="Invalid industry_id")
 
        if payload.ticker:
            cur.execute("SELECT id FROM companies WHERE ticker = %s LIMIT 1", (payload.ticker,))
            if cur.fetchone() is not None:
                raise HTTPException(status_code=409, detail=_ticker_conflict_detail(payload.ticker))
 
        try:
            cur.execute(
                """
                INSERT INTO companies (id, name, ticker, industry_id, position_factor, is_deleted, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, FALSE, %s, %s)
                """,
                (
                    company_id,
                    payload.name,
                    payload.ticker,
                    industry_id,
                    payload.position_factor,
                    now,
                    now,
                ),
            )
        except Exception as exc:
            if _is_unique_constraint_violation(exc):
                raise HTTPException(status_code=409, detail=_ticker_conflict_detail(payload.ticker))
            raise
 
        cur.execute(
            """
            SELECT id, name, ticker, industry_id, position_factor, is_deleted, created_at, updated_at
            FROM companies
            WHERE id = %s
            """,
            (company_id,),
        )
        row = cur.fetchone()
        company = _row_to_company_out(row)
        cache_set_json(
            f"company:{company.id}",
            company,
            settings.redis_ttl_company_seconds,
        )
        cache_delete_pattern("companies:list:*")
        return company
    finally:
        cur.close()
        conn.close()
 
 
@router.get("", response_model=Page[CompanyOut])
def list_companies(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    q: str | None = Query(None, description="Optional company name or ticker search"),
) -> Page[CompanyOut]:
    cache_key = _companies_list_cache_key(page, page_size, q)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return Page[CompanyOut](**cached)

    offset = (page - 1) * page_size
    search = (q or "").strip()
    like_value = f"%{search}%"

    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        # total count
        if search:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM companies
                WHERE is_deleted = FALSE
                  AND (
                    UPPER(COALESCE(ticker, '')) LIKE UPPER(%s)
                    OR UPPER(COALESCE(name, '')) LIKE UPPER(%s)
                  )
                """,
                (like_value, like_value),
            )
        else:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM companies
                WHERE is_deleted = FALSE
                """
            )
        total = cur.fetchone()[0]

        # paged data
        if search:
            cur.execute(
                """
                SELECT id, name, ticker, industry_id, position_factor,
                       is_deleted, created_at, updated_at
                FROM companies
                WHERE is_deleted = FALSE
                  AND (
                    UPPER(COALESCE(ticker, '')) LIKE UPPER(%s)
                    OR UPPER(COALESCE(name, '')) LIKE UPPER(%s)
                  )
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (like_value, like_value, page_size, offset),
            )
        else:
            cur.execute(
                """
                SELECT id, name, ticker, industry_id, position_factor,
                       is_deleted, created_at, updated_at
                FROM companies
                WHERE is_deleted = FALSE
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (page_size, offset),
            )
 
        rows = cur.fetchall()
        items = [_row_to_company_out(r) for r in rows]
 
        page_out = Page.create(
            items=items,
            page=page,
            page_size=page_size,
            total=total,
        )
        cache_set_json(
            cache_key,
            page_out.model_dump(mode="json"),
            settings.redis_ttl_seconds,
        )
        return page_out
 
    finally:
        cur.close()
        conn.close()
 
 
@router.get("/industries", response_model=list[IndustryOut])
def list_industries() -> list[IndustryOut]:
    cache_key = "industries:list"
    cached = cache_get_json(cache_key)
    if cached is not None:
        return [IndustryOut(**x) for x in cached]
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, name, sector, hr_base, created_at
            FROM industries
            ORDER BY name ASC
            """
        )
        rows = cur.fetchall()
        industries = [
            IndustryOut(
                id=r[0],
                name=r[1],
                sector=r[2],
                hr_base=float(r[3]) if r[3] is not None else None,
                created_at=r[4],
            )
            for r in rows
        ]
        cache_set_json(cache_key, [x.model_dump() for x in industries], settings.redis_ttl_industries_seconds)
        return industries
    finally:
        cur.close()
        conn.close()
 
 
@router.get("/{company_id}", response_model=CompanyOut)
def get_company(company_id: str) -> CompanyOut:
    cache_key = f"company:{company_id}"
 
    # 1) Try Redis first
    cached = cache_get_json(cache_key)
    if cached is not None:
        return CompanyOut(**cached)
 
    # 2) Fallback to Snowflake
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, name, ticker, industry_id, position_factor, is_deleted, created_at, updated_at
            FROM companies
            WHERE id = %s AND is_deleted = FALSE
            """,
            (company_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Company not found")
 
        company = _row_to_company_out(row)
 
        # 3) Store in Redis with TTL
        cache_set_json(
            cache_key,
            company,
            settings.redis_ttl_company_seconds,
        )
 
        return company
    finally:
        cur.close()
        conn.close()
 
 
@router.put("/{company_id}", response_model=CompanyOut)
def update_company(company_id: str, payload: CompanyUpdate) -> CompanyOut:
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        # Ensure exists
        cur.execute(
            "SELECT 1 FROM companies WHERE id = %s AND is_deleted = FALSE",
            (company_id,),
        )
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail="Company not found")
 
        # Optional industry existence check
        industry_id = str(payload.industry_id) if payload.industry_id is not None else None
        if industry_id:
            cur.execute("SELECT 1 FROM industries WHERE id = %s", (industry_id,))
            if cur.fetchone() is None:
                raise HTTPException(status_code=400, detail="Invalid industry_id")
 
        # Build partial update dynamically
        updates = []
        params: list[Any] = []
 
        if payload.name is not None:
            updates.append("name = %s")
            params.append(payload.name)
        if payload.ticker is not None:
            cur.execute(
                "SELECT 1 FROM companies WHERE ticker = %s AND id <> %s LIMIT 1",
                (payload.ticker, company_id),
            )
            if cur.fetchone() is not None:
                raise HTTPException(status_code=409, detail=_ticker_conflict_detail(payload.ticker))
            updates.append("ticker = %s")
            params.append(payload.ticker)
        if payload.industry_id is not None:
            updates.append("industry_id = %s")
            params.append(industry_id)
        if payload.position_factor is not None:
            updates.append("position_factor = %s")
            params.append(payload.position_factor)
 
        updates.append("updated_at = CURRENT_TIMESTAMP()")
 
        if len(updates) == 1:  # only updated_at
            # No actual fields provided
            pass
        else:
            sql = f"UPDATE companies SET {', '.join(updates)} WHERE id = %s"
            params.append(company_id)
            try:
                cur.execute(sql, tuple(params))
            except Exception as exc:
                if _is_unique_constraint_violation(exc):
                    raise HTTPException(status_code=409, detail=_ticker_conflict_detail(payload.ticker))
                raise
 
        cur.execute(
            """
            SELECT id, name, ticker, industry_id, position_factor, is_deleted, created_at, updated_at
            FROM companies
            WHERE id = %s
            """,
            (company_id,),
        )
        row = cur.fetchone()
        company = _row_to_company_out(row)
        cache_set_json(
            f"company:{company_id}",
            company,
            settings.redis_ttl_company_seconds,
        )
        cache_delete_pattern("companies:list:*")
        return company
    finally:
        cur.close()
        conn.close()
 
 
@router.delete("/{company_id}", status_code=204, response_class=Response)
def delete_company(company_id: str) -> Response:
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE companies
            SET is_deleted = TRUE, updated_at = CURRENT_TIMESTAMP()
            WHERE id = %s AND is_deleted = FALSE
            """,
            (company_id,),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Company not found")
        cache_delete(f"company:{company_id}")
        cache_delete_pattern("companies:list:*")
        return Response(status_code=204)
    finally:
        cur.close()
        conn.close()
 
 
