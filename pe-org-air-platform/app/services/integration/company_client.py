from __future__ import annotations
 
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional
 
from app.services.redis_cache import cache_get_json, cache_set_json
from app.services.snowflake import get_snowflake_connection
from app.config import settings
 
 
@dataclass(frozen=True)
class CompanyRecord:
    id: str
    name: str
    ticker: Optional[str]
    industry_id: Optional[str]
    position_factor: float
    is_deleted: bool
    created_at: Any
    updated_at: Any
 
 
class CompanyClient:
    """
    Read-only company integration client for internal service usage.
 
    Mirrors the data shape used by routers/companies.py, but avoids HTTP calls
    and reads directly from Snowflake / Redis.
    """
 
    def _row_to_record(self, row: tuple[Any, ...]) -> CompanyRecord:
        return CompanyRecord(
            id=str(row[0]),
            name=str(row[1]),
            ticker=str(row[2]) if row[2] is not None else None,
            industry_id=str(row[3]) if row[3] is not None else None,
            position_factor=float(row[4]) if row[4] is not None else 0.0,
            is_deleted=bool(row[5]),
            created_at=row[6],
            updated_at=row[7],
        )
 
    def get_company(self, company_id: str) -> Dict[str, Any]:
        if not company_id or not company_id.strip():
            raise ValueError("company_id is required")
 
        cache_key = f"company:{company_id}"
        cached = cache_get_json(cache_key)
        if cached is not None:
            return cached if isinstance(cached, dict) else dict(cached)
 
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
                raise ValueError("Company not found")
 
            company = self._row_to_record(row)
            payload = asdict(company)
 
            cache_set_json(
                cache_key,
                payload,
                settings.redis_ttl_company_seconds,
            )
            return payload
        finally:
            cur.close()
            conn.close()
 
    def list_companies(self, limit: int = 100) -> List[Dict[str, Any]]:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, name, ticker, industry_id, position_factor, is_deleted, created_at, updated_at
                FROM companies
                WHERE is_deleted = FALSE
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
            return [asdict(self._row_to_record(r)) for r in rows]
        finally:
            cur.close()
            conn.close()