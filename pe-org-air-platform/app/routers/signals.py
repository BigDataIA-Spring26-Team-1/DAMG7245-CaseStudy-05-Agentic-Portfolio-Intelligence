from __future__ import annotations
 
from fastapi import APIRouter, HTTPException, Query
 
from app.config import settings
from app.services.redis_cache import cache_get_json, cache_set_json
from app.services.snowflake import get_snowflake_connection
 
router = APIRouter(prefix="/signals")
 
 
def _signals_list_cache_key(
    ticker: str | None,
    signal_type: str | None,
    source: str | None,
    limit: int,
) -> str:
    t = (ticker or "").strip().upper() or "all"
    st = (signal_type or "").strip().lower() or "all"
    src = (source or "").strip().lower() or "all"
    return f"signals:list:ticker:{t}:type:{st}:source:{src}:limit:{limit}"
 
 
@router.get("")
def list_signals(
    ticker: str | None = Query(default=None),
    signal_type: str | None = Query(default=None),
    source: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
):
    ticker_norm = ticker.strip().upper() if ticker else None
    signal_type_norm = signal_type.strip().lower() if signal_type else None
    source_norm = source.strip().lower() if source else None
 
    cache_key = _signals_list_cache_key(ticker_norm, signal_type_norm, source_norm, limit)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        q = """
        SELECT id, company_id, ticker, signal_type, source, title, url,
               published_at, collected_at, content_hash, metadata
          FROM external_signals
         WHERE (%s IS NULL OR ticker = %s)
           AND (%s IS NULL OR signal_type = %s)
           AND (%s IS NULL OR source = %s)
         ORDER BY collected_at DESC
         LIMIT %s
        """
        cur.execute(q, (ticker_norm, ticker_norm, signal_type_norm, signal_type_norm, source_norm, source_norm, limit))
        cols = [c[0].lower() for c in cur.description]
        out = [dict(zip(cols, r)) for r in cur.fetchall()]
        cache_set_json(cache_key, out, settings.redis_ttl_seconds)
        return out
    finally:
        cur.close()
        conn.close()
 
 
@router.get("/{signal_id}")
def get_signal(signal_id: str):
    cache_key = f"signals:item:{signal_id}"
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        q = """
        SELECT id, company_id, ticker, signal_type, source, title, url,
               published_at, collected_at, content_text, content_hash, metadata
          FROM external_signals
         WHERE id = %s
         LIMIT 1
        """
        cur.execute(q, (signal_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Signal not found")
 
        cols = [c[0].lower() for c in cur.description]
        out = dict(zip(cols, row))
        cache_set_json(cache_key, out, settings.redis_ttl_seconds)
        return out
    finally:
        cur.close()
        conn.close()
 
 