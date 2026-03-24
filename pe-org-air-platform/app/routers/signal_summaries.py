from __future__ import annotations
 
from datetime import date, datetime, timedelta
from uuid import uuid4
 
from fastapi import APIRouter, HTTPException, Query
 
from app.config import settings
from app.services.redis_cache import cache_delete_pattern, cache_get_json, cache_set_json
from app.services.snowflake import get_snowflake_connection
 
router = APIRouter(prefix="/signal-summaries")
 
 
def _summaries_list_cache_key(ticker: str | None, limit: int) -> str:
    t = ticker.strip().upper() if ticker else "all"
    return f"signal_summaries:list:ticker:{t}:limit:{limit}"
 
 
@router.get("")
def list_summaries(
    ticker: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
):
    ticker_norm = ticker.strip().upper() if ticker else None
    cache_key = _summaries_list_cache_key(ticker=ticker_norm, limit=limit)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        q = """
        SELECT id, company_id, ticker, as_of_date, summary_text, signal_count, created_at
          FROM company_signal_summaries
         WHERE (%s IS NULL OR ticker = %s)
         ORDER BY as_of_date DESC, created_at DESC
         LIMIT %s
        """
        cur.execute(q, (ticker_norm, ticker_norm, limit))
        cols = [c[0].lower() for c in cur.description]
        out = [dict(zip(cols, r)) for r in cur.fetchall()]
        cache_set_json(cache_key, out, settings.redis_ttl_seconds)
        return out
    finally:
        cur.close()
        conn.close()
 
 
@router.post("/compute")
def compute_summary(
    ticker: str = Query(..., description="Ticker like CAT"),
    as_of: date | None = Query(default=None, description="Defaults to today"),
):
    ticker_norm = ticker.strip().upper()
    as_of_date = as_of or date.today()
    window_end = datetime.combine(as_of_date + timedelta(days=1), datetime.min.time()).isoformat(sep=" ")
 
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        # 1) Find company_id
        cur.execute(
            """
            SELECT id
            FROM companies
            WHERE ticker=%s AND is_deleted=FALSE
            ORDER BY created_at DESC
            LIMIT 2
            """,
            (ticker_norm,),
        )
        rows = cur.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail=f"Company not found for ticker={ticker_norm}")
        if len(rows) > 1:
            raise HTTPException(status_code=409, detail=f"Duplicate companies found for ticker={ticker_norm}")
        company_id = str(rows[0][0])
 
        # 2) Pull recent signals (last 7 days ending at as_of_date)
        cur.execute(
            """
            SELECT signal_type, COUNT(*) AS cnt
              FROM external_signals
             WHERE ticker=%s
               AND collected_at >= DATEADD(day, -7, TO_TIMESTAMP_NTZ(%s))
               AND collected_at < TO_TIMESTAMP_NTZ(%s)
             GROUP BY signal_type
             ORDER BY cnt DESC
            """,
            (ticker_norm, window_end, window_end),
        )
        breakdown = cur.fetchall()
 
        signal_count = sum(int(r[1]) for r in breakdown) if breakdown else 0
        parts = [f"{st}: {cnt}" for (st, cnt) in breakdown] if breakdown else ["No recent signals found (last 7 days)"]
        summary_text = f"Signals last 7 days for {ticker_norm}: " + ", ".join(parts)
 
        # 3) Upsert into company_signal_summaries
        sid = str(uuid4())
        cur.execute(
            """
            MERGE INTO company_signal_summaries t
            USING (SELECT %s AS company_id, %s AS ticker, %s AS as_of_date) s
               ON t.company_id = s.company_id AND t.as_of_date = s.as_of_date
            WHEN MATCHED THEN UPDATE SET
              ticker = s.ticker,
              summary_text = %s,
              signal_count = %s
            WHEN NOT MATCHED THEN INSERT
              (id, company_id, ticker, as_of_date, summary_text, signal_count, created_at)
            VALUES
              (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
            """,
            (
                company_id,
                ticker_norm,
                as_of_date,
                summary_text,
                signal_count,
                sid,
                company_id,
                ticker_norm,
                as_of_date,
                summary_text,
                signal_count,
            ),
        )
 
        out = {
            "ticker": ticker_norm,
            "as_of_date": str(as_of_date),
            "signal_count": signal_count,
            "summary_text": summary_text,
        }
        cache_delete_pattern("signal_summaries:list:*")
        return out
    finally:
        cur.close()
        conn.close()