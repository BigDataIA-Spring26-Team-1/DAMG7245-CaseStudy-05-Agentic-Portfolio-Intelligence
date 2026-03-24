from __future__ import annotations
 
import json
from typing import Any, Dict, Optional
from uuid import uuid4
 
from app.services.snowflake import get_snowflake_connection
 
 
class SignalStore:
    def __init__(self):
        self.conn = get_snowflake_connection()
        try:
            self.conn.autocommit(True)
        except Exception:
            pass
 
    def close(self):
        self.conn.close()
 
    def signal_exists_by_hash(self, content_hash: str) -> bool:
        q = "SELECT 1 FROM external_signals WHERE content_hash = %s LIMIT 1"
        cur = self.conn.cursor()
        try:
            cur.execute(q, (content_hash,))
            return cur.fetchone() is not None
        finally:
            cur.close()
 
    def insert_signal(
        self,
        company_id: str,
        ticker: str,
        signal_type: str,
        source: str,
        title: Optional[str],
        url: Optional[str],
        published_at,
        content_text: Optional[str],
        content_hash: Optional[str],
        metadata: Dict[str, Any],
    ) -> str:
        sid = str(uuid4())
        cur = self.conn.cursor()
        try:
            cur.execute(
            """
            INSERT INTO external_signals
            (id, company_id, ticker, signal_type, source, title, url, published_at,
            content_text, content_hash, metadata)
            SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRY_PARSE_JSON(%s)
            """,
            (
                sid,
                company_id,
                ticker,
                signal_type,
                source,
                title,
                url,
                published_at,
                content_text,
                content_hash,
                json.dumps(metadata),
            ),
        )
            return sid
        finally:
            cur.close()
    def list_signals(
        self,
        company_id: str | None = None,
        ticker: str | None = None,
        signal_type: str | None = None,
        signal_types: list[str] | None = None,
        source: str | None = None,
        limit: int = 100,
    ):
        where = []
        params = []
        if company_id:
            where.append("company_id=%s")
            params.append(company_id)
        if ticker:
            where.append("ticker=%s")
            params.append(ticker)
        if source:
            where.append("source=%s")
            params.append(source)
 
        if signal_types:
            where.append(f"signal_type IN ({','.join(['%s']*len(signal_types))})")
            params.extend(signal_types)
        elif signal_type:
            where.append("signal_type=%s")
            params.append(signal_type)
 
        w = (" WHERE " + " AND ".join(where)) if where else ""
        q = f"""
        SELECT id, company_id, ticker, signal_type, source, title, url, published_at, collected_at,
               content_hash, metadata
          FROM external_signals
          {w}
         ORDER BY collected_at DESC
         LIMIT {int(limit)}
        """
        cur = self.conn.cursor()
        try:
            cur.execute(q, tuple(params))
            cols = [c[0].lower() for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            cur.close()
 
    def company_signal_summary(self, company_id: str):
        q = """
        SELECT signal_type, COUNT(*) AS cnt, MAX(collected_at) AS last_collected
          FROM external_signals
         WHERE company_id=%s
         GROUP BY signal_type
         ORDER BY cnt DESC
        """
        cur = self.conn.cursor()
        try:
            cur.execute(q, (company_id,))
            rows = cur.fetchall()
            if not rows:
                return None
            by_type = [{"signal_type": r[0], "count": int(r[1]), "last_collected_at": r[2]} for r in rows]
            total = sum(x["count"] for x in by_type)
            return {"company_id": company_id, "total_signals": total, "by_type": by_type}
        finally:
            cur.close()
 
    def signal_stats(self):
        q = """
        SELECT
          (SELECT COUNT(*) FROM external_signals) AS signals_count
        """
        cur = self.conn.cursor()
        try:
            cur.execute(q)
            row = cur.fetchone()
            cols = [c[0].lower() for c in cur.description]
            return dict(zip(cols, row))
        finally:
            cur.close()