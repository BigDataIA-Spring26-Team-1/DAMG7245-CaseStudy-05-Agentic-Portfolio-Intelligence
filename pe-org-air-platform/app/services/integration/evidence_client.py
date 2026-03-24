from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.services.snowflake import get_snowflake_connection


@dataclass(frozen=True)
class EvidenceChunkRow:
    """
    A normalized view of a chunk row joined with its document metadata.

    Keep fields minimal but sufficient for:
      - indexing
      - metadata filtering
      - justification citations
    """
    document_id: str
    chunk_id: str
    chunk_text: str
    chunk_index: int

    # metadata (from documents table)
    company_id: str
    source_type: str
    signal_category: Optional[str]
    confidence: Optional[float]
    source_url: Optional[str]
    fiscal_year: Optional[int]

    # optional extra fields
    title: Optional[str]
    doc_type: Optional[str]
    published_at: Optional[str]


class EvidenceClient:
    """
    Snowflake-backed evidence reader.

    Code review notes:
    - Snowflake is the source of truth
    - Supports batch pagination
    - Chroma is a derived, rebuildable index
    """

    def __init__(self, schema: Optional[str] = None) -> None:
        self.schema = schema

    def _qualify(self, table: str) -> str:
        return f"{self.schema}.{table}" if self.schema else table

    def iter_chunks_for_company(
        self,
        company_id: str,
        batch_size: int = 500,
        min_confidence: Optional[float] = None,
    ) -> Iterable[List[EvidenceChunkRow]]:
        """
        Yields batches of joined (document_chunks + documents) for a given company.
        """
        docs = self._qualify("documents")
        chunks = self._qualify("document_chunks")

        where_parts = ["d.company_id = %s"]
        params: List[Any] = [company_id]

        if min_confidence is not None:
            # documents table currently has no confidence column;
            # treat SEC document chunks as high-confidence evidence.
            where_parts.append("1.0 >= %s")
            params.append(float(min_confidence))

        where_sql = " AND ".join(where_parts)

        offset = 0

        sql = f"""
        SELECT
            d.id AS document_id,
            c.id AS chunk_id,
            c.content AS chunk_text,
            c.chunk_index,

            d.company_id,
            'sec_filing' AS source_type,
            c.section AS signal_category,
            1.0 AS confidence,
            d.source_url,
            YEAR(d.filing_date) AS fiscal_year,
            d.filing_type AS title,
            d.filing_type AS doc_type,
            TO_VARCHAR(d.filing_date) AS published_at
        FROM {chunks} c
        JOIN {docs} d
          ON d.id = c.document_id
        WHERE {where_sql}
        ORDER BY c.document_id, c.chunk_index
        LIMIT %s OFFSET %s
        """

        conn = get_snowflake_connection()
        try:
            while True:
                cur = conn.cursor()
                try:
                    cur.execute(sql, params + [batch_size, offset])
                    rows = cur.fetchall()
                finally:
                    cur.close()

                if not rows:
                    break

                batch: List[EvidenceChunkRow] = []
                for r in rows:
                    batch.append(
                        EvidenceChunkRow(
                            document_id=str(r[0]),
                            chunk_id=str(r[1]),
                            chunk_text=str(r[2] or ""),
                            chunk_index=int(r[3] or 0),
                            company_id=str(r[4]),
                            source_type=str(r[5] or ""),
                            signal_category=r[6],
                            confidence=float(r[7]) if r[7] is not None else None,
                            source_url=r[8],
                            fiscal_year=int(r[9]) if r[9] is not None else None,
                            title=r[10],
                            doc_type=r[11],
                            published_at=str(r[12]) if r[12] is not None else None,
                        )
                    )

                yield batch
                offset += batch_size
        finally:
            conn.close()

    def get_chunk_metadata_by_uids(self, chunk_uids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        chunk_uid format: '{document_id}:{chunk_id}'

        Aligns with Snowflake schema used above:
          - documents: d.id
          - document_chunks: c.id, c.content, c.chunk_index, c.document_id

        Returns: {chunk_uid: metadata_dict}, includes '_chunk_text' for convenience.
        """
        if not chunk_uids:
            return {}

        pairs: List[Tuple[str, str]] = []
        for uid in chunk_uids:
            if ":" not in uid:
                continue
            doc_id, chunk_id = uid.split(":", 1)
            pairs.append((doc_id, chunk_id))

        if not pairs:
            return {}

        docs = self._qualify("documents")
        chunks = self._qualify("document_chunks")

        values_sql = ", ".join(["(%s, %s)"] * len(pairs))
        params: List[Any] = []
        for doc_id, chunk_id in pairs:
            params.extend([doc_id, chunk_id])

        sql = f"""
        WITH req(document_id, chunk_id) AS (
            SELECT column1, column2
            FROM VALUES {values_sql}
        )
        SELECT
            d.id AS document_id,
            c.id AS chunk_id,
            c.chunk_index,
            c.content AS chunk_text,

            d.company_id,
            'sec_filing' AS source_type,
            c.section AS signal_category,
            1.0 AS confidence,
            d.source_url,
            YEAR(d.filing_date) AS fiscal_year,
            d.filing_type AS title,
            d.filing_type AS doc_type,
            TO_VARCHAR(d.filing_date) AS published_at
        FROM req
        JOIN {chunks} c
          ON c.document_id = req.document_id AND c.id = req.chunk_id
        JOIN {docs} d
          ON d.id = c.document_id
        """

        conn = get_snowflake_connection()
        try:
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
                rows = cur.fetchall()
            finally:
                cur.close()
        finally:
            conn.close()

        out: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            document_id = str(r[0])
            chunk_id = str(r[1])
            chunk_uid = f"{document_id}:{chunk_id}"

            out[chunk_uid] = {
                "document_id": document_id,
                "chunk_id": chunk_id,
                "chunk_index": int(r[2] or 0),
                "company_id": str(r[4]),
                "source_type": str(r[5] or ""),
                "signal_category": r[6],
                "confidence": float(r[7]) if r[7] is not None else 0.0,
                "source_url": r[8],
                "fiscal_year": int(r[9]) if r[9] is not None else None,
                "title": r[10],
                "doc_type": r[11],
                "published_at": str(r[12]) if r[12] is not None else None,
                "_chunk_text": str(r[3] or ""),
            }

        return out