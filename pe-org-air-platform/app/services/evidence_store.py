from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Sequence

from app.services.snowflake import get_snowflake_connection


class DocumentStatus(str, Enum):
    PENDING = "pending"
    DOWNLOADED = "downloaded"
    PARSED = "parsed"
    CHUNKED = "chunked"
    INDEXED = "indexed"
    FAILED = "failed"
    PROCESSED = "processed"


@dataclass(frozen=True)
class DocumentRow:
    id: str
    company_id: str
    ticker: str
    filing_type: str
    filing_date: str  # YYYY-MM-DD
    source_url: Optional[str]
    local_path: Optional[str]
    content_hash: str
    word_count: int
    chunk_count: int
    status: str = DocumentStatus.PROCESSED.value


@dataclass(frozen=True)
class ChunkRow:
    id: str
    document_id: str
    chunk_index: int
    content: str
    section: Optional[str]
    start_char: int
    end_char: int
    word_count: int


class EvidenceStore:
    def __init__(self) -> None:
        self.conn = get_snowflake_connection()
        try:
            self.conn.autocommit(True)
        except Exception:
            pass

    def close(self) -> None:
        self.conn.close()

    # -------------------------
    # Documents
    # -------------------------
    def document_exists_by_hash(self, content_hash: str) -> bool:
        q = "SELECT 1 FROM documents WHERE content_hash = %s LIMIT 1"
        cur = self.conn.cursor()
        try:
            cur.execute(q, (content_hash,))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def insert_document(self, doc: DocumentRow) -> None:
        q = """
        INSERT INTO documents (
            id, company_id, ticker, filing_type, filing_date,
            source_url, local_path, content_hash, word_count, chunk_count, status
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cur = self.conn.cursor()
        try:
            cur.execute(
                q,
                (
                    doc.id,
                    doc.company_id,
                    doc.ticker,
                    doc.filing_type,
                    doc.filing_date,
                    doc.source_url,
                    doc.local_path,
                    doc.content_hash,
                    doc.word_count,
                    doc.chunk_count,
                    doc.status,
                ),
            )
        finally:
            cur.close()

    def update_document_status(
        self, document_id: str, status: str, error_message: str | None = None
    ) -> bool:
        q = """
        UPDATE documents
           SET status=%s,
               error_message=COALESCE(%s, error_message),
               processed_at=CASE WHEN %s IN ('indexed','failed') THEN CURRENT_TIMESTAMP() ELSE processed_at END
         WHERE id=%s
        """
        cur = self.conn.cursor()
        try:
            cur.execute(q, (status, error_message, status, document_id))
            affected = getattr(cur, "rowcount", None)
            if isinstance(affected, int):
                if affected > 0:
                    return True
                if affected == 0:
                    return False

            cur.execute("SELECT 1 FROM documents WHERE id=%s LIMIT 1", (document_id,))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def insert_failed_stub(
        self,
        doc_id: str,
        company_id: str,
        ticker: str,
        filing_type: str,
        filing_date: str,
        source_url: str | None,
        local_path: str | None,
        content_hash: str | None,
        error_message: str,
    ) -> None:
        q = """
        INSERT INTO documents (
          id, company_id, ticker, filing_type, filing_date,
          source_url, local_path, content_hash, word_count, chunk_count,
          status, error_message, processed_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP())
        """
        cur = self.conn.cursor()
        try:
            cur.execute(
                q,
                (
                    doc_id,
                    company_id,
                    ticker,
                    filing_type,
                    filing_date,
                    source_url,
                    local_path,
                    content_hash,
                    0,
                    0,
                    DocumentStatus.FAILED.value,
                    error_message[:8000],
                ),
            )
        finally:
            cur.close()

    def list_documents(
        self,
        company_id: str | None = None,
        ticker: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        where = []
        params: list[Any] = []

        if company_id:
            where.append("company_id = %s")
            params.append(company_id)
        if ticker:
            where.append("ticker = %s")
            params.append(ticker)

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        q = f"""
        SELECT
          id, company_id, ticker, filing_type, filing_date,
          source_url, local_path, content_hash, word_count, chunk_count,
          status, error_message, created_at, processed_at
        FROM documents
        {where_sql}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])

        cur = self.conn.cursor()
        try:
            cur.execute(q, tuple(params))
            rows = cur.fetchall()
            out: list[dict[str, Any]] = []
            for r in rows:
                out.append(
                    {
                        "id": r[0],
                        "company_id": r[1],
                        "ticker": r[2],
                        "filing_type": r[3],
                        "filing_date": r[4],
                        "source_url": r[5],
                        "local_path": r[6],
                        "content_hash": r[7],
                        "word_count": int(r[8]) if r[8] is not None else 0,
                        "chunk_count": int(r[9]) if r[9] is not None else 0,
                        "status": r[10],
                        "error_message": r[11],
                        "created_at": r[12],
                        "processed_at": r[13],
                    }
                )
            return out
        finally:
            cur.close()

    def get_document(self, document_id: str) -> dict[str, Any] | None:
        q = """
        SELECT
          id, company_id, ticker, filing_type, filing_date,
          source_url, local_path, content_hash, word_count, chunk_count,
          status, error_message, created_at, processed_at
        FROM documents
        WHERE id = %s
        LIMIT 1
        """
        cur = self.conn.cursor()
        try:
            cur.execute(q, (document_id,))
            r = cur.fetchone()
            if not r:
                return None
            return {
                "id": r[0],
                "company_id": r[1],
                "ticker": r[2],
                "filing_type": r[3],
                "filing_date": r[4],
                "source_url": r[5],
                "local_path": r[6],
                "content_hash": r[7],
                "word_count": int(r[8]) if r[8] is not None else 0,
                "chunk_count": int(r[9]) if r[9] is not None else 0,
                "status": r[10],
                "error_message": r[11],
                "created_at": r[12],
                "processed_at": r[13],
            }
        finally:
            cur.close()

    # -------------------------
    # Chunks
    # -------------------------
    def insert_chunks_bulk(self, chunks: Sequence[ChunkRow]) -> None:
        if not chunks:
            return
        q = """
        INSERT INTO document_chunks (
            id, document_id, chunk_index, content, section, start_char, end_char, word_count
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cur = self.conn.cursor()
        try:
            cur.executemany(
                q,
                [
                    (
                        c.id,
                        c.document_id,
                        c.chunk_index,
                        c.content,
                        c.section,
                        c.start_char,
                        c.end_char,
                        c.word_count,
                    )
                    for c in chunks
                ],
            )
        finally:
            cur.close()

    def list_chunks(
        self,
        document_id: str,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        q = """
        SELECT id, document_id, chunk_index, content, section, start_char, end_char, word_count, created_at
        FROM document_chunks
        WHERE document_id = %s
        ORDER BY chunk_index ASC
        LIMIT %s OFFSET %s
        """
        cur = self.conn.cursor()
        try:
            cur.execute(q, (document_id, limit, offset))
            rows = cur.fetchall()
            return [
                {
                    "id": r[0],
                    "document_id": r[1],
                    "chunk_index": int(r[2]),
                    "content": r[3],
                    "section": r[4],
                    "start_char": int(r[5]) if r[5] is not None else 0,
                    "end_char": int(r[6]) if r[6] is not None else 0,
                    "word_count": int(r[7]) if r[7] is not None else 0,
                    "created_at": r[8],
                }
                for r in rows
            ]
        finally:
            cur.close()

    def get_chunk(self, chunk_id: str) -> dict[str, Any] | None:
        q = """
        SELECT id, document_id, chunk_index, content, section, start_char, end_char, word_count, created_at
        FROM document_chunks
        WHERE id = %s
        LIMIT 1
        """
        cur = self.conn.cursor()
        try:
            cur.execute(q, (chunk_id,))
            r = cur.fetchone()
            if not r:
                return None
            return {
                "id": r[0],
                "document_id": r[1],
                "chunk_index": int(r[2]),
                "content": r[3],
                "section": r[4],
                "start_char": int(r[5]) if r[5] is not None else 0,
                "end_char": int(r[6]) if r[6] is not None else 0,
                "word_count": int(r[7]) if r[7] is not None else 0,
                "created_at": r[8],
            }
        finally:
            cur.close()

    # -------------------------
    # Stats
    # -------------------------
    def stats(self) -> dict[str, int]:
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM documents")
            docs = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM document_chunks")
            chunks = int(cur.fetchone()[0])
            return {"documents": docs, "chunks": chunks}
        finally:
            cur.close()
