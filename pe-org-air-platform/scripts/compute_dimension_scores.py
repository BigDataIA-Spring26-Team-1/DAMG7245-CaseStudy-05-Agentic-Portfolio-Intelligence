from __future__ import annotations
import argparse
import sys
from pathlib import Path
from uuid import uuid4
from datetime import date
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from app.services.snowflake import get_snowflake_connection
from app.scoring_engine.evidence_mapper import EvidenceItem, map_evidence_to_dimensions
from app.scoring_engine.rubric_scorer import score_dimensions

def get_or_create_assessment(cur, company_id: str) -> str:
    # Use latest assessment if exists
    cur.execute(
        """
        SELECT id
        FROM assessments
        WHERE company_id = %s
        ORDER BY assessment_date DESC, created_at DESC
        LIMIT 1
        """,
        (company_id,),
    )
    row = cur.fetchone()
    if row:
        return str(row[0])
    assessment_id = str(uuid4())
    cur.execute(
        """
        INSERT INTO assessments (id, company_id, assessment_type, assessment_date, status)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (assessment_id, company_id, "cs3_auto", date.today().isoformat(), "draft"),
    )
    return assessment_id

def fetch_evidence(cur, company_id: str, chunk_limit: int = 200) -> list[EvidenceItem]:
    items: list[EvidenceItem] = []
    # Document chunks (10-K/10-Q/8-K)
    cur.execute(
        """
        SELECT d.filing_type, d.source_url, c.content
        FROM documents d
        JOIN document_chunks c ON c.document_id = d.id
        WHERE d.company_id = %s
        ORDER BY d.filing_date DESC, c.chunk_index ASC
        LIMIT %s
        """,
        (company_id, chunk_limit),
    )
    for filing_type, url, content in cur.fetchall() or []:
        items.append(
            EvidenceItem(
                source="document_chunk",
                evidence_type=str(filing_type),
                text=str(content or ""),
                url=str(url) if url else None,
            )
        )
    # External signals (jobs/news/patents/tech)
    cur.execute(
        """
        SELECT signal_type, url, title, content_text, published_at
        FROM external_signals
        WHERE company_id = %s
        ORDER BY collected_at DESC
        LIMIT 200
        """,
        (company_id,),
    )
    for sig_type, url, title, content_text, published_at in cur.fetchall() or []:
        text = f"{title or ''}\n{content_text or ''}".strip()
        items.append(
            EvidenceItem(
                source="external_signal",
                evidence_type=str(sig_type),
                text=text,
                url=str(url) if url else None,
                published_at=str(published_at) if published_at else None,
            )
        )
    return items

def upsert_dimension_score(cur, assessment_id: str, dim: str, score: float, confidence: float, evidence_count: int) -> None:
    # Upsert by uq_assessment_dimension (assessment_id, dimension)
    cur.execute(
        """
        SELECT id
        FROM dimension_scores
        WHERE assessment_id = %s AND dimension = %s
        LIMIT 1
        """,
        (assessment_id, dim),
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            """
            UPDATE dimension_scores
            SET score = %s, confidence = %s, evidence_count = %s, created_at = CURRENT_TIMESTAMP()
            WHERE id = %s
            """,
            (score, confidence, evidence_count, str(row[0])),
        )
        return
    cur.execute(
        """
        INSERT INTO dimension_scores (id, assessment_id, dimension, score, confidence, evidence_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (str(uuid4()), assessment_id, dim, score, confidence, evidence_count),
    )
    
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--company-id", required=True)
    parser.add_argument("--chunk-limit", type=int, default=200)
    args = parser.parse_args()
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        assessment_id = get_or_create_assessment(cur, args.company_id)
        evidence = fetch_evidence(cur, args.company_id, chunk_limit=args.chunk_limit)
        mapped = map_evidence_to_dimensions(evidence)
        results = score_dimensions(mapped)
        for r in results:
            upsert_dimension_score(cur, assessment_id, r.dimension, r.score, r.confidence, r.evidence_count)
        conn.commit()
        print("✅ Dimension scoring complete")
        print(f"company_id:    {args.company_id}")
        print(f"assessment_id: {assessment_id}")
        for r in results:
            print(f"- {r.dimension:18s} score={r.score:6.2f} conf={r.confidence:.2f} ev={r.evidence_count:3d} kws={r.top_keywords[:3]}")
        return 0
    finally:
        cur.close()
        conn.close()
if __name__ == "__main__":
    raise SystemExit(main())
 