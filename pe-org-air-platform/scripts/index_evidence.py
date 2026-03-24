from __future__ import annotations

import argparse
from typing import Any, Dict, List

from app.services.integration.cs1_client import CS1Client
from app.services.integration.cs2_client import CS2Client, CS2Evidence
from app.services.result_artifacts import write_json_artifact
from app.services.retrieval.dimension_mapper import DimensionMapper
from app.services.search.vector_store import DocumentChunk, VectorStore


def evidence_to_docchunk(evidence: CS2Evidence, mapper: DimensionMapper) -> DocumentChunk:
    dimension = mapper.get_primary_dimension(
        signal_category=evidence.signal_category.value,
        source_type=evidence.source_type.value,
        public_names=False,
    )

    metadata: Dict[str, Any] = {
        "company_id": evidence.company_id,
        "dimension": dimension,
        "source_type": evidence.source_type.value,
        "signal_category": evidence.signal_category.value,
        "confidence": evidence.confidence if evidence.confidence is not None else 0.0,
        "source_url": evidence.source_url,
        "fiscal_year": evidence.fiscal_year,
        "title": evidence.title,
        "published_at": evidence.extracted_at.isoformat(),
    }

    return DocumentChunk(
        id=evidence.evidence_id,
        text=evidence.content,
        metadata=metadata,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--company-id", required=True)
    ap.add_argument("--schema", default=None, help="Optional Snowflake schema, e.g. PUBLIC")
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--min-confidence", type=float, default=None)
    ap.add_argument("--reindex", action="store_true", help="Delete existing index entries for this company first")
    args = ap.parse_args()

    store = VectorStore()
    client = CS2Client()
    companies = CS1Client()
    mapper = DimensionMapper()

    if args.reindex:
        deleted = store.delete_by_filter({"company_id": args.company_id})
        print(f"Deleted {deleted} existing vectors for company_id={args.company_id}")

    evidence = client.get_evidence(
        company_id=args.company_id,
        min_confidence=args.min_confidence,
    )
    source_type_counts: Dict[str, int] = {}
    for item in evidence:
        source_type_counts[item.source_type.value] = source_type_counts.get(item.source_type.value, 0) + 1

    total = 0
    for start in range(0, len(evidence), args.batch_size):
        batch = evidence[start : start + args.batch_size]
        chunks: List[DocumentChunk] = [
            evidence_to_docchunk(item, mapper)
            for item in batch
            if (item.content or "").strip()
        ]
        upserted = store.upsert(chunks)
        client.mark_indexed([chunk.id for chunk in chunks])
        total += upserted
        print(f"Upserted {upserted} chunks (running total={total})")

    ticker = None
    try:
        ticker = companies.get_company(args.company_id).ticker
    except Exception:
        ticker = None

    write_json_artifact(
        ticker=(ticker or args.company_id),
        category="retrieval",
        filename="latest_index_summary.json",
        payload={
            "company_id": args.company_id,
            "ticker": ticker,
            "indexed_chunks": total,
            "evidence_count": len(evidence),
            "source_type_counts": source_type_counts,
            "min_confidence": args.min_confidence,
            "reindex": bool(args.reindex),
        },
    )

    print(f"Done. Indexed total chunks={total} for company_id={args.company_id}")


if __name__ == "__main__":
    main()
