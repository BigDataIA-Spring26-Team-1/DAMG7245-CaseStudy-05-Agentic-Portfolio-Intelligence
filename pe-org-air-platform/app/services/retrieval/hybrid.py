from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from app.services.integration.evidence_client import EvidenceClient
from app.services.retrieval.bm25_store import BM25Hit, BM25Store
from app.services.retrieval.hyde import HyDEGenerator
from app.services.search.vector_store import DocumentChunk, SearchHit, VectorStore


@dataclass(frozen=True)
class HybridHit:
    id: str
    text: str
    score: float  # fused score (RRF)
    metadata: Dict[str, Any]
    semantic_score: Optional[float] = None
    bm25_score: Optional[float] = None


def rrf_fuse(
    semantic_hits: List[SearchHit],
    bm25_hits: List[BM25Hit],
    k: int = 60,
) -> List[HybridHit]:
    """
    Reciprocal Rank Fusion (RRF):
      score(doc) = Σ 1 / (k + rank_i(doc))
    """
    fused: Dict[str, Dict[str, Any]] = {}

    for rank, h in enumerate(semantic_hits, start=1):
        entry = fused.setdefault(
            h.id,
            {"id": h.id, "text": h.text, "metadata": h.metadata or {}},
        )
        entry["semantic_score"] = h.score
        entry["rrf"] = entry.get("rrf", 0.0) + 1.0 / (k + rank)

    for rank, h in enumerate(bm25_hits, start=1):
        entry = fused.setdefault(
            h.chunk_uid,
            {"id": h.chunk_uid, "text": h.text, "metadata": {}},
        )
        entry["bm25_score"] = h.score
        entry["rrf"] = entry.get("rrf", 0.0) + 1.0 / (k + rank)

    results: List[HybridHit] = []
    for v in fused.values():
        results.append(
            HybridHit(
                id=v["id"],
                text=v.get("text", ""),
                metadata=v.get("metadata", {}) or {},
                score=float(v.get("rrf", 0.0)),
                semantic_score=v.get("semantic_score"),
                bm25_score=v.get("bm25_score"),
            )
        )

    results.sort(key=lambda x: x.score, reverse=True)
    return results


class HybridRetriever:
    """
    Hybrid retrieval = Semantic (Chroma) + Lexical (BM25 over Snowflake chunks), fused via RRF.

    Enhancements:
    - Chroma-compatible metadata filtering
    - Optional HyDE query expansion
    - BM25-only hit enrichment from Snowflake metadata
    """

    def __init__(self, schema: Optional[str] = None) -> None:
        self.vector_store = VectorStore()
        self.bm25_store = BM25Store(schema=schema)
        self.evidence = EvidenceClient(schema=schema)
        self.hyde = HyDEGenerator()

    def index_documents(self, docs: Sequence[Any]) -> int:
        """
        Accept either DocumentChunk objects or simple dict payloads and upsert them
        into the semantic store. This keeps the exercise and indexing flows close to
        the assignment's public interface.
        """
        chunks: List[DocumentChunk] = []
        for doc in docs:
            if isinstance(doc, DocumentChunk):
                chunks.append(doc)
                continue

            if not isinstance(doc, dict):
                continue

            doc_id = str(doc.get("doc_id") or doc.get("id") or "").strip()
            content = str(doc.get("content") or doc.get("text") or "").strip()
            metadata = doc.get("metadata") or {}
            if not doc_id or not content or not isinstance(metadata, dict):
                continue

            chunks.append(
                DocumentChunk(
                    id=doc_id,
                    text=content,
                    metadata=dict(metadata),
                )
            )

        return self.vector_store.upsert(chunks)

    def _build_chroma_where(
        self,
        company_id: Optional[str] = None,
        dimension: Optional[str] = None,
        min_confidence: Optional[float] = None,
        source_types: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        filters: List[Dict[str, Any]] = []

        if company_id:
            filters.append({"company_id": company_id})

        if dimension:
            filters.append({"dimension": dimension})

        if min_confidence is not None:
            filters.append({"confidence": {"$gte": float(min_confidence)}})

        normalized_source_types = [
            str(source_type).strip()
            for source_type in (source_types or [])
            if str(source_type).strip()
        ]
        if len(normalized_source_types) == 1:
            filters.append({"source_type": normalized_source_types[0]})
        elif len(normalized_source_types) > 1:
            filters.append({"$or": [{"source_type": source_type} for source_type in normalized_source_types]})

        if not filters:
            return None

        if len(filters) == 1:
            return filters[0]

        return {"$and": filters}

    def search(
        self,
        query: str,
        top_k: int = 5,
        company_id: Optional[str] = None,
        dimension: Optional[str] = None,
        min_confidence: Optional[float] = None,
        source_types: Optional[List[str]] = None,
        semantic_k: int = 10,
        bm25_k: int = 10,
        use_hyde: bool = True,
    ) -> List[HybridHit]:
        effective_query = query
        if use_hyde:
            hyde_result = self.hyde.generate(
                query=query,
                dimension=dimension,
                company_id=company_id,
            )
            effective_query = hyde_result.expanded_query

        where = self._build_chroma_where(
            company_id=company_id,
            dimension=dimension,
            min_confidence=min_confidence,
            source_types=source_types,
        )

        semantic_hits = self.vector_store.query(
            query_text=effective_query,
            top_k=semantic_k,
            where=where,
        )

        bm25_hits: List[BM25Hit] = []
        if company_id:
            bm25_hits = self.bm25_store.search(
                company_id=company_id,
                query=effective_query,
                top_k=bm25_k,
                min_confidence=min_confidence,
                dimension=dimension,
            )

        fused = rrf_fuse(semantic_hits=semantic_hits, bm25_hits=bm25_hits)[:top_k]

        missing_uids = [h.id for h in fused if not h.metadata]
        if missing_uids:
            meta_map = self.evidence.get_chunk_metadata_by_uids(missing_uids)

            enriched: List[HybridHit] = []
            for h in fused:
                if h.metadata:
                    enriched.append(h)
                    continue

                md = meta_map.get(h.id, {}) or {}
                chunk_text = md.get("_chunk_text", "")

                md_clean = dict(md)
                md_clean.pop("_chunk_text", None)

                enriched.append(
                    HybridHit(
                        id=h.id,
                        text=h.text or chunk_text,
                        score=h.score,
                        metadata=md_clean,
                        semantic_score=h.semantic_score,
                        bm25_score=h.bm25_score,
                    )
                )
            fused = enriched

        normalized_source_types = {
            str(source_type).strip()
            for source_type in (source_types or [])
            if str(source_type).strip()
        }
        if normalized_source_types:
            fused = [
                hit
                for hit in fused
                if str(hit.metadata.get("source_type", "")).strip() in normalized_source_types
            ][:top_k]

        return fused
