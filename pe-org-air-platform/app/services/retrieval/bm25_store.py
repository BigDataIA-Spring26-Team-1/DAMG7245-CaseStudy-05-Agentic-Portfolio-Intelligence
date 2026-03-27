from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

try:
    from rank_bm25 import BM25Okapi
except ImportError:
    BM25Okapi = None

from app.services.integration.cs2_client import CS2Client
from app.services.integration.evidence_client import EvidenceClient

_WORD_RE = re.compile(r"[A-Za-z0-9]+")


def tokenize(text: str) -> List[str]:
    return _WORD_RE.findall((text or "").lower())


def _fallback_scores(query_tokens: List[str], corpus_tokens: List[List[str]]) -> List[float]:
    query_set = set(query_tokens)
    scores: List[float] = []
    for doc_tokens in corpus_tokens:
        if not doc_tokens:
            scores.append(0.0)
            continue
        doc_set = set(doc_tokens)
        overlap = len(query_set & doc_set)
        coverage = overlap / max(len(query_set), 1)
        density = overlap / max(len(doc_set), 1)
        scores.append(float(coverage + (0.25 * density)))
    return scores


@dataclass(frozen=True)
class BM25Hit:
    chunk_uid: str  # "{document_id}:{chunk_id}"
    score: float
    text: str
    document_id: str
    chunk_id: str
    chunk_index: int


class BM25Store:
    """
    Build BM25 on-the-fly per company_id from Snowflake chunks.

    Notes:
    - Snowflake remains the system of record
    - BM25 is derived at query time (no persistent lexical index required)
    - We keep only top_k candidates in memory for fusion / response
    """

    def __init__(self, schema: Optional[str] = None) -> None:
        self.client = EvidenceClient(schema=schema)
        self.cs2_client = CS2Client()

    def search(
        self,
        company_id: str,
        query: str,
        top_k: int = 10,
        batch_size: int = 1000,
        max_chunks: int = 8000,
        min_confidence: Optional[float] = None,
        dimension: Optional[str] = None,
    ) -> List[BM25Hit]:
        """
        Returns BM25 hits for a company.

        Dimension filtering is a no-op unless your Snowflake chunks store dimension.
        (Most teams tag dimension during vector indexing, not in Snowflake.)
        """
        query_tokens = tokenize(query)
        if not query_tokens:
            return []

        texts: List[str] = []
        meta: List[Tuple[str, str, int, str]] = []  # (doc_id, chunk_id, chunk_index, text)

        evidence_getter = getattr(getattr(self, "cs2_client", None), "get_evidence", None)
        if callable(evidence_getter):
            evidence = evidence_getter(
                company_id=company_id,
                min_confidence=min_confidence or 0.0,
            )
            for item in evidence[:max_chunks]:
                if not (item.content or "").strip():
                    continue
                _ = dimension
                texts.append(item.content)
                meta.append((item.evidence_id, "0", 0, item.content))
        else:
            seen = 0
            for batch in self.client.iter_chunks_for_company(
                company_id=company_id,
                batch_size=batch_size,
                min_confidence=min_confidence,
            ):
                for r in batch:
                    if not (r.chunk_text or "").strip():
                        continue

                    # dimension not stored in Snowflake by default; keep no-op for now
                    _ = dimension

                    texts.append(r.chunk_text)
                    meta.append((r.document_id, r.chunk_id, r.chunk_index, r.chunk_text))
                    seen += 1
                    if seen >= max_chunks:
                        break
                if seen >= max_chunks:
                    break

        if not texts:
            return []

        corpus_tokens = [tokenize(t) for t in texts]
        if BM25Okapi is not None:
            bm25 = BM25Okapi(corpus_tokens)
            scores = bm25.get_scores(query_tokens)
        else:
            # Keep lexical retrieval available when rank_bm25 is not installed.
            scores = _fallback_scores(query_tokens, corpus_tokens)

        top_idx = sorted(range(len(scores)), key=lambda i: float(scores[i]), reverse=True)[:top_k]

        hits: List[BM25Hit] = []
        for i in top_idx:
            doc_id, chunk_id, chunk_index, chunk_text = meta[i]
            hits.append(
                BM25Hit(
                    chunk_uid=f"{doc_id}:{chunk_id}",
                    score=float(scores[i]),
                    text=chunk_text,
                    document_id=doc_id,
                    chunk_id=chunk_id,
                    chunk_index=chunk_index,
                )
            )

        return hits
