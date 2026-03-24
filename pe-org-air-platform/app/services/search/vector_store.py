from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import chromadb
from chromadb.api.models.Collection import Collection

# Force transformers/sentence-transformers to run in PyTorch mode only.
os.environ.setdefault("TRANSFORMERS_NO_TF", "1")
os.environ.setdefault("USE_TF", "0")
from sentence_transformers import SentenceTransformer


@dataclass(frozen=True)
class DocumentChunk:
    """
    A single evidence chunk to index & retrieve.

    id: unique (recommended: f"{doc_id}:{chunk_id}")
    text: chunk text
    metadata: JSON-serializable dict (company_id, dimension, confidence, source_type, etc.)
    """
    id: str
    text: str
    metadata: Dict[str, Any]


@dataclass(frozen=True)
class SearchHit:
    id: str
    text: str
    score: float  # higher is better
    metadata: Dict[str, Any]


class VectorStore:
    """
    Persistent ChromaDB vector store with local embeddings (SentenceTransformers).

    Code review notes:
    - Persistence makes results reproducible across runs.
    - Metadata filtering happens in Chroma (server-side 'where').
    - Converts cosine distance to a [0,1] "score" for API consumers.
    """

    def __init__(
        self,
        persist_path: Optional[str] = None,
        collection_name: str = "evidence_chunks",
        embedding_model: Optional[str] = None,
    ) -> None:
        self.persist_path = persist_path or os.getenv("CHROMA_PATH", "./chroma")
        self.collection_name = collection_name
        self.embedding_model_name = embedding_model or os.getenv(
            "EMBEDDING_MODEL",
            "sentence-transformers/all-MiniLM-L6-v2",
        )

        self.client = chromadb.PersistentClient(path=self.persist_path)
        self.embedder = SentenceTransformer(self.embedding_model_name)

        self.collection: Collection = self.client.get_or_create_collection(
            name=self.collection_name,
            metadata={"hnsw:space": "cosine"},
        )

    def _embed_texts(self, texts: Sequence[str]) -> List[List[float]]:
        vectors = self.embedder.encode(list(texts), normalize_embeddings=True)
        return [v.tolist() for v in vectors]

    def upsert(self, chunks: Sequence[DocumentChunk]) -> int:
        if not chunks:
            return 0

        ids = [c.id for c in chunks]
        docs = [c.text for c in chunks]
        metas = [c.metadata for c in chunks]
        embeds = self._embed_texts(docs)

        try:
            self.collection.upsert(ids=ids, documents=docs, metadatas=metas, embeddings=embeds)
        except AttributeError:
            # Older chroma versions: emulate upsert
            existing = self.collection.get(ids=ids)
            if existing and existing.get("ids"):
                self.collection.delete(ids=ids)
            self.collection.add(ids=ids, documents=docs, metadatas=metas, embeddings=embeds)

        return len(chunks)

    def query(
        self,
        query_text: str,
        top_k: int = 5,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[SearchHit]:
        if not query_text.strip():
            return []

        count: Optional[int] = None
        try:
            count_fn = getattr(self.collection, "count", None)
            if callable(count_fn):
                count = int(count_fn())
                if count == 0:
                    return []
        except Exception:
            # Allow lightweight fake collections used in tests that do not expose count()
            count = None

        q_embed = self._embed_texts([query_text])[0]
        requested_k = max(1, top_k)

        if count is None:
            fetch_k = max(requested_k, requested_k * 5)
        else:
            fetch_k = min(max(requested_k, requested_k * 5), count)

        res = self.collection.query(
            query_embeddings=[q_embed],
            n_results=fetch_k,
            where=where or None,
            include=["documents", "metadatas", "distances"],
        )

        ids = (res.get("ids") or [[]])[0]
        docs = (res.get("documents") or [[]])[0]
        metas = (res.get("metadatas") or [[]])[0]
        dists = (res.get("distances") or [[]])[0]

        hits: List[SearchHit] = []
        for i, doc_id in enumerate(ids):
            dist = float(dists[i]) if i < len(dists) else 1.0
            score = max(0.0, 1.0 - dist)

            hits.append(
                SearchHit(
                    id=str(doc_id),
                    text=str(docs[i]) if i < len(docs) else "",
                    score=score,
                    metadata=dict(metas[i]) if i < len(metas) and metas[i] else {},
                )
            )

        hits.sort(key=lambda h: h.score, reverse=True)

        deduped_hits: List[SearchHit] = []
        seen_text_keys = set()
        for hit in hits:
            text_key = " ".join(hit.text.split()).strip().lower()
            if text_key and text_key in seen_text_keys:
                continue
            if text_key:
                seen_text_keys.add(text_key)

            deduped_hits.append(hit)
            if len(deduped_hits) >= requested_k:
                break

        return deduped_hits

    def delete_by_filter(self, where: Dict[str, Any]) -> int:
        before = self.collection.count()
        self.collection.delete(where=where)
        after = self.collection.count()
        return max(0, before - after)
