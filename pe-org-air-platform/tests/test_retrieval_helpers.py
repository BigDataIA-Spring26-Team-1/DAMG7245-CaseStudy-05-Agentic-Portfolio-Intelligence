from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.services.retrieval.bm25_store import BM25Store, BM25Hit
from app.services.retrieval.dimension_mapper import DimensionMapper, map_dimension
from app.services.retrieval.hybrid import HybridRetriever, rrf_fuse
from app.services.retrieval.hyde import HyDEGenerator
from app.services.search.vector_store import DocumentChunk
from app.services.search.vector_store import SearchHit


def test_hyde_generator_expands_query_with_dimension_hints_and_company():
    generator = HyDEGenerator()

    out = generator.generate(
        query="AI leadership readiness",
        dimension="leadership",
        company_id="company-1",
    )

    assert out.mode == "deterministic_hyde"
    assert out.dimension == "leadership"
    assert "company-1" in out.expanded_query
    assert "executive sponsorship" in out.expanded_query
    assert "AI leadership readiness" in out.hypothetical_document


def test_hyde_generator_requires_query():
    generator = HyDEGenerator()

    with pytest.raises(ValueError, match="query is required"):
        generator.generate(query=" ")


def test_rrf_fuse_merges_semantic_and_bm25_scores():
    semantic_hits = [
        SearchHit(id="doc-1:chunk-1", text="AI roadmap", score=0.92, metadata={"source_type": "sec_filing"}),
        SearchHit(id="doc-2:chunk-1", text="Board oversight", score=0.61, metadata={"source_type": "board"}),
    ]
    bm25_hits = [
        BM25Hit(
            chunk_uid="doc-1:chunk-1",
            score=12.0,
            text="AI roadmap",
            document_id="doc-1",
            chunk_id="chunk-1",
            chunk_index=0,
        )
    ]

    out = rrf_fuse(semantic_hits=semantic_hits, bm25_hits=bm25_hits)

    assert out[0].id == "doc-1:chunk-1"
    assert out[0].semantic_score == 0.92
    assert out[0].bm25_score == 12.0
    assert out[1].id == "doc-2:chunk-1"


def test_bm25_store_ranks_most_relevant_chunk_first():
    store = object.__new__(BM25Store)
    store.client = SimpleNamespace(
        iter_chunks_for_company=lambda **kwargs: [
            [
                SimpleNamespace(
                    document_id="doc-1",
                    chunk_id="chunk-1",
                    chunk_index=0,
                    chunk_text="Machine learning engineer hiring and AI platform buildout",
                ),
                SimpleNamespace(
                    document_id="doc-1",
                    chunk_id="chunk-2",
                    chunk_index=1,
                    chunk_text="Audit controls and compliance discussion for governance",
                ),
            ]
        ]
    )

    hits = store.search(company_id="company-1", query="machine learning engineer", top_k=1)

    assert len(hits) == 1
    assert hits[0].chunk_uid == "doc-1:chunk-1"
    assert hits[0].chunk_index == 0


def test_hybrid_retriever_builds_compound_chroma_filters():
    retriever = object.__new__(HybridRetriever)

    where = retriever._build_chroma_where(
        company_id="company-1",
        dimension="culture",
        min_confidence=0.6,
    )

    assert where == {
        "$and": [
            {"company_id": "company-1"},
            {"dimension": "culture"},
            {"confidence": {"$gte": 0.6}},
        ]
    }


def test_hybrid_retriever_indexes_simple_dict_documents():
    seen = {}
    retriever = object.__new__(HybridRetriever)

    def _upsert(chunks):
        seen["chunks"] = chunks
        return len(chunks)

    retriever.vector_store = SimpleNamespace(upsert=_upsert)

    out = retriever.index_documents(
        [
            {
                "doc_id": "doc-1",
                "content": "Board oversight and governance review cadence are documented.",
                "metadata": {"company_id": "company-1", "dimension": "ai_governance"},
            },
            DocumentChunk(
                id="doc-2",
                text="Cloud platform automation and MLOps tooling are in place.",
                metadata={"company_id": "company-1", "dimension": "technology_stack"},
            ),
        ]
    )

    assert out == 2
    assert len(seen["chunks"]) == 2
    assert seen["chunks"][0].id == "doc-1"
    assert seen["chunks"][1].id == "doc-2"


def test_dimension_mapper_uses_signal_mapping_keyword_and_default_fallbacks():
    assert map_dimension(source_type="sec_filing", signal_category="jobs") == "talent_skills"
    assert (
        map_dimension(
            source_type="unknown",
            signal_category=None,
            chunk_text="Bias controls, audit readiness, and compliance reviews are in place.",
        )
        == "ai_governance"
    )
    assert map_dimension(source_type="unknown", signal_category=None, chunk_text="plain unrelated text") == "technology_stack"


def test_dimension_mapper_facade_returns_public_dimension_names():
    mapper = DimensionMapper()

    weights = mapper.get_dimension_weights("technology_hiring")
    primary = mapper.get_primary_dimension("technology_hiring")

    assert "talent" in weights
    assert "talent_skills" not in weights
    assert primary == "talent"
