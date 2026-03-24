from app.services.search.vector_store import VectorStore


class _FakeCollection:
    def __init__(self):
        self.last_n_results = None

    def query(self, query_embeddings, n_results, where, include):
        self.last_n_results = n_results
        return {
            "ids": [[
                "doc-1:chunk-a",
                "doc-1:chunk-b",
                "doc-2:chunk-a",
            ]],
            "documents": [[
                "Employee stock purchase plans were expanded in 2025.",
                "  Employee   stock purchase plans were expanded in 2025.  ",
                "Board discussed leadership pipeline and succession planning.",
            ]],
            "metadatas": [[
                {"dimension": "leadership_vision"},
                {"dimension": "leadership_vision"},
                {"dimension": "leadership_vision"},
            ]],
            "distances": [[0.3, 0.35, 0.4]],
        }


def test_query_dedupes_duplicate_text_and_keeps_top_k():
    store = VectorStore.__new__(VectorStore)
    store.collection = _FakeCollection()
    store._embed_texts = lambda texts: [[0.1, 0.2, 0.3]]

    hits = store.query("employee stock purchase plans", top_k=2)

    assert len(hits) == 2
    assert [h.id for h in hits] == ["doc-1:chunk-a", "doc-2:chunk-a"]
    assert store.collection.last_n_results == 10  # top_k * 5 over-fetch
