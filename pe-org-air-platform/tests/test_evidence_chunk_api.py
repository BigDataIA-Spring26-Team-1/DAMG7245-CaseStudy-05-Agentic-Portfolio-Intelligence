from __future__ import annotations


def test_documents_get_document_returns_payload(monkeypatch, client):
    class _Store:
        def get_document(self, document_id):
            assert document_id == "doc-1"
            return {"id": document_id, "ticker": "CAT", "status": "indexed"}

        def close(self):
            return None

    monkeypatch.setattr("app.routers.documents.EvidenceStore", lambda: _Store())

    resp = client.get("/api/v1/documents/doc-1")

    assert resp.status_code == 200
    assert resp.json()["status"] == "indexed"


def test_documents_list_returns_503_when_storage_is_unavailable(monkeypatch, client):
    def raise_store_error():
        raise RuntimeError("warehouse offline")

    monkeypatch.setattr("app.routers.documents.EvidenceStore", raise_store_error)

    resp = client.get("/api/v1/documents")

    assert resp.status_code == 503
    assert "Storage unavailable" in resp.json()["detail"]


def test_evidence_stats_returns_counts(monkeypatch, client):
    class _Store:
        def stats(self):
            return {"documents": 3, "chunks": 9}

        def close(self):
            return None

    monkeypatch.setattr("app.routers.evidence.EvidenceStore", lambda: _Store())

    resp = client.get("/api/v1/evidence/stats")

    assert resp.status_code == 200
    assert resp.json() == {"documents": 3, "chunks": 9}


def test_chunks_list_endpoint_passes_document_and_offset(monkeypatch, client):
    seen: dict[str, object] = {}

    class _Store:
        def list_chunks(self, document_id, limit=200, offset=0):
            seen["document_id"] = document_id
            seen["limit"] = limit
            seen["offset"] = offset
            return [{"id": "chunk-1", "document_id": document_id}]

        def close(self):
            return None

    monkeypatch.setattr("app.routers.chunk.EvidenceStore", lambda: _Store())

    resp = client.get("/api/v1/chunks/?document_id=doc-9&limit=2&offset=4")

    assert resp.status_code == 200
    assert resp.json()[0]["id"] == "chunk-1"
    assert seen == {"document_id": "doc-9", "limit": 2, "offset": 4}


def test_get_chunk_returns_404_when_missing(monkeypatch, client):
    class _Store:
        def get_chunk(self, chunk_id):
            assert chunk_id == "missing-chunk"
            return None

        def close(self):
            return None

    monkeypatch.setattr("app.routers.chunk.EvidenceStore", lambda: _Store())

    resp = client.get("/api/v1/chunks/missing-chunk")

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Chunk not found"


def test_evidence_document_chunks_alias_returns_chunk_list(monkeypatch, client):
    class _Store:
        def list_chunks(self, document_id, limit=200, offset=0):
            assert document_id == "doc-1"
            assert limit == 3
            assert offset == 1
            return [{"id": "chunk-7", "document_id": document_id, "chunk_index": 0}]

        def close(self):
            return None

    monkeypatch.setattr("app.routers.evidence.EvidenceStore", lambda: _Store())

    resp = client.get("/api/v1/evidence/documents/doc-1/chunks?limit=3&offset=1")

    assert resp.status_code == 200
    assert resp.json()[0]["id"] == "chunk-7"
