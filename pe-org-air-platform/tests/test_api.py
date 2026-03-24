from datetime import date, datetime
from uuid import uuid4

COMPANY_ID = "550e8400-e29b-41d4-a716-446655440001"
COMPANY_ID_2 = "550e8400-e29b-41d4-a716-446655440002"
INDUSTRY_ID = "550e8400-e29b-41d4-a716-446655440003"
ASSESSMENT_ID = "550e8400-e29b-41d4-a716-446655440004"
ASSESSMENT_ID_2 = "550e8400-e29b-41d4-a716-446655440005"
SCORE_ID = "550e8400-e29b-41d4-a716-446655440006"
MISSING_UUID = str(uuid4()) # Valid UUID format that definitely doesn't exist

def _payload_to_dict(payload):
    # Added mode='json' to handle UUID serialization
    return payload.model_dump(mode='json') if hasattr(payload, "model_dump") else payload

def test_health_ok(client, monkeypatch):
    monkeypatch.setattr("app.routers.health.ping_redis", lambda: (True, "ok"))
    monkeypatch.setattr("app.routers.health.ping_snowflake", lambda: (True, "ok"))
    monkeypatch.setattr("app.routers.health.ping_s3", lambda: (True, "ok"))
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

def test_health_returns_503_when_dep_down(client, monkeypatch):
    monkeypatch.setattr("app.routers.health.ping_redis", lambda: (False, "down"))
    monkeypatch.setattr("app.routers.health.ping_snowflake", lambda: (True, "ok"))
    monkeypatch.setattr("app.routers.health.ping_s3", lambda: (True, "ok"))
    r = client.get("/health")
    assert r.status_code == 503
    assert r.json()["status"] == "degraded"

def test_health_detailed_degraded_when_deps_fail(client, monkeypatch):
    monkeypatch.setattr("app.routers.health.ping_redis", lambda: (False, "down"))
    monkeypatch.setattr("app.routers.health.ping_snowflake", lambda: (True, "ok"))
    monkeypatch.setattr("app.routers.health.ping_s3", lambda: (True, "ok"))
    r = client.get("/health/detailed")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "degraded"
    assert body["dependencies"]["redis"]["ok"] is False

def test_health_detailed_ok_when_all_good(client, monkeypatch):
    monkeypatch.setattr("app.routers.health.ping_redis", lambda: (True, "ok"))
    monkeypatch.setattr("app.routers.health.ping_snowflake", lambda: (True, "ok"))
    monkeypatch.setattr("app.routers.health.ping_s3", lambda: (True, "ok"))
    r = client.get("/health/detailed")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["dependencies"]["redis"]["ok"] is True

def test_list_companies_returns_page_shape(client, fake_sf):
    fake_sf._one = (2,)
    fake_sf._all = [
        (COMPANY_ID, "Test A", "TCA", INDUSTRY_ID, 0.25, False, datetime.now(), datetime.now()),
        (COMPANY_ID_2, "Test B", "TCB", INDUSTRY_ID, 0.25, False, datetime.now(), datetime.now()),
    ]
    r = client.get("/api/v1/companies?page=1&page_size=20")
    assert r.status_code == 200
    body = r.json()
    assert "items" in body
    assert body["page"] == 1
    assert body["page_size"] == 20
    assert body["total"] == 2
    assert len(body["items"]) == 2

def test_list_companies_empty(client, fake_sf):
    fake_sf._one = (0,)
    fake_sf._all = []
    r = client.get("/api/v1/companies?page=1&page_size=20")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 0
    assert body["total_pages"] == 0
    assert body["items"] == []


def test_list_companies_supports_query_filter(client, fake_sf):
    fake_sf._one = (1,)
    fake_sf._all = [
        (COMPANY_ID, "NVIDIA Corporation", "NVDA", INDUSTRY_ID, 0.25, False, datetime.now(), datetime.now()),
    ]
    r = client.get("/api/v1/companies?page=1&page_size=20&q=nvda")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert len(body["items"]) == 1
    assert body["items"][0]["ticker"] == "NVDA"

def test_create_company_invalid_industry(client, fake_sf):
    payload = {"name": "Test Co", "ticker": "TCO", "industry_id": INDUSTRY_ID, "position_factor": 0.25}
    fake_sf._one = None
    r = client.post("/api/v1/companies", json=payload)
    assert r.status_code == 400
    assert r.json()["detail"] == "Invalid industry_id"

def test_create_company_success(client, fake_sf):
    payload = {"name": "Test Co", "ticker": "TCO", "industry_id": INDUSTRY_ID, "position_factor": 0.25}
    row = (COMPANY_ID, "Test Co", "TCO", INDUSTRY_ID, 0.25, False, datetime.now(), datetime.now())
    fake_sf._one_queue = [(1,), None, row]
    r = client.post("/api/v1/companies", json=payload)
    assert r.status_code == 201
    body = r.json()
    assert body["name"] == "Test Co"
    assert body["ticker"] == "TCO"

def test_get_company_with_cached_value_returns_ok(client, fake_sf):
    from app.services import redis_cache
    cached = {"id": COMPANY_ID, "name": "Cached Co", "ticker": "CCO", "industry_id": INDUSTRY_ID, "position_factor": 0.5, "is_deleted": False, "created_at": datetime.now().isoformat(), "updated_at": None}
    redis_cache.cache_set_json(f"company:{COMPANY_ID}", cached, 60)
    row = (COMPANY_ID, "Cached Co", "CCO", INDUSTRY_ID, 0.5, False, datetime.now(), None)
    fake_sf._one = row
    r = client.get(f"/api/v1/companies/{COMPANY_ID}")
    assert r.status_code == 200
    assert r.json()["name"] == "Cached Co"

def test_get_company_cache_miss_sets_cache(client, fake_sf, monkeypatch):
    from app.routers import companies
    seen = {}
    def _cache_set_json(key, payload, ttl_seconds): # FIXED: Matches real signature
        seen["key"] = key
        seen["payload"] = payload
        seen["ttl"] = ttl_seconds
    monkeypatch.setattr(companies, "cache_set_json", _cache_set_json)
    row = (COMPANY_ID_2, "Fresh Co", "FCO", INDUSTRY_ID, 0.2, False, datetime.now(), None)
    fake_sf._one = row
    r = client.get(f"/api/v1/companies/{COMPANY_ID_2}")
    assert r.status_code == 200
    assert seen["key"] == f"company:{COMPANY_ID_2}"
    payload = _payload_to_dict(seen["payload"])
    assert payload["name"] == "Fresh Co"

def test_get_company_not_found(client, fake_sf):
    fake_sf._one = None
    r = client.get(f"/api/v1/companies/{MISSING_UUID}") # FIXED: Used valid UUID
    assert r.status_code == 404

def test_update_company_no_fields(client, fake_sf):
    row = (COMPANY_ID, "Test Co", "TCO", INDUSTRY_ID, 0.25, False, datetime.now(), datetime.now())
    fake_sf._one_queue = [(1,), row]
    r = client.put(f"/api/v1/companies/{COMPANY_ID}", json={})
    assert r.status_code == 200
    # Fixed assertion to expect actual UUID
    assert r.json()["id"] == COMPANY_ID

def test_update_company_with_fields(client, fake_sf):
    row = (COMPANY_ID, "Updated Co", "UCO", INDUSTRY_ID, 0.3, False, datetime.now(), datetime.now())
    fake_sf._one_queue = [(1,), None, row]
    r = client.put(f"/api/v1/companies/{COMPANY_ID}", json={"name": "Updated Co", "ticker": "UCO", "position_factor": 0.3})
    assert r.status_code == 200
    assert r.json()["name"] == "Updated Co"

def test_update_company_not_found(client, fake_sf):
    fake_sf._one = None
    r = client.put(f"/api/v1/companies/{MISSING_UUID}", json={"name": "X"}) # FIXED
    assert r.status_code == 404

def test_delete_company_not_found(client, fake_sf):
    fake_sf.rowcount = 0
    r = client.delete(f"/api/v1/companies/{MISSING_UUID}") # FIXED
    assert r.status_code == 404

def test_delete_company_success(client, fake_sf):
    fake_sf.rowcount = 1
    r = client.delete(f"/api/v1/companies/{COMPANY_ID}")
    assert r.status_code == 204

def test_create_assessment_happy_path(client, fake_sf):
    fake_sf._one_queue = [(1,)]
    payload = {"company_id": COMPANY_ID, "assessment_type": "screening", "assessment_date": str(date.today()), "primary_assessor": "Raghav", "secondary_assessor": "Ayush"}
    fake_sf._one_queue.append((ASSESSMENT_ID, COMPANY_ID, "screening", str(date.today()), "draft", "Raghav", "Ayush", None, None, None, datetime.now()))
    r = client.post("/api/v1/assessments", json=payload)
    assert r.status_code in (200, 201)
    body = r.json()
    assert body["company_id"] == COMPANY_ID
    assert body["assessment_type"] == "screening"

def test_create_assessment_invalid_company(client, fake_sf):
    fake_sf._one = None
    payload = {"company_id": COMPANY_ID_2, "assessment_type": "screening", "assessment_date": str(date.today()), "primary_assessor": "Raghav", "secondary_assessor": "Ayush"}
    r = client.post("/api/v1/assessments", json=payload)
    assert r.status_code == 400

def test_get_dimension_scores_empty(client, fake_sf):
    # Fixed to return 0 count for empty check
    fake_sf._one = (0,)
    fake_sf._all = []
    r = client.get(f"/api/v1/assessments/{ASSESSMENT_ID}/scores?page=1&page_size=20")
    assert r.status_code == 200
    body = r.json()
    assert body["items"] == []
    assert body["total"] == 0

def test_get_dimension_scores_not_found(client, fake_sf):
    fake_sf._one = None
    r = client.get(f"/api/v1/assessments/{MISSING_UUID}/scores") # FIXED
    assert r.status_code == 404

def test_get_assessment_cache_hit(client, fake_sf):
    from app.services import redis_cache
    cached = {"id": ASSESSMENT_ID, "company_id": COMPANY_ID, "assessment_type": "screening", "assessment_date": str(date.today()), "status": "draft", "primary_assessor": "A", "secondary_assessor": "B", "vr_score": None, "confidence_lower": None, "confidence_upper": None, "created_at": datetime.now().isoformat()}
    redis_cache.cache_set_json(f"assessment:{ASSESSMENT_ID}", cached, 60)
    r = client.get(f"/api/v1/assessments/{ASSESSMENT_ID}")
    assert r.status_code == 200
    assert r.json()["id"] == ASSESSMENT_ID

def test_get_assessment_cache_miss_sets_cache(client, fake_sf, monkeypatch):
    from app.routers import assessments
    seen = {}
    def _cache_set_json(key, payload, ttl_seconds): # FIXED
        seen["key"] = key
        seen["payload"] = payload
        seen["ttl"] = ttl_seconds
    monkeypatch.setattr(assessments, "cache_set_json", _cache_set_json)
    row = (ASSESSMENT_ID_2, COMPANY_ID_2, "screening", str(date.today()), "draft", "A", "B", None, None, None, datetime.now())
    fake_sf._one = row
    r = client.get(f"/api/v1/assessments/{ASSESSMENT_ID_2}")
    assert r.status_code == 200
    assert seen["key"] == f"assessment:{ASSESSMENT_ID_2}"
    payload = _payload_to_dict(seen["payload"])
    assert payload["company_id"] == COMPANY_ID_2

def test_get_assessment_not_found(client, fake_sf):
    fake_sf._one = None
    r = client.get(f"/api/v1/assessments/{MISSING_UUID}") # FIXED
    assert r.status_code == 404

def test_list_assessments_with_filter(client, fake_sf):
    row = (ASSESSMENT_ID, COMPANY_ID, "screening", str(date.today()), "draft", "A", "B", None, None, None, datetime.now())
    fake_sf._one = (1,)
    fake_sf._all = [row]
    r = client.get(f"/api/v1/assessments?company_id={COMPANY_ID}&page=1&page_size=20")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert len(body["items"]) == 1

def test_list_assessments_no_filter(client, fake_sf):
    row = (ASSESSMENT_ID, COMPANY_ID, "screening", str(date.today()), "draft", "A", "B", None, None, None, datetime.now())
    fake_sf._one = (1,)
    fake_sf._all = [row]
    r = client.get("/api/v1/assessments?page=1&page_size=20")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert len(body["items"]) == 1

def test_update_assessment_status(client, fake_sf):
    row = (ASSESSMENT_ID, COMPANY_ID, "screening", str(date.today()), "submitted", "A", "B", None, None, None, datetime.now())
    fake_sf._one_queue = [("draft",), row]
    r = client.patch(f"/api/v1/assessments/{ASSESSMENT_ID}/status", json={"status": "submitted"})
    assert r.status_code == 200
    assert r.json()["status"] == "submitted"

def test_update_assessment_not_found(client, fake_sf):
    fake_sf._one = None
    r = client.patch(f"/api/v1/assessments/{MISSING_UUID}/status", json={"status": "submitted"}) # FIXED
    assert r.status_code == 404

def test_get_dimension_scores_returns_items(client, fake_sf):
    fake_sf._one = (1,)
    row = (SCORE_ID, ASSESSMENT_ID, "ai_governance", 80.0, 0.5, 0.9, 3, datetime.now())
    fake_sf._all = [row]
    r = client.get(f"/api/v1/assessments/{ASSESSMENT_ID}/scores?page=1&page_size=20")
    assert r.status_code == 200
    body = r.json()
    assert body["items"][0]["id"] == SCORE_ID

def test_get_dimension_scores_pagination(client, fake_sf):
    fake_sf._one = (1,)
    row = (SCORE_ID, ASSESSMENT_ID, "ai_governance", 70.0, 0.5, 0.8, 1, datetime.now())
    fake_sf._all = [row]
    r = client.get(f"/api/v1/assessments/{ASSESSMENT_ID}/scores?page=1&page_size=1")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert len(body["items"]) == 1

def test_upsert_dimension_score_not_found(client, fake_sf):
    fake_sf._one = None
    payload = {"assessment_id": ASSESSMENT_ID, "dimension": "ai_governance", "score": 75, "weight": 0.6, "confidence": 0.9, "evidence_count": 2}
    r = client.post(f"/api/v1/assessments/{ASSESSMENT_ID}/scores", json=payload)
    assert r.status_code == 404

def test_upsert_dimension_score_success(client, fake_sf):
    row = (SCORE_ID, ASSESSMENT_ID, "ai_governance", 75.0, 0.6, 0.9, 2, datetime.now())
    fake_sf._one_queue = [(1,), row]
    payload = {"assessment_id": ASSESSMENT_ID, "dimension": "ai_governance", "score": 75, "weight": 0.6, "confidence": 0.9, "evidence_count": 2}
    r = client.post(f"/api/v1/assessments/{ASSESSMENT_ID}/scores", json=payload)
    assert r.status_code == 201
    assert r.json()["id"] == SCORE_ID

def test_upsert_dimension_score_validation_error(client, fake_sf):
    # This test expects validation to run BEFORE DB check, so we don't mock DB.
    # But if it checks DB first, we might need to mock it.
    # To be safe, let's mock the assessment lookup as success so it proceeds to validation.
    fake_sf._one = (1,)
    payload = {"assessment_id": ASSESSMENT_ID, "dimension": "ai_governance", "score": 120, "weight": 0.6, "confidence": 0.9, "evidence_count": 2}
    r = client.post(f"/api/v1/assessments/{ASSESSMENT_ID}/scores", json=payload)
    assert r.status_code == 422


def test_create_company_duplicate_ticker_returns_409(client, fake_sf):
    payload = {"name": "Dup Co", "ticker": "TCO", "industry_id": INDUSTRY_ID, "position_factor": 0.25}
    fake_sf._one_queue = [(1,), ("existing-id",)]
    r = client.post("/api/v1/companies", json=payload)
    assert r.status_code == 409


def test_update_company_duplicate_ticker_returns_409(client, fake_sf):
    fake_sf._one_queue = [(1,), (1,), (COMPANY_ID, "X", "XXX", INDUSTRY_ID, 0.2, False, datetime.now(), datetime.now())]
    r = client.put(f"/api/v1/companies/{COMPANY_ID}", json={"ticker": "DUP"})
    assert r.status_code == 409


def test_update_assessment_status_invalid_transition_returns_400(client, fake_sf):
    fake_sf._one_queue = [("draft",)]
    r = client.patch(f"/api/v1/assessments/{ASSESSMENT_ID}/status", json={"status": "approved"})
    assert r.status_code == 400
    assert "Invalid status transition" in r.json()["detail"]


def test_documents_list_accepts_offset(monkeypatch, client):
    seen = {}

    class _Store:
        def list_documents(self, ticker=None, company_id=None, limit=200, offset=0):
            seen["offset"] = offset
            seen["limit"] = limit
            return []

        def close(self):
            return None

    monkeypatch.setattr("app.routers.documents.EvidenceStore", lambda: _Store())
    r = client.get("/api/v1/documents?limit=10&offset=7")
    assert r.status_code == 200
    assert seen["limit"] == 10
    assert seen["offset"] == 7


def test_evidence_documents_alias_accepts_offset(monkeypatch, client):
    seen = {}

    def _list_documents_from_documents_router(ticker=None, company_id=None, limit=100, offset=0):
        seen["offset"] = offset
        return []

    monkeypatch.setattr(
        "app.routers.evidence.list_documents_from_documents_router",
        _list_documents_from_documents_router,
    )
    r = client.get("/api/v1/evidence/documents?offset=5")
    assert r.status_code == 200
    assert seen["offset"] == 5


def test_collection_rejects_empty_ticker_input(client):
    r = client.post("/api/v1/collection/evidence?companies=   ")
    assert r.status_code == 422
    assert r.json()["detail"] == "No valid tickers provided"


def test_collection_rejects_invalid_ticker_format(client):
    r = client.post("/api/v1/collection/signals?companies=CAT,!!")
    assert r.status_code == 422
    assert "Invalid ticker format" in r.json()["detail"]


def test_collection_queue_and_task_status(client, monkeypatch):
    monkeypatch.setattr("app.routers.collection.run_collect_evidence", lambda _task_id, _tickers: None)
    r = client.post("/api/v1/collection/evidence?companies=CAT")
    assert r.status_code == 200
    task_id = r.json()["task_id"]

    s = client.get(f"/api/v1/collection/tasks/{task_id}")
    assert s.status_code == 200
    body = s.json()
    assert body["status"] == "queued"
    assert body["type"] == "evidence"


def test_signals_list_endpoint(client, fake_sf):
    fake_sf.description = [
        ("ID",),
        ("COMPANY_ID",),
        ("TICKER",),
        ("SIGNAL_TYPE",),
        ("SOURCE",),
        ("TITLE",),
        ("URL",),
        ("PUBLISHED_AT",),
        ("COLLECTED_AT",),
        ("CONTENT_HASH",),
        ("METADATA",),
    ]
    fake_sf._all = [("sig-1", COMPANY_ID, "CAT", "news", "google_news_rss", "title", "url", None, datetime.now(), "h", {})]
    r = client.get("/api/v1/signals?ticker=CAT")
    assert r.status_code == 200
    assert r.json()[0]["ticker"] == "CAT"


def test_get_signal_not_found(client, fake_sf):
    fake_sf._one = None
    r = client.get("/api/v1/signals/sig-missing")
    assert r.status_code == 404


def test_signal_summaries_list_endpoint(client, fake_sf):
    fake_sf.description = [
        ("ID",),
        ("COMPANY_ID",),
        ("TICKER",),
        ("AS_OF_DATE",),
        ("SUMMARY_TEXT",),
        ("SIGNAL_COUNT",),
        ("CREATED_AT",),
    ]
    fake_sf._all = [("sum-1", COMPANY_ID, "CAT", str(date.today()), "summary", 5, datetime.now())]
    r = client.get("/api/v1/signal-summaries?ticker=CAT")
    assert r.status_code == 200
    assert r.json()[0]["ticker"] == "CAT"


def test_signal_summaries_compute_duplicate_companies_returns_409(client, fake_sf):
    fake_sf._all_queue = [[("id-1",), ("id-2",)]]
    r = client.post("/api/v1/signal-summaries/compute?ticker=CAT")
    assert r.status_code == 409


def test_search_combines_multiple_filters_with_and(client, monkeypatch):
    seen = {}

    class _Store:
        def query(self, query_text, top_k=5, where=None):
            seen["query_text"] = query_text
            seen["top_k"] = top_k
            seen["where"] = where
            return []

    monkeypatch.setattr("app.routers.search.get_vector_store", lambda: _Store())
    r = client.get(
        "/api/v1/search?q=human%20capital&top_k=5&company_id=company-1&dimension=talent_skills"
    )
    assert r.status_code == 200
    assert seen["query_text"] == "human capital"
    assert seen["top_k"] == 5
    assert seen["where"] == {
        "$and": [
            {"company_id": "company-1"},
            {"dimension": "talent_skills"},
        ]
    }


def test_search_uses_single_filter_without_and(client, monkeypatch):
    seen = {}

    class _Store:
        def query(self, query_text, top_k=5, where=None):
            seen["where"] = where
            return []

    monkeypatch.setattr("app.routers.search.get_vector_store", lambda: _Store())
    r = client.get("/api/v1/search?q=leadership&company_id=company-1")
    assert r.status_code == 200
    assert seen["where"] == {"company_id": "company-1"}


def test_search_supports_multiple_source_type_filters(client, monkeypatch):
    seen = {}

    class _Store:
        def query(self, query_text, top_k=5, where=None):
            seen["where"] = where
            return []

    monkeypatch.setattr("app.routers.search.get_vector_store", lambda: _Store())
    r = client.get(
        "/api/v1/search?q=governance&company_id=company-1&source_type=sec_10k_item_1a,board_proxy_def14a"
    )
    assert r.status_code == 200
    assert seen["where"] == {
        "$and": [
            {"company_id": "company-1"},
            {
                "$or": [
                    {"source_type": "sec_10k_item_1a"},
                    {"source_type": "board_proxy_def14a"},
                ]
            },
        ]
    }


def test_hybrid_search_passes_source_types_to_retriever(client, monkeypatch):
    seen = {}

    class _Hybrid:
        def search(self, **kwargs):
            seen.update(kwargs)
            return []

    monkeypatch.setattr("app.routers.search.get_hybrid", lambda: _Hybrid())
    r = client.get(
        "/api/v1/search?q=data%20platform&mode=hybrid&company_id=company-1&source_type=sec_10k_item_7,job_posting_linkedin"
    )
    assert r.status_code == 200
    assert seen["source_types"] == ["sec_10k_item_7", "job_posting_linkedin"]
