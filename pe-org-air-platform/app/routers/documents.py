from __future__ import annotations
import logging
from fastapi import APIRouter, HTTPException, Query
from app.config import settings
from app.services.evidence_store import EvidenceStore
from app.services.redis_cache import cache_get_json, cache_set_json

router = APIRouter(prefix="/documents")
logger = logging.getLogger("uvicorn.error")


def _documents_list_cache_key(
   ticker: str | None,
   company_id: str | None,
   limit: int,
   offset: int,
) -> str:
   t = (ticker or "").strip().upper() or "all"
   c = (company_id or "").strip() or "all"
   return f"documents:list:ticker:{t}:company:{c}:limit:{limit}:offset:{offset}"


@router.get("")
def list_documents(
   ticker: str | None = Query(default=None),
   company_id: str | None = Query(default=None),
   limit: int = Query(default=100, ge=1, le=500),
   offset: int = Query(default=0, ge=0),
):
   cache_key = _documents_list_cache_key(ticker=ticker, company_id=company_id, limit=limit, offset=offset)
   cached = cache_get_json(cache_key)
   if cached is not None:
       return cached

   try:
       store = EvidenceStore()
   except Exception as exc:
       logger.error("documents_store_init_failed err=%s", exc)
       raise HTTPException(status_code=503, detail=f"Storage unavailable: {exc}")

   try:
       docs = store.list_documents(ticker=ticker, company_id=company_id, limit=limit, offset=offset)
       # Normalize any non-JSON-serializable values (datetime -> str)
       import json
       docs = json.loads(json.dumps(docs, default=str))
       cache_set_json(cache_key, docs, settings.redis_ttl_seconds)
       return docs
   except HTTPException:
       raise
   except Exception as exc:
       logger.error("documents_list_failed err=%s", exc)
       raise HTTPException(status_code=503, detail=f"Query failed: {exc}")
   finally:
       store.close()

@router.get("/{document_id}")
def get_document(document_id: str):
   cache_key = f"documents:item:{document_id}"
   cached = cache_get_json(cache_key)
   if cached is not None:
       return cached

   try:
       store = EvidenceStore()
   except Exception as exc:
       logger.error("documents_store_init_failed err=%s", exc)
       raise HTTPException(status_code=503, detail=f"Storage unavailable: {exc}")

   try:
       doc = store.get_document(document_id)
       if not doc:
           raise HTTPException(status_code=404, detail="Document not found")
       import json
       doc = json.loads(json.dumps(doc, default=str))
       cache_set_json(cache_key, doc, settings.redis_ttl_seconds)
       return doc
   except HTTPException:
       raise
   except Exception as exc:
       logger.error("documents_get_failed err=%s", exc)
       raise HTTPException(status_code=503, detail=f"Query failed: {exc}")
   finally:
       store.close()
