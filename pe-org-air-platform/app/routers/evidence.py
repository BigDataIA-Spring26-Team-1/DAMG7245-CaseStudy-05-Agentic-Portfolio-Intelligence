# app/routers/evidence.py
from __future__ import annotations
from fastapi import APIRouter, Query
 
from app.config import settings
from app.routers.documents import get_document as get_document_from_documents_router
from app.routers.documents import list_documents as list_documents_from_documents_router
from app.services.evidence_store import EvidenceStore
from app.services.redis_cache import cache_get_json, cache_set_json
 
router = APIRouter(prefix="/evidence")
 
@router.get("/stats")
def stats():
   cache_key = "evidence:stats"
   cached = cache_get_json(cache_key)
   if cached is not None:
       return cached
 
   store = EvidenceStore()
   try:
       out = store.stats()
       cache_set_json(cache_key, out, settings.redis_ttl_seconds)
       return out
   finally:
       store.close()
 
@router.get("/documents")
def list_documents(
   ticker: str | None = Query(default=None),
   company_id: str | None = Query(default=None),
   limit: int = Query(default=100, ge=1, le=500),
   offset: int = Query(default=0, ge=0),
):
   # Backward-compatible alias of /documents endpoint.
   return list_documents_from_documents_router(ticker=ticker, company_id=company_id, limit=limit, offset=offset)
 
@router.get("/documents/{document_id}")
def get_document(document_id: str):
   # Backward-compatible alias of /documents/{document_id} endpoint.
   return get_document_from_documents_router(document_id=document_id)
 
@router.get("/documents/{document_id}/chunks")
def get_chunks(
   document_id: str,
   limit: int = Query(default=200, ge=1, le=1000),
   offset: int = Query(default=0, ge=0),
):
   cache_key = f"chunks:list:document:{document_id}:limit:{limit}:offset:{offset}"
   cached = cache_get_json(cache_key)
   if cached is not None:
       return cached
 
   store = EvidenceStore()
   try:
        out = store.list_chunks(document_id=document_id, limit=limit, offset=offset)
        cache_set_json(cache_key, out, settings.redis_ttl_seconds)
        return out
   finally:
       store.close()
 
 