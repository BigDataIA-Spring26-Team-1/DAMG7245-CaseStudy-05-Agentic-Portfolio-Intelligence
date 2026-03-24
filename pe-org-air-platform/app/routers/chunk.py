from __future__ import annotations
 
from fastapi import APIRouter, HTTPException, Query
 
from app.config import settings
from app.services.evidence_store import EvidenceStore
from app.services.redis_cache import cache_get_json, cache_set_json
 
router = APIRouter(prefix="/chunks")
 
 
@router.get("/")
def list_chunks(
    document_id: str = Query(..., description="Document ID"),
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
 
 
@router.get("/{chunk_id}")
def get_chunk(chunk_id: str):
    cache_key = f"chunks:item:{chunk_id}"
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached
 
    store = EvidenceStore()
    try:
        row = store.get_chunk(chunk_id)
        if not row:
            raise HTTPException(status_code=404, detail="Chunk not found")
        cache_set_json(cache_key, row, settings.redis_ttl_seconds)
        return row
    finally:
        store.close()
 
 