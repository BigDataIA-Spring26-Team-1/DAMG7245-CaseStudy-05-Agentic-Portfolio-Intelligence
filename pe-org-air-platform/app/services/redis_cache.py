from __future__ import annotations
 
import json
import logging
from typing import Any, Optional
 
import redis
 
from app.config import settings
 
logger = logging.getLogger("uvicorn.error")
 
 
def get_redis_client() -> redis.Redis:
    # decode_responses=True gives you strings instead of bytes
    return redis.Redis.from_url(settings.redis_url, decode_responses=True)
 
 
def ping_redis() -> tuple[bool, str]:
    try:
        client = get_redis_client()
        ok = client.ping()
        return (True, "ok") if ok else (False, "ping_failed")
    except Exception as e:
        return (False, f"{type(e).__name__}: {e}")
 
 
def _to_jsonable(payload: Any) -> Any:
    if hasattr(payload, "model_dump"):
        return payload.model_dump()
    return payload
 
 
def cache_get_json(key: str) -> Optional[Any]:
    try:
        r = get_redis_client()
        val = r.get(key)
        if not val:
            logger.info("cache_miss key=%s", key)
            return None
        logger.info("cache_hit key=%s", key)
        return json.loads(val)
    except Exception as exc:
        logger.warning("cache_get_failed key=%s err=%s", key, exc)
        return None
 
 
def cache_set_json(key: str, payload: Any, ttl_seconds: int) -> None:
    try:
        r = get_redis_client()
        r.setex(key, ttl_seconds, json.dumps(_to_jsonable(payload), default=str))
    except Exception as exc:
        logger.warning("cache_set_failed key=%s err=%s", key, exc)
 
 
def cache_delete(key: str) -> None:
    try:
        r = get_redis_client()
        r.delete(key)
    except Exception as exc:
        logger.warning("cache_delete_failed key=%s err=%s", key, exc)
 
 
def cache_delete_pattern(pattern: str) -> int:
    """
    Deletes all keys matching a Redis glob pattern.
    Returns number of deleted keys.
    """
    try:
        r = get_redis_client()
        keys = list(r.scan_iter(match=pattern, count=500))
        if not keys:
            return 0
        return int(r.delete(*keys))
    except Exception as exc:
        logger.warning("cache_delete_pattern_failed pattern=%s err=%s", pattern, exc)
        return 0