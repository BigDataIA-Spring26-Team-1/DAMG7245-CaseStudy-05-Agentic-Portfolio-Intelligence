from __future__ import annotations

from typing import Dict, Tuple

import boto3
from fastapi import APIRouter, Response, status

from app.services.redis_cache import get_redis_client
from app.services.snowflake import get_snowflake_connection

router = APIRouter(tags=["health"])


def ping_redis() -> Tuple[bool, str]:
    try:
        client = get_redis_client()
        client.ping()
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def ping_snowflake() -> Tuple[bool, str]:
    try:
        conn = get_snowflake_connection()
        try:
            cur = conn.cursor()
            try:
                cur.execute("SELECT 1")
                cur.fetchone()
            finally:
                cur.close()
        finally:
            conn.close()
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def ping_s3() -> Tuple[bool, str]:
    try:
        client = boto3.client("s3")
        client.list_buckets()
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def _dependency_status() -> Dict[str, Dict[str, object]]:
    redis_ok, redis_msg = ping_redis()
    snowflake_ok, snowflake_msg = ping_snowflake()
    s3_ok, s3_msg = ping_s3()

    return {
        "redis": {"ok": redis_ok, "message": redis_msg},
        "snowflake": {"ok": snowflake_ok, "message": snowflake_msg},
        "s3": {"ok": s3_ok, "message": s3_msg},
    }


def _overall_ok(deps: Dict[str, Dict[str, object]]) -> bool:
    return all(bool(v["ok"]) for v in deps.values())


@router.get("/health")
def health(response: Response) -> Dict[str, str]:
    deps = _dependency_status()
    ok = _overall_ok(deps)

    if not ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "degraded"}

    return {"status": "ok"}


@router.get("/health/detailed")
def health_detailed(response: Response) -> Dict[str, object]:
    deps = _dependency_status()
    ok = _overall_ok(deps)

    if not ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "status": "ok" if ok else "degraded",
        "dependencies": deps,
    }