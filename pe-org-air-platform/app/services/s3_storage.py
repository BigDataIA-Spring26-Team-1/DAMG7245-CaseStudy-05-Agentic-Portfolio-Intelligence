from __future__ import annotations

import json
from functools import lru_cache

import boto3
from botocore.exceptions import ClientError

from app.config import settings


def is_s3_configured() -> bool:
    return bool(
        settings.s3_bucket_name
        and settings.aws_region
        and settings.aws_access_key_id
        and settings.aws_secret_access_key
    )


@lru_cache(maxsize=1)
def _get_s3_client():
    if not is_s3_configured():
        raise RuntimeError("S3 is not configured. Set bucket, region, and credentials.")
    return boto3.client(
        "s3",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )


def _normalize_key(key: str) -> str:
    return key.strip().lstrip("/")


def s3_uri_for_key(key: str) -> str:
    if not settings.s3_bucket_name:
        raise RuntimeError("S3 bucket is not configured.")
    k = _normalize_key(key)
    return f"s3://{settings.s3_bucket_name}/{k}"


def upload_bytes(content: bytes, key: str, content_type: str | None = None) -> str:
    client = _get_s3_client()
    normalized_key = _normalize_key(key)
    extra = {"ContentType": content_type} if content_type else None
    client.put_object(
        Bucket=settings.s3_bucket_name,
        Key=normalized_key,
        Body=content,
        **(extra or {}),
    )
    return s3_uri_for_key(normalized_key)


def upload_text(text: str, key: str, encoding: str = "utf-8", content_type: str = "text/plain") -> str:
    return upload_bytes(text.encode(encoding, errors="ignore"), key, content_type=content_type)


def upload_json(obj, key: str, content_type: str = "application/json") -> str:
    payload = json.dumps(obj, indent=2, sort_keys=True)
    return upload_text(payload, key, content_type=content_type)


def ping_s3() -> tuple[bool, str]:
    # If bucket not configured, treat as "not configured" (not a hard failure for local dev)
    if not settings.s3_bucket_name:
        return True, "not_configured"

    try:
        client = _get_s3_client()
        client.head_bucket(Bucket=settings.s3_bucket_name)
        return True, "ok"
    except ClientError as e:
        return False, f"ClientError: {e}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
