from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from app.config import ROOT_DIR, settings
from app.services.s3_storage import is_s3_configured, upload_bytes
from app.scoring_engine.portfolio_validation import EXPECTED_PORTFOLIO_SCORE_RANGES


@dataclass(frozen=True)
class ArtifactWriteResult:
    ticker: str
    relative_key: str
    local_path: Optional[str]
    s3_uri: Optional[str]


def configured_portfolio_tickers() -> tuple[str, ...]:
    raw = (settings.results_portfolio_tickers or "").strip()
    if raw:
        tickers = [item.strip().upper() for item in raw.split(",") if item.strip()]
        if tickers:
            return tuple(dict.fromkeys(tickers))
    return tuple(EXPECTED_PORTFOLIO_SCORE_RANGES.keys())


def should_write_local_results(ticker: str) -> bool:
    if not settings.results_local_copy_enabled:
        return False
    normalized = str(ticker or "").strip().upper()
    if normalized == "PORTFOLIO":
        return True
    return normalized in configured_portfolio_tickers()


def should_upload_result_artifacts_to_s3() -> bool:
    return bool(settings.results_upload_to_s3 and is_s3_configured())


def results_root() -> Path:
    relative = (settings.results_dir or "results").strip().strip("/\\")
    return ROOT_DIR / relative


def _safe_segment(value: str) -> str:
    cleaned = str(value or "").strip().replace("\\", "/").strip("/")
    return cleaned or "unknown"


def _relative_key(ticker: str, category: str, filename: str) -> str:
    normalized_ticker = _safe_segment(ticker).upper()
    normalized_category = _safe_segment(category)
    normalized_filename = _safe_segment(filename)
    return f"{normalized_ticker}/{normalized_category}/{normalized_filename}"


def write_bytes_artifact(
    *,
    ticker: str,
    category: str,
    filename: str,
    content: bytes,
    content_type: str,
) -> ArtifactWriteResult:
    relative_key = _relative_key(ticker, category, filename)
    local_path: Optional[str] = None
    s3_uri: Optional[str] = None

    if should_write_local_results(ticker):
        target = results_root() / Path(relative_key)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        local_path = str(target)

    if should_upload_result_artifacts_to_s3():
        key = f"{_safe_segment(settings.results_s3_prefix)}/{relative_key}"
        s3_uri = upload_bytes(content, key=key, content_type=content_type)

    return ArtifactWriteResult(
        ticker=str(ticker or "").upper(),
        relative_key=relative_key,
        local_path=local_path,
        s3_uri=s3_uri,
    )


def write_text_artifact(
    *,
    ticker: str,
    category: str,
    filename: str,
    text: str,
    encoding: str = "utf-8",
    content_type: str = "text/plain",
) -> ArtifactWriteResult:
    return write_bytes_artifact(
        ticker=ticker,
        category=category,
        filename=filename,
        content=(text or "").encode(encoding, errors="ignore"),
        content_type=content_type,
    )


def write_json_artifact(
    *,
    ticker: str,
    category: str,
    filename: str,
    payload: Any,
) -> ArtifactWriteResult:
    content = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8")
    return write_bytes_artifact(
        ticker=ticker,
        category=category,
        filename=filename,
        content=content,
        content_type="application/json",
    )
