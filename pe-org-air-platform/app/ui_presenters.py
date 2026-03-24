from __future__ import annotations

from typing import Any


def extract_orgair_score(payload: dict[str, Any] | None) -> float:
    if not isinstance(payload, dict):
        return 0.0
    value = payload.get("org_air_score", payload.get("composite_score", 0.0))
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def sanitize_generated_summary(
    text: str | None,
    *,
    company_name: str,
    company_id: str | None = None,
    ticker: str | None = None,
) -> str:
    summary = str(text or "No generated summary available.")
    cleaned_name = company_name.strip() or (ticker or "Company")
    if company_id:
        summary = summary.replace(company_id, cleaned_name)
    if ticker:
        summary = summary.replace(f"{cleaned_name} score", f"{cleaned_name} ({ticker}) score")
    return summary


def compact_recommendation(text: str | None) -> str:
    raw = str(text or "n/a").strip()
    if not raw:
        return "n/a"
    return raw.split(" - ", 1)[0].strip()


def display_evidence_count(
    justification: dict[str, Any] | None,
    ic_packet: dict[str, Any] | None,
    dimension_score: dict[str, Any] | None,
) -> int:
    justification = justification or {}
    ic_packet = ic_packet or {}
    dimension_score = dimension_score or {}

    candidates = [
        justification.get("evidence_count"),
        ic_packet.get("total_evidence_count"),
        (justification.get("score_context") or {}).get("evidence_count"),
        dimension_score.get("evidence_count"),
    ]
    for value in candidates:
        try:
            numeric = int(value)
        except (TypeError, ValueError):
            continue
        if numeric > 0:
            return numeric
    return 0


def humanize_source_type(source_type: str | None) -> str:
    raw = str(source_type or "").strip().lower()
    if not raw:
        return "Unknown Source"
    if raw.startswith("sec_"):
        return raw.replace("sec_", "SEC ").replace("_", " ").upper()
    return raw.replace("_", " ").title()
