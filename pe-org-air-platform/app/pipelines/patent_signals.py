from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
from typing import List
from xml.etree import ElementTree as ET

AI_PATENT_KEYWORDS = [
    "ai",
    "artificial intelligence", "machine learning", "deep learning", "neural", "computer vision",
    "nlp", "generative", "llm", "model training", "inference",
]
 
 
@dataclass(frozen=True)
class PatentMention:
    title: str
    url: str | None
    published_at: datetime | None
 
 
@dataclass(frozen=True)
class PatentSignalSummary:
    total_mentions: int
    ai_mentions: int
    ai_ratio: float
    recency_days_median: float
    score: float
 
 
def _safe_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None
 
 
def parse_patents_rss(rss_xml: str) -> List[PatentMention]:
    if not rss_xml.strip():
        return []
 
    try:
        root = ET.fromstring(rss_xml)
    except Exception:
        return []
 
    out: List[PatentMention] = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip() or None
        pub = _safe_dt((item.findtext("pubDate") or "").strip())
        out.append(PatentMention(title=title, url=link, published_at=pub))
 
    return out


def parse_patents_serpapi(payload_json: str) -> List[PatentMention]:
    """
    Parse SerpApi Google Patents JSON payload into normalized mentions.
    """
    if not payload_json.strip():
        return []

    try:
        payload = json.loads(payload_json)
    except Exception:
        return []

    rows = []
    if isinstance(payload, dict):
        rows = payload.get("organic_results", [])
    if not isinstance(rows, list):
        return []

    out: List[PatentMention] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title", "")).strip()
        link = str(row.get("link", "")).strip() or None

        # SerpApi may provide a date field in different keys
        date_raw = (
            row.get("filing_date")
            or row.get("publication_date")
            or row.get("date")
            or row.get("priority_date")
            or None
        )
        pub = _safe_dt(str(date_raw)) if date_raw else None
        out.append(PatentMention(title=title, url=link, published_at=pub))

    return out


def parse_patents_payload(payload_text: str, source: str) -> List[PatentMention]:
    """
    Parse patents payload by source type.
    """
    s = (source or "").lower()
    if "serpapi" in s:
        return parse_patents_serpapi(payload_text)
    return parse_patents_rss(payload_text)
 
 
def summarize_patent_signals(mentions: List[PatentMention]) -> PatentSignalSummary:
    if not mentions:
        return PatentSignalSummary(0, 0, 0.0, 365.0, 0.0)
 
    now = datetime.now(timezone.utc)
    total = len(mentions)
    ai_mentions = 0
    ages: List[float] = []
 
    for m in mentions:
        t = (m.title or "").lower()
        if any(k in t for k in AI_PATENT_KEYWORDS):
            ai_mentions += 1
 
        if m.published_at:
            dt = m.published_at
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ages.append(max(0.0, (now - dt).total_seconds() / 86400.0))
 
    ai_ratio = ai_mentions / total
 
    ages_sorted = sorted(ages)
    if not ages_sorted:
        recency_days = 365.0
    else:
        mid = len(ages_sorted) // 2
        recency_days = ages_sorted[mid] if len(ages_sorted) % 2 == 1 else (ages_sorted[mid - 1] + ages_sorted[mid]) / 2
 
    recency_factor = max(0.0, min(1.0, 1.0 - recency_days / 365.0))
 
    # Innovation-activity proxy (0-100)
    score = 100.0 * (0.70 * ai_ratio + 0.20 * min(1.0, total / 20.0) + 0.10 * recency_factor)
    score = max(0.0, min(100.0, score))
 
    return PatentSignalSummary(
        total_mentions=total,
        ai_mentions=ai_mentions,
        ai_ratio=round(ai_ratio, 4),
        recency_days_median=round(recency_days, 2),
        score=round(score, 2),
    )
 
 
def score_innovation_activity(mentions: List[PatentMention]) -> float:
    return summarize_patent_signals(mentions).score
 
 
