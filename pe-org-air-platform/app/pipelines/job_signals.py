from __future__ import annotations
 
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import math
from typing import Any, Dict, List
from xml.etree import ElementTree as ET
 
AI_HIRING_KEYWORDS = [
    "machine learning", "ml engineer", "data scientist", "ai engineer", "mlops",
    "artificial intelligence", "computer vision", "nlp", "deep learning", "llm",
]
 
SENIOR_KEYWORDS = ["principal", "staff", "director", "vp", "head", "chief"]
 
 
@dataclass(frozen=True)
class JobPosting:
    title: str
    url: str | None
    published_at: datetime | None
    location: str | None = None
    department: str | None = None
    raw: Dict[str, Any] | None = None
 
 
@dataclass(frozen=True)
class JobSignalSummary:
    total_jobs: int
    ai_jobs: int
    ai_ratio: float
    senior_ai_jobs: int
    diversity_locations: int
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
 
 
def _is_ai_job(title: str, text: str = "") -> bool:
    t = f"{title} {text}".lower()
    return any(k in t for k in AI_HIRING_KEYWORDS)
 
 
def _is_senior(title: str) -> bool:
    t = (title or "").lower()
    return any(k in t for k in SENIOR_KEYWORDS)
 
 
def parse_jobs_rss(rss_xml: str) -> List[JobPosting]:
    if not rss_xml.strip():
        return []
 
    out: List[JobPosting] = []
    try:
        root = ET.fromstring(rss_xml)
    except Exception:
        return out
 
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip() or None
        pub = _safe_dt((item.findtext("pubDate") or "").strip())
        out.append(JobPosting(title=title, url=link, published_at=pub, raw={"source": "rss"}))
 
    return out
 
 
def summarize_job_signals(postings: List[JobPosting]) -> JobSignalSummary:
    if not postings:
        return JobSignalSummary(
            total_jobs=0,
            ai_jobs=0,
            ai_ratio=0.0,
            senior_ai_jobs=0,
            diversity_locations=0,
            recency_days_median=365.0,
            score=0.0,
        )
 
    now = datetime.now(timezone.utc)
    total_jobs = len(postings)
 
    ai_jobs = 0
    senior_ai_jobs = 0
    locations: set[str] = set()
    ages: List[float] = []
 
    for p in postings:
        if p.location:
            locations.add(p.location.strip().lower())
        if p.published_at:
            dt = p.published_at
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ages.append(max(0.0, (now - dt).total_seconds() / 86400.0))
 
        if _is_ai_job(p.title):
            ai_jobs += 1
            if _is_senior(p.title):
                senior_ai_jobs += 1
 
    ai_ratio = ai_jobs / total_jobs
    senior_ratio = senior_ai_jobs / ai_jobs if ai_jobs else 0.0
    loc_div = min(1.0, len(locations) / 8.0)
 
    ages_sorted = sorted(ages)
    if not ages_sorted:
        recency_days = 365.0
    else:
        mid = len(ages_sorted) // 2
        recency_days = ages_sorted[mid] if len(ages_sorted) % 2 == 1 else (ages_sorted[mid - 1] + ages_sorted[mid]) / 2
 
    recency_factor = max(0.0, min(1.0, 1.0 - recency_days / 180.0))
 
    # Weighted hiring readiness proxy (0-100)
    score = 100.0 * (0.55 * ai_ratio + 0.20 * senior_ratio + 0.15 * loc_div + 0.10 * recency_factor)
    score = max(0.0, min(100.0, score))
 
    return JobSignalSummary(
        total_jobs=total_jobs,
        ai_jobs=ai_jobs,
        ai_ratio=round(ai_ratio, 4),
        senior_ai_jobs=senior_ai_jobs,
        diversity_locations=len(locations),
        recency_days_median=round(recency_days, 2),
        score=round(score, 2),
    )
 
 
def score_technology_hiring(postings: List[JobPosting]) -> float:
    return summarize_job_signals(postings).score
 
 
def normalize_job_rows(rows: List[Dict[str, Any]]) -> List[JobPosting]:
    out: List[JobPosting] = []
    for row in rows:
        title = str(row.get("title", "")).strip()
        url = row.get("url")
        published_at = _safe_dt(row.get("published_at"))
        location = row.get("location")
        department = row.get("department")
        out.append(
            JobPosting(
                title=title,
                url=str(url) if url else None,
                published_at=published_at,
                location=str(location) if location else None,
                department=str(department) if department else None,
                raw=row,
            )
        )
    return out