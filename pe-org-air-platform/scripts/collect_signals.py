from __future__ import annotations
 
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
 
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
 
from app.config import settings
from app.services.result_artifacts import write_json_artifact, write_text_artifact
from app.services.snowflake import get_snowflake_connection
from app.services.s3_storage import is_s3_configured, upload_json, upload_text
from app.services.signal_store import SignalStore
from app.pipelines.external_signals import ExternalSignalCollector, sha256_text
from app.pipelines.job_signals import normalize_job_rows, parse_jobs_rss, summarize_job_signals
from app.pipelines.patent_signals import parse_patents_payload, summarize_patent_signals
from app.pipelines.tech_signals import summarize_tech_signals
 
# TechStackCollector is optional depending on your file.
# We'll import if available and fallback gracefully if not.
try:
    from app.pipelines.external_signals import TechStackCollector  # type: ignore
except Exception:  # pragma: no cover
    TechStackCollector = None  # type: ignore
 
# Optional job board tokens; leave blank to use RSS fallback.
JOB_BOARD_TOKENS: dict[str, dict[str, str]] = {}
 
 
def _normalize_tickers(raw: str) -> list[str]:
    tickers = [t.strip().upper() for t in (raw or "").split(",") if t.strip()]
    return list(dict.fromkeys(tickers))


def get_all_active_tickers() -> list[str]:
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT ticker
            FROM companies
            WHERE is_deleted = FALSE
              AND ticker IS NOT NULL
            ORDER BY ticker
            """
        )
        return [str(r[0]).upper() for r in (cur.fetchall() or []) if r and r[0]]
    finally:
        cur.close()
        conn.close()


def get_company_profile(ticker: str) -> tuple[str, str]:
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, name
            FROM companies
            WHERE ticker=%s AND is_deleted=FALSE
            ORDER BY created_at DESC
            LIMIT 2
            """,
            (ticker,),
        )
        rows = cur.fetchall()
        if not rows:
            raise RuntimeError(f"Missing company row for {ticker}. Run backfill_companies.py")
        if len(rows) > 1:
            raise RuntimeError(f"Duplicate active company rows found for ticker={ticker}")
        company_id = str(rows[0][0])
        company_name = str(rows[0][1] or ticker).strip() or ticker
        return company_id, company_name
    finally:
        cur.close()
        conn.close()
 
 
def _write_text(path: Path, text: str, limit: int = 20000) -> None:
    path.write_text((text or "")[:limit], encoding="utf-8", errors="ignore")
 
 
def _write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8", errors="ignore")
 
 
def _normalize_prefix(prefix: str, default_prefix: str) -> str:
    normalized = prefix.strip().strip("/\\").replace("\\", "/")
    return normalized or default_prefix


def _mirror_text_result(ticker: str, filename: str, text: str) -> None:
    write_text_artifact(
        ticker=ticker,
        category="signals",
        filename=filename,
        text=(text or "")[:20000],
    )


def _mirror_json_result(ticker: str, filename: str, payload: Any) -> None:
    write_json_artifact(
        ticker=ticker,
        category="signals",
        filename=filename,
        payload=payload,
    )
 
 
def _safe_get_patents_rss(collector: ExternalSignalCollector, query: str) -> Tuple[Optional[str], Optional[str], str]:
    """
    Returns (url, rss_text, source_label).
    We try a dedicated method if your collector has it; otherwise we fallback to Google News RSS with a "patent" query.
    """
    # Prefer SerpApi Google Patents if configured
    if hasattr(collector, "google_patents_serpapi"):
        fn = getattr(collector, "google_patents_serpapi")
        try:
            url, payload = fn(query, num=20, page=1)
            return url, json.dumps(payload), "serpapi_google_patents"
        except Exception:
            pass

    # If you implemented something like patents_uspto_stub(query)
    if hasattr(collector, "patents_uspto_stub"):
        fn = getattr(collector, "patents_uspto_stub")
        url, rss = fn(query)
        return url, rss, "uspto_stub_rss"
 
    # If you implemented something like google_patents_rss(query)
    if hasattr(collector, "google_patents_rss"):
        fn = getattr(collector, "google_patents_rss")
        url, rss = fn(query)
        return url, rss, "google_patents_rss"
 
    # Fallback (still external)
    # This is not the USPTO API; this fallback keeps the pipeline end-to-end.
    url, rss = collector.google_news_rss(f"{query} patent")
    return url, rss, "google_news_rss_patent_fallback"
 
 
def _extract_tech_counts(collector: ExternalSignalCollector, tech_obj: Any, blob: str) -> Dict[str, int]:
    """
    Supports multiple possible implementations:
    - TechStackCollector.extract(text) -> dict
    - collector.extract_tech_stack(text) -> dict
    """
    if not blob.strip():
        return {}
 
    # Preferred: TechStackCollector if present
    if tech_obj is not None:
        # expected method name: extract()
        if hasattr(tech_obj, "extract"):
            counts = tech_obj.extract(blob)
            return counts or {}
        # if someone used extract_tech_stack() inside collector class
        if hasattr(tech_obj, "extract_tech_stack"):
            counts = tech_obj.extract_tech_stack(blob)
            return counts or {}
 
    # Fallback to collector method if you placed it there
    if hasattr(collector, "extract_tech_stack"):
        counts = collector.extract_tech_stack(blob)
        return counts or {}
 
    return {}
 
 
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/signals", help="Output folder for proof artifacts")
    ap.add_argument("--companies", required=True, help="Ticker list like CAT,DE or 'all'")
    args = ap.parse_args()
 
    tickers = get_all_active_tickers() if args.companies.lower().strip() == "all" else _normalize_tickers(args.companies)
    if not tickers:
        raise SystemExit("No tickers selected. Ensure companies exist in the companies table or pass --companies.")
 
    collector = ExternalSignalCollector(user_agent=settings.sec_user_agent)
    store = SignalStore()
    tech = TechStackCollector() if TechStackCollector is not None else None
    s3_enabled = is_s3_configured()
    out_prefix = _normalize_prefix(args.out, "data/signals")
 
    try:
        for ticker in tickers:
            out_dir = ROOT / Path(out_prefix) / ticker
            artifact_prefix = f"{out_prefix}/{ticker}"
            if not s3_enabled:
                out_dir.mkdir(parents=True, exist_ok=True)
 
            try:
                company_id, company_name = get_company_profile(ticker)
            except Exception as exc:
                print(f"SKIP: {ticker} ({exc})")
                continue
 
            # Keep these defined no matter which branch runs
            jobs_rss: str = ""
            news_rss: str = ""
            patents_rss: str = ""
 
            # =========================================================
            # 1) JOBS signals (Greenhouse / Lever / RSS fallback)
            # =========================================================
            tokens = JOB_BOARD_TOKENS.get(ticker, {})
            gh = (tokens.get("greenhouse") or "").strip()
            lv = (tokens.get("lever") or "").strip()
 
            jobs: list[dict] = []
            source_used: Optional[str] = None
 
            try:
                if gh:
                    jobs = collector.greenhouse_jobs(gh)
                    source_used = "greenhouse"
                elif lv:
                    jobs = collector.lever_jobs(lv)
                    source_used = "lever"
            except Exception as e:
                print(f"WARN: {ticker} jobs board fetch failed ({e}); falling back to RSS")
                jobs = []
                source_used = None
 
            inserted_jobs = 0
            if jobs:
                job_summary = summarize_job_signals(normalize_job_rows(jobs))
                for j in jobs[:50]:
                    title = (j.get("title") or "").strip()
                    url = j.get("url")
                    published_at = j.get("published_at")
 
                    content_hash = sha256_text(f"jobs|{ticker}|{title}|{url or ''}")
                    if store.signal_exists_by_hash(content_hash):
                        continue
 
                    store.insert_signal(
                        company_id=company_id,
                        ticker=ticker,
                        signal_type="jobs",
                        source=source_used or "job_board",
                        title=title[:500] if title else None,
                        url=url,
                        published_at=published_at,
                        content_text=(json.dumps(j.get("raw", {}))[:20000] if j.get("raw") else None),
                        content_hash=content_hash,
                        metadata={
                            "location": j.get("location"),
                            "department": j.get("department"),
                            "collector": source_used,
                            "score": job_summary.score,
                            "ai_jobs": job_summary.ai_jobs,
                            "ai_ratio": job_summary.ai_ratio,
                            "senior_ai_jobs": job_summary.senior_ai_jobs,
                            "total_jobs": job_summary.total_jobs,
                        },
                    )
                    inserted_jobs += 1
 
                # Proof artifact: small summary JSON
                jobs_sample = {"source": source_used, "inserted": inserted_jobs, "sample": jobs[:3]}
                _mirror_json_result(ticker, "jobs_board_sample.json", jobs_sample)
                if s3_enabled:
                    upload_json(jobs_sample, f"{artifact_prefix}/jobs_board_sample.json")
                else:
                    _write_json(out_dir / "jobs_board_sample.json", jobs_sample)
                print(f"STORED: {ticker} jobs inserted={inserted_jobs} source={source_used}")
 
            else:
                jobs_q = f"{company_name} {ticker} hiring jobs"
                jobs_url, jobs_rss = collector.google_jobs_rss(jobs_q)
                _mirror_text_result(ticker, "jobs_rss.txt", jobs_rss)
                if s3_enabled:
                    upload_text((jobs_rss or "")[:20000], f"{artifact_prefix}/jobs_rss.txt")
                else:
                    _write_text(out_dir / "jobs_rss.txt", jobs_rss)
 
                if jobs_rss:
                    job_mentions = parse_jobs_rss(jobs_rss)
                    job_summary = summarize_job_signals(job_mentions)
                    jobs_hash = sha256_text(f"jobs_rss|{ticker}|{jobs_rss}")
                    if not store.signal_exists_by_hash(jobs_hash):
                        store.insert_signal(
                            company_id=company_id,
                            ticker=ticker,
                            signal_type="jobs",
                            source="google_jobs_rss_fallback",
                            title=f"{company_name} jobs RSS",
                            url=jobs_url,
                            published_at=None,
                            content_text=jobs_rss[:20000],
                            content_hash=jobs_hash,
                            metadata={
                                "query": jobs_q,
                                "note": "fallback rss stored (truncated to 20k)",
                                "score": job_summary.score,
                                "item_count": job_summary.total_jobs,
                                "ai_jobs": job_summary.ai_jobs,
                                "ai_ratio": job_summary.ai_ratio,
                            },
                        )
                        print(f"STORED: {ticker} jobs rss hash={jobs_hash[:10]}")
                    else:
                        print(f"SKIP: {ticker} jobs rss already stored (hash={jobs_hash[:10]})")
                else:
                    print(f"SKIP: {ticker} no jobs rss returned for query={jobs_q}")
 
            # =========================================================
            # 2) NEWS signals (Google News RSS)
            # =========================================================
            news_q = f"{company_name} {ticker}"
            news_url, news_rss = collector.google_news_rss(news_q)
            _mirror_text_result(ticker, "news_rss.txt", news_rss)
            if s3_enabled:
                upload_text((news_rss or "")[:20000], f"{artifact_prefix}/news_rss.txt")
            else:
                _write_text(out_dir / "news_rss.txt", news_rss)
 
            if news_rss:
                news_hash = sha256_text(f"news_rss|{ticker}|{news_rss}")
                if store.signal_exists_by_hash(news_hash):
                    print(f"SKIP: {ticker} news rss already stored (hash={news_hash[:10]})")
                else:
                    store.insert_signal(
                        company_id=company_id,
                        ticker=ticker,
                        signal_type="news",
                        source="google_news_rss",
                        title=f"{company_name} news RSS",
                        url=news_url,
                        published_at=None,
                        content_text=news_rss[:20000],
                        content_hash=news_hash,
                        metadata={"query": news_q, "note": "rss stored (truncated to 20k)"},
                    )
                    print(f"STORED: {ticker} news rss hash={news_hash[:10]}")
            else:
                print(f"SKIP: {ticker} no news rss returned for query={news_q}")
 
            # =========================================================
            # 3) TECH STACK signals (external-only proxy)
            # =========================================================
            tech_blob = "\n".join([x for x in [news_rss, jobs_rss] if x])
            tech_counts = _extract_tech_counts(collector, tech, tech_blob)
            tech_summary = summarize_tech_signals(tech_counts)
            tech_payload = {
                "counts": tech_counts,
                "summary": {
                    "score": tech_summary.score,
                    "unique_keywords": tech_summary.unique_keywords,
                    "cloud_ml_count": tech_summary.cloud_ml_count,
                    "ml_framework_count": tech_summary.ml_framework_count,
                    "data_platform_count": tech_summary.data_platform_count,
                    "ai_api_count": tech_summary.ai_api_count,
                },
            }
            _mirror_json_result(ticker, "tech_counts.json", tech_payload)
            if s3_enabled:
                upload_json(tech_payload, f"{artifact_prefix}/tech_counts.json")
            else:
                _write_json(out_dir / "tech_counts.json", tech_payload)
 
            if tech_counts:
                tech_hash = sha256_text(f"tech|{ticker}|" + json.dumps(tech_counts, sort_keys=True))
                if store.signal_exists_by_hash(tech_hash):
                    print(f"SKIP: {ticker} tech stack already stored (hash={tech_hash[:10]})")
                else:
                    store.insert_signal(
                        company_id=company_id,
                        ticker=ticker,
                        signal_type="tech",
                        source="tech_stack_extractor",
                        title=f"{company_name} tech stack (extracted)",
                        url=None,
                        published_at=None,
                        content_text=None,
                        content_hash=tech_hash,
                        metadata={
                            "counts": tech_counts,
                            "score": tech_summary.score,
                            "unique_keywords": tech_summary.unique_keywords,
                            "cloud_ml_count": tech_summary.cloud_ml_count,
                            "ml_framework_count": tech_summary.ml_framework_count,
                            "data_platform_count": tech_summary.data_platform_count,
                            "ai_api_count": tech_summary.ai_api_count,
                            "note": "tech extracted from external RSS blobs (jobs/news)",
                        },
                    )
                    print(f"STORED: {ticker} tech stack hash={tech_hash[:10]}")
            else:
                print(f"SKIP: {ticker} no tech keywords found in external blobs")
 
            # =========================================================
            # 4) PATENTS signals (external)
            # =========================================================
            pat_q = f"{company_name} {ticker}"
            pat_url, pat_rss, pat_source = _safe_get_patents_rss(collector, pat_q)
            patents_rss = pat_rss or ""
            _mirror_text_result(ticker, "patents_rss.txt", patents_rss)
            if s3_enabled:
                upload_text((patents_rss or "")[:20000], f"{artifact_prefix}/patents_rss.txt")
            else:
                _write_text(out_dir / "patents_rss.txt", patents_rss)
 
            if patents_rss:
                patent_mentions = parse_patents_payload(patents_rss, pat_source)
                patent_summary = summarize_patent_signals(patent_mentions)
                pat_hash = sha256_text(f"patents_rss|{ticker}|{patents_rss}")
                if store.signal_exists_by_hash(pat_hash):
                    print(f"SKIP: {ticker} patents rss already stored (hash={pat_hash[:10]})")
                else:
                    store.insert_signal(
                        company_id=company_id,
                        ticker=ticker,
                        signal_type="patents",
                        source=pat_source,
                        title=f"{company_name} patents RSS",
                        url=pat_url,
                        published_at=None,
                        content_text=patents_rss[:20000],
                        content_hash=pat_hash,
                        metadata={
                            "query": pat_q,
                            "note": "patents rss stored (truncated to 20k)",
                            "score": patent_summary.score,
                            "item_count": patent_summary.total_mentions,
                            "ai_mentions": patent_summary.ai_mentions,
                            "ai_ratio": patent_summary.ai_ratio,
                            "recency_days_median": patent_summary.recency_days_median,
                        },
                    )
                    print(f"STORED: {ticker} patents rss hash={pat_hash[:10]} source={pat_source}")
            else:
                print(f"SKIP: {ticker} no patents rss returned for query={pat_q}")
 
        print("\nOK: External signals collection completed")
        return 0
 
    finally:
        try:
            collector.close()
        except Exception:
            pass
        store.close()
 
 
if __name__ == "__main__":
    raise SystemExit(main())
 
 
