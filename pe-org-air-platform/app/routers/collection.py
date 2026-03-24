from __future__ import annotations
 
from datetime import datetime, timezone
import logging
from pathlib import Path
import re
from threading import Lock
from typing import Any
from uuid import uuid4
 
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
 
from app.config import settings
from app.pipelines.document_parser import chunk_document, parse_filing_bytes
from app.pipelines.external_signals import ExternalSignalCollector, sha256_text
from app.pipelines.sec_edgar import SecEdgarClient, store_raw_filing
from app.services.evidence_store import ChunkRow, DocumentRow, DocumentStatus, EvidenceStore
from app.services.redis_cache import cache_delete_pattern, cache_get_json, cache_set_json
from app.services.result_artifacts import write_json_artifact, write_text_artifact
from app.services.signal_store import SignalStore
from app.services.snowflake import get_snowflake_connection
 
router = APIRouter(prefix="/collection")
logger = logging.getLogger("uvicorn.error")
 
_TASK_TTL_SECONDS = 24 * 60 * 60
_TASK_MAX_LOCAL = 2000
_TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
TASKS: dict[str, dict[str, Any]] = {}
_TASKS_LOCK = Lock()
 
 
def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
 
 
def _task_cache_key(task_id: str) -> str:
    return f"collection:task:{task_id}"
 
 
def _updated_at_ts(task: dict[str, Any]) -> float:
    raw = task.get("updated_at")
    if not isinstance(raw, str):
        return 0.0
    try:
        return datetime.fromisoformat(raw).timestamp()
    except Exception:
        return 0.0
 
 
def _prune_local_tasks_locked(now_ts: float) -> None:
    cutoff_ts = now_ts - _TASK_TTL_SECONDS
    stale_task_ids = [task_id for task_id, task in TASKS.items() if _updated_at_ts(task) < cutoff_ts]
    for task_id in stale_task_ids:
        TASKS.pop(task_id, None)
 
    excess = len(TASKS) - _TASK_MAX_LOCAL
    if excess > 0:
        oldest = sorted(TASKS.items(), key=lambda item: _updated_at_ts(item[1]))[:excess]
        for task_id, _ in oldest:
            TASKS.pop(task_id, None)
 
 
def _load_task(task_id: str) -> dict[str, Any] | None:
    try:
        cached = cache_get_json(_task_cache_key(task_id))
        if isinstance(cached, dict):
            with _TASKS_LOCK:
                TASKS[task_id] = dict(cached)
            return dict(cached)
    except Exception as exc:
        logger.warning("collection_task_cache_load_failed task_id=%s err=%s", task_id, exc)
 
    with _TASKS_LOCK:
        local = TASKS.get(task_id)
        return dict(local) if local is not None else None
 
 
def _store_task(task_id: str, task: dict[str, Any]) -> None:
    with _TASKS_LOCK:
        TASKS[task_id] = dict(task)
        _prune_local_tasks_locked(datetime.now(timezone.utc).timestamp())
 
    try:
        cache_set_json(_task_cache_key(task_id), task, _TASK_TTL_SECONDS)
    except Exception as exc:
        logger.warning("collection_task_cache_store_failed task_id=%s err=%s", task_id, exc)
 
 
def _update_task(task_id: str, **fields: Any) -> None:
    task = _load_task(task_id) or {"task_id": task_id}
    now_iso = _utcnow_iso()
    task.setdefault("task_id", task_id)
    task.setdefault("created_at", now_iso)
    task.update(fields)
    task["updated_at"] = now_iso
    _store_task(task_id, task)
 
 
def _invalidate_cs2_cache() -> None:
    """
    Invalidate cached CS2 read endpoints after new evidence/signals are collected.
    """
    cache_delete_pattern("documents:*")
    cache_delete_pattern("evidence:*")
    cache_delete_pattern("chunks:*")
    cache_delete_pattern("signals:*")
    cache_delete_pattern("signal_summaries:list:*")


def _mirror_collection_text(ticker: str, category: str, filename: str, text: str) -> None:
    write_text_artifact(
        ticker=ticker,
        category=category,
        filename=filename,
        text=(text or "")[:20000],
    )


def _mirror_collection_json(ticker: str, category: str, filename: str, payload: dict[str, Any]) -> None:
    write_json_artifact(
        ticker=ticker,
        category=category,
        filename=filename,
        payload=payload,
    )
 
 
def _parse_requested_tickers(companies: str) -> list[str]:
    if companies.strip().lower() == "all":
        tickers = _get_active_tickers()
        if not tickers:
            raise HTTPException(status_code=404, detail="No active companies found in database")
        return tickers
 
    tickers = [t.strip().upper() for t in companies.split(",") if t.strip()]
    if not tickers:
        raise HTTPException(status_code=422, detail="No valid tickers provided")
 
    invalid = [t for t in tickers if not _TICKER_PATTERN.fullmatch(t)]
    if invalid:
        raise HTTPException(status_code=422, detail=f"Invalid ticker format: {', '.join(invalid)}")
 
    # Preserve order while removing duplicates.
    return list(dict.fromkeys(tickers))


def _get_active_tickers() -> list[str]:
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


def _get_company_id(ticker: str) -> str | None:
    profile = _get_company_profile(ticker)
    if profile is None:
        return None
    return profile["id"]


def _get_company_profile(ticker: str) -> dict[str, str] | None:
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
            return None
        if len(rows) > 1:
            raise RuntimeError(f"Duplicate active company rows found for ticker={ticker}")
        return {
            "id": str(rows[0][0]),
            "name": str(rows[0][1] or ticker).strip() or ticker,
        }
    finally:
        cur.close()
        conn.close()
 
 
def run_collect_evidence(task_id: str, companies: list[str]) -> None:
    _update_task(task_id, status="running", type="evidence", companies=companies, message="")
    root = Path(__file__).resolve().parents[2]  # app/routers -> app -> repo root
    client = SecEdgarClient(user_agent=settings.sec_user_agent, rate_limit_per_sec=5.0)
    store = EvidenceStore()
    try:
        ticker_map = client.get_ticker_to_cik_map()
        ticker_errors: list[dict[str, str]] = []
        for ticker in companies:
            try:
                cik = ticker_map.get(ticker)
                if not cik:
                    continue
                company_id = _get_company_id(ticker)
                if not company_id:
                    continue
                filings = client.list_recent_filings(
                    ticker=ticker,
                    cik_10=cik,
                    forms=["10-K", "10-Q", "8-K", "DEF-14A"],
                    limit_per_form=1,
                )
                for filing in filings:
                    doc_id = str(uuid4())
                    source_url = f"{filing.filing_dir_url}/{filing.primary_doc}"
                    raw_path = None
                    content_hash = None
                    try:
                        raw = client.download_primary_document(filing)
                        raw_path = store_raw_filing(root, filing, raw)
                        parsed = parse_filing_bytes(raw, file_hint=str(raw_path))
                        content_hash = parsed.content_hash
                        if store.document_exists_by_hash(content_hash):
                            continue
 
                        chunks = chunk_document(parsed)
                        base_name = f"{filing.form}_{filing.filing_date}_{filing.accession}"
                        body_text = parsed.sections.get("Item 1A") or parsed.full_text[:20000]
                        chunks_text = "\n\n--- CHUNK ---\n\n".join([chunk.content[:1500] for chunk in chunks[:10]])
                        _mirror_collection_text(ticker, "evidence/processed", f"{base_name}.txt", body_text)
                        _mirror_collection_text(
                            ticker,
                            "evidence/processed",
                            f"{base_name}_chunks.txt",
                            chunks_text,
                        )
                        store.insert_document(
                            DocumentRow(
                                id=doc_id,
                                company_id=company_id,
                                ticker=ticker,
                                filing_type=filing.form,
                                filing_date=filing.filing_date,
                                source_url=source_url,
                                local_path=str(raw_path),
                                content_hash=content_hash,
                                word_count=parsed.word_count,
                                chunk_count=len(chunks),
                                status=DocumentStatus.CHUNKED.value,
                            )
                        )
                        store.insert_chunks_bulk(
                            [
                                ChunkRow(
                                    id=str(uuid4()),
                                    document_id=doc_id,
                                    chunk_index=chunk.chunk_index,
                                    content=chunk.content,
                                    section=chunk.section,
                                    start_char=chunk.start_char,
                                    end_char=chunk.end_char,
                                    word_count=chunk.word_count,
                                )
                                for chunk in chunks
                            ]
                        )
                        store.update_document_status(doc_id, DocumentStatus.INDEXED.value)
                    except Exception as exc:
                        err = str(exc)[:8000]
                        status_updated = False
                        try:
                            status_updated = store.update_document_status(
                                doc_id,
                                DocumentStatus.FAILED.value,
                                error_message=err,
                            )
                        except Exception:
                            status_updated = False
                        if not status_updated:
                            store.insert_failed_stub(
                                doc_id=doc_id,
                                company_id=company_id,
                                ticker=ticker,
                                filing_type=filing.form,
                                filing_date=filing.filing_date,
                                source_url=source_url,
                                local_path=str(raw_path) if raw_path else None,
                                content_hash=content_hash,
                                error_message=err,
                            )
                        continue
            except Exception as exc:
                ticker_errors.append({"ticker": ticker, "error": str(exc)[:500]})
                continue
 
        if ticker_errors:
            _update_task(
                task_id,
                status="done",
                message=f"Evidence collection completed with {len(ticker_errors)} ticker error(s)",
                had_errors=True,
                errors=ticker_errors[:50],
            )
        else:
            _update_task(task_id, status="done", message="Evidence collection completed")
        _invalidate_cs2_cache()
    except Exception as exc:
        _update_task(task_id, status="failed", message=str(exc))
    finally:
        try:
            client.close()
        except Exception:
            pass
        store.close()
 
 
def run_collect_signals(task_id: str, companies: list[str]) -> None:
    _update_task(task_id, status="running", type="signals", companies=companies, message="")
    collector = ExternalSignalCollector(user_agent=settings.sec_user_agent)
    store = SignalStore()
    try:
        ticker_errors: list[dict[str, str]] = []
        for ticker in companies:
            try:
                profile = _get_company_profile(ticker)
                if not profile:
                    continue
                company_id = profile["id"]
                name = profile["name"] or ticker
 
                jobs_q = f"{name} {ticker} hiring jobs"
                jobs_url, jobs_rss = collector.google_jobs_rss(jobs_q)
                _mirror_collection_text(ticker, "signals", "jobs_rss.txt", jobs_rss)
                if jobs_rss:
                    jobs_hash = sha256_text(f"jobs_rss|{ticker}|{jobs_rss}")
                    if not store.signal_exists_by_hash(jobs_hash):
                        store.insert_signal(
                            company_id=company_id,
                            ticker=ticker,
                            signal_type="jobs",
                            source="google_jobs_rss_fallback",
                            title=f"{name} jobs RSS",
                            url=jobs_url,
                            published_at=None,
                            content_text=jobs_rss[:20000],
                            content_hash=jobs_hash,
                            metadata={"query": jobs_q, "note": "rss stored (truncated)"},
                        )
 
                news_q = f"{name} {ticker}"
                news_url, news_rss = collector.google_news_rss(news_q)
                _mirror_collection_text(ticker, "signals", "news_rss.txt", news_rss)
                if news_rss:
                    news_hash = sha256_text(f"news_rss|{ticker}|{news_rss}")
                    if not store.signal_exists_by_hash(news_hash):
                        store.insert_signal(
                            company_id=company_id,
                            ticker=ticker,
                            signal_type="news",
                            source="google_news_rss",
                            title=f"{name} news RSS",
                            url=news_url,
                            published_at=None,
                            content_text=news_rss[:20000],
                            content_hash=news_hash,
                            metadata={"query": news_q, "note": "rss stored (truncated)"},
                        )
                _mirror_collection_json(
                    ticker,
                    "signals",
                    "collection_summary.json",
                    {
                        "ticker": ticker,
                        "company_id": company_id,
                        "jobs_url": jobs_url,
                        "news_url": news_url,
                        "jobs_rss_present": bool(jobs_rss),
                        "news_rss_present": bool(news_rss),
                    },
                )
            except Exception as exc:
                ticker_errors.append({"ticker": ticker, "error": str(exc)[:500]})
                continue
 
        if ticker_errors:
            _update_task(
                task_id,
                status="done",
                message=f"Signals collection completed with {len(ticker_errors)} ticker error(s)",
                had_errors=True,
                errors=ticker_errors[:50],
            )
        else:
            _update_task(task_id, status="done", message="Signals collection completed")
        _invalidate_cs2_cache()
    except Exception as exc:
        _update_task(task_id, status="failed", message=str(exc))
    finally:
        try:
            collector.close()
        except Exception:
            pass
        store.close()
 
 
@router.post("/evidence")
def collect_evidence(background_tasks: BackgroundTasks, companies: str = Query(..., description="CAT,DE or all")):
    tickers = _parse_requested_tickers(companies)
    task_id = str(uuid4())
    _update_task(task_id, status="queued", type="evidence", companies=tickers, message="queued")
    background_tasks.add_task(run_collect_evidence, task_id, tickers)
    return {"task_id": task_id, "status": "queued"}
 
 
@router.post("/signals")
def collect_signals(background_tasks: BackgroundTasks, companies: str = Query(..., description="CAT,DE or all")):
    tickers = _parse_requested_tickers(companies)
    task_id = str(uuid4())
    _update_task(task_id, status="queued", type="signals", companies=tickers, message="queued")
    background_tasks.add_task(run_collect_signals, task_id, tickers)
    return {"task_id": task_id, "status": "queued"}
 
 
@router.get("/tasks/{task_id}")
def task_status(task_id: str):
    task = _load_task(task_id)
    if task is None:
        return {"status": "unknown", "message": "task_id not found"}
    return task
 
 
