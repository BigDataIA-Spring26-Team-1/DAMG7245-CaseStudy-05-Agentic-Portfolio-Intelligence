from __future__ import annotations

import json
import re
from typing import Dict

from app.pipelines.job_signals import parse_jobs_rss, summarize_job_signals
from app.pipelines.patent_signals import parse_patents_rss, summarize_patent_signals
from app.pipelines.tech_signals import summarize_tech_signals
from app.services.result_artifacts import write_json_artifact
from app.services.snowflake import get_snowflake_connection


def score_jobs(n: int) -> float:
    return min(100.0, (n / 50.0) * 100.0)


def score_news(n: int) -> float:
    return min(100.0, (n / 40.0) * 100.0)


def score_tech(unique_keywords: int) -> float:
    return min(100.0, (unique_keywords / 10.0) * 100.0)


def score_patents(n: int) -> float:
    return min(100.0, (n / 20.0) * 100.0)


def extract_rss_item_count(text: str) -> int:
    if not text:
        return 0
    return len(re.findall(r"<item>", text, re.IGNORECASE))


def normalize_metadata(metadata) -> Dict:
    if metadata is None:
        return {}
    if isinstance(metadata, str):
        return json.loads(metadata)
    return dict(metadata)


def main() -> int:
    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT id, signal_type, content_text, metadata
            FROM external_signals
            """
        )
        rows = cur.fetchall()

        for sid, signal_type, content_text, metadata in rows:
            meta = normalize_metadata(metadata)

            if "score" in meta:
                continue

            score = 0.0

            if signal_type == "jobs":
                mentions = parse_jobs_rss(content_text or "")
                summary = summarize_job_signals(mentions)
                meta["count"] = summary.total_jobs
                meta["ai_jobs"] = summary.ai_jobs
                meta["ai_ratio"] = summary.ai_ratio
                meta["senior_ai_jobs"] = summary.senior_ai_jobs
                score = summary.score if summary.total_jobs > 0 else score_jobs(summary.total_jobs)

            elif signal_type == "news":
                count = extract_rss_item_count(content_text)
                meta["count"] = count
                score = score_news(count)

            elif signal_type == "tech":
                counts = meta.get("counts", {})
                summary = summarize_tech_signals(counts if isinstance(counts, dict) else {})
                meta["unique_keywords"] = summary.unique_keywords
                meta["cloud_ml_count"] = summary.cloud_ml_count
                meta["ml_framework_count"] = summary.ml_framework_count
                meta["data_platform_count"] = summary.data_platform_count
                meta["ai_api_count"] = summary.ai_api_count
                score = summary.score if summary.unique_keywords > 0 else score_tech(summary.unique_keywords)

            elif signal_type == "patents":
                mentions = parse_patents_rss(content_text or "")
                summary = summarize_patent_signals(mentions)
                meta["count"] = summary.total_mentions
                meta["ai_mentions"] = summary.ai_mentions
                meta["ai_ratio"] = summary.ai_ratio
                meta["recency_days_median"] = summary.recency_days_median
                score = summary.score if summary.total_mentions > 0 else score_patents(summary.total_mentions)

            meta["score"] = round(score, 2)

            cur.execute(
                """
                UPDATE external_signals
                SET metadata = PARSE_JSON(%s)
                WHERE id = %s
                """,
                (json.dumps(meta), sid),
            )

        cur.execute(
            """
            SELECT
              UPPER(ticker) AS ticker,
              signal_type,
              COUNT(*) AS signal_count,
              AVG(TRY_TO_DOUBLE(metadata:score::string)) AS avg_score
            FROM external_signals
            WHERE ticker IS NOT NULL
              AND metadata:score IS NOT NULL
            GROUP BY UPPER(ticker), signal_type
            ORDER BY UPPER(ticker), signal_type
            """
        )

        grouped: Dict[str, Dict[str, Dict[str, float]]] = {}
        for ticker, signal_type, signal_count, avg_score in cur.fetchall() or []:
            normalized_ticker = str(ticker).upper()
            grouped.setdefault(normalized_ticker, {})
            grouped[normalized_ticker][str(signal_type)] = {
                "signal_count": int(signal_count or 0),
                "avg_score": round(float(avg_score or 0.0), 2),
            }

        for ticker, snapshot in grouped.items():
            write_json_artifact(
                ticker=ticker,
                category="signal_scores",
                filename="latest_signal_scores.json",
                payload={"ticker": ticker, "signal_scores": snapshot},
            )

        print("Signal-level scoring completed")
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
