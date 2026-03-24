from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.snowflake import get_snowflake_connection


DEFAULT_WEIGHTS = {
    "jobs": 0.30,
    "tech": 0.25,
    "patents": 0.25,
    "news": 0.20,
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", default=str(date.today()), help="YYYY-MM-DD")
    args = ap.parse_args()

    asof = args.asof

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        # Aggregate per company/ticker
        cur.execute(
            """
            WITH base AS (
              SELECT
                company_id,
                ticker,
                signal_type,
                COUNT(*) AS cnt
              FROM external_signals
              GROUP BY company_id, ticker, signal_type
            ),
            pivoted AS (
              SELECT
                company_id,
                ticker,
                COALESCE(SUM(CASE WHEN signal_type='jobs' THEN cnt END),0) AS jobs_cnt,
                COALESCE(SUM(CASE WHEN signal_type='tech' THEN cnt END),0) AS tech_cnt,
                COALESCE(SUM(CASE WHEN signal_type='patents' THEN cnt END),0) AS patents_cnt,
                COALESCE(SUM(CASE WHEN signal_type='news' THEN cnt END),0) AS news_cnt,
                COALESCE(SUM(cnt),0) AS total_cnt
              FROM base
              GROUP BY company_id, ticker
            )
            SELECT company_id, ticker, jobs_cnt, tech_cnt, patents_cnt, news_cnt, total_cnt
            FROM pivoted
            """
        )
        rows = cur.fetchall()

        # Write one summary row per company
        for (company_id, ticker, jobs_cnt, tech_cnt, patents_cnt, news_cnt, total_cnt) in rows:
            # Normalize counts to 0-100 (very simple, rubric-friendly)
            # cap each component so outliers don't dominate
            def cap100(x: int, cap: int) -> float:
                return min(100.0, (float(x) / float(cap)) * 100.0) if cap > 0 else 0.0

            jobs_score = cap100(int(jobs_cnt), cap=20)
            tech_score = cap100(int(tech_cnt), cap=5)
            patents_score = cap100(int(patents_cnt), cap=10)
            news_score = cap100(int(news_cnt), cap=30)

            composite = (
                jobs_score * DEFAULT_WEIGHTS["jobs"]
                + tech_score * DEFAULT_WEIGHTS["tech"]
                + patents_score * DEFAULT_WEIGHTS["patents"]
                + news_score * DEFAULT_WEIGHTS["news"]
            )

            summary = {
                "as_of": asof,
                "counts": {
                    "jobs": int(jobs_cnt),
                    "tech": int(tech_cnt),
                    "patents": int(patents_cnt),
                    "news": int(news_cnt),
                    "total": int(total_cnt),
                },
                "scores_0_100": {
                    "jobs": round(jobs_score, 2),
                    "tech": round(tech_score, 2),
                    "patents": round(patents_score, 2),
                    "news": round(news_score, 2),
                    "composite": round(composite, 2),
                },
                "weights": DEFAULT_WEIGHTS,
            }

            summary_text = json.dumps(summary, indent=2)

            cur.execute(
                """
                MERGE INTO company_signal_summaries t
                USING (
                  SELECT %s AS company_id, %s AS ticker, %s AS as_of_date
                ) s
                ON t.company_id = s.company_id AND t.ticker = s.ticker AND t.as_of_date = s.as_of_date
                WHEN MATCHED THEN UPDATE SET
                  summary_text = %s,
                  signal_count = %s
                WHEN NOT MATCHED THEN INSERT
                  (id, company_id, ticker, as_of_date, summary_text, signal_count, created_at)
                VALUES
                  (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """,
                (
                    company_id, ticker, asof,
                    summary_text, int(total_cnt),
                    str(uuid4()), company_id, ticker, asof, summary_text, int(total_cnt),
                ),
            )

        print(f"✅ OK: company_signal_summaries updated for as_of_date={asof} rows={len(rows)}")
        return 0

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
