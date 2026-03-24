from __future__ import annotations

import argparse
import json
from datetime import date
from uuid import uuid4

from app.services.result_artifacts import write_json_artifact
from app.services.snowflake import get_snowflake_connection


WEIGHTS = {
    "jobs": 0.35,
    "tech": 0.30,
    "patents": 0.20,
    "news": 0.15,
}


def safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as_of_date", default=str(date.today()), help="YYYY-MM-DD (default: today)")
    args = ap.parse_args()
    as_of = args.as_of_date

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        # Add UPDATED_AT safely because Snowflake lacks ADD COLUMN IF NOT EXISTS.
        try:
            cur.execute(
                "ALTER TABLE company_signal_summaries "
                "ADD COLUMN updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
            )
        except Exception:
            pass

        # Cast metadata:score safely before aggregating signal averages.
        cur.execute(
            """
            SELECT
              company_id,
              ticker,
              signal_type,
              COUNT(*) AS n,
              AVG(TRY_TO_DOUBLE(metadata:score::string)) AS avg_score
            FROM external_signals
            WHERE company_id IS NOT NULL
              AND ticker IS NOT NULL
              AND signal_type IS NOT NULL
              AND metadata:score IS NOT NULL
            GROUP BY company_id, ticker, signal_type
            """
        )
        rows = cur.fetchall()

        agg: dict[tuple[str, str], dict[str, dict[str, float]]] = {}
        total_counts: dict[tuple[str, str], int] = {}

        for company_id, ticker, signal_type, n, avg_score in rows:
            key = (str(company_id), str(ticker))
            agg.setdefault(key, {})
            agg[key][str(signal_type)] = {
                "count": int(n or 0),
                "avg_score": safe_float(avg_score),
            }
            total_counts[key] = total_counts.get(key, 0) + int(n or 0)

        upserted = 0
        for (company_id, ticker), by_type in agg.items():
            composite = 0.0
            weight_used = 0.0

            for signal_type, weight in WEIGHTS.items():
                if signal_type in by_type:
                    composite += by_type[signal_type]["avg_score"] * weight
                    weight_used += weight

            if 0 < weight_used < 1.0:
                composite = composite / weight_used

            composite = round(composite, 2)

            summary = {
                "as_of_date": as_of,
                "composite_score": composite,
                "weights": WEIGHTS,
                "breakdown": by_type,
                "notes": [
                    "avg_score sourced from external_signals.metadata.score",
                    "composite is weighted sum normalized if some signal types missing",
                ],
            }
            summary_text = json.dumps(summary, ensure_ascii=False)

            summary_id = str(uuid4())
            signal_count = total_counts.get((company_id, ticker), 0)

            cur.execute(
                """
                MERGE INTO company_signal_summaries t
                USING (
                  SELECT
                    %s AS id,
                    %s AS company_id,
                    %s AS ticker,
                    TO_DATE(%s) AS as_of_date,
                    %s AS summary_text,
                    %s AS signal_count
                ) s
                ON t.company_id = s.company_id AND t.as_of_date = s.as_of_date
                WHEN MATCHED THEN UPDATE SET
                  t.ticker = s.ticker,
                  t.summary_text = s.summary_text,
                  t.signal_count = s.signal_count,
                  t.updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                  (id, company_id, ticker, as_of_date, summary_text, signal_count, created_at, updated_at)
                VALUES
                  (s.id, s.company_id, s.ticker, s.as_of_date, s.summary_text, s.signal_count,
                   CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                """,
                (summary_id, company_id, ticker, as_of, summary_text, signal_count),
            )

            payload = {
                "company_id": company_id,
                "ticker": ticker,
                **summary,
                "signal_count": signal_count,
            }
            write_json_artifact(
                ticker=ticker,
                category="signal_summaries",
                filename=f"company_signal_summary_{as_of}.json",
                payload=payload,
            )
            write_json_artifact(
                ticker=ticker,
                category="signal_summaries",
                filename="latest_company_signal_summary.json",
                payload=payload,
            )

            upserted += 1

        print(f"OK: Company summaries computed and upserted: {upserted} (as_of_date={as_of})")
        return 0

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
