from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scoring_engine.portfolio_validation import (
    all_portfolio_scores_in_range,
    validate_portfolio_score_ranges,
)
from app.services.result_artifacts import write_json_artifact
from app.services.snowflake import get_snowflake_connection


def _fetch_latest_scores(cur) -> dict[str, float]:
    cur.execute(
        """
        WITH ranked AS (
          SELECT
            UPPER(c.ticker) AS ticker,
            o.composite_score,
            ROW_NUMBER() OVER (PARTITION BY c.ticker ORDER BY o.scored_at DESC, o.created_at DESC) AS rn
          FROM org_air_scores o
          JOIN companies c
            ON c.id = o.company_id
          WHERE c.ticker IS NOT NULL
        )
        SELECT ticker, composite_score
        FROM ranked
        WHERE rn = 1
        """
    )
    return {
        str(ticker).upper(): float(score)
        for ticker, score in (cur.fetchall() or [])
        if ticker and score is not None
    }


def main() -> int:
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        scores = _fetch_latest_scores(cur)
        checks = validate_portfolio_score_ranges(scores)
        ok = all_portfolio_scores_in_range(checks)

        for ticker in sorted(checks.keys()):
            c = checks[ticker]
            score = "missing" if c.score is None else f"{c.score:.2f}"
            status = "PASS" if c.in_range else "FAIL"
            print(f"{ticker}: {score} expected=[{c.lower_bound:.2f}, {c.upper_bound:.2f}] {status}")

        write_json_artifact(
            ticker="portfolio",
            category="validation",
            filename="latest_portfolio_validation.json",
            payload={
                "ok": ok,
                "scores": scores,
                "checks": {
                    ticker: {
                        "score": item.score,
                        "lower_bound": item.lower_bound,
                        "upper_bound": item.upper_bound,
                        "in_range": item.in_range,
                    }
                    for ticker, item in checks.items()
                },
            },
        )

        return 0 if ok else 1
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
