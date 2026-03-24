from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.pipelines.glassdoor_collector import GlassdoorCultureCollector


def _merge_company_id_map(ticker: str, company_id: str) -> None:
    base: dict[str, str] = {}
    raw = (os.getenv("GLASSDOOR_COMPANY_ID_MAP") or "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                for k, v in parsed.items():
                    kk = str(k or "").strip().upper()
                    vv = str(v or "").strip()
                    if kk and vv:
                        base[kk] = vv
        except Exception:
            pass
    base[ticker.upper()] = str(company_id).strip()
    os.environ["GLASSDOOR_COMPANY_ID_MAP"] = json.dumps(base)


def main() -> int:
    ap = argparse.ArgumentParser(description="Minimal Glassdoor reviews smoke test.")
    ap.add_argument("--ticker", required=True, help="Ticker, e.g. NVDA")
    ap.add_argument("--company-id", required=True, help="RapidAPI companyId for /companies/reviews")
    ap.add_argument("--limit", type=int, default=1, help="Number of reviews to request (default: 1)")
    ap.add_argument(
        "--raw-only",
        action="store_true",
        help="Only perform one direct API call and print raw response metadata.",
    )
    ap.add_argument(
        "--allow-fallback",
        action="store_true",
        help="Allow discovery/query fallback calls (default: disabled to minimize credits).",
    )
    ap.add_argument(
        "--roundtrip",
        action="store_true",
        help="Run collector.fetch_reviews after raw call (this can issue an extra API request).",
    )
    args = ap.parse_args()

    ticker = args.ticker.strip().upper()
    if not ticker:
        raise SystemExit("Invalid --ticker")
    if not str(args.company_id).strip():
        raise SystemExit("Invalid --company-id")

    _merge_company_id_map(ticker=ticker, company_id=args.company_id)
    os.environ["GLASSDOOR_DISABLE_DISCOVERY_FALLBACK"] = "false" if args.allow_fallback else "true"
    os.environ.setdefault("GLASSDOOR_REVIEWS_COMPANY_ID_PARAM", "companyId")
    os.environ.setdefault("GLASSDOOR_REVIEWS_PATH", "/companies/reviews")
    os.environ["GLASSDOOR_REVIEWS_PAGE_SIZE"] = str(max(1, int(args.limit)))

    collector = GlassdoorCultureCollector()
    api_key = collector.rapidapi_key
    if not api_key:
        raise SystemExit("RAPIDAPI_KEY is missing. Set it in environment or pe-org-air-platform/.env")

    # Single-request debug path: verify raw response without fallback calls.
    host = collector.rapidapi_host
    path = collector.reviews_path
    company_param = collector.reviews_company_id_param
    params = {company_param: str(args.company_id).strip(), "limit": max(1, int(args.limit))}
    headers = {"x-rapidapi-key": api_key, "x-rapidapi-host": host}
    resp = httpx.get(f"https://{host}{path}", params=params, headers=headers, timeout=20.0, follow_redirects=True)
    print(f"http_status={resp.status_code} url={resp.url}")
    print(f"response_chars={len(resp.text or '')}")
    try:
        payload = resp.json()
        if isinstance(payload, dict):
            print(f"top_level_keys={list(payload.keys())[:15]}")
        elif isinstance(payload, list):
            print(f"top_level_type=list size={len(payload)}")
        else:
            print(f"top_level_type={type(payload).__name__}")
    except Exception:
        payload = None
        print("response_json=INVALID")
    print("response_preview:")
    print((resp.text or "")[:1200])

    if args.raw_only:
        return 0

    # Parse from the same payload to avoid extra API calls.
    parsed_from_payload = collector._parse_reviews_payload(payload=payload, ticker=ticker) if payload is not None else []
    reviews = parsed_from_payload[: max(1, int(args.limit))]
    print(f"ticker={ticker} requested_limit={max(1, int(args.limit))} parsed_from_single_response={len(reviews)}")
    if reviews:
        sample = reviews[0]
        print(
            json.dumps(
                {
                    "review_id": sample.review_id,
                    "rating": sample.rating,
                    "title": sample.title[:120],
                    "is_current_employee": sample.is_current_employee,
                    "review_date": sample.review_date.isoformat(),
                },
                indent=2,
            )
        )

    if args.roundtrip:
        roundtrip = collector.fetch_reviews(ticker=ticker, limit=max(1, int(args.limit)))
        print(f"collector_roundtrip_fetched={len(roundtrip)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
