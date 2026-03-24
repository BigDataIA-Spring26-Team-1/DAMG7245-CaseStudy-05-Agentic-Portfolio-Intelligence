from __future__ import annotations
 
import argparse
import sys
from pathlib import Path
from uuid import uuid4
 
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
 
import httpx

from app.config import settings
from app.services.snowflake import get_snowflake_connection
 
 
DEFAULT_COMPANIES: dict[str, dict[str, str]] = {
    # Technology (mapped to business services industry id in current schema seed)
    "NVDA": {
        "name": "NVIDIA Corporation",
        "industry_id": "550e8400-e29b-41d4-a716-446655440003",
    },
 
    # Financial services
    "JPM": {
        "name": "JPMorgan Chase & Co.",
        "industry_id": "550e8400-e29b-41d4-a716-446655440005",
    },
 
    # Retail
    "WMT": {
        "name": "Walmart Inc",
        "industry_id": "550e8400-e29b-41d4-a716-446655440004",
    },
    "DG": {
        "name": "Dollar General Corporation",
        "industry_id": "550e8400-e29b-41d4-a716-446655440004",
    },
 
    # Manufacturing
    "GE": {
        "name": "General Electric Company",
        "industry_id": "550e8400-e29b-41d4-a716-446655440001",
    },
}
DEFAULT_INDUSTRY_ID = "550e8400-e29b-41d4-a716-446655440003"  # Business Services
SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"


def _normalize_tickers(raw: str) -> list[str]:
    tickers = [t.strip().upper() for t in (raw or "").split(",") if t.strip()]
    return list(dict.fromkeys(tickers))


def load_sec_company_names(user_agent: str) -> dict[str, str]:
    try:
        with httpx.Client(headers={"User-Agent": user_agent}, timeout=20.0, follow_redirects=True) as client:
            resp = client.get(SEC_TICKER_URL)
            resp.raise_for_status()
            payload = resp.json()
    except Exception:
        return {}

    out: dict[str, str] = {}
    if isinstance(payload, dict):
        for row in payload.values():
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or "").strip().upper()
            title = str(row.get("title") or "").strip()
            if ticker and title:
                out[ticker] = title
    return out


def get_company_row(cur, ticker: str) -> tuple[str | None, str | None]:
    cur.execute(
        """
        SELECT name, industry_id
        FROM companies
        WHERE ticker = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (ticker,),
    )
    row = cur.fetchone()
    if not row:
        return None, None
    name = str(row[0]) if row[0] is not None else None
    industry_id = str(row[1]) if row[1] is not None else None
    return name, industry_id


def resolve_industry_id(cur, explicit_id: str | None, explicit_name: str | None) -> str | None:
    if explicit_id:
        return explicit_id
    if explicit_name:
        cur.execute(
            """
            SELECT id
            FROM industries
            WHERE UPPER(name) = UPPER(%s)
            LIMIT 1
            """,
            (explicit_name,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
    return DEFAULT_INDUSTRY_ID
 
 
def upsert_company(cur, ticker: str, name: str, industry_id: str | None) -> str:
    cur.execute("SELECT id FROM companies WHERE ticker = %s LIMIT 1", (ticker,))
    row = cur.fetchone()
    if row:
        company_id = str(row[0])
        cur.execute(
            """
            UPDATE companies
               SET name = %s,
                   industry_id = %s,
                   is_deleted = FALSE,
                   updated_at = CURRENT_TIMESTAMP()
             WHERE id = %s
            """,
            (name, industry_id, company_id),
        )
        return "updated"
 
    company_id = str(uuid4())
    cur.execute(
        """
        INSERT INTO companies (id, name, ticker, industry_id)
        VALUES (%s, %s, %s, %s)
        """,
        (company_id, name, ticker, industry_id),
    )
    return "inserted"
 
 
def main() -> int:
    parser = argparse.ArgumentParser(description="Seed or upsert companies into Snowflake.")
    parser.add_argument(
        "--companies",
        help="Comma-separated tickers (e.g., CAT,DE). If omitted, seeds defaults.",
    )
    parser.add_argument(
        "--industry-id",
        default=None,
        help="Optional industry_id override for all requested tickers.",
    )
    parser.add_argument(
        "--industry-name",
        default=None,
        help="Optional industry name to resolve id from industries table (used if --industry-id is not provided).",
    )
    args = parser.parse_args()
 
    requested = _normalize_tickers(args.companies or "")
    seed_defaults_only = not requested

    if seed_defaults_only:
        requested = list(DEFAULT_COMPANIES.keys())

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        sec_names = load_sec_company_names(settings.sec_user_agent)
        industry_id_override = resolve_industry_id(cur, explicit_id=args.industry_id, explicit_name=args.industry_name)

        inserted = 0
        updated = 0
        unresolved_names: list[str] = []
        for ticker in requested:
            if ticker in DEFAULT_COMPANIES:
                default_info = DEFAULT_COMPANIES[ticker]
                name = default_info["name"]
                industry_id = args.industry_id or default_info["industry_id"]
            else:
                existing_name, existing_industry = get_company_row(cur, ticker)
                name = existing_name or sec_names.get(ticker) or ticker
                industry_id = industry_id_override or existing_industry
                if name == ticker:
                    unresolved_names.append(ticker)

            result = upsert_company(cur, ticker, name, industry_id)
            if result == "inserted":
                inserted += 1
            else:
                updated += 1
 
        conn.commit()
        print(f"Companies upserted: inserted={inserted}, updated={updated}")
        if unresolved_names:
            print(f"Name fallback used (ticker as name): {', '.join(unresolved_names)}")
        return 0
    finally:
        cur.close()
        conn.close()
 
 
if __name__ == "__main__":
    raise SystemExit(main())
 
 
