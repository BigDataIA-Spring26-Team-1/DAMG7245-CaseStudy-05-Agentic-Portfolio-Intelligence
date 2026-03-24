from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.services.s3_storage import is_s3_configured, upload_bytes


SEC_WWW_BASE = "https://www.sec.gov"
SEC_DATA_BASE = "https://data.sec.gov"
SEC_ARCHIVES_BASE = "https://www.sec.gov/Archives"
url = f"{SEC_WWW_BASE}/files/company_tickers.json"



@dataclass(frozen=True)
class FilingRef:
    ticker: str
    cik: str  # zero-padded 10 digits
    accession: str  # accession number with dashes
    form: str  # 10-K / 10-Q / 8-K
    filing_date: str  # YYYY-MM-DD
    primary_doc: str  # filename like 'd12345d10k.htm'
    filing_dir_url: str  # Archives directory URL


class SecEdgarClient:
    """
    Minimal SEC EDGAR client using official JSON submission endpoints.
    - Fetches CIK for a ticker
    - Lists recent filings
    - Resolves primary document URL for each filing
    - Downloads primary document bytes
    """

    def __init__(self, user_agent: str, rate_limit_per_sec: float = 5.0, timeout_s: float = 30.0):
        if not user_agent or "@" not in user_agent:
            raise ValueError("SEC user_agent must include contact email (e.g., 'AppName email@domain').")
        self.user_agent = user_agent
        self.rate_limit_per_sec = max(rate_limit_per_sec, 0.1)
        self._min_interval = 1.0 / self.rate_limit_per_sec
        self._last_call = 0.0
        self._client = httpx.Client(
            headers={"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"},
            timeout=timeout_s,
        )

    def close(self) -> None:
        self._client.close()

    def _throttle(self) -> None:
        now = time.time()
        wait = self._min_interval - (now - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.time()

    def get_ticker_to_cik_map(self) -> Dict[str, str]:
        """
        SEC provides a JSON mapping of tickers to CIKs.
        """
        self._throttle()
        url = "https://www.sec.gov/files/company_tickers.json"
        r = self._client.get(url)
        r.raise_for_status()
        data = r.json()
        out: Dict[str, str] = {}
        for _, row in data.items():
            t = str(row.get("ticker", "")).upper().strip()
            cik_int = row.get("cik_str")
            if t and cik_int is not None:
                out[t] = str(cik_int).zfill(10)
        return out

    def get_company_submissions(self, cik_10: str) -> Dict[str, Any]:
        self._throttle()
        url = f"{SEC_DATA_BASE}/submissions/CIK{cik_10}.json"
        r = self._client.get(url)
        r.raise_for_status()
        return r.json()

    def list_recent_filings(
        self,
        ticker: str,
        cik_10: str,
        forms: List[str],
        limit_per_form: int = 6,
    ) -> List[FilingRef]:
        """
        Returns FilingRef objects for the most recent filings per form.
        Uses submissions JSON -> filings.recent arrays.
        """
        subs = self.get_company_submissions(cik_10)
        recent = subs.get("filings", {}).get("recent", {})
        forms_arr = recent.get("form", [])
        acc_arr = recent.get("accessionNumber", [])
        date_arr = recent.get("filingDate", [])
        prim_arr = recent.get("primaryDocument", [])

        rows: List[Tuple[str, str, str, str]] = []
        for form, acc, fdate, pdoc in zip(forms_arr, acc_arr, date_arr, prim_arr):
            rows.append((form, acc, fdate, pdoc))

        result: List[FilingRef] = []
        for target_form in forms:
            picked = [r for r in rows if r[0] == target_form][:limit_per_form]
            for form, acc, fdate, pdoc in picked:
                acc_nodash = acc.replace("-", "")
                filing_dir_url = f"{SEC_ARCHIVES_BASE}/edgar/data/{int(cik_10)}/{acc_nodash}"
                result.append(
                    FilingRef(
                        ticker=ticker,
                        cik=cik_10,
                        accession=acc,
                        form=form,
                        filing_date=fdate,
                        primary_doc=pdoc,
                        filing_dir_url=filing_dir_url,
                    )
                )
        return result

    def download_primary_document(self, filing: FilingRef) -> bytes:
        """
        Downloads the primary document for a filing.
        """
        self._throttle()
        url = f"{filing.filing_dir_url}/{filing.primary_doc}"
        r = self._client.get(url)
        r.raise_for_status()
        return r.content


def safe_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_", ".", "+") else "_" for c in name)


def store_raw_filing(
    base_dir: Path,
    filing: FilingRef,
    content: bytes,
) -> Path | str:
    """
    Stores raw bytes under data/raw/<ticker>/<form>/<accession>_<primaryDoc>
    """
    fname = safe_filename(f"{filing.accession}_{filing.primary_doc}")
    key = f"data/raw/{filing.ticker}/{filing.form}/{fname}"

    if is_s3_configured():
        return upload_bytes(content, key, content_type="application/octet-stream")

    out_dir = base_dir / "data" / "raw" / filing.ticker / filing.form
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / fname
    out_path.write_bytes(content)
    return out_path
