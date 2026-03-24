from __future__ import annotations

from pathlib import Path

from app.pipelines.sec_edgar import FilingRef, SecEdgarClient, safe_filename, store_raw_filing


class _FakeResponse:
    def __init__(self, *, json_data=None, content: bytes = b""):
        self._json_data = json_data
        self.content = content

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._json_data


def test_sec_edgar_requires_contact_email():
    try:
        SecEdgarClient(user_agent="NoEmailUserAgent")
        assert False, "Expected ValueError for user agent without contact email"
    except ValueError:
        assert True


def test_get_ticker_to_cik_map_normalizes_ticker_and_cik():
    client = SecEdgarClient(user_agent="Tests tests@example.com")
    try:
        payload = {
            "0": {"ticker": "cat", "cik_str": 12345},
            "1": {"ticker": "DE", "cik_str": 9},
        }
        client._client.get = lambda _url: _FakeResponse(json_data=payload)  # type: ignore[method-assign]
        mapping = client.get_ticker_to_cik_map()
        assert mapping["CAT"] == "0000012345"
        assert mapping["DE"] == "0000000009"
    finally:
        client.close()


def test_list_recent_filings_filters_forms_and_limits_per_form():
    client = SecEdgarClient(user_agent="Tests tests@example.com")
    try:
        subs = {
            "filings": {
                "recent": {
                    "form": ["10-K", "10-K", "8-K", "10-Q"],
                    "accessionNumber": ["0001-11-000001", "0001-11-000002", "0001-11-000003", "0001-11-000004"],
                    "filingDate": ["2025-01-01", "2024-01-01", "2025-01-05", "2025-01-10"],
                    "primaryDocument": ["a.htm", "b.htm", "c.htm", "d.htm"],
                }
            }
        }
        client.get_company_submissions = lambda _cik: subs  # type: ignore[method-assign]

        filings = client.list_recent_filings(
            ticker="CAT",
            cik_10="0001234567",
            forms=["10-K", "8-K"],
            limit_per_form=1,
        )

        assert len(filings) == 2
        assert filings[0].form == "10-K"
        assert filings[1].form == "8-K"
        assert filings[0].filing_dir_url.endswith("/1234567/000111000001")
    finally:
        client.close()


def test_safe_filename_and_store_raw_filing(monkeypatch):
    from app.pipelines import sec_edgar
    monkeypatch.setattr(sec_edgar, "is_s3_configured", lambda: False)
    writes: dict[str, object] = {}

    def fake_mkdir(self, parents=False, exist_ok=False):
        writes["dir"] = self
        return None

    def fake_write_bytes(self, payload):
        writes["path"] = self
        writes["payload"] = payload
        return len(payload)

    monkeypatch.setattr(Path, "mkdir", fake_mkdir)
    monkeypatch.setattr(Path, "write_bytes", fake_write_bytes)

    filing = FilingRef(
        ticker="CAT",
        cik="0001234567",
        accession="0001-11-000001",
        form="10-K",
        filing_date="2025-01-01",
        primary_doc="my<>doc?.htm",
        filing_dir_url="https://example.com",
    )

    out_path = store_raw_filing(Path("sandbox-root"), filing, b"hello")
    assert writes["payload"] == b"hello"
    assert writes["path"] == out_path
    assert "<" not in out_path.name
    assert "?" not in out_path.name
    assert safe_filename("a:b/c") == "a_b_c"
