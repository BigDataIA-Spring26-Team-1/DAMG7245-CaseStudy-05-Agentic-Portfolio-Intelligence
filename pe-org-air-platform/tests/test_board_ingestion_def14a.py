from __future__ import annotations

from scripts.run_scoring_engine import _load_latest_def14a_proxy_text


class _FakeCursor:
    def __init__(self, fetchone_values=None, fetchall_values=None):
        self._fetchone_values = list(fetchone_values or [])
        self._fetchall_values = list(fetchall_values or [])

    def execute(self, query, params):
        return None

    def fetchone(self):
        if self._fetchone_values:
            return self._fetchone_values.pop(0)
        return None

    def fetchall(self):
        if self._fetchall_values:
            return self._fetchall_values.pop(0)
        return []


def test_load_latest_def14a_proxy_text_empty_when_no_document():
    cur = _FakeCursor(fetchone_values=[None])
    out = _load_latest_def14a_proxy_text(cur, company_id="cid-1")
    assert out == ""


def test_load_latest_def14a_proxy_text_joins_chunks_in_order():
    cur = _FakeCursor(
        fetchone_values=[("doc-1",)],
        fetchall_values=[
            [("Board Technology Committee oversees AI strategy",), ("Risk management and cybersecurity",)],
        ],
    )
    out = _load_latest_def14a_proxy_text(cur, company_id="cid-1")
    assert "Technology Committee" in out
    assert "Risk management" in out

