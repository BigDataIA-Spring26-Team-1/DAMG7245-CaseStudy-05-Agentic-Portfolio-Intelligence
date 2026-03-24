from __future__ import annotations

from app.pipelines import external_signals


class _FakeResponse:
    def __init__(self, *, text: str = "", json_data=None):
        self.text = text
        self._json_data = json_data

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._json_data


def test_sha256_text_is_deterministic():
    assert external_signals.sha256_text("abc") == external_signals.sha256_text("abc")
    assert external_signals.sha256_text("abc") != external_signals.sha256_text("abcd")


def test_safe_dt_parses_supported_formats():
    rfc_dt = external_signals._safe_dt("Wed, 01 Jan 2025 10:00:00 GMT")
    iso_dt = external_signals._safe_dt("2025-01-01T10:00:00Z")
    bad_dt = external_signals._safe_dt("not-a-date")

    assert rfc_dt is not None
    assert iso_dt is not None
    assert bad_dt is None


def test_tech_stack_collector_extracts_keywords():
    collector = external_signals.TechStackCollector()
    counts = collector.extract("We use Snowflake and OpenAI. Snowflake powers analytics.")

    assert counts.get("snowflake", 0) >= 2
    assert counts.get("openai", 0) >= 1


def test_score_tech_stack_rewards_diversity():
    assert external_signals.score_tech_stack({}) == 0.0
    assert external_signals.score_tech_stack({"a": 2, "b": 1}) == 20.0


def test_google_news_rss_builds_expected_url_and_returns_text():
    collector = external_signals.ExternalSignalCollector(user_agent="Tests tests@example.com")
    seen: dict[str, str] = {}
    try:
        def _fake_get(url: str):
            seen["url"] = url
            return _FakeResponse(text="<rss>news</rss>")

        collector.client.get = _fake_get  # type: ignore[method-assign]
        url, rss = collector.google_news_rss("Acme Corp")

        assert url == seen["url"]
        assert "news.google.com/rss/search?q=Acme+Corp" in url
        assert rss == "<rss>news</rss>"
    finally:
        collector.close()


def test_greenhouse_jobs_maps_payload_shape():
    payload = {
        "jobs": [
            {
                "title": "ML Engineer",
                "absolute_url": "https://example.com/job",
                "updated_at": "2025-01-01T00:00:00Z",
                "location": {"name": "Boston"},
                "departments": [{"name": "AI"}],
            }
        ]
    }

    collector = external_signals.ExternalSignalCollector(user_agent="Tests tests@example.com")
    try:
        collector.client.get = lambda _url: _FakeResponse(json_data=payload)  # type: ignore[method-assign]
        jobs = collector.greenhouse_jobs("acme")
        assert len(jobs) == 1
        assert jobs[0]["title"] == "ML Engineer"
        assert jobs[0]["department"] == "AI"
    finally:
        collector.close()
