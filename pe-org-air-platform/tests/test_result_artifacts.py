from __future__ import annotations

import json
import shutil
from pathlib import Path
from uuid import uuid4

from app.config import ROOT_DIR
from app.services import result_artifacts


def _local_tmp_dir() -> Path:
    base = ROOT_DIR / ".tmp_result_artifacts"
    base.mkdir(parents=True, exist_ok=True)
    target = base / str(uuid4())
    target.mkdir(parents=True, exist_ok=True)
    return target


def test_write_json_artifact_writes_local_copy_for_portfolio_ticker(monkeypatch):
    tmp_dir = _local_tmp_dir()
    try:
        monkeypatch.setattr(result_artifacts, "results_root", lambda: tmp_dir)
        monkeypatch.setattr(result_artifacts, "should_upload_result_artifacts_to_s3", lambda: False)

        out = result_artifacts.write_json_artifact(
            ticker="NVDA",
            category="scoring",
            filename="latest.json",
            payload={"score": 91.2},
        )

        assert out.local_path is not None
        assert out.s3_uri is None

        path = tmp_dir / "NVDA" / "scoring" / "latest.json"
        assert path.exists()
        assert json.loads(path.read_text(encoding="utf-8"))["score"] == 91.2
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_write_json_artifact_skips_local_copy_for_non_portfolio_ticker(monkeypatch):
    tmp_dir = _local_tmp_dir()
    try:
        monkeypatch.setattr(result_artifacts, "results_root", lambda: tmp_dir)
        monkeypatch.setattr(result_artifacts, "should_upload_result_artifacts_to_s3", lambda: False)

        out = result_artifacts.write_json_artifact(
            ticker="CAT",
            category="signals",
            filename="latest.json",
            payload={"count": 4},
        )

        assert out.local_path is None
        assert not (tmp_dir / "CAT").exists()
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_write_json_artifact_writes_portfolio_summary_locally(monkeypatch):
    tmp_dir = _local_tmp_dir()
    try:
        monkeypatch.setattr(result_artifacts, "results_root", lambda: tmp_dir)
        monkeypatch.setattr(result_artifacts, "should_upload_result_artifacts_to_s3", lambda: False)

        out = result_artifacts.write_json_artifact(
            ticker="portfolio",
            category="validation",
            filename="latest.json",
            payload={"ok": True},
        )

        assert out.local_path is not None
        assert (tmp_dir / "PORTFOLIO" / "validation" / "latest.json").exists()
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_write_text_artifact_uploads_to_s3_when_enabled(monkeypatch):
    seen = {}
    tmp_dir = _local_tmp_dir()
    try:
        monkeypatch.setattr(result_artifacts, "results_root", lambda: tmp_dir)
        monkeypatch.setattr(result_artifacts, "should_upload_result_artifacts_to_s3", lambda: True)

        def _upload_bytes(content, key, content_type):
            seen["call"] = {
                "content": content.decode("utf-8"),
                "key": key,
                "content_type": content_type,
            }
            return "s3://bucket/test"

        monkeypatch.setattr(result_artifacts, "upload_bytes", _upload_bytes)
        monkeypatch.setattr(result_artifacts.settings, "results_s3_prefix", "results")

        out = result_artifacts.write_text_artifact(
            ticker="JPM",
            category="signals",
            filename="news.txt",
            text="hello world",
        )

        assert out.local_path is not None
        assert out.s3_uri == "s3://bucket/test"
        assert seen["call"]["key"] == "results/JPM/signals/news.txt"
        assert seen["call"]["content"] == "hello world"
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
