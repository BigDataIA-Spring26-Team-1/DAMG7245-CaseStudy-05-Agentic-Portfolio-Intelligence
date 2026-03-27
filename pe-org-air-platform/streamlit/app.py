
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import importlib.util
import importlib
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

# Load shared UI helpers directly from the backend app directory. The Streamlit
# entrypoint is also named `app.py`, so importing `app.*` can collide with the
# script module name in Streamlit Cloud.
STREAMLIT_DIR = Path(__file__).resolve().parent
APP_ROOT_DIR = STREAMLIT_DIR.parents[0]
if str(APP_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(APP_ROOT_DIR))


def _ensure_backend_app_package() -> None:
    backend_package_dir = APP_ROOT_DIR / "app"
    current_app_module = sys.modules.get("app")
    if current_app_module is not None:
        current_paths = getattr(current_app_module, "__path__", None)
        if current_paths and str(backend_package_dir) in {str(Path(path)) for path in current_paths}:
            return

        current_file = getattr(current_app_module, "__file__", None)
        if current_file:
            try:
                if Path(current_file).resolve() == Path(__file__).resolve():
                    sys.modules["orgair_streamlit_entrypoint"] = current_app_module
            except OSError:
                pass

        sys.modules.pop("app", None)

    backend_app_spec = importlib.util.spec_from_file_location(
        "app",
        backend_package_dir / "__init__.py",
        submodule_search_locations=[str(backend_package_dir)],
    )
    if backend_app_spec is None or backend_app_spec.loader is None:
        raise ImportError(f"Unable to load backend app package from {backend_package_dir}")

    backend_app_module = importlib.util.module_from_spec(backend_app_spec)
    sys.modules["app"] = backend_app_module
    backend_app_spec.loader.exec_module(backend_app_module)


_ensure_backend_app_package()

ui_presenters = importlib.import_module("app.ui_presenters")
dashboard_view = importlib.import_module("app.dashboard.view")

compact_recommendation = ui_presenters.compact_recommendation
display_evidence_count = ui_presenters.display_evidence_count
extract_orgair_score = ui_presenters.extract_orgair_score
humanize_source_type = ui_presenters.humanize_source_type
sanitize_generated_summary = ui_presenters.sanitize_generated_summary

# ============================================================
# Config
# ============================================================

DEFAULT_API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")
DEFAULT_API_PREFIX = os.getenv("API_PREFIX", "/api/v1")
DEFAULT_SCORING_PREFIX = os.getenv("SCORING_PREFIX", "/api/v1/scoring")
DEFAULT_CLOUD_RUN_API_BASE = os.getenv(
    "CLOUD_RUN_API_BASE",
    "https://org-air-api-334893558229.us-central1.run.app",
)

ASSESSMENT_TYPES = ["screening", "due_diligence", "quarterly", "exit_prep"]
ASSESSMENT_STATUSES = ["draft", "in_progress", "submitted", "approved", "superseded"]
DIMENSIONS = [
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent_skills",
    "leadership_vision",
    "use_case_portfolio",
    "culture_change",
]

ROOT_DIR = APP_ROOT_DIR
SCRIPTS_DIR = ROOT_DIR / "scripts"
RESULTS_DIR = ROOT_DIR / "results"
PORTFOLIO_TICKERS = ["NVDA", "JPM", "WMT", "GE", "DG"]
RESULT_CATEGORIES = [
    "cs4",
    "evidence",
    "signals",
    "signal_scores",
    "signal_summaries",
    "scoring",
    "retrieval",
    "validation",
]

CATEGORY_LABELS = {
    "cs4": "Diligence Narrative",
    "evidence": "Evidence Lake",
    "signals": "Signal Capture",
    "signal_scores": "Signal Scoring",
    "signal_summaries": "Signal Summaries",
    "scoring": "OrgAIR Scoring",
    "retrieval": "Retrieval QA",
    "validation": "Portfolio Validation",
}

PIPELINE_BLUEPRINT = [
    {
        "title": "Evidence Ingestion",
        "description": "SEC evidence collection, parsing, chunking, and local artifact mirroring.",
        "categories": ["evidence"],
        "scope": "ticker",
        "accent": "#2f7cf6",
    },
    {
        "title": "Signal Harvesting",
        "description": "Jobs, patents, tech stack, and external signal capture across the portfolio.",
        "categories": ["signals"],
        "scope": "ticker",
        "accent": "#28a59c",
    },
    {
        "title": "Signal Scoring",
        "description": "Normalized company signal scores and summary rollups for each name.",
        "categories": ["signal_scores", "signal_summaries"],
        "scope": "ticker",
        "accent": "#6e86ff",
    },
    {
        "title": "OrgAIR Scoring",
        "description": "Dimension scoring, confidence, and composite readiness outputs for each company.",
        "categories": ["scoring"],
        "scope": "ticker",
        "accent": "#c8a977",
    },
    {
        "title": "Portfolio Validation",
        "description": "Portfolio-level validation checks and scoring envelope review.",
        "categories": ["validation"],
        "scope": "portfolio",
        "accent": "#3ecf8e",
    },
    {
        "title": "Diligence Narrative",
        "description": "Generated justifications, diligence narrative, and investment packet artifacts.",
        "categories": ["cs4"],
        "scope": "ticker",
        "accent": "#f08a5d",
    },
]

REPORT_BLUEPRINT = [
    {
        "title": "Investment Committee Memo",
        "description": "Company-level memo export in Markdown and Word format.",
        "type": "document_prefix",
        "value": "ic_memo_",
        "accent": "#c8a977",
    },
    {
        "title": "LP Update Letter",
        "description": "Portfolio-level investor update with Fund-AI-R and ROI context.",
        "type": "document_prefix",
        "value": "lp_letter_",
        "accent": "#2f7cf6",
    },
    {
        "title": "Semantic Memory",
        "description": "Reusable diligence memory captured across companies and funds.",
        "type": "json_records",
        "value": "mem0_memory.json",
        "accent": "#28a59c",
    },
    {
        "title": "Value Creation Tracker",
        "description": "Active program tracking with ROI, MOIC, and value realization context.",
        "type": "json_records",
        "value": "investment_tracker.json",
        "accent": "#3ecf8e",
    },
]


# ============================================================
# HTTP helpers
# ============================================================


def _join_url(base: str, path: str) -> str:
    return base.rstrip("/") + "/" + path.lstrip("/")


def _api_url(base: str, prefix: str, path: str, include_prefix: bool = True) -> str:
    if include_prefix:
        return _join_url(base, _join_url(prefix, path))
    return _join_url(base, path)


def _scoring_url(base: str, scoring_prefix: str, path: str) -> str:
    return _join_url(base, _join_url(scoring_prefix, path))


def _request(method: str, url: str, **kwargs: Any) -> requests.Response:
    timeout = kwargs.pop("timeout", 15)
    return requests.request(method, url, timeout=timeout, **kwargs)


def _request_json(method: str, url: str, **kwargs: Any) -> Any:
    resp = _request(method, url, **kwargs)
    if not resp.ok:
        raise requests.HTTPError(resp.text, response=resp)
    if resp.status_code == 204 or not resp.text.strip():
        return None
    return resp.json()


def _show_http_error(exc: requests.HTTPError) -> None:
    resp = exc.response
    if resp is None:
        st.error(f"Request failed: {exc}")
        return

    st.error(f"Request failed: {resp.status_code}")
    if not resp.text:
        return

    try:
        st.json(resp.json())
    except ValueError:
        st.code(resp.text)


def _show_payload(payload: Any) -> None:
    if payload is None:
        st.info("No content")
        return

    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            meta = {k: v for k, v in payload.items() if k != "items"}
            if meta:
                st.json(meta)
            st.dataframe(items, use_container_width=True)
            return
        st.json(payload)
        return

    if isinstance(payload, list):
        if payload and all(isinstance(x, dict) for x in payload):
            st.dataframe(payload, use_container_width=True)
        else:
            st.json(payload)
        return

    st.write(payload)


def _parse_json_input(label: str, text: str, allow_empty: bool = True) -> tuple[bool, Any]:
    raw = text.strip()
    if not raw and allow_empty:
        return True, {}
    try:
        return True, json.loads(raw)
    except json.JSONDecodeError as exc:
        st.error(f"{label} has invalid JSON: {exc}")
        return False, None


def _build_headers(bearer_token: str, extra_headers_text: str) -> tuple[dict[str, str], str | None]:
    headers: dict[str, str] = {}

    token = bearer_token.strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    raw = extra_headers_text.strip()
    if not raw:
        return headers, None

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        return headers, f"Extra headers JSON is invalid: {exc}"

    if not isinstance(parsed, dict):
        return headers, "Extra headers JSON must be an object"

    for key, value in parsed.items():
        headers[str(key)] = str(value)

    return headers, None


def _list_repo_scripts() -> list[str]:
    if not SCRIPTS_DIR.exists():
        return []
    return sorted([p.name for p in SCRIPTS_DIR.glob("*.py")])


def _inject_ui_theme() -> None:
    st.markdown(
        """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=Outfit:wght@500;600;700;800&display=swap');

:root {
    --ui-bg: #06111c;
    --ui-bg-soft: #0c1a29;
    --ui-panel: rgba(12, 23, 37, 0.9);
    --ui-panel-strong: rgba(10, 18, 30, 0.94);
    --ui-border: rgba(96, 122, 152, 0.32);
    --ui-border-strong: rgba(200, 169, 119, 0.28);
    --ui-ink: #f5f7fb;
    --ui-muted: #93a8bf;
    --ui-blue: #2f7cf6;
    --ui-teal: #28a59c;
    --ui-gold: #c8a977;
    --ui-gold-soft: #f2dfbc;
    --ui-emerald: #3ecf8e;
    --ui-coral: #f08a5d;
}

html, body, [class*="css"] {
    font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
    color: var(--ui-ink);
}

h1, h2, h3, h4, h5, h6 {
    font-family: "Outfit", "IBM Plex Sans", sans-serif !important;
    letter-spacing: -0.02em;
}

.stApp {
    background:
        radial-gradient(1100px 520px at 8% -10%, rgba(47, 124, 246, 0.16) 0%, transparent 58%),
        radial-gradient(900px 480px at 94% -8%, rgba(200, 169, 119, 0.14) 0%, transparent 56%),
        linear-gradient(180deg, #040b13 0%, #07111b 42%, #091520 100%);
}

[data-testid="stHeader"] {
    background: transparent;
}

[data-testid="stToolbar"] {
    visibility: hidden;
}

.block-container {
    padding-top: 1.1rem;
    padding-bottom: 3rem;
    max-width: 1420px;
}

section[data-testid="stSidebar"] {
    background:
        radial-gradient(600px 220px at 0% 0%, rgba(47, 124, 246, 0.14), transparent 56%),
        linear-gradient(180deg, #081320 0%, #0b1624 100%);
    border-right: 1px solid rgba(96, 122, 152, 0.22);
}

section[data-testid="stSidebar"] * {
    color: #e7eef7 !important;
}

[data-testid="stMetric"] {
    background: linear-gradient(180deg, rgba(13, 25, 39, 0.96), rgba(9, 17, 28, 0.96));
    border: 1px solid var(--ui-border);
    border-radius: 18px;
    padding: 0.4rem 0.55rem;
    box-shadow: 0 16px 34px rgba(1, 6, 12, 0.34);
}

[data-testid="stMetricLabel"] {
    font-size: 0.78rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

div[data-baseweb="tab-list"] {
    gap: 0.45rem;
    padding: 0.35rem;
    border-radius: 16px;
    border: 1px solid var(--ui-border);
    background: rgba(9, 18, 31, 0.88);
    backdrop-filter: blur(10px);
}

button[data-baseweb="tab"] {
    border-radius: 12px !important;
    padding: 0.48rem 0.95rem !important;
    font-family: "Outfit", "IBM Plex Sans", sans-serif !important;
    font-weight: 700 !important;
    color: #94abc3 !important;
    transition: transform 0.18s ease, color 0.18s ease, box-shadow 0.2s ease;
}

button[data-baseweb="tab"]:hover {
    transform: translateY(-1px);
    color: #eef4fb !important;
}

button[data-baseweb="tab"][aria-selected="true"] {
    color: #ffffff !important;
    background: linear-gradient(90deg, rgba(47, 124, 246, 0.92), rgba(40, 165, 156, 0.88)) !important;
    box-shadow: 0 10px 26px rgba(24, 84, 172, 0.36);
}

div.stButton > button,
div.stDownloadButton > button,
div[data-testid="stForm"] button[kind] {
    border: 0;
    border-radius: 14px;
    font-family: "Outfit", "IBM Plex Sans", sans-serif;
    font-weight: 700;
    color: #ffffff;
    background: linear-gradient(135deg, rgba(47, 124, 246, 1) 0%, rgba(30, 103, 215, 1) 54%, rgba(40, 165, 156, 1) 100%);
    box-shadow: 0 14px 28px rgba(12, 47, 99, 0.34);
    transition: transform 0.18s ease, box-shadow 0.18s ease;
}

div.stButton > button:hover,
div.stDownloadButton > button:hover,
div[data-testid="stForm"] button[kind]:hover {
    transform: translateY(-2px);
    box-shadow: 0 16px 34px rgba(12, 47, 99, 0.4);
}

div.stTextInput > div > div > input,
div.stNumberInput > div > div > input,
div.stTextArea textarea {
    border-radius: 14px !important;
    border: 1px solid var(--ui-border) !important;
    background: rgba(9, 18, 31, 0.95) !important;
    color: var(--ui-ink) !important;
}

div[data-testid="stDataFrame"],
div[data-testid="stTable"],
div[data-testid="stCodeBlock"] pre,
div.streamlit-expanderContent {
    border-radius: 18px;
    border: 1px solid var(--ui-border);
    background: rgba(8, 16, 27, 0.9) !important;
    box-shadow: 0 14px 28px rgba(1, 6, 12, 0.28);
}

.top-ribbon {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    margin-bottom: 0.8rem;
    padding: 0.65rem 0.95rem;
    border: 1px solid rgba(96, 122, 152, 0.2);
    border-radius: 999px;
    background: rgba(9, 18, 31, 0.72);
}

.top-ribbon-copy {
    color: var(--ui-muted);
    font-size: 0.84rem;
}

.hero-shell {
    position: relative;
    overflow: hidden;
    margin-bottom: 1rem;
    border-radius: 30px;
    border: 1px solid rgba(96, 122, 152, 0.22);
    background:
        radial-gradient(720px 280px at 0% 0%, rgba(47, 124, 246, 0.18) 0%, transparent 62%),
        radial-gradient(760px 320px at 100% 0%, rgba(200, 169, 119, 0.14) 0%, transparent 66%),
        linear-gradient(145deg, rgba(8, 16, 27, 0.98) 0%, rgba(11, 23, 37, 0.96) 52%, rgba(7, 15, 25, 0.98) 100%);
    box-shadow: 0 28px 60px rgba(0, 0, 0, 0.34);
}

.hero-shell::before {
    content: "";
    position: absolute;
    inset: 0;
    background:
        linear-gradient(125deg, transparent 0%, rgba(255, 255, 255, 0.03) 34%, transparent 68%),
        repeating-linear-gradient(90deg, rgba(255, 255, 255, 0.02) 0, rgba(255, 255, 255, 0.02) 1px, transparent 1px, transparent 130px);
    pointer-events: none;
}

.hero-grid {
    position: relative;
    display: grid;
    grid-template-columns: 1.32fr 0.92fr;
    gap: 1rem;
    padding: 1.5rem;
}

.hero-kicker,
.section-label,
.insight-kicker {
    color: var(--ui-gold-soft);
    text-transform: uppercase;
    letter-spacing: 0.12em;
    font-size: 0.74rem;
    font-weight: 700;
}

.hero-title {
    margin: 0.45rem 0 0;
    max-width: 12ch;
    font-size: 3rem;
    line-height: 0.95;
    font-weight: 800;
}

.hero-copy {
    margin: 0.95rem 0 0;
    max-width: 46rem;
    color: #c7d4e2;
    font-size: 1.02rem;
    line-height: 1.68;
}

.hero-chip-row,
.micro-pill-row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.55rem;
    margin-top: 1rem;
}

.hero-chip,
.micro-pill {
    padding: 0.42rem 0.74rem;
    border-radius: 999px;
    border: 1px solid rgba(200, 169, 119, 0.18);
    background: rgba(255, 255, 255, 0.04);
    color: #ecf2f8;
    font-size: 0.8rem;
    font-weight: 600;
}

.hero-side {
    display: grid;
    gap: 0.9rem;
}

.hero-panel,
.insight-card,
.nav-card,
.pipeline-card,
.report-card,
.dashboard-band {
    border-radius: 22px;
    border: 1px solid var(--ui-border);
    background: linear-gradient(180deg, rgba(12, 23, 37, 0.9), rgba(8, 16, 27, 0.92));
    box-shadow: 0 18px 36px rgba(1, 6, 12, 0.24);
}

.hero-panel {
    padding: 1.15rem;
}

.hero-panel-title {
    margin: 0;
    font-size: 1rem;
    font-weight: 700;
}

.hero-stat-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.8rem;
    margin-top: 0.95rem;
}

.hero-stat {
    padding: 0.95rem;
    border-radius: 18px;
    border: 1px solid rgba(96, 122, 152, 0.18);
    background: rgba(255, 255, 255, 0.03);
}

.hero-stat-label,
.card-meta-label {
    color: var(--ui-muted);
    font-size: 0.76rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

.hero-stat-value {
    margin-top: 0.4rem;
    font-family: "Outfit", "IBM Plex Sans", sans-serif;
    font-size: 1.45rem;
    font-weight: 700;
}

.hero-note {
    color: #b6c6d6;
    font-size: 0.9rem;
    line-height: 1.55;
}

.section-label {
    margin-bottom: 0.55rem;
}

.insight-card,
.nav-card,
.pipeline-card,
.report-card,
.dashboard-band {
    padding: 1rem 1.05rem;
}

.insight-title,
.nav-card-title,
.pipeline-card-title,
.report-card-title,
.dashboard-band-title {
    margin: 0.35rem 0 0.42rem;
    font-size: 1.05rem;
    font-weight: 700;
}

.insight-copy,
.nav-card-copy,
.pipeline-card-copy,
.report-card-copy,
.dashboard-band-copy {
    margin: 0;
    color: #b8c9da;
    line-height: 1.56;
}

.nav-card-grid,
.pipeline-grid,
.report-grid {
    display: grid;
    gap: 0.9rem;
}

.nav-card-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
    margin-bottom: 1rem;
}

.pipeline-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
}

.report-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
}

.pipeline-status,
.report-status {
    display: inline-flex;
    align-items: center;
    gap: 0.45rem;
    margin-top: 0.9rem;
    padding: 0.32rem 0.62rem;
    border-radius: 999px;
    font-size: 0.76rem;
    font-weight: 700;
    color: #ecf4fb;
    background: rgba(255, 255, 255, 0.05);
}

.pipeline-meta,
.report-meta {
    margin-top: 0.7rem;
    color: var(--ui-muted);
    font-size: 0.84rem;
    line-height: 1.5;
}

.section-callout {
    padding: 0.95rem 1.05rem;
    border-radius: 18px;
    border: 1px solid rgba(200, 169, 119, 0.22);
    background: linear-gradient(90deg, rgba(200, 169, 119, 0.1), rgba(255, 255, 255, 0.02));
    color: #d7e1ea;
}

.masthead {
    margin-bottom: 0.95rem;
}

.masthead-title {
    margin: 0;
    font-size: 0.95rem;
    color: #dbe7f5;
}

.masthead-copy {
    margin: 0.22rem 0 0;
    color: var(--ui-muted);
    font-size: 0.88rem;
}

@media (max-width: 1080px) {
    .hero-grid,
    .nav-card-grid,
    .pipeline-grid,
    .report-grid {
        grid-template-columns: 1fr;
    }

    .hero-title {
        max-width: none;
        font-size: 2.4rem;
    }
}

div.stTextInput > div > div > input,
div.stNumberInput > div > div > input,
div.stTextArea textarea {
    border-radius: 14px !important;
    border: 1px solid var(--ui-border) !important;
    background-color: rgba(9, 18, 31, 0.92) !important;
    color: var(--ui-ink) !important;
}

div.stTextInput > div > div > input::placeholder,
div.stNumberInput > div > div > input::placeholder,
div.stTextArea textarea::placeholder {
    color: var(--ui-muted) !important;
}

label, p, span, .stMarkdown, [data-testid="stMarkdownContainer"], .stCaption {
    color: var(--ui-ink) !important;
}

div[data-testid="stAlert"] {
    border-radius: 16px;
}

div[data-testid="stCodeBlock"] pre {
    border: 1px solid var(--ui-border);
    border-radius: 18px;
    background: rgba(7, 14, 24, 0.95) !important;
}

@keyframes click-pop {
    0% { transform: scale(1); }
    40% { transform: scale(0.93); }
    100% { transform: scale(1); }
}

@keyframes click-flash {
    0% { filter: brightness(1); }
    25% { filter: brightness(1.3); }
    100% { filter: brightness(1); }
}

@keyframes click-ripple {
    0% { opacity: 0.55; transform: scale(0.35); }
    100% { opacity: 0; transform: scale(1.75); }
}

@keyframes section-fade {
    from { opacity: 0; transform: translateY(6px); }
    to { opacity: 1; transform: translateY(0); }
}

[data-testid="stVerticalBlock"] > div {
    animation: section-fade 0.28s ease-out both;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_scoring_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict) and "composite_score" in item]
    if isinstance(payload, dict):
        if "composite_score" in payload:
            return [payload]
        items = payload.get("items")
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict) and "composite_score" in item]
    return []


def _vega_common_config() -> dict[str, Any]:
    return {
        "background": "transparent",
        "config": {
            "view": {"stroke": None},
            "axis": {
                "labelColor": "#dce8f8",
                "titleColor": "#dce8f8",
                "gridColor": "rgba(158, 179, 204, 0.22)",
                "domainColor": "rgba(158, 179, 204, 0.35)",
            },
            "legend": {
                "labelColor": "#dce8f8",
                "titleColor": "#dce8f8",
                "orient": "top",
            },
        },
    }


def _result_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _safe_child_dirs(path: Path) -> list[Path]:
    try:
        return sorted([child for child in path.iterdir() if child.is_dir()])
    except OSError:
        return []


def _safe_collect_files(path: Path) -> list[Path]:
    if not path.exists():
        return []

    files: list[Path] = []
    stack = [path]
    while stack:
        current = stack.pop()
        try:
            children = list(current.iterdir())
        except OSError:
            continue

        for child in children:
            try:
                if child.is_dir():
                    stack.append(child)
                elif child.is_file():
                    files.append(child)
            except OSError:
                continue
    return files


def _latest_timestamp(paths: list[Path]) -> float | None:
    timestamps: list[float] = []
    for path in paths:
        try:
            timestamps.append(path.stat().st_mtime)
        except OSError:
            continue
    return max(timestamps) if timestamps else None


def _format_timestamp(timestamp: float | None) -> str:
    if timestamp is None:
        return "Awaiting output"
    return datetime.fromtimestamp(timestamp).strftime("%b %d, %Y %I:%M %p")


def _friendly_category_name(category: str) -> str:
    return CATEGORY_LABELS.get(category, category.replace("_", " ").title())


def _read_json_records(path: Path) -> list[dict[str, Any]]:
    payload = _result_json(path)
    return payload if isinstance(payload, list) else []


def _collect_pipeline_cards() -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    ticker_targets = len(PORTFOLIO_TICKERS)

    for stage in PIPELINE_BLUEPRINT:
        matching_files: list[Path] = []
        covered_entities: set[str] = set()

        if stage["scope"] == "portfolio":
            for category in stage["categories"]:
                category_files = _safe_collect_files(RESULTS_DIR / "PORTFOLIO" / category)
                if category_files:
                    covered_entities.add("PORTFOLIO")
                    matching_files.extend(category_files)
            target_total = 1
            coverage_text = "Portfolio-level workflow"
        else:
            for ticker in PORTFOLIO_TICKERS:
                ticker_has_output = False
                for category in stage["categories"]:
                    category_files = _safe_collect_files(RESULTS_DIR / ticker / category)
                    if category_files:
                        ticker_has_output = True
                        matching_files.extend(category_files)
                if ticker_has_output:
                    covered_entities.add(ticker)
            target_total = ticker_targets
            coverage_text = f"{len(covered_entities)}/{target_total} portfolio companies"

        coverage = len(covered_entities)
        if coverage == 0:
            status = "Awaiting run"
        elif coverage == target_total:
            status = "Ready"
        else:
            status = "Partial"

        cards.append(
            {
                "title": stage["title"],
                "description": stage["description"],
                "accent": stage["accent"],
                "status": status,
                "coverage_text": coverage_text,
                "artifact_count": len(matching_files),
                "updated_at": _format_timestamp(_latest_timestamp(matching_files)),
            }
        )
    return cards


def _collect_report_cards() -> list[dict[str, Any]]:
    bonus_root = RESULTS_DIR / "bonus"
    documents_root = bonus_root / "documents"
    document_files = _safe_collect_files(documents_root)

    cards: list[dict[str, Any]] = []
    for report in REPORT_BLUEPRINT:
        if report["type"] == "document_prefix":
            matching_files = [path for path in document_files if path.name.startswith(str(report["value"]))]
            count = len(matching_files)
            updated_at = _format_timestamp(_latest_timestamp(matching_files))
            unit_label = "report artifacts"
        else:
            records = _read_json_records(bonus_root / str(report["value"]))
            count = len(records)
            updated_at = _format_timestamp(_latest_timestamp([bonus_root / str(report["value"])]))
            unit_label = "tracked records"

        cards.append(
            {
                "title": report["title"],
                "description": report["description"],
                "accent": report["accent"],
                "count": count,
                "unit_label": unit_label,
                "updated_at": updated_at,
                "status": "Live" if count else "Awaiting activity",
            }
        )
    return cards


def _collect_recent_artifacts(limit: int = 8) -> list[dict[str, str]]:
    rows: list[tuple[float, dict[str, str]]] = []
    for path in _safe_collect_files(RESULTS_DIR):
        suffix = path.suffix.lower()
        if suffix not in {".json", ".md", ".docx", ".txt"}:
            continue

        try:
            relative = path.relative_to(RESULTS_DIR)
            parts = relative.parts
            entity = parts[0] if len(parts) > 0 else "results"
            category = parts[1] if len(parts) > 1 else "misc"
            timestamp = path.stat().st_mtime
            updated_at = datetime.fromtimestamp(timestamp).strftime("%b %d, %Y %I:%M %p")
        except (ValueError, OSError):
            continue

        rows.append(
            (
                timestamp,
                {
                "updated_at": updated_at,
                "entity": entity,
                "workflow": _friendly_category_name(category),
                "artifact": path.name,
                },
            )
        )

    rows.sort(key=lambda item: item[0], reverse=True)
    return [item[1] for item in rows[:limit]]


def _collect_result_summary() -> dict[str, Any]:
    tickers = [ticker for ticker in PORTFOLIO_TICKERS if (RESULTS_DIR / ticker).exists()]
    portfolio_dir = RESULTS_DIR / "PORTFOLIO"
    local_files = _safe_collect_files(RESULTS_DIR)
    local_file_count = len(local_files)
    bonus_document_files = _safe_collect_files(RESULTS_DIR / "bonus" / "documents")
    memory_records = _read_json_records(RESULTS_DIR / "bonus" / "mem0_memory.json")
    investment_records = _read_json_records(RESULTS_DIR / "bonus" / "investment_tracker.json")

    latest_scores: list[dict[str, Any]] = []
    for ticker in tickers:
        score_path = RESULTS_DIR / ticker / "scoring" / "latest_org_air_score.json"
        payload = _result_json(score_path)
        if isinstance(payload, dict):
            latest_scores.append(
                {
                    "ticker": ticker,
                    "score": extract_orgair_score(payload),
                    "score_band": str(payload.get("score_band", "unknown")),
                }
            )

    latest_scores.sort(key=lambda item: item["score"], reverse=True)

    validation_path = portfolio_dir / "validation" / "latest_portfolio_validation.json"
    validation_payload = _result_json(validation_path)
    validation_rows = validation_payload.get("results", []) if isinstance(validation_payload, dict) else []
    passing = sum(1 for item in validation_rows if isinstance(item, dict) and item.get("status") == "PASS")

    return {
        "tickers": tickers,
        "portfolio_exists": portfolio_dir.exists(),
        "local_file_count": local_file_count,
        "latest_scores": latest_scores,
        "validation_rows": validation_rows,
        "validation_pass_count": passing,
        "validation_total": len(validation_rows),
        "bonus_document_count": len([path for path in bonus_document_files if path.suffix.lower() in {".md", ".docx"}]),
        "memory_count": len(memory_records),
        "investment_count": len(investment_records),
        "latest_artifact_at": _format_timestamp(_latest_timestamp(local_files)),
    }


def _render_hero(summary: dict[str, Any], api_base: str, api_prefix: str, scoring_prefix: str) -> None:
    pipeline_cards = _collect_pipeline_cards()
    ready_pipelines = sum(1 for item in pipeline_cards if item["status"] == "Ready")
    report_cards = _collect_report_cards()
    live_report_items = sum(int(item["count"]) for item in report_cards)
    top_company = summary["latest_scores"][0] if summary["latest_scores"] else None
    top_label = (
        f"{top_company['ticker']} {top_company['score']:.2f}"
        if isinstance(top_company, dict)
        else "Awaiting scored portfolio"
    )
    pass_rate = (
        f"{summary['validation_pass_count']}/{summary['validation_total']}"
        if summary["validation_total"]
        else "No validation file yet"
    )
    st.markdown(
        f"""
        <div class="top-ribbon">
          <div class="top-ribbon-copy">Private equity analytics, diligence review, and report generation in one workspace.</div>
          <div class="top-ribbon-copy">Portfolio: {" | ".join(PORTFOLIO_TICKERS)} | Latest artifact: {summary["latest_artifact_at"]}</div>
        </div>
        <section class="hero-shell">
          <div class="hero-grid">
            <div>
              <div class="hero-kicker">Private Equity Operating Layer</div>
              <h1 class="hero-title">OrgAIR Portfolio Intelligence</h1>
              <p class="hero-copy">
                Evidence-led portfolio analytics for investment teams. Review score movement, inspect source-backed
                diligence narratives, track value creation programs, and generate committee-ready outputs without
                dropping into raw tooling unless you actually need it.
              </p>
              <div class="hero-chip-row">
                <span class="hero-chip">Portfolio analytics</span>
                <span class="hero-chip">Evidence traceability</span>
                <span class="hero-chip">Report automation</span>
                <span class="hero-chip">Value creation tracking</span>
              </div>
            </div>
            <div class="hero-side">
              <div class="hero-panel">
                <p class="hero-panel-title">Live platform snapshot</p>
                <div class="hero-stat-grid">
                  <div class="hero-stat">
                    <div class="hero-stat-label">Tracked Names</div>
                    <div class="hero-stat-value">{len(summary["tickers"])}</div>
                  </div>
                  <div class="hero-stat">
                    <div class="hero-stat-label">Ready Workflows</div>
                    <div class="hero-stat-value">{ready_pipelines}/{len(pipeline_cards)}</div>
                  </div>
                  <div class="hero-stat">
                    <div class="hero-stat-label">Strategic Artifacts</div>
                    <div class="hero-stat-value">{live_report_items}</div>
                  </div>
                  <div class="hero-stat">
                    <div class="hero-stat-label">Top Performer</div>
                    <div class="hero-stat-value">{top_label}</div>
                  </div>
                </div>
              </div>
              <div class="hero-panel">
                <div class="hero-kicker">Current Validation</div>
                <div class="hero-panel-title">{pass_rate}</div>
                <p class="hero-note">
                  Pipeline artifacts are mirrored locally for review. Portfolio validation, diligence narratives,
                  scoring outputs, and downloadable reports are surfaced below.
                </p>
              </div>
            </div>
          </div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def _render_overview(summary: dict[str, Any]) -> None:
    score_rows = summary["latest_scores"]
    top_score = score_rows[0]["score"] if score_rows else 0.0
    avg_score = (
        sum(item["score"] for item in score_rows) / len(score_rows)
        if score_rows else 0.0
    )
    weakest = score_rows[-1] if score_rows else None
    report_cards = _collect_report_cards()
    report_total = sum(int(item["count"]) for item in report_cards)
    recent_outputs = _collect_recent_artifacts(limit=6)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Portfolio Coverage", f"{len(summary['tickers'])}/{len(PORTFOLIO_TICKERS)}")
    m2.metric("Average OrgAIR", f"{avg_score:.2f}" if score_rows else "n/a")
    m3.metric("Top OrgAIR", f"{top_score:.2f}" if score_rows else "n/a")
    m4.metric("Strategic Artifacts", str(report_total))

    insight_cols = st.columns(3)
    with insight_cols[0]:
        st.markdown(
            """
            <div class="insight-card">
              <div class="insight-kicker">Executive Lens</div>
              <div class="insight-title">One portfolio surface, full workflow coverage</div>
              <p class="insight-copy">
                The primary experience now centers on score analytics, diligence review, and report generation.
                Platform diagnostics and raw API controls remain available in the console section when needed.
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with insight_cols[1]:
        validation = (
            f"{summary['validation_pass_count']} of {summary['validation_total']} checks passing"
            if summary["validation_total"]
            else "Portfolio validation has not been generated yet"
        )
        st.markdown(
            f"""
            <div class="insight-card">
              <div class="insight-kicker">Validation</div>
              <div class="insight-title">{validation}</div>
              <p class="insight-copy">
                Validation outputs are mirrored into the local results tree and surfaced alongside scoring
                and diligence artifacts for direct inspection.
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with insight_cols[2]:
        leader_text = ", ".join(item["ticker"] for item in score_rows[:3]) if score_rows else "Awaiting scores"
        weakest_text = (
            f"{weakest['ticker']} {weakest['score']:.2f}"
            if isinstance(weakest, dict)
            else "No laggard available"
        )
        st.markdown(
            f"""
            <div class="insight-card">
              <div class="insight-kicker">Leaders And Laggards</div>
              <div class="insight-title">{leader_text}</div>
              <p class="insight-copy">
                Current laggard: {weakest_text}. Use the company review surface to inspect dimension profile,
                supporting evidence, and diligence questions.
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    if score_rows:
        chart_col, activity_col = st.columns([1.3, 0.9])
        with chart_col:
            chart_df = pd.DataFrame(score_rows)
            fig = px.bar(
                chart_df.sort_values("score", ascending=True),
                x="score",
                y="ticker",
                orientation="h",
                color="score",
                color_continuous_scale=["#0f2438", "#2f7cf6", "#3ecf8e", "#c8a977"],
                labels={"score": "OrgAIR Score", "ticker": ""},
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=0, r=0, t=10, b=0),
                coloraxis_showscale=False,
                font=dict(color="#eaf1f7"),
            )
            fig.update_yaxes(showgrid=False)
            fig.update_xaxes(gridcolor="rgba(147, 168, 191, 0.18)")
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        with activity_col:
            st.markdown(
                f"""
                <div class="insight-card">
                  <div class="insight-kicker">Artifact Activity</div>
                  <div class="insight-title">{summary["local_file_count"]} local files mirrored</div>
                  <p class="insight-copy">
                    Latest artifact refresh: {summary["latest_artifact_at"]}. Recent activity is listed below so
                    you can move directly from high-level overview into the exact output that changed.
                  </p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if recent_outputs:
                st.dataframe(recent_outputs, use_container_width=True, hide_index=True)


def _render_results_explorer() -> None:
    st.caption("Inspect mirrored workflow outputs, diligence artifacts, and generated documents from the local results tree.")
    available_entities = [path.name for path in _safe_child_dirs(RESULTS_DIR)]
    if not available_entities:
        st.info("No local results folder found yet.")
        return

    entity = st.selectbox("Entity", sorted(available_entities), key="results_entity")
    entity_dir = RESULTS_DIR / entity
    available_categories = [path.name for path in _safe_child_dirs(entity_dir)]
    if not available_categories:
        st.info("No result categories found for this entity.")
        return

    preferred_order = [name for name in RESULT_CATEGORIES if name in available_categories]
    category = st.selectbox(
        "Workflow",
        preferred_order + [name for name in available_categories if name not in preferred_order],
        key="results_category",
        format_func=_friendly_category_name,
    )
    category_dir = entity_dir / category
    files = sorted(_safe_collect_files(category_dir))
    if not files:
        st.info("No files found in this category.")
        return

    selected_path = st.selectbox(
        "Artifact",
        files,
        key="results_file",
        format_func=lambda path: str(path.relative_to(entity_dir)),
    )
    preview_col, meta_col = st.columns([1.4, 0.8])
    with meta_col:
        st.markdown(
            f"""
            <div class="insight-card">
              <div class="insight-kicker">{_friendly_category_name(category)}</div>
              <div class="insight-title">{selected_path.name}</div>
              <p class="insight-copy">
                Entity: {entity}<br/>
                Size: {selected_path.stat().st_size:,} bytes<br/>
                Modified: {datetime.fromtimestamp(selected_path.stat().st_mtime).strftime("%b %d, %Y %I:%M %p")}
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.download_button(
            "Download Artifact",
            data=selected_path.read_bytes(),
            file_name=selected_path.name,
            mime="application/octet-stream",
            use_container_width=True,
        )

    with preview_col:
        suffix = selected_path.suffix.lower()
        if suffix == ".json":
            payload = _result_json(selected_path)
            if payload is None:
                st.code(selected_path.read_text(encoding="utf-8", errors="replace"))
            else:
                _show_payload(payload)
        else:
            text = selected_path.read_text(encoding="utf-8", errors="replace")
            st.code(text[:12000] if len(text) > 12000 else text)


def _load_company_artifacts(ticker: str) -> dict[str, Any]:
    ticker_dir = RESULTS_DIR / ticker
    return {
        "scoring": _result_json(ticker_dir / "scoring" / "latest_org_air_score.json"),
        "cs4": _result_json(ticker_dir / "cs4" / "complete_pipeline_latest.json"),
        "signal_summary": _result_json(ticker_dir / "signal_summaries" / "latest_company_signal_summary.json"),
        "signal_scores": _result_json(ticker_dir / "signal_scores" / "latest_signal_scores.json"),
    }


def _portfolio_company_lookup(summary: dict[str, Any]) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    for ticker in summary.get("tickers", []):
        cs4_payload = _load_company_artifacts(ticker).get("cs4") or {}
        company = cs4_payload.get("company", {}) if isinstance(cs4_payload, dict) else {}
        lookup[ticker] = {
            "company_id": str(company.get("company_id", "")).strip(),
            "company_name": str(company.get("name", ticker)).strip() or ticker,
        }
    return lookup


def _search_companies(
    api_base: str,
    api_prefix: str,
    timeout: int,
    headers: dict[str, str],
    verify_tls: bool,
    query: str,
    limit: int = 20,
) -> list[dict[str, str]]:
    try:
        url = _api_url(api_base, api_prefix, "/companies")
        payload = _request_json(
            "GET",
            url,
            params={"page": 1, "page_size": int(limit), "q": query},
            timeout=timeout,
            headers=headers,
            verify=verify_tls,
        )
    except Exception:
        return []

    items = payload.get("items", []) if isinstance(payload, dict) else []
    out: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "id": str(item.get("id", "")).strip(),
                "ticker": str(item.get("ticker", "")).strip().upper(),
                "name": str(item.get("name", "")).strip(),
            }
        )
    return out


def _render_source_evidence(item: dict[str, Any]) -> None:
    source_type = humanize_source_type(item.get("source_type"))
    source_url = str(item.get("source_url") or "").strip()
    title = str(item.get("title") or source_type)
    confidence = _to_float(item.get("confidence"))
    excerpt = str(item.get("text") or item.get("content") or "").strip()
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}

    st.markdown(
        f"""
        <div class="insight-card">
          <div class="insight-kicker">{source_type}</div>
          <div class="insight-title">{title}</div>
          <p class="insight-copy">
            Confidence: {confidence:.2f}<br/>
            Dimension: {metadata.get("dimension", item.get("dimension", "n/a"))}<br/>
            Source URL present: {"yes" if source_url else "no"}
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if source_url:
        st.link_button("Open Source Filing", source_url, use_container_width=True)
    st.code(excerpt[:2400] if len(excerpt) > 2400 else excerpt)


def _render_search_source_check(
    summary: dict[str, Any],
    api_base: str,
    api_prefix: str,
    timeout: int,
    headers: dict[str, str],
    verify_tls: bool,
) -> None:
    st.caption("Run evidence search and inspect the exact SEC-backed excerpt, item mapping, and source filing URL.")
    company_lookup = _portfolio_company_lookup(summary)
    ticker_options = ["All Portfolio Companies"] + list(company_lookup.keys())

    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
    query = filter_col1.text_input("Query", value="data infrastructure api platform", key="search_source_query")
    ticker_choice = filter_col2.selectbox("Portfolio Company", ticker_options, key="search_source_ticker")
    mode = filter_col3.selectbox("Mode", ["hybrid", "semantic", "bm25"], index=0, key="search_source_mode")
    top_k = filter_col4.slider("Top K", min_value=1, max_value=10, value=5, key="search_source_topk")

    filter_col5, filter_col6, filter_col7 = st.columns(3)
    source_type = filter_col5.selectbox(
        "Source Type",
        ["All", "sec_10k_item_1", "sec_10k_item_1a", "sec_10k_item_7"],
        key="search_source_type",
    )
    min_confidence = filter_col6.slider(
        "Min Confidence",
        min_value=0.0,
        max_value=1.0,
        value=0.0,
        step=0.05,
        key="search_source_confidence",
    )
    use_hyde = filter_col7.checkbox("Use HyDE", value=True, key="search_source_hyde")

    st.divider()
    lookup_col1, lookup_col2 = st.columns([1.0, 1.2])
    company_search_query = lookup_col1.text_input(
        "Search Any Company",
        value="",
        placeholder="Type ticker or company name",
        key="search_source_company_lookup",
    )
    company_matches = _search_companies(
        api_base,
        api_prefix,
        timeout,
        headers,
        verify_tls,
        company_search_query.strip(),
    ) if company_search_query.strip() else []
    any_company_labels = [
        f"{item['ticker'] or 'NO-TICKER'} - {item['name']} ({item['id']})"
        for item in company_matches
    ]
    selected_any_company = lookup_col2.selectbox(
        "API Company Match",
        ["None"] + any_company_labels,
        key="search_source_company_match",
    )

    selected_company = company_lookup.get(ticker_choice, {})
    company_id = selected_company.get("company_id") if ticker_choice != "All Portfolio Companies" else None
    company_name = selected_company.get("company_name") if ticker_choice != "All Portfolio Companies" else "Portfolio"

    if selected_any_company != "None":
        matched = company_matches[any_company_labels.index(selected_any_company)]
        company_id = matched["id"]
        company_name = matched["name"] or matched["ticker"] or company_id

    if st.button("Run Search", key="search_source_run"):
        try:
            params: dict[str, Any] = {
                "q": query,
                "mode": mode,
                "top_k": int(top_k),
                "use_hyde": bool(use_hyde),
            }
            if company_id:
                params["company_id"] = company_id
            if source_type != "All":
                params["source_type"] = source_type
            if min_confidence > 0:
                params["min_confidence"] = float(min_confidence)

            url = _api_url(api_base, api_prefix, "/search")
            payload = _request_json("GET", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
            st.session_state["search_source_last_payload"] = payload
        except requests.HTTPError as exc:
            _show_http_error(exc)
        except Exception as exc:
            st.error(f"Search failed: {exc}")

    payload = st.session_state.get("search_source_last_payload")
    results = payload.get("results", []) if isinstance(payload, dict) else []
    if not results:
        st.info("Run a query to inspect exact source evidence.")
        return

    st.success(f"Showing {len(results)} result(s) for {company_name}.")
    for idx, item in enumerate(results, start=1):
        metadata = item.get("metadata", {}) if isinstance(item.get("metadata"), dict) else {}
        source_item = {
            "title": metadata.get("title") or metadata.get("doc_type") or f"Result {idx}",
            "source_type": metadata.get("source_type"),
            "source_url": metadata.get("source_url"),
            "confidence": metadata.get("confidence", 0.0),
            "text": item.get("text", ""),
            "metadata": metadata,
        }
        with st.expander(
            f"{idx}. {humanize_source_type(metadata.get('source_type'))} | score={_to_float(item.get('score')):.3f}",
            expanded=(idx == 1),
        ):
            _render_source_evidence(source_item)


def _render_analytics_hub(summary: dict[str, Any]) -> None:
    st.caption("Interactive score analytics sourced directly from mirrored scoring and validation artifacts.")
    validation_payload = _result_json(RESULTS_DIR / "PORTFOLIO" / "validation" / "latest_portfolio_validation.json") or {}
    score_rows = summary["latest_scores"]
    report_cards = _collect_report_cards()

    if not score_rows:
        st.info("No local scoring outputs are available yet.")
        return

    avg_score = sum(item["score"] for item in score_rows) / len(score_rows)
    score_range = max(item["score"] for item in score_rows) - min(item["score"] for item in score_rows)
    top_band = score_rows[0]["score_band"] if score_rows else "n/a"

    metric_cols = st.columns(4)
    metric_cols[0].metric("Average OrgAIR", f"{avg_score:.2f}")
    metric_cols[1].metric("Score Spread", f"{score_range:.2f}")
    metric_cols[2].metric(
        "Validation Passes",
        f"{summary['validation_pass_count']}/{summary['validation_total']}" if summary["validation_total"] else "n/a",
    )
    metric_cols[3].metric("Live Report Modules", sum(1 for item in report_cards if int(item["count"]) > 0))

    analytics_cols = st.columns([1.2, 0.8])
    with analytics_cols[0]:
        score_df = pd.DataFrame(score_rows)
        score_fig = px.bar(
            score_df.sort_values("score", ascending=True),
            x="score",
            y="ticker",
            orientation="h",
            color="score_band",
            color_discrete_sequence=["#2f7cf6", "#28a59c", "#6e86ff", "#c8a977", "#f08a5d"],
            labels={"score": "OrgAIR Score", "ticker": ""},
            category_orders={"score_band": list(dict.fromkeys(score_df["score_band"].tolist()))},
        )
        score_fig.update_layout(
            title="Portfolio Score Spread",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#edf3f9"),
            legend_title_text="Band",
            margin=dict(l=0, r=0, t=48, b=0),
        )
        score_fig.update_xaxes(gridcolor="rgba(147, 168, 191, 0.16)")
        score_fig.update_yaxes(showgrid=False)
        st.plotly_chart(score_fig, use_container_width=True, config={"displayModeBar": False})
    with analytics_cols[1]:
        checks = validation_payload.get("checks", {})
        check_rows = [
            {
                "ticker": ticker,
                "score": _to_float(item.get("score")),
                "lower_bound": _to_float(item.get("lower_bound")),
                "upper_bound": _to_float(item.get("upper_bound")),
                "in_range": bool(item.get("in_range")),
            }
            for ticker, item in checks.items()
            if isinstance(item, dict)
        ]
        st.markdown(
            f"""
            <div class="insight-card">
              <div class="insight-kicker">Validation Envelope</div>
              <div class="insight-title">Top score band: {top_band}</div>
              <p class="insight-copy">
                Portfolio validation checks compare current scores against the expected envelope and flag any outliers.
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if check_rows:
            st.dataframe(check_rows, use_container_width=True, hide_index=True)
        else:
            st.info("No validation envelope found yet.")

    dimension_rows: list[dict[str, Any]] = []
    for ticker in summary["tickers"]:
        scoring_payload = _load_company_artifacts(ticker).get("scoring") or {}
        dimensions = scoring_payload.get("dimension_scores", [])
        for item in dimensions:
            if isinstance(item, dict):
                dimension_rows.append(
                    {
                        "ticker": ticker,
                        "dimension": str(item.get("dimension", "unknown")).replace("_", " ").title(),
                        "score": _to_float(item.get("score")),
                        "confidence": _to_float(item.get("confidence")),
                    }
                )

    if dimension_rows:
        dimension_df = pd.DataFrame(dimension_rows)
        lower_cols = st.columns([1.15, 0.85])
        with lower_cols[0]:
            heatmap_df = (
                dimension_df.pivot(index="ticker", columns="dimension", values="score")
                .fillna(0.0)
                .sort_index()
            )
            heatmap = go.Figure(
                data=go.Heatmap(
                    z=heatmap_df.values,
                    x=list(heatmap_df.columns),
                    y=list(heatmap_df.index),
                    colorscale=[
                        [0.0, "#0d1c2b"],
                        [0.35, "#24528b"],
                        [0.65, "#2aa6a0"],
                        [1.0, "#c8a977"],
                    ],
                    hovertemplate="Company=%{y}<br>Dimension=%{x}<br>Score=%{z:.2f}<extra></extra>",
                )
            )
            heatmap.update_layout(
                title="Dimension Heatmap",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#edf3f9"),
                margin=dict(l=0, r=0, t=48, b=0),
            )
            st.plotly_chart(heatmap, use_container_width=True, config={"displayModeBar": False})
        with lower_cols[1]:
            bubble = px.scatter(
                dimension_df,
                x="confidence",
                y="score",
                size="evidence_count",
                color="ticker",
                hover_name="dimension",
                labels={
                    "confidence": "Confidence",
                    "score": "Dimension Score",
                    "evidence_count": "Evidence Count",
                },
            )
            bubble.update_layout(
                title="Confidence vs Dimension Score",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#edf3f9"),
                margin=dict(l=0, r=0, t=48, b=0),
                legend_title_text="Ticker",
            )
            bubble.update_xaxes(gridcolor="rgba(147, 168, 191, 0.16)")
            bubble.update_yaxes(gridcolor="rgba(147, 168, 191, 0.16)")
            st.plotly_chart(bubble, use_container_width=True, config={"displayModeBar": False})


def _render_analysis_studio(summary: dict[str, Any]) -> None:
    st.caption("Company-level narrative analysis sourced from mirrored diligence and scoring artifacts.")
    if not summary["tickers"]:
        st.info("No portfolio result folders found yet.")
        return

    ticker = st.selectbox("Company", summary["tickers"], key="analysis_ticker")
    artifacts = _load_company_artifacts(ticker)
    cs4_payload = artifacts.get("cs4") or {}
    scoring_payload = artifacts.get("scoring") or {}

    company = cs4_payload.get("company", {})
    assessment = cs4_payload.get("assessment", {})
    justification = cs4_payload.get("justification", {})
    ic_packet = cs4_payload.get("ic_packet", {})
    dimension_score = cs4_payload.get("dimension_score", {})
    company_name = str(company.get("name", ticker))
    company_id = str(company.get("company_id", "")).strip()
    company_label = company_name if not ticker else f"{company_name} ({ticker})"
    summary_text = sanitize_generated_summary(
        justification.get("generated_summary"),
        company_name=company_name,
        company_id=company_id,
        ticker=ticker,
    )
    recommendation = str(ic_packet.get("recommendation", "n/a"))
    evidence_total = display_evidence_count(justification, ic_packet, dimension_score)

    head_cols = st.columns(4)
    head_cols[0].metric("Company", company_label)
    head_cols[1].metric("OrgAIR", f"{_to_float(assessment.get('org_air_score')):.2f}")
    head_cols[2].metric("Decision", compact_recommendation(recommendation))
    head_cols[3].metric("Evidence Count", evidence_total)

    story_col, evidence_col = st.columns([1.2, 0.8])
    with story_col:
        st.markdown(
            f"""
            <div class="insight-card">
              <div class="insight-kicker">Justification Summary</div>
              <div class="insight-title">{str(dimension_score.get("dimension", "analysis")).replace("_", " ").title()}</div>
              <p class="insight-copy">{summary_text}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with evidence_col:
        st.markdown(
            f"""
            <div class="insight-card">
              <div class="insight-kicker">IC Snapshot</div>
              <div class="insight-title">{ic_packet.get("score_band", scoring_payload.get("score_band", "n/a"))}</div>
              <p class="insight-copy">
                Recommendation: {recommendation}<br/>
                Evidence strength: {justification.get("evidence_strength", "n/a")}<br/>
                Avg evidence strength: {ic_packet.get("avg_evidence_strength", "n/a")}<br/>
                Risks captured: {len(ic_packet.get("risks", []))}
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    dimension_rows = []
    for item in scoring_payload.get("dimension_scores", []):
        if isinstance(item, dict):
            dimension_rows.append(
                {
                    "dimension": str(item.get("dimension", "unknown")).replace("_", " ").title(),
                    "score": _to_float(item.get("score")),
                    "confidence": _to_float(item.get("confidence")),
                    "evidence_count": int(item.get("evidence_count", 0) or 0),
                }
            )

    if dimension_rows:
        st.divider()
        st.caption("Dimension Profile")
        st.vega_lite_chart(
            dimension_rows,
            {
                **_vega_common_config(),
                "mark": {"type": "bar", "cornerRadiusEnd": 8},
                "encoding": {
                    "y": {"field": "dimension", "type": "nominal", "sort": "-x", "title": None},
                    "x": {"field": "score", "type": "quantitative", "title": "Score"},
                    "color": {
                        "field": "confidence",
                        "type": "quantitative",
                        "scale": {"range": ["#f08a5d", "#31c6e6"]},
                        "title": "Confidence",
                    },
                    "tooltip": [
                        {"field": "dimension", "type": "nominal"},
                        {"field": "score", "type": "quantitative", "format": ".2f"},
                        {"field": "confidence", "type": "quantitative", "format": ".2f"},
                        {"field": "evidence_count", "type": "quantitative"},
                    ],
                },
            },
            use_container_width=True,
        )

    lower_cols = st.columns(2)
    with lower_cols[0]:
        st.subheader("Top Evidence")
        evidence_rows = justification.get("supporting_evidence", [])[:5]
        if evidence_rows:
            for item in evidence_rows:
                st.markdown(
                    f"**{item.get('title') or item.get('source_type') or 'Evidence'}**  \n"
                    f"`conf={_to_float(item.get('confidence')):.2f}`  \n"
                    f"{(item.get('content') or '')[:260]}"
                )
        else:
            st.info("No supporting evidence found.")
    with lower_cols[1]:
        st.subheader("Diligence Risks")
        risks = ic_packet.get("risks", [])
        questions = ic_packet.get("diligence_questions", [])
        if risks:
            for risk in risks:
                st.write(f"- {risk}")
        else:
            st.write("- No explicit risks captured.")
        st.subheader("Diligence Questions")
        if questions:
            for question in questions:
                st.write(f"- {question}")
        else:
            st.write("- No diligence questions captured.")


def _hydrate_company_names(
    company_ids: list[str],
    api_base: str,
    api_prefix: str,
    timeout: int,
    headers: dict[str, str],
    verify_tls: bool,
) -> dict[str, str]:
    cache = st.session_state.setdefault("company_name_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        st.session_state["company_name_cache"] = cache

    for company_id in company_ids:
        cid = str(company_id).strip()
        if not cid or cid in cache:
            continue
        try:
            url = _api_url(api_base, api_prefix, f"/companies/{cid}")
            out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
            if isinstance(out, dict):
                company_name = str(out.get("name", "")).strip()
                cache[cid] = company_name or cid
            else:
                cache[cid] = cid
        except Exception:
            cache[cid] = cid
    return cache


def _render_scoring_visuals(records: list[dict[str, Any]], company_names: dict[str, str] | None = None) -> None:
    if not records:
        st.info("Load scoring results to render charts.")
        return

    names = company_names or {}
    ranked = sorted(records, key=lambda row: _to_float(row.get("composite_score")), reverse=True)
    preview = ranked[:15]
    score_min = min(_to_float(r.get("composite_score")) for r in ranked)
    score_max = max(_to_float(r.get("composite_score")) for r in ranked)
    avg_score = sum(_to_float(r.get("composite_score")) for r in ranked) / len(ranked)

    m1, m2, m3 = st.columns(3)
    m1.metric("Tracked Companies", len(ranked))
    m2.metric("Average Composite Score", f"{avg_score:.2f}")
    m3.metric("Score Range", f"{score_min:.2f} - {score_max:.2f}")

    leaderboard_data: list[dict[str, Any]] = []
    scatter_data: list[dict[str, Any]] = []
    for row in preview:
        company_id = str(row.get("company_id", "unknown"))
        company_label = names.get(company_id, company_id)
        leaderboard_data.append(
            {
                "company": company_label,
                "company_id": company_id,
                "composite_score": _to_float(row.get("composite_score")),
                "score_band": str(row.get("score_band", "unknown")).lower(),
            }
        )

    for row in ranked:
        company_id = str(row.get("company_id", "unknown"))
        company_label = names.get(company_id, company_id)
        scatter_data.append(
            {
                "company": company_label,
                "company_id": company_id,
                "vr_score": _to_float(row.get("vr_score")),
                "composite_score": _to_float(row.get("composite_score")),
                "synergy_bonus": _to_float(row.get("synergy_bonus")),
                "talent_penalty": _to_float(row.get("talent_penalty")),
                "score_band": str(row.get("score_band", "unknown")).lower(),
            }
        )

    chart_theme = _vega_common_config()

    col1, col2 = st.columns(2)
    with col1:
        st.caption("Top Composite Scores")
        st.vega_lite_chart(
            leaderboard_data,
            {
                **chart_theme,
                "mark": {"type": "bar", "cornerRadiusEnd": 8},
                "encoding": {
                    "y": {"field": "company", "type": "nominal", "sort": "-x", "title": None},
                    "x": {"field": "composite_score", "type": "quantitative", "title": "Composite"},
                    "color": {
                        "field": "score_band",
                        "type": "nominal",
                        "scale": {"range": ["#31c6e6", "#2c8bff", "#5c8eff", "#f6c85f", "#f08a5d"]},
                        "title": "Band",
                    },
                    "tooltip": [
                        {"field": "company", "type": "nominal"},
                        {"field": "company_id", "type": "nominal", "title": "Company ID"},
                        {"field": "composite_score", "type": "quantitative", "format": ".3f"},
                        {"field": "score_band", "type": "nominal"},
                    ],
                },
            },
            use_container_width=True,
        )

    with col2:
        st.caption("Composite Score Distribution")
        st.vega_lite_chart(
            ranked,
            {
                **chart_theme,
                "mark": {"type": "area", "line": {"color": "#2c8bff"}, "color": {"gradient": "linear", "stops": [{"offset": 0, "color": "rgba(49,198,230,0.6)"}, {"offset": 1, "color": "rgba(44,139,255,0.1)"}]}},
                "encoding": {
                    "x": {"field": "composite_score", "type": "quantitative", "bin": {"maxbins": 12}, "title": "Composite Score"},
                    "y": {"aggregate": "count", "type": "quantitative", "title": "Companies"},
                    "tooltip": [{"aggregate": "count", "type": "quantitative", "title": "Count"}],
                },
            },
            use_container_width=True,
        )

    st.caption("VR vs Composite (Bubble size = Synergy bonus, color = Score band)")
    st.vega_lite_chart(
        scatter_data,
        {
            **chart_theme,
            "mark": {"type": "circle", "opacity": 0.82, "stroke": "#0b1a2b", "strokeWidth": 1},
            "encoding": {
                "x": {"field": "vr_score", "type": "quantitative", "title": "VR Score"},
                "y": {"field": "composite_score", "type": "quantitative", "title": "Composite Score"},
                "size": {"field": "synergy_bonus", "type": "quantitative", "title": "Synergy Bonus"},
                "color": {
                    "field": "score_band",
                    "type": "nominal",
                    "scale": {"range": ["#31c6e6", "#2c8bff", "#5c8eff", "#f6c85f", "#f08a5d"]},
                    "title": "Score Band",
                },
                "tooltip": [
                    {"field": "company", "type": "nominal"},
                    {"field": "company_id", "type": "nominal", "title": "Company ID"},
                    {"field": "vr_score", "type": "quantitative", "format": ".3f"},
                    {"field": "composite_score", "type": "quantitative", "format": ".3f"},
                    {"field": "synergy_bonus", "type": "quantitative", "format": ".3f"},
                    {"field": "talent_penalty", "type": "quantitative", "format": ".3f"},
                ],
            },
        },
        use_container_width=True,
    )


def _render_company_breakdown(record: dict[str, Any], company_names: dict[str, str] | None = None) -> None:
    dimensions = record.get("dimension_breakdown")
    if not isinstance(dimensions, list) or not dimensions:
        st.info("No dimension breakdown found for this company.")
        return

    chart_data: list[dict[str, Any]] = []
    for item in dimensions:
        if not isinstance(item, dict):
            continue
        chart_data.append(
            {
                "dimension": str(item.get("dimension", "unknown")).replace("_", " ").title(),
                "weighted_score": _to_float(item.get("weighted_score")),
                "raw_score": _to_float(item.get("raw_score")),
                "confidence": _to_float(item.get("confidence")),
            }
        )

    if not chart_data:
        st.info("No dimension breakdown found for this company.")
        return

    company_id = str(record.get("company_id", "Unknown Company"))
    company_label = (company_names or {}).get(company_id, company_id)
    st.caption(f"Dimension Breakdown: {company_label}")
    st.vega_lite_chart(
        chart_data,
        {
            **_vega_common_config(),
            "layer": [
                {
                    "mark": {"type": "bar", "cornerRadiusEnd": 8},
                    "encoding": {
                        "y": {"field": "dimension", "type": "nominal", "sort": "-x", "title": None},
                        "x": {"field": "weighted_score", "type": "quantitative", "title": "Weighted Score"},
                        "color": {
                            "field": "confidence",
                            "type": "quantitative",
                            "scale": {"range": ["#f08a5d", "#31c6e6"]},
                            "title": "Confidence",
                        },
                        "tooltip": [
                            {"field": "dimension", "type": "nominal"},
                            {"field": "weighted_score", "type": "quantitative", "format": ".3f"},
                            {"field": "raw_score", "type": "quantitative", "format": ".3f"},
                            {"field": "confidence", "type": "quantitative", "format": ".3f"},
                        ],
                    },
                }
            ],
        },
        use_container_width=True,
    )


def _render_bonus_deliverables() -> None:
    dashboard_view.render_cs5_dashboard(
        embedded=True,
        key_prefix="legacy_bonus_dashboard",
    )


def _render_experience_home(summary: dict[str, Any]) -> None:
    latest_scores = summary.get("latest_scores", [])
    latest_leaders = latest_scores[:3]
    leader_text = " • ".join(
        f"{item['ticker']} {item['score']:.1f}" for item in latest_leaders if isinstance(item, dict)
    ) or "Awaiting current scoring outputs"
    validation_text = (
        f"{summary['validation_pass_count']} of {summary['validation_total']} portfolio checks passing"
        if summary.get("validation_total")
        else "Portfolio validation artifacts have not been generated yet"
    )

    st.markdown(
        f"""
        <div class="nav-card-grid">
          <div class="nav-card">
            <div class="insight-kicker">Start Here</div>
            <div class="nav-card-title">Portfolio Intelligence</div>
            <p class="nav-card-copy">
              Open the integrated CS5 dashboard, portfolio analytics, and company narrative review without touching raw endpoints.
            </p>
            <div class="micro-pill-row">
              <span class="micro-pill">CS5 dashboard</span>
              <span class="micro-pill">Analytics</span>
              <span class="micro-pill">Bonus deliverables</span>
            </div>
          </div>
          <div class="nav-card">
            <div class="insight-kicker">Workbench</div>
            <div class="nav-card-title">Evidence And Diligence Flow</div>
            <p class="nav-card-copy">
              Inspect source retrieval, trace justifications, and review generated artifacts before moving into operator tooling.
            </p>
            <div class="micro-pill-row">
              <span class="micro-pill">Source check</span>
              <span class="micro-pill">Artifact explorer</span>
              <span class="micro-pill">Submission review</span>
            </div>
          </div>
          <div class="nav-card">
            <div class="insight-kicker">Operations</div>
            <div class="nav-card-title">Advanced Controls Are Still Here</div>
            <p class="nav-card-copy">
              Collection, CRUD APIs, scoring runs, script execution, and raw HTTP access now live under one advanced workspace.
            </p>
            <div class="micro-pill-row">
              <span class="micro-pill">API console</span>
              <span class="micro-pill">Script runner</span>
              <span class="micro-pill">Health checks</span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    flow_col, status_col = st.columns([1.15, 0.85])
    with flow_col:
        st.markdown(
            """
            <div class="section-callout">
              <strong>Recommended journey:</strong> 1) review executive health and current outputs, 2) open Portfolio Intelligence for CS5 analysis and bonus deliverables, 3) use Diligence Workbench for evidence tracing, 4) go to Advanced Ops only when you need to run or debug the platform.
            </div>
            """,
            unsafe_allow_html=True,
        )
    with status_col:
        st.markdown(
            f"""
            <div class="insight-card">
              <div class="insight-kicker">Latest Leaders</div>
              <div class="insight-title">{leader_text}</div>
              <p class="insight-copy">{validation_text}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    home_tabs = st.tabs(["Executive Snapshot", "Recent Output Signals"])
    with home_tabs[0]:
        _render_overview(summary)
    with home_tabs[1]:
        st.caption("Recent portfolio files and mirrored scoring outputs available from the local results tree.")
        artifact_cols = st.columns([1.0, 1.0])
        with artifact_cols[0]:
            if latest_scores:
                st.dataframe(latest_scores[:5], use_container_width=True)
            else:
                st.info("No current OrgAIR result files found yet.")
        with artifact_cols[1]:
            st.metric("Local Result Files", f"{summary.get('local_file_count', 0):,}")
            st.metric("Tracked Portfolio Companies", f"{len(summary.get('tickers', []))}")
            st.caption("Use Results & Evidence to inspect the underlying artifacts and source documents.")


def _render_experience_hub(summary: dict[str, Any]) -> None:
    latest_scores = summary.get("latest_scores", [])
    latest_leaders = latest_scores[:3]
    leader_text = " | ".join(
        f"{item['ticker']} {item['score']:.1f}" for item in latest_leaders if isinstance(item, dict)
    ) or "Awaiting current scoring outputs"
    validation_text = (
        f"{summary['validation_pass_count']} of {summary['validation_total']} validation checks passing"
        if summary.get("validation_total")
        else "Portfolio validation has not been generated yet"
    )
    pipeline_cards = _collect_pipeline_cards()
    report_cards = _collect_report_cards()
    recent_artifacts = _collect_recent_artifacts(limit=10)

    pipeline_markup = "".join(
        f"""
        <div class="pipeline-card" style="border-top: 1px solid {item['accent']}">
          <div class="insight-kicker">Workflow</div>
          <div class="pipeline-card-title">{item['title']}</div>
          <p class="pipeline-card-copy">{item['description']}</p>
          <div class="pipeline-status" style="box-shadow: inset 0 0 0 1px {item['accent']}33;">{item['status']}</div>
          <div class="pipeline-meta">{item['coverage_text']}<br/>Artifacts: {item['artifact_count']}<br/>Updated: {item['updated_at']}</div>
        </div>
        """
        for item in pipeline_cards
    )
    report_markup = "".join(
        f"""
        <div class="report-card" style="border-top: 1px solid {item['accent']}">
          <div class="insight-kicker">Report And Memory Layer</div>
          <div class="report-card-title">{item['title']}</div>
          <p class="report-card-copy">{item['description']}</p>
          <div class="report-status" style="box-shadow: inset 0 0 0 1px {item['accent']}33;">{item['status']}</div>
          <div class="report-meta">{item['count']} {item['unit_label']}<br/>Updated: {item['updated_at']}</div>
        </div>
        """
        for item in report_cards
    )

    st.markdown(
        """
        <div class="nav-card-grid">
          <div class="nav-card">
            <div class="insight-kicker">Start Here</div>
            <div class="nav-card-title">Portfolio Analytics</div>
            <p class="nav-card-copy">
              Open the executive dashboard, score analytics, and company review surface without touching raw endpoints.
            </p>
            <div class="micro-pill-row">
              <span class="micro-pill">Executive dashboard</span>
              <span class="micro-pill">Score analytics</span>
              <span class="micro-pill">Strategic outputs</span>
            </div>
          </div>
          <div class="nav-card">
            <div class="insight-kicker">Workbench</div>
            <div class="nav-card-title">Evidence And Diligence Review</div>
            <p class="nav-card-copy">
              Inspect source retrieval, trace justifications, and review generated diligence artifacts before moving into platform controls.
            </p>
            <div class="micro-pill-row">
              <span class="micro-pill">Source check</span>
              <span class="micro-pill">Artifact explorer</span>
              <span class="micro-pill">Narrative review</span>
            </div>
          </div>
          <div class="nav-card">
            <div class="insight-kicker">Platform Console</div>
            <div class="nav-card-title">Direct Controls Stay Available</div>
            <p class="nav-card-copy">
              Collection jobs, CRUD APIs, scoring runs, script execution, and raw HTTP access now live in one consolidated console.
            </p>
            <div class="micro-pill-row">
              <span class="micro-pill">API console</span>
              <span class="micro-pill">Script runner</span>
              <span class="micro-pill">Diagnostics</span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    flow_col, status_col = st.columns([1.15, 0.85])
    with flow_col:
        st.markdown(
            """
            <div class="section-callout">
              <strong>Recommended journey:</strong> 1) review executive metrics and recent output movement, 2) open portfolio analytics for score and company review, 3) inspect evidence and diligence artifacts, 4) use the platform console only when you need direct execution or diagnostics.
            </div>
            """,
            unsafe_allow_html=True,
        )
    with status_col:
        st.markdown(
            f"""
            <div class="insight-card">
              <div class="insight-kicker">Latest Leaders</div>
              <div class="insight-title">{leader_text}</div>
              <p class="insight-copy">{validation_text}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown('<div class="section-label">Pipeline Coverage</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="pipeline-grid">{pipeline_markup}</div>', unsafe_allow_html=True)

    st.markdown('<div class="section-label" style="margin-top: 1rem;">Reports And Outputs</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="report-grid">{report_markup}</div>', unsafe_allow_html=True)

    home_tabs = st.tabs(["Portfolio Snapshot", "Workflow Coverage", "Recent Deliverables"])
    with home_tabs[0]:
        _render_overview(summary)
    with home_tabs[1]:
        st.dataframe(
            [
                {
                    "workflow": item["title"],
                    "status": item["status"],
                    "coverage": item["coverage_text"],
                    "artifacts": item["artifact_count"],
                    "updated_at": item["updated_at"],
                }
                for item in pipeline_cards
            ],
            use_container_width=True,
            hide_index=True,
        )
    with home_tabs[2]:
        st.dataframe(recent_artifacts, use_container_width=True, hide_index=True)


def _render_section_header(kicker: str, title: str, copy: str) -> None:
    st.markdown(
        f"""
        <div class="dashboard-band" style="margin: 1.1rem 0 0.85rem;">
          <div class="insight-kicker">{kicker}</div>
          <div class="dashboard-band-title">{title}</div>
          <p class="dashboard-band-copy">{copy}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _workspace_roots() -> dict[str, Path]:
    return {
        "Generated Results": RESULTS_DIR,
        "Generated Reports": RESULTS_DIR / "bonus" / "documents",
        "Automation Scripts": SCRIPTS_DIR,
        "Dashboard Module": ROOT_DIR / "app" / "dashboard",
        "Streamlit Shell": STREAMLIT_DIR,
        "Architecture Docs": ROOT_DIR / "docs",
    }


def _language_for_suffix(path: Path) -> str | None:
    mapping = {
        ".py": "python",
        ".json": "json",
        ".md": "markdown",
        ".txt": "text",
        ".yml": "yaml",
        ".yaml": "yaml",
        ".toml": "toml",
        ".ps1": "powershell",
        ".sql": "sql",
        ".svg": "xml",
    }
    return mapping.get(path.suffix.lower())


def _render_file_preview(path: Path) -> None:
    suffix = path.suffix.lower()
    if suffix == ".json":
        payload = _result_json(path)
        if payload is not None:
            _show_payload(payload)
            return

    if suffix in {".py", ".md", ".txt", ".yml", ".yaml", ".toml", ".ps1", ".sql", ".svg"}:
        text = path.read_text(encoding="utf-8", errors="replace")
        st.code(text[:20000] if len(text) > 20000 else text, language=_language_for_suffix(path))
        return

    st.info("Preview is not available for this file type. Use the download action to inspect it locally.")


def _render_workspace_browser() -> None:
    st.caption("Browse the files and folders that power the dashboard directly inside Streamlit.")
    roots = _workspace_roots()
    root_label = st.selectbox("Workspace Root", list(roots.keys()), key="workspace_browser_root")
    root_path = roots[root_label]

    if not root_path.exists():
        st.info("This workspace root does not exist yet.")
        return

    files = sorted(_safe_collect_files(root_path))
    if not files:
        st.info("No files found in this workspace root.")
        return

    folders = sorted(
        {
            "."
            if file_path.parent == root_path
            else str(file_path.parent.relative_to(root_path))
            for file_path in files
        }
    )
    selected_folder = st.selectbox("Folder", ["All"] + folders, key="workspace_browser_folder")
    filtered_files = [
        file_path
        for file_path in files
        if selected_folder == "All"
        or (file_path.parent == root_path and selected_folder == ".")
        or str(file_path.parent.relative_to(root_path)) == selected_folder
    ]

    if not filtered_files:
        st.info("No files found in this folder.")
        return

    selected_file = st.selectbox(
        "File",
        filtered_files,
        key="workspace_browser_file",
        format_func=lambda path: str(path.relative_to(root_path)),
    )

    preview_col, meta_col = st.columns([1.35, 0.8])
    with meta_col:
        st.markdown(
            f"""
            <div class="insight-card">
              <div class="insight-kicker">{root_label}</div>
              <div class="insight-title">{selected_file.name}</div>
              <p class="insight-copy">
                Folder: {selected_file.parent.relative_to(root_path) if selected_file.parent != root_path else '.'}<br/>
                Size: {selected_file.stat().st_size:,} bytes<br/>
                Modified: {datetime.fromtimestamp(selected_file.stat().st_mtime).strftime("%b %d, %Y %I:%M %p")}
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.download_button(
            "Download File",
            data=selected_file.read_bytes(),
            file_name=selected_file.name,
            mime="application/octet-stream",
            use_container_width=True,
            key=f"workspace_download_{root_label}_{selected_file.name}",
        )
    with preview_col:
        _render_file_preview(selected_file)


# ============================================================
# UI setup
# ============================================================

st.set_page_config(page_title="OrgAIR Portfolio Intelligence", layout="wide")
_inject_ui_theme()
st.markdown(
    """
    <div class="masthead">
      <p class="masthead-title">OrgAIR Portfolio Intelligence</p>
      <p class="masthead-copy">Executive analytics, diligence review, and report generation for the portfolio.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

if "connection_mode" not in st.session_state:
    st.session_state["connection_mode"] = "Cloud Run"
if "api_base_value" not in st.session_state:
    st.session_state["api_base_value"] = DEFAULT_CLOUD_RUN_API_BASE

with st.sidebar:
    st.header("Workspace")
    st.caption("Executive analytics first. Platform controls remain available in the console section below.")
    st.info("Primary journey: Unified Workspace -> Portfolio Command -> Review And Files -> Platform Console")

    with st.expander("Connection", expanded=False):
        connection_mode = st.radio(
            "Environment",
            ["Cloud Run", "Local", "Custom"],
            key="connection_mode",
            horizontal=False,
        )

        if connection_mode == "Cloud Run":
            st.session_state["api_base_value"] = DEFAULT_CLOUD_RUN_API_BASE
            st.success("Cloud Run API preset active")
        elif connection_mode == "Local":
            st.session_state["api_base_value"] = DEFAULT_API_BASE
            st.info("Local API preset active")

        if connection_mode == "Custom":
            api_base = st.text_input("API Base URL", value=st.session_state.get("api_base_value", DEFAULT_API_BASE))
            st.session_state["api_base_value"] = api_base
        else:
            api_base = st.text_input(
                "API Base URL",
                value=st.session_state.get("api_base_value", DEFAULT_API_BASE),
                disabled=True,
            )

        api_prefix = st.text_input("API Prefix", value=DEFAULT_API_PREFIX)
        scoring_prefix = st.text_input("Scoring Prefix", value=DEFAULT_SCORING_PREFIX)
        timeout = st.number_input("HTTP Timeout (seconds)", min_value=1, max_value=300, value=20)
        verify_tls = st.checkbox("Verify TLS certificates", value=True)

    with st.expander("Auth And Headers", expanded=False):
        bearer_token = st.text_input("Bearer Token", value="", type="password")
        extra_headers_text = st.text_area("Extra Headers JSON", value="{}", height=100)

    headers, headers_error = _build_headers(bearer_token, extra_headers_text)
    if headers_error:
        st.error(headers_error)

    with st.expander("Platform Details", expanded=False):
        st.write("- Health endpoints do not use the API prefix")
        st.write("- Scoring routes use the dedicated scoring prefix")
        st.write(f"- Cloud Run preset: `{DEFAULT_CLOUD_RUN_API_BASE}`")
        st.write(f"- Local results root: `{RESULTS_DIR}`")
        st.write(f"- Default portfolio: `{', '.join(PORTFOLIO_TICKERS)}`")

result_summary = _collect_result_summary()
_render_hero(result_summary, api_base, api_prefix, scoring_prefix)


_render_section_header(
    "Unified Workspace",
    "One operating surface for portfolio intelligence",
    "The dashboard, diligence review, reports, files, and execution tools now live inside one continuous Streamlit workspace.",
)
_render_experience_hub(result_summary)

_render_section_header(
    "Portfolio Command",
    "Executive dashboard, analytics, and company drilldown",
    "Core portfolio review stays together here: live company metrics, score analytics, and narrative diligence analysis.",
)
portfolio_tabs = st.tabs(["Executive Dashboard", "Score Analytics", "Company Review"])

with portfolio_tabs[0]:
    dashboard_view.render_cs5_dashboard(
        embedded=True,
        key_prefix="platform_cs5_dashboard",
    )

with portfolio_tabs[1]:
    _render_analytics_hub(result_summary)

with portfolio_tabs[2]:
    _render_analysis_studio(result_summary)

_render_section_header(
    "Review And Files",
    "Evidence review, mirrored outputs, and repository browser",
    "Search sources, inspect generated artifacts, and browse dashboard and workflow files without leaving the app.",
)
review_tabs = st.tabs(["Source Review", "Results Library", "Workspace Browser"])

with review_tabs[0]:
    _render_search_source_check(result_summary, api_base, api_prefix, int(timeout), headers, verify_tls)

with review_tabs[1]:
    _render_results_explorer()

with review_tabs[2]:
    _render_workspace_browser()

_render_section_header(
    "Platform Console",
    "Direct execution, diagnostics, and API control",
    "Operational tooling remains available below for collection jobs, scoring runs, scripts, and direct endpoint access.",
)
ops_tabs = st.tabs(
    [
        "Health",
        "Companies",
        "Assessments",
        "Collection Jobs",
        "Documents",
        "Signals",
        "Signal Intelligence",
        "Evidence",
        "Scoring",
        "Script Runner",
        "API Console",
    ]
)

with ops_tabs[0]:
    col1, col2 = st.columns(2)

    with col1:
        if st.button("GET /health", key="health_simple"):
            try:
                url = _api_url(api_base, api_prefix, "/health", include_prefix=False)
                payload = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(payload)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Health check failed: {exc}")

    with col2:
        if st.button("GET /health/detailed", key="health_detailed"):
            try:
                url = _api_url(api_base, api_prefix, "/health/detailed", include_prefix=False)
                payload = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(payload)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Detailed health check failed: {exc}")

# ============================================================
# Companies
# ============================================================

with ops_tabs[1]:
    tabs = st.tabs(["List", "Industries", "Get", "Create", "Update", "Delete"])

    with tabs[0]:
        page = st.number_input("Page", min_value=1, value=1, key="companies_page")
        page_size = st.number_input("Page Size", min_value=1, max_value=100, value=20, key="companies_page_size")
        if st.button("GET /companies", key="companies_list_btn"):
            try:
                url = _api_url(api_base, api_prefix, "/companies")
                payload = _request_json(
                    "GET",
                    url,
                    params={"page": int(page), "page_size": int(page_size)},
                    timeout=timeout,
                    headers=headers,
                    verify=verify_tls,
                )
                _show_payload(payload)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List companies failed: {exc}")

    with tabs[1]:
        if st.button("GET /companies/industries", key="companies_industries_btn"):
            try:
                url = _api_url(api_base, api_prefix, "/companies/industries")
                payload = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(payload)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List industries failed: {exc}")

    with tabs[2]:
        company_id = st.text_input("Company ID", key="companies_get_id")
        if st.button("GET /companies/{company_id}", key="companies_get_btn"):
            if not company_id.strip():
                st.error("Company ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/companies/{company_id.strip()}")
                    payload = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(payload)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get company failed: {exc}")

    with tabs[3]:
        with st.form("companies_create_form"):
            name = st.text_input("Name")
            ticker = st.text_input("Ticker (uppercase, optional)")
            industry_id = st.text_input("Industry ID (optional)")
            position_factor = st.number_input("Position Factor", min_value=-1.0, max_value=1.0, value=0.0, step=0.01)
            submitted = st.form_submit_button("POST /companies")

        if submitted:
            if not name.strip():
                st.error("Name is required")
            else:
                payload: dict[str, Any] = {
                    "name": name.strip(),
                    "position_factor": float(position_factor),
                }
                if ticker.strip():
                    payload["ticker"] = ticker.strip().upper()
                if industry_id.strip():
                    payload["industry_id"] = industry_id.strip()

                try:
                    url = _api_url(api_base, api_prefix, "/companies")
                    out = _request_json("POST", url, json=payload, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Create company failed: {exc}")

    with tabs[4]:
        with st.form("companies_update_form"):
            update_id = st.text_input("Company ID")
            update_name = st.text_input("Name (optional)")
            update_ticker = st.text_input("Ticker (optional)")
            update_industry = st.text_input("Industry ID (optional)")
            include_position = st.checkbox("Include position_factor", value=False)
            update_position = st.number_input("Position Factor", min_value=-1.0, max_value=1.0, value=0.0, step=0.01)
            submitted_update = st.form_submit_button("PUT /companies/{company_id}")

        if submitted_update:
            if not update_id.strip():
                st.error("Company ID is required")
            else:
                payload: dict[str, Any] = {}
                if update_name.strip():
                    payload["name"] = update_name.strip()
                if update_ticker.strip():
                    payload["ticker"] = update_ticker.strip().upper()
                if update_industry.strip():
                    payload["industry_id"] = update_industry.strip()
                if include_position:
                    payload["position_factor"] = float(update_position)

                if not payload:
                    st.error("Provide at least one field to update")
                else:
                    try:
                        url = _api_url(api_base, api_prefix, f"/companies/{update_id.strip()}")
                        out = _request_json("PUT", url, json=payload, timeout=timeout, headers=headers, verify=verify_tls)
                        _show_payload(out)
                    except requests.HTTPError as exc:
                        _show_http_error(exc)
                    except Exception as exc:
                        st.error(f"Update company failed: {exc}")

    with tabs[5]:
        delete_id = st.text_input("Company ID", key="companies_delete_id")
        if st.button("DELETE /companies/{company_id}", key="companies_delete_btn"):
            if not delete_id.strip():
                st.error("Company ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/companies/{delete_id.strip()}")
                    resp = _request("DELETE", url, timeout=timeout, headers=headers, verify=verify_tls)
                    if not resp.ok:
                        raise requests.HTTPError(resp.text, response=resp)
                    st.success(f"Deleted ({resp.status_code})")
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Delete company failed: {exc}")


# ============================================================
# Assessments
# ============================================================

with ops_tabs[2]:
    tabs = st.tabs(["List", "Get", "Create", "Update Status", "List Scores", "Upsert Score"])

    with tabs[0]:
        page = st.number_input("Page", min_value=1, value=1, key="assessments_page")
        page_size = st.number_input("Page Size", min_value=1, max_value=100, value=20, key="assessments_page_size")
        company_id = st.text_input("Company ID filter (optional)", key="assessments_filter_company")
        if st.button("GET /assessments", key="assessments_list_btn"):
            try:
                params: dict[str, Any] = {"page": int(page), "page_size": int(page_size)}
                if company_id.strip():
                    params["company_id"] = company_id.strip()
                url = _api_url(api_base, api_prefix, "/assessments")
                payload = _request_json("GET", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(payload)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List assessments failed: {exc}")

    with tabs[1]:
        assessment_id = st.text_input("Assessment ID", key="assessments_get_id")
        if st.button("GET /assessments/{id}", key="assessments_get_btn"):
            if not assessment_id.strip():
                st.error("Assessment ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/assessments/{assessment_id.strip()}")
                    payload = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(payload)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get assessment failed: {exc}")

    with tabs[2]:
        with st.form("assessments_create_form"):
            create_company_id = st.text_input("Company ID")
            assessment_type = st.selectbox("Assessment Type", ASSESSMENT_TYPES)
            assessment_date = st.date_input("Assessment Date", value=date.today())
            primary_assessor = st.text_input("Primary Assessor (optional)")
            secondary_assessor = st.text_input("Secondary Assessor (optional)")
            include_vr = st.checkbox("Include VR score", value=False)
            vr_score = st.number_input("VR Score", min_value=0.0, max_value=100.0, value=50.0)
            include_bounds = st.checkbox("Include confidence bounds", value=False)
            conf_lower = st.number_input("Confidence Lower", min_value=0.0, max_value=100.0, value=40.0)
            conf_upper = st.number_input("Confidence Upper", min_value=0.0, max_value=100.0, value=60.0)
            submitted = st.form_submit_button("POST /assessments")

        if submitted:
            if not create_company_id.strip():
                st.error("Company ID is required")
            else:
                payload: dict[str, Any] = {
                    "company_id": create_company_id.strip(),
                    "assessment_type": assessment_type,
                    "assessment_date": assessment_date.isoformat(),
                }
                if primary_assessor.strip():
                    payload["primary_assessor"] = primary_assessor.strip()
                if secondary_assessor.strip():
                    payload["secondary_assessor"] = secondary_assessor.strip()
                if include_vr:
                    payload["vr_score"] = float(vr_score)
                if include_bounds:
                    payload["confidence_lower"] = float(conf_lower)
                    payload["confidence_upper"] = float(conf_upper)

                try:
                    url = _api_url(api_base, api_prefix, "/assessments")
                    out = _request_json("POST", url, json=payload, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Create assessment failed: {exc}")

    with tabs[3]:
        with st.form("assessments_status_form"):
            update_assessment_id = st.text_input("Assessment ID")
            status_value = st.selectbox("New Status", ASSESSMENT_STATUSES)
            submitted = st.form_submit_button("PATCH /assessments/{id}/status")

        if submitted:
            if not update_assessment_id.strip():
                st.error("Assessment ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/assessments/{update_assessment_id.strip()}/status")
                    out = _request_json(
                        "PATCH",
                        url,
                        json={"status": status_value},
                        timeout=timeout,
                        headers=headers,
                        verify=verify_tls,
                    )
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Update status failed: {exc}")

    with tabs[4]:
        assessment_id = st.text_input("Assessment ID", key="assessments_scores_id")
        page = st.number_input("Page", min_value=1, value=1, key="assessments_scores_page")
        page_size = st.number_input("Page Size", min_value=1, max_value=100, value=20, key="assessments_scores_page_size")
        if st.button("GET /assessments/{id}/scores", key="assessments_scores_btn"):
            if not assessment_id.strip():
                st.error("Assessment ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/assessments/{assessment_id.strip()}/scores")
                    out = _request_json(
                        "GET",
                        url,
                        params={"page": int(page), "page_size": int(page_size)},
                        timeout=timeout,
                        headers=headers,
                        verify=verify_tls,
                    )
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"List dimension scores failed: {exc}")

    with tabs[5]:
        with st.form("assessments_upsert_score_form"):
            score_assessment_id = st.text_input("Assessment ID")
            score_dimension = st.selectbox("Dimension", DIMENSIONS)
            score_value = st.number_input("Score", min_value=0.0, max_value=100.0, value=50.0)
            include_weight = st.checkbox("Include weight", value=False)
            weight_value = st.number_input("Weight", min_value=0.0, max_value=1.0, value=0.15)
            confidence_value = st.slider("Confidence", min_value=0.0, max_value=1.0, value=0.8)
            evidence_count = st.number_input("Evidence Count", min_value=0, value=0)
            submitted = st.form_submit_button("POST /assessments/{id}/scores")

        if submitted:
            if not score_assessment_id.strip():
                st.error("Assessment ID is required")
            else:
                payload: dict[str, Any] = {
                    "assessment_id": score_assessment_id.strip(),
                    "dimension": score_dimension,
                    "score": float(score_value),
                    "confidence": float(confidence_value),
                    "evidence_count": int(evidence_count),
                }
                if include_weight:
                    payload["weight"] = float(weight_value)

                try:
                    url = _api_url(api_base, api_prefix, f"/assessments/{score_assessment_id.strip()}/scores")
                    out = _request_json("POST", url, json=payload, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Upsert score failed: {exc}")

# ============================================================
# Collection
# ============================================================

with ops_tabs[3]:
    tabs = st.tabs(["Collect Evidence", "Collect Signals", "Task Status"])

    with tabs[0]:
        with st.form("collection_evidence_form"):
            companies = st.text_input("Tickers (comma-separated) or 'all'", value="all")
            submitted = st.form_submit_button("POST /collection/evidence")

        if submitted:
            if not companies.strip():
                st.error("companies is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, "/collection/evidence")
                    out = _request_json(
                        "POST",
                        url,
                        params={"companies": companies.strip()},
                        timeout=timeout,
                        headers=headers,
                        verify=verify_tls,
                    )
                    if isinstance(out, dict) and out.get("task_id"):
                        st.session_state["last_collection_task_id"] = out["task_id"]
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Collect evidence failed: {exc}")

    with tabs[1]:
        with st.form("collection_signals_form"):
            companies = st.text_input("Tickers (comma-separated) or 'all'", value="all", key="collection_signals_companies")
            submitted = st.form_submit_button("POST /collection/signals")

        if submitted:
            if not companies.strip():
                st.error("companies is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, "/collection/signals")
                    out = _request_json(
                        "POST",
                        url,
                        params={"companies": companies.strip()},
                        timeout=timeout,
                        headers=headers,
                        verify=verify_tls,
                    )
                    if isinstance(out, dict) and out.get("task_id"):
                        st.session_state["last_collection_task_id"] = out["task_id"]
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Collect signals failed: {exc}")

    with tabs[2]:
        default_task = st.session_state.get("last_collection_task_id", "")
        task_id = st.text_input("Task ID", value=default_task)
        if st.button("GET /collection/tasks/{task_id}", key="collection_task_status_btn"):
            if not task_id.strip():
                st.error("Task ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/collection/tasks/{task_id.strip()}")
                    out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get task status failed: {exc}")


# ============================================================
# Documents & Chunks
# ============================================================

with ops_tabs[4]:
    tabs = st.tabs(["List Documents", "Get Document", "List Chunks", "Get Chunk"])

    with tabs[0]:
        ticker = st.text_input("Ticker filter (optional)", key="documents_list_ticker")
        company_id = st.text_input("Company ID filter (optional)", key="documents_list_company")
        limit = st.number_input("Limit", min_value=1, max_value=500, value=100, key="documents_list_limit")
        offset = st.number_input("Offset", min_value=0, value=0, key="documents_list_offset")
        if st.button("GET /documents", key="documents_list_btn"):
            try:
                params: dict[str, Any] = {"limit": int(limit), "offset": int(offset)}
                if ticker.strip():
                    params["ticker"] = ticker.strip().upper()
                if company_id.strip():
                    params["company_id"] = company_id.strip()
                url = _api_url(api_base, api_prefix, "/documents")
                out = _request_json("GET", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(out)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List documents failed: {exc}")

    with tabs[1]:
        document_id = st.text_input("Document ID", key="documents_get_id")
        if st.button("GET /documents/{document_id}", key="documents_get_btn"):
            if not document_id.strip():
                st.error("Document ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/documents/{document_id.strip()}")
                    out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get document failed: {exc}")

    with tabs[2]:
        document_id = st.text_input("Document ID", key="chunks_list_document_id")
        limit = st.number_input("Limit", min_value=1, max_value=1000, value=200, key="chunks_list_limit")
        offset = st.number_input("Offset", min_value=0, value=0, key="chunks_list_offset")
        if st.button("GET /chunks/?document_id=...", key="chunks_list_btn"):
            if not document_id.strip():
                st.error("Document ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, "/chunks/")
                    out = _request_json(
                        "GET",
                        url,
                        params={"document_id": document_id.strip(), "limit": int(limit), "offset": int(offset)},
                        timeout=timeout,
                        headers=headers,
                        verify=verify_tls,
                    )
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"List chunks failed: {exc}")

    with tabs[3]:
        chunk_id = st.text_input("Chunk ID", key="chunks_get_id")
        if st.button("GET /chunks/{chunk_id}", key="chunks_get_btn"):
            if not chunk_id.strip():
                st.error("Chunk ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/chunks/{chunk_id.strip()}")
                    out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get chunk failed: {exc}")


# ============================================================
# Signals
# ============================================================

with ops_tabs[5]:
    tabs = st.tabs(["List", "Get"])

    with tabs[0]:
        ticker = st.text_input("Ticker (optional)", key="signals_list_ticker")
        signal_type = st.text_input("Signal Type (optional)", key="signals_list_type")
        source = st.text_input("Source (optional)", key="signals_list_source")
        limit = st.number_input("Limit", min_value=1, max_value=500, value=100, key="signals_list_limit")
        if st.button("GET /signals", key="signals_list_btn"):
            try:
                params: dict[str, Any] = {"limit": int(limit)}
                if ticker.strip():
                    params["ticker"] = ticker.strip().upper()
                if signal_type.strip():
                    params["signal_type"] = signal_type.strip().lower()
                if source.strip():
                    params["source"] = source.strip().lower()
                url = _api_url(api_base, api_prefix, "/signals")
                out = _request_json("GET", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(out)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List signals failed: {exc}")

    with tabs[1]:
        signal_id = st.text_input("Signal ID", key="signals_get_id")
        if st.button("GET /signals/{signal_id}", key="signals_get_btn"):
            if not signal_id.strip():
                st.error("Signal ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/signals/{signal_id.strip()}")
                    out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get signal failed: {exc}")

# ============================================================
# Signal Summaries
# ============================================================

with ops_tabs[6]:
    tabs = st.tabs(["List", "Compute"])

    with tabs[0]:
        ticker = st.text_input("Ticker (optional)", key="summaries_list_ticker")
        limit = st.number_input("Limit", min_value=1, max_value=200, value=50, key="summaries_list_limit")
        if st.button("GET /signal-summaries", key="summaries_list_btn"):
            try:
                params: dict[str, Any] = {"limit": int(limit)}
                if ticker.strip():
                    params["ticker"] = ticker.strip().upper()
                url = _api_url(api_base, api_prefix, "/signal-summaries")
                out = _request_json("GET", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(out)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List signal summaries failed: {exc}")

    with tabs[1]:
        with st.form("summaries_compute_form"):
            ticker = st.text_input("Ticker", key="summaries_compute_ticker")
            include_as_of = st.checkbox("Include as_of date", value=False)
            as_of_date = st.date_input("as_of", value=date.today(), key="summaries_compute_as_of")
            submitted = st.form_submit_button("POST /signal-summaries/compute")

        if submitted:
            if not ticker.strip():
                st.error("Ticker is required")
            else:
                params: dict[str, Any] = {"ticker": ticker.strip().upper()}
                if include_as_of:
                    params["as_of"] = as_of_date.isoformat()
                try:
                    url = _api_url(api_base, api_prefix, "/signal-summaries/compute")
                    out = _request_json("POST", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Compute summary failed: {exc}")


# ============================================================
# Evidence
# ============================================================

with ops_tabs[7]:
    tabs = st.tabs(["Stats", "List Documents", "Get Document", "Get Document Chunks"])

    with tabs[0]:
        if st.button("GET /evidence/stats", key="evidence_stats_btn"):
            try:
                url = _api_url(api_base, api_prefix, "/evidence/stats")
                out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(out)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Evidence stats failed: {exc}")

    with tabs[1]:
        ticker = st.text_input("Ticker (optional)", key="evidence_docs_ticker")
        company_id = st.text_input("Company ID (optional)", key="evidence_docs_company")
        limit = st.number_input("Limit", min_value=1, max_value=500, value=100, key="evidence_docs_limit")
        offset = st.number_input("Offset", min_value=0, value=0, key="evidence_docs_offset")
        if st.button("GET /evidence/documents", key="evidence_docs_list_btn"):
            try:
                params: dict[str, Any] = {"limit": int(limit), "offset": int(offset)}
                if ticker.strip():
                    params["ticker"] = ticker.strip().upper()
                if company_id.strip():
                    params["company_id"] = company_id.strip()
                url = _api_url(api_base, api_prefix, "/evidence/documents")
                out = _request_json("GET", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(out)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List evidence documents failed: {exc}")

    with tabs[2]:
        document_id = st.text_input("Document ID", key="evidence_doc_get_id")
        if st.button("GET /evidence/documents/{document_id}", key="evidence_doc_get_btn"):
            if not document_id.strip():
                st.error("Document ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/evidence/documents/{document_id.strip()}")
                    out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get evidence document failed: {exc}")

    with tabs[3]:
        document_id = st.text_input("Document ID", key="evidence_chunks_doc_id")
        limit = st.number_input("Limit", min_value=1, max_value=1000, value=200, key="evidence_chunks_limit")
        offset = st.number_input("Offset", min_value=0, value=0, key="evidence_chunks_offset")
        if st.button("GET /evidence/documents/{document_id}/chunks", key="evidence_chunks_btn"):
            if not document_id.strip():
                st.error("Document ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/evidence/documents/{document_id.strip()}/chunks")
                    out = _request_json(
                        "GET",
                        url,
                        params={"limit": int(limit), "offset": int(offset)},
                        timeout=timeout,
                        headers=headers,
                        verify=verify_tls,
                    )
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get evidence chunks failed: {exc}")


# ============================================================
# Scoring
# ============================================================

with ops_tabs[8]:
    tabs = st.tabs(["Compute", "Latest by Company", "Leaderboard", "Visuals"])

    with tabs[0]:
        with st.form("scoring_compute_form"):
            company_id = st.text_input("Company ID")
            version = st.text_input("Version", value="v1.0")
            submitted = st.form_submit_button("POST {scoring_prefix}/compute/{company_id}")

        if submitted:
            if not company_id.strip():
                st.error("Company ID is required")
            else:
                try:
                    url = _scoring_url(api_base, scoring_prefix, f"/compute/{company_id.strip()}")
                    out = _request_json(
                        "POST",
                        url,
                        params={"version": version.strip() or "v1.0"},
                        timeout=max(timeout, 60),
                        headers=headers,
                        verify=verify_tls,
                    )
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Compute scoring failed: {exc}")

    with tabs[1]:
        company_id = st.text_input("Company ID", key="scoring_results_company_id")
        if st.button("GET {scoring_prefix}/results/{company_id}", key="scoring_results_company_btn"):
            if not company_id.strip():
                st.error("Company ID is required")
            else:
                try:
                    url = _scoring_url(api_base, scoring_prefix, f"/results/{company_id.strip()}")
                    out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    st.session_state["scoring_last_company"] = out
                    _show_payload(out)
                    records = _as_scoring_records(out)
                    if records:
                        name_map = _hydrate_company_names(
                            [str(records[0].get("company_id", ""))],
                            api_base,
                            api_prefix,
                            int(timeout),
                            headers,
                            verify_tls,
                        )
                        st.divider()
                        _render_company_breakdown(records[0], name_map)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get latest company score failed: {exc}")

    with tabs[2]:
        limit = st.number_input("Limit", min_value=1, max_value=200, value=50, key="scoring_results_limit")
        if st.button("GET {scoring_prefix}/results", key="scoring_results_list_btn"):
            try:
                url = _scoring_url(api_base, scoring_prefix, "/results")
                out = _request_json(
                    "GET",
                    url,
                    params={"limit": int(limit)},
                    timeout=timeout,
                    headers=headers,
                    verify=verify_tls,
                )
                st.session_state["scoring_last_results"] = out
                _show_payload(out)
                records = _as_scoring_records(out)
                if records:
                    ids = [str(item.get("company_id", "")) for item in records]
                    name_map = _hydrate_company_names(
                        ids,
                        api_base,
                        api_prefix,
                        int(timeout),
                        headers,
                        verify_tls,
                    )
                    st.divider()
                    _render_scoring_visuals(records, name_map)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Get score leaderboard failed: {exc}")

    with tabs[3]:
        st.caption("Interactive charts for leaderboard and company scoring outputs.")
        visual_limit = st.number_input(
            "Leaderboard Limit",
            min_value=1,
            max_value=200,
            value=50,
            key="scoring_visual_limit",
        )
        if st.button("Load Visual Dashboard", key="scoring_visual_load"):
            try:
                url = _scoring_url(api_base, scoring_prefix, "/results")
                out = _request_json(
                    "GET",
                    url,
                    params={"limit": int(visual_limit)},
                    timeout=timeout,
                    headers=headers,
                    verify=verify_tls,
                )
                st.session_state["scoring_last_results"] = out
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Load scoring visuals failed: {exc}")

        records = _as_scoring_records(st.session_state.get("scoring_last_results"))
        ids = [str(item.get("company_id", "")) for item in records]
        name_map = _hydrate_company_names(
            ids,
            api_base,
            api_prefix,
            int(timeout),
            headers,
            verify_tls,
        ) if ids else {}
        _render_scoring_visuals(records, name_map)

        company_record = _as_scoring_records(st.session_state.get("scoring_last_company"))
        if company_record:
            company_ids = [str(company_record[0].get("company_id", ""))]
            company_name_map = _hydrate_company_names(
                company_ids,
                api_base,
                api_prefix,
                int(timeout),
                headers,
                verify_tls,
            )
            st.divider()
            _render_company_breakdown(company_record[0], company_name_map)

# ============================================================
# Scripts
# ============================================================

with ops_tabs[9]:
    scripts = _list_repo_scripts()

    st.caption("Run repository scripts from the Streamlit UI (working directory: repo root).")
    st.write(f"Scripts directory: `{SCRIPTS_DIR}`")

    if not scripts:
        st.warning("No scripts found")
    else:
        st.dataframe([{"script": s} for s in scripts], use_container_width=True)

        script_name = st.selectbox("Script", scripts, key="scripts_selected")
        script_args = st.text_input("Arguments", value="", key="scripts_args")
        script_timeout = st.number_input("Timeout (seconds)", min_value=1, max_value=7200, value=600, key="scripts_timeout")

        if st.button("Run Script", key="scripts_run_btn"):
            script_path = SCRIPTS_DIR / script_name
            if not script_path.exists():
                st.error(f"Script not found: {script_path}")
            else:
                cmd = [sys.executable, str(script_path)]
                if script_args.strip():
                    try:
                        cmd.extend(shlex.split(script_args.strip(), posix=(os.name != "nt")))
                    except ValueError as exc:
                        st.error(f"Invalid arguments: {exc}")
                        st.stop()

                st.code("$ " + " ".join(shlex.quote(x) for x in cmd), language="bash")
                try:
                    proc = subprocess.run(
                        cmd,
                        cwd=str(ROOT_DIR),
                        capture_output=True,
                        text=True,
                        timeout=int(script_timeout),
                    )
                    st.write(f"Exit code: {proc.returncode}")
                    if proc.stdout:
                        st.subheader("STDOUT")
                        st.code(proc.stdout)
                    if proc.stderr:
                        st.subheader("STDERR")
                        st.code(proc.stderr)
                except subprocess.TimeoutExpired as exc:
                    st.error(f"Script timed out after {script_timeout} seconds")
                    if exc.stdout:
                        st.subheader("Partial STDOUT")
                        st.code(exc.stdout)
                    if exc.stderr:
                        st.subheader("Partial STDERR")
                        st.code(exc.stderr)
                except Exception as exc:
                    st.error(f"Script execution failed: {exc}")

        st.divider()
        st.caption("Common examples")
        st.code(
            "python scripts/collect_evidence.py --companies all\n"
            "python scripts/collect_signals.py --companies NVDA,JPM\n"
            "python scripts/run_scoring_engine.py --batch --tickers NVDA,JPM --version v1.0"
        )


# ============================================================
# Raw API
# ============================================================

with ops_tabs[10]:
    st.caption("Manual API console for any path/method.")

    method = st.selectbox("Method", ["GET", "POST", "PUT", "PATCH", "DELETE"], key="raw_method")
    path = st.text_input("Path", value="/companies", key="raw_path")
    include_prefix = st.checkbox("Include API Prefix", value=True, key="raw_include_prefix")
    params_text = st.text_area("Query Params JSON", value="{}", key="raw_params")
    body_text = st.text_area("Request Body JSON", value="{}", key="raw_body")

    if st.button("Send Request", key="raw_send_btn"):
        if not path.strip().startswith("/"):
            st.error("Path must start with '/'")
        else:
            ok_params, params_obj = _parse_json_input("Query Params", params_text)
            if not ok_params:
                st.stop()
            if not isinstance(params_obj, dict):
                st.error("Query Params JSON must be an object")
                st.stop()

            req_kwargs: dict[str, Any] = {
                "timeout": timeout,
                "headers": headers,
                "verify": verify_tls,
                "params": params_obj,
            }

            if method in {"POST", "PUT", "PATCH", "DELETE"}:
                ok_body, body_obj = _parse_json_input("Request Body", body_text)
                if not ok_body:
                    st.stop()
                if body_text.strip():
                    req_kwargs["json"] = body_obj

            try:
                url = _api_url(api_base, api_prefix, path.strip(), include_prefix=include_prefix)
                resp = _request(method, url, **req_kwargs)
                st.write(f"Status: {resp.status_code}")
                if not resp.ok:
                    raise requests.HTTPError(resp.text, response=resp)
                if resp.text.strip():
                    try:
                        _show_payload(resp.json())
                    except ValueError:
                        st.code(resp.text)
                else:
                    st.info("No content")
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Raw request failed: {exc}")
