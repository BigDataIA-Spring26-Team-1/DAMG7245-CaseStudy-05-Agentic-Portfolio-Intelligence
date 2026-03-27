"""
PE Org-AI-R Portfolio Intelligence Dashboard
=============================================
Streamlit application for enterprise portfolio intelligence:
  - Portfolio and company analytics
  - Assessment history tracking
  - Evidence-backed justifications
  - Agentic due-diligence workflow
  - Strategic output generation
  - MCP tooling and observability

Bonus Extensions:
  - Mem0 Semantic Memory (+5 pts)
  - Investment Tracker with ROI (+5 pts)
  - IC Memo Generator (.docx) (+5 pts)
  - LP Letter Generator (.docx) (+5 pts)

All data is sourced from connected live services through the platform APIs.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st


# ---------------------------------------------------------------------------
# Path setup — make sure `app` package is importable
# ---------------------------------------------------------------------------
_STREAMLIT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Keep streamlit script directory from shadowing backend package imports.
sys.path = [p for p in sys.path if Path(p or ".").resolve() != _STREAMLIT_DIR]

# Force project root at the front so `import app...` resolves to backend package.
project_root_str = str(_PROJECT_ROOT)
sys.path = [p for p in sys.path if p != project_root_str]
sys.path.insert(0, project_root_str)

# Streamlit can load this file as module `app`, which can shadow the
# backend package named `app` in project root.
_loaded_app = sys.modules.get("app")
if _loaded_app is not None and not hasattr(_loaded_app, "__path__"):
    _loaded_file = str(getattr(_loaded_app, "__file__", ""))
    if _loaded_file and Path(_loaded_file).resolve() == Path(__file__).resolve():
        del sys.modules["app"]


# ---------------------------------------------------------------------------
# Lazy imports from the platform (only resolved when actually needed)
# ---------------------------------------------------------------------------
from app.dashboard.evidence_display import (  # noqa: E402
    render_company_evidence_panel,
    render_evidence_summary_table,
    LEVEL_COLORS,
    LEVEL_NAMES,
)
from app.services.analytics.fund_air import SECTOR_BENCHMARKS  # noqa: E402

DEFAULT_API_BASE = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
DEFAULT_API_PREFIX = os.getenv("API_PREFIX", "/api/v1")
FUND_DISPLAY_ALIASES: dict[str, str] = {
    "growth_fund_v": "Flagship Growth Portfolio",
    "default": "Core Portfolio",
}

# ======================================================================
# Page config
# ======================================================================
st.set_page_config(
    page_title="PE Org-AI-R Portfolio Intelligence",
    page_icon="\U0001F4C8",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ======================================================================
# Custom CSS — dark professional theme
# ======================================================================
st.markdown(
    """
<style>
    /* ---- global overrides ---- */
    .block-container { padding-top: 2.6rem; }
    h1, h2, h3 { font-family: 'Segoe UI', system-ui, sans-serif; }

    /* metric cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #0e1726 60%, #142236);
        border: 1px solid rgba(47,124,246,0.25);
        border-radius: 10px;
        padding: 14px 18px;
    }
    div[data-testid="stMetric"] label { color: #7a93b4 !important; font-size: 0.82rem; }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] { color: #edf3f9 !important; }

    /* tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 6px; padding-top: 0.25rem; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 10px 20px;
        font-weight: 600;
        min-height: 42px;
        line-height: 1.25;
        align-items: center;
    }

    /* dashboard band */
    .dashboard-band {
        background: linear-gradient(135deg, #0b1120 0%, #142236 100%);
        border: 1px solid rgba(47,124,246,0.2);
        border-radius: 12px;
        padding: 22px 28px;
        margin-bottom: 22px;
    }
    .dashboard-band-title {
        font-size: 1.6rem; font-weight: 700; color: #edf3f9;
        margin-bottom: 4px;
    }
    .dashboard-band-copy { color: #7a93b4; font-size: 0.92rem; }
    .insight-kicker {
        font-size: 0.72rem; text-transform: uppercase; letter-spacing: 2px;
        color: #2f7cf6; margin-bottom: 6px; font-weight: 600;
    }

    /* status badges */
    .status-badge {
        display: inline-block; padding: 3px 12px; border-radius: 12px;
        font-weight: 600; font-size: 0.78rem;
    }
    .badge-success { background: #064e3b; color: #34d399; }
    .badge-warning { background: #78350f; color: #fbbf24; }
    .badge-danger  { background: #7f1d1d; color: #f87171; }
    .badge-info    { background: #1e3a5f; color: #60a5fa; }

    /* agent timeline */
    .agent-step {
        border-left: 3px solid #2f7cf6;
        padding: 8px 16px;
        margin-bottom: 10px;
        background: rgba(47,124,246,0.06);
        border-radius: 0 8px 8px 0;
    }
    .agent-step-name { font-weight: 700; color: #60a5fa; font-size: 0.9rem; }
    .agent-step-msg  { color: #cbd5e1; font-size: 0.84rem; margin-top: 2px; }
</style>
""",
    unsafe_allow_html=True,
)


# ======================================================================
# Helpers
# ======================================================================
def _wk(suffix: str) -> str:
    """Scoped widget key."""
    return f"platform_{suffix}"


def _pretty_fund_name(raw_value: str) -> str:
    return str(raw_value or "").strip().replace("_", " ").title()


def _display_fund_name(fund_id: str, backend_name: str | None = None) -> str:
    alias = FUND_DISPLAY_ALIASES.get(str(fund_id or "").strip())
    if alias:
        return alias
    candidate = str(backend_name or "").strip()
    if candidate and candidate != str(fund_id or "").strip():
        return _pretty_fund_name(candidate)
    return _pretty_fund_name(fund_id)


def _join_url(base: str, path: str) -> str:
    return base.rstrip("/") + "/" + path.lstrip("/")


def _api_url(path: str) -> str:
    return _join_url(DEFAULT_API_BASE, _join_url(DEFAULT_API_PREFIX, path))


def _request_json(method: str, url: str, **kwargs: Any) -> Any:
    response = requests.request(method, url, timeout=kwargs.pop("timeout", 60), **kwargs)
    if not response.ok:
        raise requests.HTTPError(response.text, response=response)
    if response.status_code == 204 or not response.text.strip():
        return None
    return response.json()


def _error_message(exc: Exception) -> str:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        try:
            payload = exc.response.json()
            detail = payload.get("detail") if isinstance(payload, dict) else payload
            if detail:
                return f"{exc.response.status_code}: {detail}"
        except ValueError:
            pass
        return f"{exc.response.status_code}: {exc.response.text.strip()}"
    return str(exc)


def _platform_get_json(path: str, *, params: dict[str, Any] | None = None) -> Any:
    return _request_json("GET", _api_url(path), params=params)


def _platform_post_json(path: str, payload: dict[str, Any] | None = None) -> Any:
    return _request_json("POST", _api_url(path), json=payload or {})


def _style_plotly(fig: go.Figure, *, legend_title: str | None = None) -> go.Figure:
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#edf3f9"),
        margin=dict(l=0, r=0, t=48, b=0),
        legend_title_text=legend_title,
    )
    fig.update_xaxes(gridcolor="rgba(147,168,191,0.16)")
    fig.update_yaxes(gridcolor="rgba(147,168,191,0.12)")
    return fig


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _find_company_by_ticker(ticker: str) -> dict[str, Any] | None:
    payload = _platform_get_json(
        "/companies",
        params={"q": ticker, "page": 1, "page_size": 100},
    )
    items = payload.get("items", []) if isinstance(payload, dict) else []
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("ticker", "")).strip().upper() == ticker.upper():
            return item
    return None


def _ensure_company_for_ticker(ticker: str, company_name: str | None = None) -> dict[str, Any]:
    existing = _find_company_by_ticker(ticker)
    if existing:
        return existing

    payload = {
        "name": (company_name or "").strip() or f"{ticker} Corporation",
        "ticker": ticker.upper(),
        "position_factor": 0.0,
    }
    created = _platform_post_json("/companies", payload)
    if not isinstance(created, dict) or not created.get("id"):
        raise RuntimeError("Company creation failed for external ticker analysis")
    return created


def _start_collection_job(job_type: str, ticker: str) -> str:
    if job_type not in {"evidence", "signals"}:
        raise ValueError(f"Unsupported collection job type: {job_type}")
    payload = _request_json(
        "POST",
        _api_url(f"/collection/{job_type}"),
        params={"companies": ticker.upper()},
        timeout=60,
    )
    task_id = (payload or {}).get("task_id") if isinstance(payload, dict) else None
    if not task_id:
        raise RuntimeError(f"Collection job ({job_type}) did not return task_id")
    return str(task_id)


def _wait_for_collection_task(task_id: str, timeout_seconds: int = 240, poll_seconds: float = 2.0) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    latest: dict[str, Any] = {}
    while time.time() < deadline:
        payload = _platform_get_json(f"/collection/tasks/{task_id}")
        latest = payload if isinstance(payload, dict) else {}
        status = str(latest.get("status", "")).strip().lower()
        if status in {"done", "failed"}:
            return latest
        time.sleep(poll_seconds)
    return latest


# ======================================================================
# Data loaders (cached)
# ======================================================================
@st.cache_data(ttl=300, show_spinner=False)
def load_portfolio(_fund_id: str) -> pd.DataFrame:
    """Load portfolio from the backend platform API."""
    payload = _platform_get_json(f"/platform/portfolio/{_fund_id}")
    if isinstance(payload, dict) and payload.get("error"):
        raise RuntimeError(str(payload.get("error")))
    companies = payload.get("companies", []) if isinstance(payload, dict) else []
    if not companies:
        return pd.DataFrame(
            columns=[
                "company_id",
                "ticker",
                "name",
                "sector",
                "org_air",
                "vr_score",
                "hr_score",
                "synergy_score",
                "delta",
                "evidence_count",
                "enterprise_value_mm",
                "ev_source",
            ]
        )

    records = []
    for company in companies:
        if not isinstance(company, dict):
            continue
        records.append(
            {
                "company_id": company.get("company_id"),
                "ticker": company.get("ticker"),
                "name": company.get("name"),
                "sector": company.get("sector"),
                "org_air": float(company.get("org_air", 0.0) or 0.0),
                "vr_score": float(company.get("vr_score", 0.0) or 0.0),
                "hr_score": float(company.get("hr_score", 0.0) or 0.0),
                "synergy_score": float(company.get("synergy_score", 0.0) or 0.0),
                "delta": float(
                    company.get("delta", company.get("delta_since_entry", 0.0)) or 0.0
                ),
                "evidence_count": int(company.get("evidence_count", 0) or 0),
                "enterprise_value_mm": float(company.get("enterprise_value_mm", 0.0) or 0.0),
                "ev_source": company.get("enterprise_value_source", "unknown"),
            }
        )
    return pd.DataFrame(records)


@st.cache_data(ttl=300, show_spinner=False)
def load_funds() -> list[dict[str, str]]:
    """Load available funds from the backend platform API."""
    payload = _platform_get_json("/platform/funds")
    raw_funds = payload.get("funds", []) if isinstance(payload, dict) else []
    funds: list[dict[str, str]] = []
    for item in raw_funds:
        if not isinstance(item, dict):
            continue
        fund_id = str(item.get("fund_id", "") or "").strip()
        if not fund_id:
            continue
        funds.append({"fund_id": fund_id, "name": str(item.get("name", fund_id) or fund_id)})
    return funds


def call_mcp_tool(tool_name: str, arguments: dict) -> str:
    """Call a backend-owned MCP-compatible tool over HTTP."""
    payload = _platform_post_json(
        f"/platform/mcp/tools/{tool_name}",
        {"arguments": arguments},
    )
    return json.dumps(payload, default=str)


# ======================================================================
# Sidebar
# ======================================================================
st.sidebar.markdown(
    """
    <div style='text-align:center; padding:8px 0 16px;'>
        <span style='font-size:1.5rem;'>📊</span><br>
        <b style='font-size:1.1rem; color:#edf3f9;'>PE Org-AI-R</b><br>
        <span style='font-size:0.72rem; color:#7a93b4; letter-spacing:1.5px;'>
            AGENTIC PORTFOLIO INTELLIGENCE
        </span>
    </div>
    """,
    unsafe_allow_html=True,
)
st.sidebar.divider()
st.sidebar.markdown("### Portfolio Scope")
st.sidebar.caption("These selectors filter all dashboards, agents, and bonus outputs.")

try:
    fund_options = load_funds()
except Exception as e:
    st.error(f"Failed to connect to backend services: {_error_message(e)}")
    if isinstance(e, (requests.ConnectionError, requests.Timeout)):
        st.info(
            "Backend API is not reachable. Start FastAPI first in a separate terminal:\n"
            "`cd pe-org-air-platform`\n"
            "`poetry run uvicorn app.main:app --host 127.0.0.1 --port 8000`\n"
            "Then refresh this page."
        )
    else:
        st.info("Backend returned an error while loading available funds. Check backend logs and retry.")
    st.stop()

if not fund_options:
    st.error("No funds are currently available from backend sources.")
    st.info("Load portfolio data and refresh.")
    st.stop()

fund_ids = [item["fund_id"] for item in fund_options]
if len(fund_ids) > 1 and "default" in fund_ids:
    fund_ids = [fid for fid in fund_ids if fid != "default"]
fund_labels = {
    item["fund_id"]: _display_fund_name(item["fund_id"], item.get("name")) for item in fund_options
}
current_fund = st.session_state.get(_wk("fund_id"))
if current_fund in fund_ids:
    fund_index = fund_ids.index(current_fund)
elif "growth_fund_v" in fund_ids:
    fund_index = fund_ids.index("growth_fund_v")
else:
    fund_index = 0
fund_id = st.sidebar.selectbox(
    "Fund",
    options=fund_ids,
    index=fund_index,
    format_func=lambda value: fund_labels.get(value, _pretty_fund_name(value)),
    key=_wk("fund_id"),
)
selected_fund_name = fund_labels.get(fund_id, _display_fund_name(fund_id, fund_id))

st.sidebar.divider()
st.sidebar.caption("MCP + Multi-Agent Intelligence Platform")
st.sidebar.caption(f"Session: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M')} UTC")

# Optional ad-hoc company workflow (kept separate from fund-scoped analytics).
st.sidebar.divider()
st.sidebar.markdown("### External Ticker Analysis")
st.sidebar.caption("Analyze a company outside the selected portfolio without changing fund-level metrics.")
external_ticker = st.sidebar.text_input(
    "Ticker Symbol",
    value="",
    placeholder="e.g., MSFT",
    key=_wk("external_ticker"),
).strip().upper()
external_name = st.sidebar.text_input(
    "Company Name (optional)",
    value="",
    placeholder="Optional display name",
    key=_wk("external_company_name"),
).strip()

if st.sidebar.button("Analyze External Ticker", key=_wk("analyze_external_ticker")):
    ticker_pattern = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
    if not external_ticker:
        st.sidebar.warning("Enter a ticker symbol to run ad-hoc analysis.")
    elif not ticker_pattern.fullmatch(external_ticker):
        st.sidebar.error("Invalid ticker format. Use uppercase ticker symbols, e.g., MSFT or BRK.B.")
    else:
        with st.spinner(f"Running ad-hoc analysis for {external_ticker}..."):
            try:
                company_obj = _ensure_company_for_ticker(external_ticker, external_name)
                company_id = str(company_obj.get("id") or "")
                company_label_name = str(company_obj.get("name") or external_name or external_ticker)
                if not company_id:
                    raise RuntimeError("Unable to resolve company id for external ticker")

                evidence_task_id = _start_collection_job("evidence", external_ticker)
                evidence_status = _wait_for_collection_task(evidence_task_id)

                signals_task_id = _start_collection_job("signals", external_ticker)
                signals_status = _wait_for_collection_task(signals_task_id)

                _request_json(
                    "POST",
                    _api_url(f"/scoring/compute/{company_id}"),
                    params={"version": "v1.0"},
                    timeout=180,
                )
                _request_json(
                    "GET",
                    _api_url(f"/scoring/results/{company_id}"),
                    timeout=60,
                )

                st.session_state[_wk("external_company")] = {
                    "company_id": company_id,
                    "ticker": external_ticker,
                    "name": company_label_name,
                    "evidence_task": evidence_status,
                    "signals_task": signals_status,
                }
                st.session_state[_wk("company")] = company_id
                load_portfolio.clear()
                st.sidebar.success(f"Ad-hoc analysis completed for {external_ticker}.")
                st.rerun()
            except Exception as ex:
                st.sidebar.error(f"Ad-hoc analysis failed for {external_ticker}: {_error_message(ex)}")

# ======================================================================
# Load portfolio data
# ======================================================================
try:
    portfolio_df = load_portfolio(fund_id)
    st.sidebar.success(f"✓ {len(portfolio_df)} companies loaded from live services")
except Exception as e:
    st.error(f"Failed to connect to backend services: {_error_message(e)}")
    if isinstance(e, (requests.ConnectionError, requests.Timeout)):
        st.info(
            "Backend API is not reachable. Start FastAPI first in a separate terminal:\n"
            "`cd pe-org-air-platform`\n"
            "`poetry run uvicorn app.main:app --host 127.0.0.1 --port 8000`\n"
            "Then refresh this page."
        )
    else:
        st.info("Backend returned an error while loading the selected fund. Check backend logs and retry.")
    st.stop()

if portfolio_df.empty:
    st.error(f"No companies found for fund '{selected_fund_name}' from backend sources.")
    st.stop()

company_options: list[str] = []
company_labels: dict[str, str] = {}
for row in portfolio_df.itertuples(index=False):
    company_id = str(getattr(row, "company_id", "") or "").strip()
    if not company_id or company_id in company_labels:
        continue
    ticker = str(getattr(row, "ticker", "") or "").strip()
    name = str(getattr(row, "name", "") or "").strip()
    if ticker and name:
        label = f"{ticker} - {name}"
    elif ticker:
        label = ticker
    elif name:
        label = name
    else:
        label = company_id
    company_options.append(company_id)
    company_labels[company_id] = label

external_company = st.session_state.get(_wk("external_company"))
if isinstance(external_company, dict):
    ext_company_id = str(external_company.get("company_id", "")).strip()
    ext_ticker = str(external_company.get("ticker", "")).strip()
    ext_name = str(external_company.get("name", "")).strip()
    if ext_company_id and ext_company_id not in company_labels:
        ext_label_core = f"{ext_ticker} - {ext_name}".strip(" -")
        company_options.append(ext_company_id)
        company_labels[ext_company_id] = f"{ext_label_core} (External)"

current_company = st.session_state.get(_wk("company"))
company_index = company_options.index(current_company) if current_company in company_options else 0
company_api_id = st.sidebar.selectbox(
    "Focus Company",
    options=company_options,
    index=company_index,
    format_func=lambda value: company_labels.get(value, value),
    key=_wk("company"),
)
company_for_detail = company_labels.get(company_api_id, company_api_id)
if isinstance(external_company, dict) and str(external_company.get("company_id", "")) == company_api_id:
    st.sidebar.info("External company selected. Fund-level portfolio metrics remain based on the selected fund.")

st.sidebar.caption("Scores update after scoring runs; use refresh below to recompute.")
if st.sidebar.button("Recompute Portfolio Scores", key=_wk("recompute_scores")):
    with st.spinner("Running scoring pipeline for all companies in scope..."):
        recompute_errors: list[str] = []
        for cid in company_options:
            try:
                _request_json(
                    "POST",
                    _api_url(f"/scoring/compute/{cid}"),
                    params={"version": "v1.0"},
                    timeout=180,
                )
            except Exception as ex:
                recompute_errors.append(f"{cid}: {_error_message(ex)}")

        load_portfolio.clear()
        if recompute_errors:
            st.sidebar.warning(f"Recompute finished with {len(recompute_errors)} errors.")
        else:
            st.sidebar.success("Recompute finished for all companies.")
    st.rerun()

with st.sidebar.expander("Intelligence", expanded=False):
    st.markdown(
        """
`Features`
- Portfolio data integration
- MCP tools, resources, and prompts
- Assessment history tracking
- Evidence display + portfolio dashboard
- LangGraph specialist agents + supervisor
- Agentic due-diligence workflow with HITL
- Fund-AI-R calculator
- Prometheus metrics
- Semantic memory
- Investment tracker with ROI
- IC memo generator (Word)
- LP letter generator (Word)
"""
    )

# ======================================================================
# Main navigation tabs
# ======================================================================
st.markdown(
    """
<div class="dashboard-band" style="margin-top: 2px;">
  <div class="insight-kicker">Platform Scope</div>
  <div class="dashboard-band-copy">
    This UI includes core analytics, workflow orchestration, and strategic output capabilities.
    The left sidebar values are scope filters only.
    Use tabs for portfolio analytics, evidence drilldown, assessment history, agentic workflow, strategic outputs
    (memory, investment tracker, IC memo, LP letter), and MCP/metrics observability.
  </div>
</div>
""",
    unsafe_allow_html=True,
)

tab_portfolio, tab_company, tab_history, tab_agents, tab_strategic, tab_infra = st.tabs(
    [
        "📈 Portfolio Overview",
        "🔍 Company Drilldown",
        "📅 Assessment History",
        "🤖 Agentic Due Diligence",
        "💼 Strategic Outputs",
        "⚙️ MCP & Metrics",
    ]
)

# ======================================================================
# TAB 1: Portfolio Overview + Fund-AI-R
# ======================================================================
with tab_portfolio:
    # Header banner
    sectors = sorted(
        {str(v) for v in portfolio_df.get("sector", pd.Series(dtype=str)).dropna().tolist() if str(v).strip()}
    )
    sector_text = " · ".join(sectors[:5]) if sectors else "Awaiting sector data"

    st.markdown(
        f"""
        <div class="dashboard-band">
            <div class="insight-kicker">Executive Portfolio Dashboard</div>
            <div class="dashboard-band-title">{selected_fund_name}</div>
            <p class="dashboard-band-copy">
                Focus company: <b>{company_for_detail}</b> &nbsp;|&nbsp;
                Sectors: {sector_text} &nbsp;|&nbsp;
                All data sourced live from connected enterprise services.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---- Fund-AI-R calculation ----
    if not portfolio_df.empty and float(portfolio_df["enterprise_value_mm"].sum()) > 0:
        fund_air = float(
            (portfolio_df["org_air"] * portfolio_df["enterprise_value_mm"]).sum()
            / portfolio_df["enterprise_value_mm"].sum()
        )
    else:
        fund_air = portfolio_df["org_air"].mean() if not portfolio_df.empty else 0.0

    avg_delta = portfolio_df["delta"].mean() if not portfolio_df.empty else 0.0
    avg_vr = portfolio_df["vr_score"].mean() if not portfolio_df.empty else 0.0
    leaders_count = int((portfolio_df["org_air"] >= 70).sum()) if not portfolio_df.empty else 0
    laggards_count = int((portfolio_df["org_air"] < 50).sum()) if not portfolio_df.empty else 0

    # KPI row
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Fund-AI-R", f"{fund_air:.1f}")
    m2.metric("Companies", len(portfolio_df))
    m3.metric("Avg V^R", f"{avg_vr:.1f}")
    m4.metric("Avg Δ Since Entry", f"{avg_delta:+.1f}")
    m5.metric("AI Leaders (≥70)", leaders_count)
    m6.metric("AI Laggards (<50)", laggards_count)

    if not portfolio_df.empty:
        # ---- Charts ----
        chart_col, rank_col = st.columns([1.2, 0.8])

        with chart_col:
            fig_scatter = px.scatter(
                portfolio_df,
                x="vr_score",
                y="hr_score",
                size="org_air",
                color="sector",
                hover_name="name",
                labels={"vr_score": "V^R (Idiosyncratic)", "hr_score": "H^R (Systematic)", "org_air": "Org-AI-R"},
                title="Portfolio AI-Readiness Map",
                color_discrete_sequence=["#2f7cf6", "#28a59c", "#6e86ff", "#c8a977", "#f08a5d", "#a78bfa"],
            )
            fig_scatter.add_hline(y=60, line_dash="dash", line_color="#c8a977", annotation_text="H^R Threshold")
            fig_scatter.add_vline(x=60, line_dash="dash", line_color="#c8a977", annotation_text="V^R Threshold")
            fig_scatter.update_traces(marker=dict(line=dict(width=1, color="#09111c"), opacity=0.9))
            st.plotly_chart(
                _style_plotly(fig_scatter, legend_title="Sector"),
                width="stretch",
                config={"displayModeBar": False},
            )

        with rank_col:
            top_companies = portfolio_df.sort_values("org_air", ascending=False).head(8)
            fig_bar = px.bar(
                top_companies.sort_values("org_air", ascending=True),
                x="org_air",
                y="ticker",
                orientation="h",
                color="delta",
                color_continuous_scale=["#f08a5d", "#2f7cf6", "#3ecf8e"],
                labels={"org_air": "Org-AI-R", "ticker": "", "delta": "Δ"},
                title="Top Companies by Org-AI-R",
            )
            fig_bar.update_layout(coloraxis_colorbar_title="Δ")
            st.plotly_chart(
                _style_plotly(fig_bar), width="stretch", config={"displayModeBar": False}
            )

        # ---- Sector breakdown (Fund-AI-R enrichment) ----
        st.subheader("Sector Concentration & Quartile Distribution")
        sec_col, q_col = st.columns(2)

        with sec_col:
            sector_agg = (
                portfolio_df.groupby("sector")
                .agg(
                    count=("ticker", "count"),
                    avg_org_air=("org_air", "mean"),
                    total_ev=("enterprise_value_mm", "sum"),
                )
                .reset_index()
            )
            fig_sector = px.treemap(
                sector_agg,
                path=["sector"],
                values="total_ev",
                color="avg_org_air",
                color_continuous_scale="YlGnBu",
                title="Sector Allocation by EV (color = Avg Org-AI-R)",
            )
            fig_sector.update_layout(margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig_sector, width="stretch", config={"displayModeBar": False})

        with q_col:
            # Quartile distribution from sector benchmarks
            def _quartile(score: float, sector: str) -> int:
                bench = SECTOR_BENCHMARKS.get(sector, SECTOR_BENCHMARKS.get("technology", {}))
                if score >= bench.get("q1", 75):
                    return 1
                if score >= bench.get("q2", 65):
                    return 2
                if score >= bench.get("q3", 55):
                    return 3
                return 4

            portfolio_df["quartile"] = portfolio_df.apply(
                lambda r: _quartile(r["org_air"], r["sector"]), axis=1
            )
            q_dist = (
                portfolio_df["quartile"]
                .value_counts()
                .reindex([1, 2, 3, 4], fill_value=0)
                .sort_index()
            )
            fig_q = px.bar(
                x=[f"Q{q}" for q in q_dist.index],
                y=q_dist.values,
                color=[f"Q{q}" for q in q_dist.index],
                color_discrete_map={"Q1": "#14b8a6", "Q2": "#22c55e", "Q3": "#eab308", "Q4": "#ef4444"},
                title="Quartile Distribution (Sector-Adjusted)",
                labels={"x": "Quartile", "y": "Companies"},
            )
            st.plotly_chart(
                _style_plotly(fig_q), width="stretch", config={"displayModeBar": False}
            )

        # ---- Portfolio Scoreboard ----
        st.subheader("Portfolio Scoreboard")
        display_df = portfolio_df[
            ["ticker", "name", "sector", "org_air", "vr_score", "hr_score", "delta", "evidence_count", "enterprise_value_mm"]
        ].copy()
        display_df.columns = ["Ticker", "Company", "Sector", "Org-AI-R", "V^R", "H^R", "Δ Entry", "Evidence", "EV ($M)"]

        st.dataframe(
            display_df.style.format(
                {"Org-AI-R": "{:.1f}", "V^R": "{:.1f}", "H^R": "{:.1f}", "Δ Entry": "{:+.1f}", "EV ($M)": "{:.1f}"}
            ),
            width="stretch",
            hide_index=True,
        )

        # ---- Advanced Analytics ----
        st.subheader("Advanced Analytics")
        aa_col1, aa_col2 = st.columns(2)

        with aa_col1:
            sector_view = (
                portfolio_df.groupby("sector")
                .agg(avg_org_air=("org_air", "mean"), companies=("ticker", "count"))
                .reset_index()
            )
            sector_view["sector_benchmark_q2"] = sector_view["sector"].map(
                lambda s: float(SECTOR_BENCHMARKS.get(str(s), {}).get("q2", 65.0))
            )
            sector_view["gap_vs_benchmark"] = sector_view["avg_org_air"] - sector_view["sector_benchmark_q2"]
            fig_gap = px.bar(
                sector_view.sort_values("gap_vs_benchmark", ascending=False),
                x="sector",
                y="gap_vs_benchmark",
                color="gap_vs_benchmark",
                color_continuous_scale=["#ef4444", "#eab308", "#22c55e"],
                labels={"sector": "Sector", "gap_vs_benchmark": "Avg Org-AI-R vs Q2 Benchmark"},
                title="Sector Benchmark Gap",
                hover_data={"avg_org_air": ":.1f", "sector_benchmark_q2": ":.1f", "companies": True},
            )
            fig_gap.add_hline(y=0, line_dash="dash", line_color="#93a8bf")
            st.plotly_chart(
                _style_plotly(fig_gap),
                width="stretch",
                config={"displayModeBar": False},
            )

        with aa_col2:
            risk_view = portfolio_df.copy()
            risk_view["vr_hr_spread"] = risk_view["vr_score"] - risk_view["hr_score"]
            fig_risk = px.scatter(
                risk_view,
                x="vr_hr_spread",
                y="delta",
                size="enterprise_value_mm",
                color="sector",
                hover_name="name",
                labels={
                    "vr_hr_spread": "V^R - H^R Spread",
                    "delta": "Δ Since Entry",
                    "enterprise_value_mm": "EV ($M)",
                },
                title="Risk-Change Map",
                color_discrete_sequence=["#2f7cf6", "#28a59c", "#6e86ff", "#c8a977", "#f08a5d", "#a78bfa"],
            )
            fig_risk.add_hline(y=0, line_dash="dash", line_color="#93a8bf")
            fig_risk.add_vline(x=0, line_dash="dash", line_color="#93a8bf")
            st.plotly_chart(
                _style_plotly(fig_risk, legend_title="Sector"),
                width="stretch",
                config={"displayModeBar": False},
            )

        corr_cols = ["org_air", "vr_score", "hr_score", "delta", "evidence_count", "enterprise_value_mm"]
        corr_df = portfolio_df[corr_cols].corr(numeric_only=True).round(2)
        fig_corr = px.imshow(
            corr_df,
            text_auto=True,
            color_continuous_scale="RdBu_r",
            zmin=-1,
            zmax=1,
            title="Metric Correlation Heatmap",
        )
        fig_corr.update_layout(xaxis_title="", yaxis_title="")
        st.plotly_chart(
            _style_plotly(fig_corr),
            width="stretch",
            config={"displayModeBar": False},
        )

# ======================================================================
# TAB 2: Company Drilldown
# ======================================================================
with tab_company:
    st.header(f"Company Drilldown: {company_for_detail}")

    try:
        score_payload = call_mcp_tool("calculate_org_air_score", {"company_id": company_api_id})
        score_data = json.loads(score_payload)

        # Score snapshot
        st.subheader("Current Score Snapshot")
        s1, s2, s3, s4 = st.columns(4)
        org_air_val = _safe_float(score_data.get("org_air", score_data.get("org_air_score", 0.0)))
        vr_val = _safe_float(score_data.get("vr_score", 0.0))
        hr_val = _safe_float(score_data.get("hr_score", 0.0))
        synergy_val = _safe_float(score_data.get("synergy_score", 0.0))

        s1.metric("Org-AI-R", f"{org_air_val:.1f}")
        s2.metric("V^R (Idiosyncratic)", f"{vr_val:.1f}")
        s3.metric("H^R (Systematic)", f"{hr_val:.1f}")
        s4.metric("Synergy Score", f"{synergy_val:.1f}")

        # Confidence interval
        ci = score_data.get("confidence_interval", [0, 0])
        if ci and len(ci) == 2:
            st.caption(f"95% Confidence Interval: [{_safe_float(ci[0]):.1f}, {_safe_float(ci[1]):.1f}]")

        # Dimension scores radar chart
        dimension_scores = score_data.get("dimension_scores", {})
        if dimension_scores:
            st.subheader("Dimension Breakdown")

            dim_col, radar_col = st.columns([0.4, 0.6])

            with dim_col:
                dim_df = pd.DataFrame(
                    [
                        {
                            "Dimension": d.replace("_", " ").title(),
                            "Score": _safe_float(s),
                            "Status": "✅" if _safe_float(s) >= 60 else "⚠️" if _safe_float(s) >= 40 else "🔴",
                        }
                        for d, s in dimension_scores.items()
                    ]
                )
                st.dataframe(dim_df, width="stretch", hide_index=True)

            with radar_col:
                dims = list(dimension_scores.keys())
                scores = [_safe_float(dimension_scores[d]) for d in dims]
                labels = [d.replace("_", " ").title() for d in dims]

                fig_radar = go.Figure()
                fig_radar.add_trace(
                    go.Scatterpolar(
                        r=scores + [scores[0]],
                        theta=labels + [labels[0]],
                        fill="toself",
                        fillcolor="rgba(47,124,246,0.15)",
                        line=dict(color="#2f7cf6", width=2),
                        name=company_for_detail,
                    )
                )
                fig_radar.add_trace(
                    go.Scatterpolar(
                        r=[60] * (len(labels) + 1),
                        theta=labels + [labels[0]],
                        line=dict(color="#c8a977", dash="dash", width=1),
                        name="Threshold (60)",
                    )
                )
                fig_radar.update_layout(
                    polar=dict(
                        radialaxis=dict(visible=True, range=[0, 100], gridcolor="rgba(147,168,191,0.2)"),
                        bgcolor="rgba(0,0,0,0)",
                    ),
                    paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#edf3f9"),
                    showlegend=True,
                    margin=dict(l=60, r=60, t=30, b=30),
                )
                st.plotly_chart(fig_radar, width="stretch", config={"displayModeBar": False})

        # ---- Evidence justifications ----
        st.divider()
        st.subheader("Evidence-Backed Justifications")

        low_dimensions = [dim for dim, score in dimension_scores.items() if float(score) < 60][:3]
        if not low_dimensions and dimension_scores:
            low_dimensions = list(dimension_scores.keys())[:3]

        if low_dimensions:
            justifications: dict[str, Any] = {}
            with st.spinner(f"Generating justifications for {len(low_dimensions)} dimensions..."):
                for dim in low_dimensions:
                    try:
                        payload = call_mcp_tool(
                            "generate_justification",
                            {"company_id": company_api_id, "dimension": dim},
                        )
                        justifications[dim] = json.loads(payload)
                    except Exception as ex:
                        st.warning(f"Could not load justification for {dim}: {ex}")

            if justifications:
                render_evidence_summary_table(justifications)
                render_company_evidence_panel(company_for_detail, justifications)
        else:
            st.info("No dimension scores available to generate justifications.")

        # ---- Gap Analysis & EBITDA Projection ----
        st.divider()
        st.subheader("Gap Analysis & Value Creation")

        gap_col1, gap_col2 = st.columns(2)

        with gap_col1:
            target_air = st.slider(
                "Target Org-AI-R",
                min_value=50,
                max_value=100,
                value=75,
                step=5,
                key=_wk("target_air"),
            )
            if st.button("Run Gap Analysis", key=_wk("run_gap")):
                with st.spinner("Running gap analysis via MCP..."):
                    try:
                        gap_payload = call_mcp_tool(
                            "run_gap_analysis",
                            {"company_id": company_api_id, "target_org_air": float(target_air)},
                        )
                        gap_data = json.loads(gap_payload)
                        st.session_state[_wk("gap_result")] = gap_data
                    except Exception as ex:
                        st.error(f"Gap analysis failed: {ex}")

        with gap_col2:
            if st.button("Project EBITDA Impact", key=_wk("run_ebitda")):
                with st.spinner("Projecting EBITDA impact via MCP..."):
                    try:
                        ebitda_payload = call_mcp_tool(
                            "project_ebitda_impact",
                            {
                                "company_id": company_api_id,
                                "entry_score": float(org_air_val),
                                "target_score": float(target_air),
                                "h_r_score": float(hr_val),
                            },
                        )
                        ebitda_data = json.loads(ebitda_payload)
                        st.session_state[_wk("ebitda_result")] = ebitda_data
                    except Exception as ex:
                        st.error(f"EBITDA projection failed: {ex}")

        # Display results
        gap_result = st.session_state.get(_wk("gap_result"))
        if gap_result:
            st.markdown("**Gap Analysis Results:**")
            st.json(gap_result)

        ebitda_result = st.session_state.get(_wk("ebitda_result"))
        if ebitda_result:
            st.markdown("**EBITDA Impact Projection:**")
            scenarios = ebitda_result.get("scenarios", {})
            if scenarios:
                e1, e2, e3 = st.columns(3)
                e1.metric("Conservative", scenarios.get("conservative", "N/A"))
                e2.metric("Base Case", scenarios.get("base", "N/A"))
                e3.metric("Optimistic", scenarios.get("optimistic", "N/A"))
            risk_adj = ebitda_result.get("risk_adjusted")
            if risk_adj:
                st.metric("Risk-Adjusted Impact", risk_adj)
            if ebitda_result.get("requires_approval"):
                st.warning("⚠️ HITL Approval Required — projection exceeds threshold")

    except Exception as e:
        st.error(f"Could not load score data for {company_for_detail}: {_error_message(e)}")
        st.info("Ensure the MCP server and scoring service are running.")


# ======================================================================
# TAB 3: Assessment History
# ======================================================================
with tab_history:
    st.header("Assessment History Tracking")
    st.caption("Track Org-AI-R scores over time via the platform data and scoring services.")

    hist_company_id = st.selectbox(
        "Company for History",
        options=company_options,
        index=company_options.index(company_api_id) if company_api_id in company_options else 0,
        format_func=lambda value: company_labels.get(value, value),
        key=_wk("hist_company_id"),
    )
    hist_company_label = company_labels.get(hist_company_id, hist_company_id)

    hist_col1, hist_col2 = st.columns([0.3, 0.7])

    with hist_col1:
        st.markdown("**Record New Assessment**")
        assessor_id = st.text_input("Assessor ID", value="analyst_01", key=_wk("assessor"))
        assessment_type = st.selectbox(
            "Assessment Type",
            ["screening", "limited", "full"],
            index=2,
            key=_wk("assess_type"),
        )

        if st.button("📸 Record Snapshot", key=_wk("record_snapshot")):
            with st.spinner("Recording assessment snapshot..."):
                try:
                    snapshot = _platform_post_json(
                        f"/platform/history/{hist_company_id}/record",
                        {
                            "assessor_id": assessor_id,
                            "assessment_type": assessment_type,
                        },
                    )
                    st.success(
                        f"Recorded snapshot: Org-AI-R = {float(snapshot.get('org_air', 0.0)):.1f} "
                        f"at {str(snapshot.get('timestamp', ''))[:16].replace('T', ' ')}"
                    )
                except Exception as ex:
                    st.error(f"Failed to record snapshot: {_error_message(ex)}")

    with hist_col2:
        st.markdown("**Trend Analysis**")
        if st.button("Calculate Trend", key=_wk("calc_trend")):
            with st.spinner("Calculating trend..."):
                try:
                    trend = _platform_get_json(f"/platform/history/{hist_company}/trend")
                    st.session_state[_wk("trend_result")] = trend
                except Exception as ex:
                    st.error(f"Trend calculation failed: {_error_message(ex)}")

        trend = st.session_state.get(_wk("trend_result"))
        if trend:
            t1, t2, t3, t4 = st.columns(4)
            t1.metric("Current Org-AI-R", f"{float(trend.get('current_org_air', 0.0)):.1f}")
            t2.metric("Entry Org-AI-R", f"{float(trend.get('entry_org_air', 0.0)):.1f}")
            t3.metric("Delta Since Entry", f"{float(trend.get('delta_since_entry', 0.0)):+.1f}")
            t4.metric("Snapshots", int(trend.get("snapshot_count", 0)))

            dir_badge = {
                "improving": ("badge-success", "📈 Improving"),
                "stable": ("badge-info", "➡️ Stable"),
                "declining": ("badge-danger", "📉 Declining"),
            }
            badge_cls, badge_text = dir_badge.get(trend.get("trend_direction"), ("badge-info", "Unknown"))
            st.markdown(
                f'<span class="status-badge {badge_cls}">{badge_text}</span>',
                unsafe_allow_html=True,
            )

            if trend.get("delta_30d") is not None:
                st.caption(f"30-day Delta: {float(trend.get('delta_30d', 0.0)):+.1f}")
            if trend.get("delta_90d") is not None:
                st.caption(f"90-day Delta: {float(trend.get('delta_90d', 0.0)):+.1f}")

    # Show history table if available
    st.divider()
    if st.button("Load Full History", key=_wk("load_history")):
        with st.spinner("Fetching history..."):
            try:
                snapshots = _platform_get_json(
                    f"/platform/history/{hist_company_id}",
                    params={"days": 365},
                )
                if snapshots:
                    hist_df = pd.DataFrame(
                        [
                            {
                                "Timestamp": str(s.get("timestamp", ""))[:16].replace("T", " "),
                                "Org-AI-R": float(s.get("org_air", 0.0)),
                                "V^R": float(s.get("vr_score", 0.0)),
                                "H^R": float(s.get("hr_score", 0.0)),
                                "Type": s.get("assessment_type", ""),
                                "Assessor": s.get("assessor_id", ""),
                            }
                            for s in snapshots
                        ]
                    )
                    st.dataframe(hist_df, width="stretch", hide_index=True)

                    # Trend line chart
                    fig_trend = px.line(
                        hist_df,
                        x="Timestamp",
                        y="Org-AI-R",
                        markers=True,
                        title=f"Org-AI-R Trend: {hist_company_label}",
                    )
                    fig_trend.add_hline(y=60, line_dash="dash", line_color="#c8a977", annotation_text="Threshold")
                    st.plotly_chart(
                        _style_plotly(fig_trend), width="stretch", config={"displayModeBar": False}
                    )
                else:
                    st.info("No history snapshots found. Record a snapshot first.")
            except Exception as ex:
                st.error(f"Failed to load history: {_error_message(ex)}")


# ======================================================================
# TAB 4: Agentic Due Diligence (Tasks 10.1-10.4 + HITL)
# ======================================================================
with tab_agents:
    st.header("🤖 Agentic Due Diligence Workflow")
    st.caption(
        "Multi-agent pipeline: SEC Analyst → Talent Analyst → Scorer → Evidence Agent → "
        "Value Creator — with HITL approval gates (Tasks 10.1-10.4)"
    )

    agent_col1, agent_col2 = st.columns([0.35, 0.65])

    with agent_col1:
        dd_company_id = st.selectbox(
            "Company for Due Diligence",
            options=company_options,
            index=company_options.index(company_api_id) if company_api_id in company_options else 0,
            format_func=lambda value: company_labels.get(value, value),
            key=_wk("dd_company_id"),
        )
        dd_company_label = company_labels.get(dd_company_id, dd_company_id)
        dd_type = st.selectbox(
            "Assessment Type",
            ["screening", "limited", "full"],
            index=2,
            key=_wk("dd_type"),
        )

        # Show the LangGraph workflow
        st.markdown("**Workflow Stages:**")
        stages = [
            ("🏛️", "SEC Analyst", "Evidence collection"),
            ("👥", "Talent Analyst", "Job & talent signals"),
            ("📊", "Scoring Agent", "Org-AI-R calculation"),
            ("📋", "Evidence Agent", "Evidence-backed justifications"),
            ("💰", "Value Creator", "Gap analysis & EBITDA"),
            ("✋", "HITL Gate", "Approval for edge cases"),
        ]
        for icon, name, desc in stages:
            st.markdown(
                f"""<div class="agent-step">
                    <span class="agent-step-name">{icon} {name}</span>
                    <div class="agent-step-msg">{desc}</div>
                </div>""",
                unsafe_allow_html=True,
            )

        run_dd = st.button(
            "▶️ Run Due Diligence Pipeline",
            key=_wk("run_dd"),
            width="stretch",
        )

    with agent_col2:
        if run_dd:
            with st.spinner(f"Running full due diligence for {dd_company_label}..."):
                start_time = time.time()
                try:
                    result = _platform_post_json(
                        f"/platform/due-diligence/{dd_company_id}",
                        {"assessment_type": dd_type},
                    )
                    elapsed = time.time() - start_time
                    st.session_state[_wk("dd_result")] = result
                    st.session_state[_wk("dd_elapsed")] = elapsed
                except Exception as ex:
                    st.error(f"Due diligence failed: {_error_message(ex)}")

        dd_result = st.session_state.get(_wk("dd_result"))
        dd_elapsed = st.session_state.get(_wk("dd_elapsed"), 0)

        if dd_result:
            st.success(f"Due diligence completed in {dd_elapsed:.1f}s")

            # Result metrics
            r1, r2, r3 = st.columns(3)
            scoring = dd_result.get("scoring_result", {})
            org_air_dd = float(scoring.get("org_air", scoring.get("org_air_score", 0)))
            r1.metric("Org-AI-R", f"{org_air_dd:.1f}")
            r2.metric(
                "HITL Required",
                "Yes" if dd_result.get("requires_approval") else "No",
            )
            r3.metric(
                "Approval Status",
                dd_result.get("approval_status", "N/A") or "N/A",
            )

            # HITL details
            if dd_result.get("requires_approval"):
                reason = dd_result.get("approval_reason", "Unknown")
                status = dd_result.get("approval_status", "pending")
                approver = dd_result.get("approved_by", "—")

                badge_cls = "badge-success" if status == "approved" else "badge-warning" if status == "pending" else "badge-danger"
                st.markdown(
                    f"""
                    <div style="background: rgba(47,124,246,0.08); border-radius: 8px; padding: 12px; margin: 8px 0;">
                        <b>✋ HITL Approval Gate</b><br>
                        Reason: {reason}<br>
                        Status: <span class="status-badge {badge_cls}">{status}</span><br>
                        Approved by: {approver}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            # Agent message timeline
            st.subheader("Agent Execution Timeline")
            messages = dd_result.get("messages", [])
            for msg in messages:
                agent = msg.get("agent_name", "unknown")
                content = msg.get("content", "")
                role = msg.get("role", "assistant")
                ts = msg.get("timestamp")
                ts_str = ts.strftime("%H:%M:%S") if hasattr(ts, "strftime") else str(ts)[:19]

                icon_map = {
                    "sec_analyst": "🏛️",
                    "talent_analyst": "👥",
                    "scorer": "📊",
                    "evidence_agent": "📋",
                    "value_creator": "💰",
                    "hitl": "✋",
                    "supervisor": "🎯",
                }
                icon = icon_map.get(agent, "🔧")

                st.markdown(
                    f"""<div class="agent-step">
                        <span class="agent-step-name">{icon} {agent}</span>
                        <span style="color:#64748b; font-size:0.75rem;"> — {ts_str}</span>
                        <div class="agent-step-msg">{content}</div>
                    </div>""",
                    unsafe_allow_html=True,
                )

            # Raw results (expandable)
            with st.expander("📄 Raw State Output"):
                # Make it JSON-serializable
                safe_result = {}
                for k, v in dd_result.items():
                    if k == "messages":
                        safe_result[k] = f"[{len(v)} messages]"
                    elif isinstance(v, datetime):
                        safe_result[k] = v.isoformat()
                    else:
                        try:
                            json.dumps(v)
                            safe_result[k] = v
                        except (TypeError, ValueError):
                            safe_result[k] = str(v)
                st.json(safe_result)
        else:
            st.info("Click **Run Due Diligence Pipeline** to start the multi-agent workflow.")


# ======================================================================
# TAB 5: Strategic Outputs (Bonus Extensions)
# ======================================================================
with tab_strategic:
    st.header("Strategic Outputs")

    bonus_tabs = st.tabs(
        ["🧠 Semantic Memory", "📈 Investment Tracker", "📝 IC Memo", "📬 LP Letter"]
    )

    # ---- Semantic Memory (Mem0) ----
    with bonus_tabs[0]:
        st.subheader("Semantic Memory (Mem0)")
        st.caption("Store and recall observations across due diligence cycles (+5 bonus)")

        try:
            stats = _platform_get_json("/platform/memory/stats")
            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Total Memories", int(stats.get("memory_count", 0)))
            mc2.metric("Companies Covered", int(stats.get("companies_covered", 0)))
            mc3.metric("Funds Covered", int(stats.get("funds_covered", 0)))
        except Exception:
            st.caption("Memory service initializing...")

        with st.form(_wk("memory_form")):
            mem_title = st.text_input(
                "Note Title",
                value=f"{company_for_detail} diligence note",
                key=_wk("mem_title"),
            )
            mem_category = st.selectbox(
                "Category",
                ["note", "due_diligence", "value_creation", "portfolio_update"],
                key=_wk("mem_cat"),
            )
            mem_content = st.text_area(
                "Observation",
                value=f"Observation for {company_for_detail}: ",
                height=120,
                key=_wk("mem_content"),
            )
            if st.form_submit_button("💾 Store Memory"):
                try:
                    payload = _platform_post_json(
                        "/platform/memory/remember",
                        {
                            "title": mem_title,
                            "content": mem_content,
                            "company_id": company_api_id,
                            "fund_id": fund_id,
                            "category": mem_category,
                            "source": "streamlit_dashboard",
                        },
                    )
                    st.success(f"Stored memory: {payload.get('memory_id', 'OK')}")
                except Exception as ex:
                    st.error(f"Failed to store memory: {_error_message(ex)}")

        st.divider()
        mem_query = st.text_input(
            "Semantic Recall Query",
            value=f"{company_for_detail} governance talent",
            key=_wk("mem_query"),
        )
        if st.button("🔍 Search Memories", key=_wk("mem_search")):
            try:
                results = _platform_post_json(
                    "/platform/memory/recall",
                    {
                        "query": mem_query,
                        "company_id": company_api_id,
                        "fund_id": fund_id,
                        "top_k": 5,
                    },
                )
                rows = results.get("results", []) if isinstance(results, dict) else results
                if rows:
                    st.dataframe(rows, width="stretch", hide_index=True)
                else:
                    st.info("No matching memories found.")
            except Exception as ex:
                st.error(f"Memory recall failed: {_error_message(ex)}")

        try:
            recent = _platform_get_json(
                "/platform/memory",
                params={"company_id": company_api_id, "fund_id": fund_id, "limit": 10},
            )
            if recent:
                st.caption("Recent Memory Entries")
                st.dataframe(recent, width="stretch", hide_index=True)
        except Exception:
            pass

    # ---- Investment Tracker ----
    with bonus_tabs[1]:
        st.subheader("Value Creation Investment Tracker")
        st.caption("Track AI transformation investments and ROI (+5 bonus)")

        try:
            summary = _platform_get_json(f"/platform/investments/summary/{fund_id}")
            iv1, iv2, iv3, iv4 = st.columns(4)
            iv1.metric("Invested", f"${float(summary.get('invested_amount_mm', 0)):.2f}M")
            iv2.metric("Current Value", f"${float(summary.get('total_value_mm', 0)):.2f}M")
            iv3.metric("ROI", f"{float(summary.get('roi_pct', 0)):.2f}%")
            iv4.metric("MOIC", f"{float(summary.get('moic', 0)):.2f}x")
        except Exception:
            st.caption("Investment tracker initializing...")

        company_row = portfolio_df[
            portfolio_df["company_id"].astype(str).str.upper() == str(company_api_id).upper()
        ]
        current_org = float(company_row.iloc[0]["org_air"]) if not company_row.empty else 0.0

        with st.form(_wk("invest_form")):
            prog_name = st.text_input(
                "Program Name",
                value=f"{company_for_detail} AI Acceleration",
                key=_wk("prog_name"),
            )
            thesis = st.text_area(
                "Investment Thesis",
                value="Fund governed AI enablement and workflow redesign to compound enterprise value.",
                height=100,
                key=_wk("invest_thesis"),
            )
            ic1, ic2, ic3 = st.columns(3)
            invested_mm = ic1.number_input("Invested ($M)", min_value=0.0, value=5.0, step=0.5, key=_wk("invested"))
            current_mm = ic2.number_input("Current Value ($M)", min_value=0.0, value=6.0, step=0.5, key=_wk("curr_val"))
            expected_mm = ic3.number_input("Expected Value ($M)", min_value=0.0, value=7.5, step=0.5, key=_wk("exp_val"))

            if st.form_submit_button("📊 Record Investment"):
                try:
                    payload = _platform_post_json(
                        "/platform/investments/record",
                        {
                            "fund_id": fund_id,
                            "company_id": company_api_id,
                            "program_name": prog_name,
                            "thesis": thesis,
                            "invested_amount_mm": invested_mm,
                            "current_value_mm": current_mm,
                            "expected_value_mm": expected_mm,
                            "current_org_air": current_org,
                            "target_org_air": max(75.0, current_org + 10.0),
                            "status": "active",
                            "notes": "Recorded from Streamlit dashboard",
                        },
                    )
                    st.success(f"Recorded investment: {payload.get('investment_id', 'OK')}")
                except Exception as ex:
                    st.error(f"Failed to record investment: {_error_message(ex)}")

        try:
            investments = _platform_get_json("/platform/investments", params={"fund_id": fund_id})
            if investments:
                st.caption("Investment Records")
                st.dataframe(investments, width="stretch", hide_index=True)
        except Exception:
            pass

    # ---- IC Memo ----
    with bonus_tabs[2]:
        st.subheader("IC Memo Generator")
        st.caption("Generate Investment Committee memo in Markdown and Word format (+5 bonus)")

        if st.button("📝 Generate IC Memo", key=_wk("gen_ic")):
            with st.spinner(f"Generating IC memo for {company_for_detail}..."):
                try:
                    ic_result = _platform_post_json(
                        "/platform/documents/ic-memo",
                        {"company_id": company_api_id, "fund_id": fund_id},
                    )
                    st.session_state[_wk("ic_memo")] = ic_result
                except Exception as ex:
                    st.error(f"IC memo generation failed: {_error_message(ex)}")

        ic_payload = st.session_state.get(_wk("ic_memo"))
        if ic_payload:
            st.text_area(
                "IC Memo Preview",
                value=ic_payload.get("preview_markdown", ""),
                height=350,
                key=_wk("ic_preview"),
            )
            docx_path = ic_payload.get("docx_path")
            if docx_path and Path(docx_path).exists():
                with open(docx_path, "rb") as f:
                    st.download_button(
                        "⬇️ Download IC Memo (.docx)",
                        data=f.read(),
                        file_name=Path(docx_path).name,
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                        key=_wk("dl_ic"),
                    )

    # ---- LP Letter ----
    with bonus_tabs[3]:
        st.subheader("LP Update Letter Generator")
        st.caption("Generate portfolio-level LP letter with Fund-AI-R context (+5 bonus)")

        if st.button("📬 Generate LP Letter", key=_wk("gen_lp")):
            with st.spinner(f"Generating LP letter for fund {fund_id}..."):
                try:
                    lp_result = _platform_post_json(
                        "/platform/documents/lp-letter",
                        {"fund_id": fund_id},
                    )
                    st.session_state[_wk("lp_letter")] = lp_result
                except Exception as ex:
                    st.error(f"LP letter generation failed: {_error_message(ex)}")

        lp_payload = st.session_state.get(_wk("lp_letter"))
        if lp_payload:
            st.text_area(
                "LP Letter Preview",
                value=lp_payload.get("preview_markdown", ""),
                height=350,
                key=_wk("lp_preview"),
            )
            docx_path = lp_payload.get("docx_path")
            if docx_path and Path(docx_path).exists():
                with open(docx_path, "rb") as f:
                    st.download_button(
                        "⬇️ Download LP Letter (.docx)",
                        data=f.read(),
                        file_name=Path(docx_path).name,
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                        key=_wk("dl_lp"),
                    )


# ======================================================================
# TAB 6: MCP & Infrastructure
# ======================================================================
with tab_infra:
    st.header("MCP Server & Observability")

    infra_tabs = st.tabs(["🔧 MCP Tools", "📚 MCP Resources & Prompts", "📊 Prometheus Metrics"])

    # ---- MCP Tools ----
    with infra_tabs[0]:
        st.subheader("MCP Server Tools")
        st.caption("Tools exposed by the MCP server for platform APIs")

        if st.button("🔄 Refresh Tool List", key=_wk("refresh_tools")):
            try:
                payload = _platform_get_json("/platform/mcp/tools")
                st.session_state[_wk("mcp_tools")] = payload.get("tools", [])
            except Exception as ex:
                st.error(f"Failed to list tools: {_error_message(ex)}")

        tools = st.session_state.get(_wk("mcp_tools"))
        if tools:
            st.success(f"✓ {len(tools)} tools available")
            for t in tools:
                st.markdown(f"- `{t}`")
        else:
            st.info("Click Refresh to discover available MCP tools.")

        st.divider()
        st.subheader("Interactive Tool Tester")

        tool_name = st.selectbox(
            "Tool",
            [
                "calculate_org_air_score",
                "get_company_evidence",
                "generate_justification",
                "project_ebitda_impact",
                "run_gap_analysis",
                "get_portfolio_summary",
            ],
            key=_wk("tool_select"),
        )
        tool_args_raw = st.text_area(
            "Arguments (JSON)",
            value=json.dumps({"company_id": company_api_id}, indent=2),
            height=100,
            key=_wk("tool_args"),
        )

        if st.button("▶️ Execute Tool", key=_wk("exec_tool")):
            try:
                args = json.loads(tool_args_raw)
                with st.spinner(f"Calling {tool_name}..."):
                    start = time.time()
                    result = call_mcp_tool(tool_name, args)
                    elapsed = time.time() - start
                st.success(f"Completed in {elapsed:.2f}s")
                st.json(json.loads(result))
            except json.JSONDecodeError:
                st.error("Invalid JSON in arguments field.")
            except Exception as ex:
                st.error(f"Tool execution failed: {_error_message(ex)}")

    # ---- MCP Resources & Prompts ----
    with infra_tabs[1]:
        st.subheader("MCP Resources")

        if st.button("🔄 Refresh Resources", key=_wk("refresh_res")):
            try:
                payload = _platform_get_json("/platform/mcp/resources")
                st.session_state[_wk("mcp_resources")] = payload.get("resources", [])
            except Exception as ex:
                st.error(f"Failed to list resources: {_error_message(ex)}")

        resources = st.session_state.get(_wk("mcp_resources"))
        if resources:
            st.success(f"✓ {len(resources)} resources available")
            for r in resources:
                if isinstance(r, dict):
                    st.markdown(f"- `{r.get('uri', '')}`")
                else:
                    st.markdown(f"- `{r}`")
        else:
            st.info("Click Refresh to discover MCP resources.")

        st.divider()
        st.subheader("MCP Prompts")
        st.markdown(
            """
            Available prompt templates for agent workflows:

            **`due_diligence_assessment`** — Complete due diligence for a company:
            1. Calculate Org-AI-R score
            2. Generate justifications for weak dimensions
            3. Run gap analysis with target Org-AI-R
            4. Project EBITDA impact

            **`ic_meeting_prep`** — Prepare Investment Committee meeting package for a company
            """
        )

    # ---- Prometheus Metrics ----
    with infra_tabs[2]:
        st.subheader("Prometheus Metrics")
        st.caption("Observable counters and histograms for MCP tools, agents, and service clients")

        try:
            metrics_response = requests.get(
                _join_url(DEFAULT_API_BASE, "/metrics"),
                timeout=30,
            )
            metrics_text = metrics_response.text
            metrics_unavailable = (
                metrics_response.status_code >= 400
                and "prometheus_client not installed" in metrics_text.lower()
            )

            if metrics_unavailable:
                st.warning(
                    "Prometheus client is not installed in the running backend environment. "
                    "Metrics collection is currently in fallback mode."
                )
                st.info(
                    "Install `prometheus-client` in the backend runtime and restart the API "
                    "to enable full Prometheus scraping."
                )
                with st.expander("📄 Raw Metrics Fallback Output"):
                    st.code(metrics_text[:5000], language="text")
            else:
                metrics_response.raise_for_status()

                # Parse key metrics for display
                st.markdown("**Defined Metric Families:**")

                metric_families = [
                    ("mcp_tool_calls_total", "MCP Tool Calls", "Counter"),
                    ("mcp_tool_duration_seconds", "MCP Tool Duration", "Histogram"),
                    ("agent_invocations_total", "Agent Invocations", "Counter"),
                    ("agent_duration_seconds", "Agent Duration", "Histogram"),
                    ("hitl_approvals_total", "HITL Approvals", "Counter"),
                    ("cs_client_calls_total", "CS Client Calls", "Counter"),
                ]

                for metric_name, display_name, metric_type in metric_families:
                    st.markdown(
                        f'<span class="status-badge badge-info">{metric_type}</span> **{display_name}** — `{metric_name}`',
                        unsafe_allow_html=True,
                    )

                st.divider()
                with st.expander("📄 Raw Prometheus Output"):
                    st.code(metrics_text[:5000], language="text")

        except Exception as ex:
            st.error(f"Failed to load metrics: {_error_message(ex)}")
            st.info("Prometheus metrics are available at /metrics endpoint when the server is running.")


# ======================================================================
# Footer
# ======================================================================
st.divider()
st.markdown(
    """
    <div style="text-align:center; padding:16px 0; color:#64748b; font-size:0.78rem;">
        <b>PE Org-AI-R Agentic Portfolio Intelligence</b><br>
        MCP + Multi-Agent Workflows · No Mock Data · All data from live services<br>
        <span style="color:#475569;">
            "End-to-end agentic due diligence and value creation for private equity portfolios, powered by a custom-built Model-Centric Platform (MCP) and multi-agent workflows."
        </span>
    </div>
    """,
    unsafe_allow_html=True,
)
