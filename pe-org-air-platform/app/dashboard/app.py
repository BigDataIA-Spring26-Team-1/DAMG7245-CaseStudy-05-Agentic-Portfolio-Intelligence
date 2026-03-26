from __future__ import annotations

import asyncio
import json

import nest_asyncio
import pandas as pd
import plotly.express as px
import streamlit as st

from app.dashboard.evidence_display import (
    render_company_evidence_panel,
    render_evidence_summary_table,
)
from app.mcp.server import call_tool
from app.services.integration.portfolio_data_service import portfolio_data_service

nest_asyncio.apply()

st.set_page_config(
    page_title="PE OrgAIR Dashboard",
    page_icon="📈",
    layout="wide",
)

st.sidebar.title("PE OrgAIR")
fund_id = st.sidebar.text_input("Fund ID", value="growth_fund_v")
selected_company = st.sidebar.text_input("Company ID / Ticker", value="NVDA")


@st.cache_data(ttl=300)
def load_portfolio(_fund_id: str) -> pd.DataFrame:
    async def _load():
        return await portfolio_data_service.get_portfolio_view(_fund_id)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        portfolio = loop.run_until_complete(_load())
        return pd.DataFrame(
            [
                {
                    "company_id": c.company_id,
                    "ticker": c.ticker,
                    "name": c.name,
                    "sector": c.sector,
                    "org_air": c.org_air,
                    "vr_score": c.vr_score,
                    "hr_score": c.hr_score,
                    "delta": c.delta_since_entry,
                    "evidence_count": c.evidence_count,
                }
                for c in portfolio
            ]
        )
    finally:
        loop.close()


def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


try:
    portfolio_df = load_portfolio(fund_id)
    st.sidebar.success(f"Loaded {len(portfolio_df)} companies from CS1-CS4")
except Exception as e:
    st.error(f"Failed to connect to CS1-CS4: {e}")
    st.info("Ensure your underlying services and MCP-exposed components are running")
    st.stop()

st.title("Portfolio Overview")

fund_air = portfolio_df["org_air"].mean() if not portfolio_df.empty else 0.0
avg_delta = portfolio_df["delta"].mean() if not portfolio_df.empty else 0.0

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Fund-AI-R", f"{fund_air:.1f}")
with col2:
    st.metric("Companies", len(portfolio_df))
with col3:
    st.metric("Avg V^R", f"{portfolio_df['vr_score'].mean():.1f}" if not portfolio_df.empty else "0.0")
with col4:
    st.metric("Avg Delta", f"{avg_delta:+.1f}")

if not portfolio_df.empty:
    fig = px.scatter(
        portfolio_df,
        x="vr_score",
        y="hr_score",
        size="org_air",
        color="sector",
        hover_name="name",
        title="Portfolio AI-Readiness Map",
        labels={"vr_score": "V^R (Idiosyncratic)", "hr_score": "H^R (Systematic)"},
    )
    fig.add_hline(y=60, line_dash="dash", line_color="gray", annotation_text="H^R Threshold")
    fig.add_vline(x=60, line_dash="dash", line_color="gray", annotation_text="V^R Threshold")
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Portfolio Companies")
st.dataframe(
    portfolio_df.style.background_gradient(subset=["org_air"], cmap="RdYlGn"),
    use_container_width=True,
    hide_index=True,
)

st.divider()
st.header("Company Evidence and Justifications")

company_for_detail = selected_company.strip() or "NVDA"
st.caption(f"Selected company: {company_for_detail}")

try:
    score_payload = run_async(
        call_tool("calculate_org_air_score", {"company_id": company_for_detail})
    )
    score_data = json.loads(score_payload)

    st.subheader("Score Snapshot")
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Org-AI-R", f"{float(score_data.get('org_air', 0.0)):.1f}")
    s2.metric("V^R", f"{float(score_data.get('vr_score', 0.0)):.1f}")
    s3.metric("H^R", f"{float(score_data.get('hr_score', 0.0)):.1f}")
    s4.metric("Synergy", f"{float(score_data.get('synergy_score', 0.0)):.1f}")

    dimension_scores = score_data.get("dimension_scores", {})
    low_dimensions = [
        dim for dim, score in dimension_scores.items() if float(score) < 60
    ][:3]

    if not low_dimensions and dimension_scores:
        low_dimensions = list(dimension_scores.keys())[:3]

    justifications = {}
    for dim in low_dimensions:
        payload = run_async(
            call_tool(
                "generate_justification",
                {"company_id": company_for_detail, "dimension": dim},
            )
        )
        justifications[dim] = json.loads(payload)

    render_evidence_summary_table(justifications)
    render_company_evidence_panel(company_for_detail, justifications)

except Exception as e:
    st.warning(f"Could not load justification details for {company_for_detail}: {e}")