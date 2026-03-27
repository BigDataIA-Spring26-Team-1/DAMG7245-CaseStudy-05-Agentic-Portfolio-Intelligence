from __future__ import annotations

import asyncio
import json
from pathlib import Path

import nest_asyncio
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.bonus_facade import (
    generate_ic_memo,
    generate_lp_letter,
    get_investment_summary,
    list_investments,
    list_memories,
    memory_stats,
    recall_company_memory,
    record_investment,
    remember_company_memory,
)
from app.dashboard.evidence_display import (
    render_company_evidence_panel,
    render_evidence_summary_table,
)
from app.mcp.client import MCPClient
from app.services.integration.portfolio_data_service import portfolio_data_service

nest_asyncio.apply()
mcp_client = MCPClient()


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
                    "enterprise_value_mm": c.enterprise_value_mm,
                    "ev_source": c.enterprise_value_source,
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


def _widget_key(prefix: str, suffix: str) -> str:
    return f"{prefix}_{suffix}"


def _style_plotly(fig: go.Figure, *, legend_title: str | None = None) -> go.Figure:
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#edf3f9"),
        margin=dict(l=0, r=0, t=48, b=0),
        legend_title_text=legend_title,
    )
    fig.update_xaxes(gridcolor="rgba(147, 168, 191, 0.16)")
    fig.update_yaxes(gridcolor="rgba(147, 168, 191, 0.12)")
    return fig


def _render_dashboard_header(fund_id: str, selected_company: str, portfolio_df: pd.DataFrame) -> None:
    sectors = sorted({str(value) for value in portfolio_df.get("sector", pd.Series(dtype=str)).dropna().tolist() if str(value).strip()})
    sector_text = " | ".join(sectors[:4]) if sectors else "Awaiting sector coverage"
    st.markdown(
        f"""
        <div class="dashboard-band">
          <div class="insight-kicker">Executive Portfolio Dashboard</div>
          <div class="dashboard-band-title">{fund_id}</div>
          <p class="dashboard-band-copy">
            Focus company: {selected_company} | Sectors tracked: {sector_text}. The dashboard below surfaces score structure,
            evidence coverage, and value creation outputs in one place.
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _controls(
    *,
    embedded: bool,
    key_prefix: str,
    default_fund_id: str,
    default_company_id: str,
) -> tuple[str, str]:
    if embedded:
        st.subheader("Portfolio Scope")
        col1, col2 = st.columns(2)
        with col1:
            fund_id = st.text_input(
                "Fund ID",
                value=default_fund_id,
                key=_widget_key(key_prefix, "fund_id"),
            )
        with col2:
            selected_company = st.text_input(
                "Company ID or Ticker",
                value=default_company_id,
                key=_widget_key(key_prefix, "selected_company"),
            )
        return fund_id, selected_company

    st.sidebar.title("Portfolio Scope")
    fund_id = st.sidebar.text_input(
        "Fund ID",
        value=default_fund_id,
        key=_widget_key(key_prefix, "fund_id"),
    )
    selected_company = st.sidebar.text_input(
        "Company ID or Ticker",
        value=default_company_id,
        key=_widget_key(key_prefix, "selected_company"),
    )
    return fund_id, selected_company


def render_cs5_dashboard(
    *,
    embedded: bool = False,
    key_prefix: str = "cs5_dashboard",
    default_fund_id: str = "growth_fund_v",
    default_company_id: str = "NVDA",
) -> None:
    fund_id, selected_company = _controls(
        embedded=embedded,
        key_prefix=key_prefix,
        default_fund_id=default_fund_id,
        default_company_id=default_company_id,
    )

    try:
        portfolio_df = load_portfolio(fund_id)
        if embedded:
            st.success(f"Loaded {len(portfolio_df)} portfolio companies")
        else:
            st.sidebar.success(f"Loaded {len(portfolio_df)} portfolio companies")
    except Exception as e:
        st.error(f"Failed to load portfolio services: {e}")
        st.info("Ensure the supporting services and MCP-exposed components are running.")
        return

    if embedded:
        st.header("Executive Portfolio Dashboard")
    else:
        st.title("Portfolio Analytics")

    company_for_detail = selected_company.strip() or default_company_id
    _render_dashboard_header(fund_id, company_for_detail, portfolio_df)

    if not portfolio_df.empty and float(portfolio_df["enterprise_value_mm"].sum()) > 0:
        fund_air = float(
            (portfolio_df["org_air"] * portfolio_df["enterprise_value_mm"]).sum()
            / portfolio_df["enterprise_value_mm"].sum()
        )
    else:
        fund_air = 0.0
    avg_delta = portfolio_df["delta"].mean() if not portfolio_df.empty else 0.0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Fund-AI-R", f"{fund_air:.1f}")
    with col2:
        st.metric("Portfolio Companies", len(portfolio_df))
    with col3:
        st.metric("Average V^R", f"{portfolio_df['vr_score'].mean():.1f}" if not portfolio_df.empty else "0.0")
    with col4:
        st.metric("Average Delta", f"{avg_delta:+.1f}")

    if not portfolio_df.empty:
        chart_col, rank_col = st.columns([1.2, 0.8])
        with chart_col:
            fig = px.scatter(
                portfolio_df,
                x="vr_score",
                y="hr_score",
                size="org_air",
                color="sector",
                hover_name="name",
                labels={"vr_score": "V^R", "hr_score": "H^R", "org_air": "OrgAIR"},
                title="Portfolio Readiness Map",
                color_discrete_sequence=["#2f7cf6", "#28a59c", "#6e86ff", "#c8a977", "#f08a5d"],
            )
            fig.add_hline(y=60, line_dash="dash", line_color="#c8a977", annotation_text="H^R threshold")
            fig.add_vline(x=60, line_dash="dash", line_color="#c8a977", annotation_text="V^R threshold")
            fig.update_traces(marker=dict(line=dict(width=1, color="#09111c"), opacity=0.9))
            st.plotly_chart(_style_plotly(fig, legend_title="Sector"), use_container_width=True, config={"displayModeBar": False})
        with rank_col:
            leaders = portfolio_df.sort_values("org_air", ascending=False).head(6)
            leaderboard = px.bar(
                leaders.sort_values("org_air", ascending=True),
                x="org_air",
                y="ticker",
                orientation="h",
                color="delta",
                color_continuous_scale=["#f08a5d", "#2f7cf6", "#3ecf8e"],
                labels={"org_air": "OrgAIR", "ticker": "", "delta": "Delta"},
                title="Top Companies By OrgAIR",
            )
            leaderboard.update_layout(coloraxis_colorbar_title="Delta")
            st.plotly_chart(_style_plotly(leaderboard), use_container_width=True, config={"displayModeBar": False})

    st.subheader("Portfolio Scoreboard")
    st.dataframe(
        portfolio_df.style.format(
            {
                "org_air": "{:.1f}",
                "vr_score": "{:.1f}",
                "hr_score": "{:.1f}",
                "delta": "{:+.1f}",
                "enterprise_value_mm": "{:.1f}",
            }
        ).background_gradient(subset=["org_air", "vr_score", "hr_score"], cmap="YlGnBu"),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()
    st.header("Company Diligence Drilldown")
    st.caption(f"Focus company: {company_for_detail}")

    try:
        score_payload = run_async(
            mcp_client.call_tool("calculate_org_air_score", {"company_id": company_for_detail})
        )
        score_data = json.loads(score_payload)

        st.subheader("Current Score Snapshot")
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
                mcp_client.call_tool(
                    "generate_justification",
                    {"company_id": company_for_detail, "dimension": dim},
                )
            )
            justifications[dim] = json.loads(payload)

        render_evidence_summary_table(justifications)
        render_company_evidence_panel(company_for_detail, justifications)

    except Exception as e:
        st.warning(f"Could not load justification details for {company_for_detail}: {e}")

    st.divider()
    st.header("Strategic Outputs")
    bonus_tabs = st.tabs(
        [
            "Semantic Memory",
            "Value Creation Tracker",
            "IC Memo",
            "LP Update",
        ]
    )

    with bonus_tabs[0]:
        stats = memory_stats()
        c1, c2, c3 = st.columns(3)
        c1.metric("Memories", int(stats["memory_count"]))
        c2.metric("Companies Covered", int(stats["companies_covered"]))
        c3.metric("Funds Covered", int(stats["funds_covered"]))

        with st.form(_widget_key(key_prefix, "memory_capture")):
            memory_title = st.text_input(
                "Note Title",
                value=f"{company_for_detail} diligence note",
                key=_widget_key(key_prefix, "memory_title"),
            )
            memory_category = st.selectbox(
                "Category",
                ["note", "due_diligence", "value_creation", "portfolio_update"],
                key=_widget_key(key_prefix, "memory_category"),
            )
            memory_content = st.text_area(
                "Observation",
                value=f"Observation for {company_for_detail}: ",
                height=140,
                key=_widget_key(key_prefix, "memory_content"),
            )
            if st.form_submit_button("Store Memory"):
                payload = remember_company_memory(
                    title=memory_title,
                    content=memory_content,
                    company_id=company_for_detail,
                    fund_id=fund_id,
                    category=memory_category,
                    source="portfolio_dashboard_embedded" if embedded else "portfolio_dashboard",
                )
                st.success(f"Stored memory {payload['memory_id']}")

        memory_query = st.text_input(
            "Semantic Recall Query",
            value=f"{company_for_detail} governance talent value creation",
            key=_widget_key(key_prefix, "memory_query"),
        )
        if st.button("Search Memory", key=_widget_key(key_prefix, "memory_search")):
            st.session_state[_widget_key(key_prefix, "memory_results")] = recall_company_memory(
                query=memory_query,
                company_id=company_for_detail,
                fund_id=fund_id,
                top_k=5,
            )

        memory_results = st.session_state.get(_widget_key(key_prefix, "memory_results"), [])
        if memory_results:
            st.dataframe(memory_results, use_container_width=True, hide_index=True)

        recent_memories = list_memories(company_id=company_for_detail, fund_id=fund_id, limit=10)
        if recent_memories:
            st.caption("Recent memory entries")
            st.dataframe(recent_memories, use_container_width=True, hide_index=True)

    with bonus_tabs[1]:
        summary = get_investment_summary(fund_id=fund_id)
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Invested", f"${float(summary['invested_amount_mm']):.2f}M")
        r2.metric("Value", f"${float(summary['total_value_mm']):.2f}M")
        r3.metric("ROI", f"{float(summary['roi_pct']):.2f}%")
        r4.metric("MOIC", f"{float(summary['moic']):.2f}x")

        company_row = portfolio_df[
            (portfolio_df["company_id"].astype(str).str.upper() == company_for_detail.upper())
            | (portfolio_df["ticker"].astype(str).str.upper() == company_for_detail.upper())
        ]
        current_org_air = float(company_row.iloc[0]["org_air"]) if not company_row.empty else 0.0

        with st.form(_widget_key(key_prefix, "investment_tracker")):
            program_name = st.text_input(
                "Program Name",
                value=f"{company_for_detail} AI Acceleration",
                key=_widget_key(key_prefix, "program_name"),
            )
            thesis = st.text_area(
                "Investment Thesis",
                value="Fund governed AI enablement and workflow redesign to compound enterprise value.",
                height=120,
                key=_widget_key(key_prefix, "investment_thesis"),
            )
            inv1, inv2, inv3 = st.columns(3)
            invested_amount_mm = inv1.number_input(
                "Invested Amount ($M)",
                min_value=0.0,
                value=5.0,
                step=0.5,
                key=_widget_key(key_prefix, "invested_amount_mm"),
            )
            current_value_mm = inv2.number_input(
                "Current Value ($M)",
                min_value=0.0,
                value=6.0,
                step=0.5,
                key=_widget_key(key_prefix, "current_value_mm"),
            )
            expected_value_mm = inv3.number_input(
                "Expected Value ($M)",
                min_value=0.0,
                value=7.5,
                step=0.5,
                key=_widget_key(key_prefix, "expected_value_mm"),
            )
            if st.form_submit_button("Record Investment"):
                payload = record_investment(
                    fund_id=fund_id,
                    company_id=company_for_detail,
                    program_name=program_name,
                    thesis=thesis,
                    invested_amount_mm=invested_amount_mm,
                    current_value_mm=current_value_mm,
                    expected_value_mm=expected_value_mm,
                    current_org_air=current_org_air,
                    target_org_air=max(75.0, current_org_air + 10.0),
                    status="active",
                    notes="Recorded from embedded portfolio dashboard" if embedded else "Recorded from portfolio dashboard",
                    metadata={"ui_surface": "platform_streamlit_dashboard" if embedded else "portfolio_dashboard"},
                )
                st.success(f"Recorded investment {payload['investment_id']}")

        investments = list_investments(fund_id=fund_id)
        if investments:
            st.dataframe(investments, use_container_width=True, hide_index=True)

    with bonus_tabs[2]:
        st.caption("Generate an investment committee memo in Markdown and Word format.")
        if st.button("Generate IC Memo", key=_widget_key(key_prefix, "generate_ic_memo")):
            st.session_state[_widget_key(key_prefix, "ic_memo")] = generate_ic_memo(
                company_for_detail,
                fund_id=fund_id,
            )

        ic_payload = st.session_state.get(_widget_key(key_prefix, "ic_memo"))
        if ic_payload:
            st.text_area(
                "IC Memo Preview",
                value=ic_payload["preview_markdown"],
                height=320,
                key=_widget_key(key_prefix, "ic_memo_preview"),
            )
            with open(ic_payload["docx_path"], "rb") as handle:
                st.download_button(
                    "Download IC Memo (.docx)",
                    data=handle.read(),
                    file_name=Path(ic_payload["docx_path"]).name,
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    key=_widget_key(key_prefix, "download_ic_memo"),
                )

    with bonus_tabs[3]:
        st.caption("Generate a portfolio-level LP update letter with Fund-AI-R and ROI context.")
        if st.button("Generate LP Update", key=_widget_key(key_prefix, "generate_lp_letter")):
            st.session_state[_widget_key(key_prefix, "lp_letter")] = generate_lp_letter(fund_id)

        lp_payload = st.session_state.get(_widget_key(key_prefix, "lp_letter"))
        if lp_payload:
            st.text_area(
                "LP Update Preview",
                value=lp_payload["preview_markdown"],
                height=320,
                key=_widget_key(key_prefix, "lp_letter_preview"),
            )
            with open(lp_payload["docx_path"], "rb") as handle:
                st.download_button(
                    "Download LP Update (.docx)",
                    data=handle.read(),
                    file_name=Path(lp_payload["docx_path"]).name,
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    key=_widget_key(key_prefix, "download_lp_letter"),
                )
