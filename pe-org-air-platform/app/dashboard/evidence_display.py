from __future__ import annotations

from typing import Dict

import pandas as pd
import streamlit as st

LEVEL_COLORS = {
    1: "#ef4444",
    2: "#f97316",
    3: "#eab308",
    4: "#22c55e",
    5: "#14b8a6",
}

LEVEL_NAMES = {
    1: "Nascent",
    2: "Developing",
    3: "Adequate",
    4: "Good",
    5: "Excellent",
}


def render_evidence_card(justification: dict) -> None:
    """
    Renders one dimension justification card.
    Works with dict output from your MCP/CS4 path.
    """
    level = int(justification.get("level", 0) or 0)
    color = LEVEL_COLORS.get(level, "#6b7280")
    level_name = LEVEL_NAMES.get(level, "Unknown")

    dimension = str(justification.get("dimension", "unknown")).replace("_", " ").title()
    score = float(justification.get("score", 0.0))
    evidence_strength = str(justification.get("evidence_strength", "unknown"))
    rubric_criteria = justification.get("rubric_criteria", "N/A")
    supporting_evidence = justification.get("supporting_evidence", [])
    gaps_identified = justification.get("gaps_identified", [])

    with st.container():
        col1, col2, col3 = st.columns([4, 1, 1])

        with col1:
            st.markdown(f"### {dimension}")

        with col2:
            st.markdown(
                f"""
                <div style="
                    background-color:{color};
                    color:white;
                    padding:4px 12px;
                    border-radius:12px;
                    text-align:center;
                    font-weight:bold;">
                    L{level}
                </div>
                """,
                unsafe_allow_html=True,
            )

        with col3:
            st.markdown(f"**{score:.1f}**")

        st.caption(f"Level: {level_name}")

        strength_colors = {
            "strong": "#22c55e",
            "moderate": "#eab308",
            "weak": "#ef4444",
        }

        st.markdown(
            f"""
            Evidence: <span style="color:{strength_colors.get(evidence_strength, '#6b7280')}">
            <b>{evidence_strength.title()}</b>
            </span>
            """,
            unsafe_allow_html=True,
        )

        st.info(f"**Rubric Match:** {rubric_criteria}")

        st.markdown("**Supporting Evidence:**")
        if supporting_evidence:
            for idx, evidence in enumerate(supporting_evidence[:5], start=1):
                source_type = evidence.get("source_type", "unknown")
                content = evidence.get("content", "")
                confidence = evidence.get("confidence", 0.0)
                source_url = evidence.get("source_url")

                with st.expander(f"[{source_type}] {content[:70]}...", expanded=False):
                    st.write(content)
                    st.caption(f"Confidence: {float(confidence):.0%}")
                    if source_url:
                        st.markdown(f"[Source]({source_url})")
        else:
            st.write("No supporting evidence available")

        if gaps_identified:
            st.warning("**Gaps Identified:**")
            for gap in gaps_identified:
                st.markdown(f"- {gap}")

        st.divider()


def render_company_evidence_panel(company_id: str, justifications: Dict[str, dict]) -> None:
    st.header(f"Evidence Analysis: {company_id}")

    if not justifications:
        st.info("No justifications available")
        return

    total_evidence = sum(len(j.get("supporting_evidence", [])) for j in justifications.values())
    avg_level = sum(float(j.get("level", 0)) for j in justifications.values()) / len(justifications)
    strong_count = sum(
        1 for j in justifications.values() if str(j.get("evidence_strength", "")).lower() == "strong"
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Evidence", total_evidence)
    col2.metric("Avg Level", f"L{avg_level:.1f}")
    col3.metric("Strong Evidence", f"{strong_count}/{len(justifications)}")
    col4.metric("Dimensions", len(justifications))

    dim_names = [d.replace("_", " ").title() for d in justifications.keys()]
    tabs = st.tabs(dim_names)

    for tab, (dim, just) in zip(tabs, justifications.items()):
        with tab:
            render_evidence_card(just)


def render_evidence_summary_table(justifications: Dict[str, dict]) -> None:
    if not justifications:
        st.info("No evidence summary available")
        return

    rows = []
    for dim, just in justifications.items():
        rows.append(
            {
                "Dimension": dim.replace("_", " ").title(),
                "Score": float(just.get("score", 0.0)),
                "Level": f"L{int(just.get('level', 0) or 0)}",
                "Evidence": str(just.get("evidence_strength", "unknown")).title(),
                "Items": len(just.get("supporting_evidence", [])),
                "Gaps": len(just.get("gaps_identified", [])),
            }
        )

    df = pd.DataFrame(rows)

    def color_level(val: str) -> str:
        try:
            level = int(val[1])
            return f"background-color: {LEVEL_COLORS.get(level, '#ffffff')}; color: white;"
        except Exception:
            return ""

    styled = df.style.map(color_level, subset=["Level"])
    st.dataframe(styled, use_container_width=True, hide_index=True)