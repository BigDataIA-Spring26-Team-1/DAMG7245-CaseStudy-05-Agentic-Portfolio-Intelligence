from __future__ import annotations

import json
import os
from typing import Any

from app.logging_utils import get_logger
from mcp.server.fastmcp import FastMCP

from app.mcp.prompts import get_prompt, list_prompt_defs
from app.mcp.resources import list_resource_defs, read_resource
from app.mcp.tools import (
    calculate_org_air_score,
    generate_ic_memo,
    generate_justification,
    generate_lp_letter,
    get_investment_tracker_summary,
    get_company_evidence,
    get_portfolio_summary,
    project_ebitda_impact,
    recall_company_memory,
    record_investment_roi,
    remember_company_memory,
    run_gap_analysis,
)

logger = get_logger(__name__)

MCP_HOST = os.getenv("MCP_HOST", "127.0.0.1")
MCP_PORT = int(os.getenv("MCP_PORT", "8000"))
MCP_PATH = os.getenv("MCP_PATH", "/mcp")

mcp = FastMCP(
    "PE OrgAIR MCP Server",
    instructions=(
        "MCP server for the PE OrgAIR platform. "
        "Exposes scoring, evidence, justification, gap analysis, EBITDA projection, "
        "portfolio summary, resources, and prompts."
    ),
    host=MCP_HOST,
    port=MCP_PORT,
    streamable_http_path=MCP_PATH,
    json_response=True,
)
# -------------------------
# Tools
# -------------------------
@mcp.tool(name="calculate_org_air_score")
async def calculate_org_air_score_mcp(company_id: str) -> dict[str, Any]:
    logger.info("mcp_tool_call", tool="calculate_org_air_score", company_id=company_id)
    payload = await calculate_org_air_score({"company_id": company_id})
    return json.loads(payload)


@mcp.tool(name="get_company_evidence")
async def get_company_evidence_mcp(
    company_id: str,
    dimension: str = "all",
    limit: int = 10,
) -> list[dict[str, Any]]:
    logger.info(
        "mcp_tool_call",
        tool="get_company_evidence",
        company_id=company_id,
        dimension=dimension,
        limit=limit,
    )
    payload = await get_company_evidence(
        {
            "company_id": company_id,
            "dimension": dimension,
            "limit": limit,
        }
    )
    return json.loads(payload)


@mcp.tool(name="generate_justification")
async def generate_justification_mcp(company_id: str, dimension: str) -> dict[str, Any]:
    logger.info(
        "mcp_tool_call",
        tool="generate_justification",
        company_id=company_id,
        dimension=dimension,
    )
    payload = await generate_justification(
        {
            "company_id": company_id,
            "dimension": dimension,
        }
    )
    return json.loads(payload)


@mcp.tool(name="project_ebitda_impact")
async def project_ebitda_impact_mcp(
    company_id: str,
    entry_score: float,
    target_score: float,
    h_r_score: float,
) -> dict[str, Any]:
    logger.info(
        "mcp_tool_call",
        tool="project_ebitda_impact",
        company_id=company_id,
    )
    payload = await project_ebitda_impact(
        {
            "company_id": company_id,
            "entry_score": entry_score,
            "target_score": target_score,
            "h_r_score": h_r_score,
        }
    )
    return json.loads(payload)


@mcp.tool(name="run_gap_analysis")
async def run_gap_analysis_mcp(company_id: str, target_org_air: float) -> dict[str, Any]:
    logger.info(
        "mcp_tool_call",
        tool="run_gap_analysis",
        company_id=company_id,
        target_org_air=target_org_air,
    )
    payload = await run_gap_analysis(
        {
            "company_id": company_id,
            "target_org_air": target_org_air,
        }
    )
    return json.loads(payload)


@mcp.tool(name="get_portfolio_summary")
async def get_portfolio_summary_mcp(fund_id: str) -> dict[str, Any]:
    logger.info("mcp_tool_call", tool="get_portfolio_summary", fund_id=fund_id)
    payload = await get_portfolio_summary({"fund_id": fund_id})
    return json.loads(payload)


@mcp.tool(name="remember_company_memory")
async def remember_company_memory_mcp(
    title: str,
    content: str,
    company_id: str | None = None,
    fund_id: str | None = None,
    category: str = "note",
) -> dict[str, Any]:
    logger.info("mcp_tool_call", tool="remember_company_memory", company_id=company_id, fund_id=fund_id)
    payload = await remember_company_memory(
        {
            "title": title,
            "content": content,
            "company_id": company_id,
            "fund_id": fund_id,
            "category": category,
        }
    )
    return json.loads(payload)


@mcp.tool(name="recall_company_memory")
async def recall_company_memory_mcp(
    query: str,
    company_id: str | None = None,
    fund_id: str | None = None,
    category: str | None = None,
    top_k: int = 5,
) -> dict[str, Any]:
    logger.info("mcp_tool_call", tool="recall_company_memory", company_id=company_id, fund_id=fund_id)
    payload = await recall_company_memory(
        {
            "query": query,
            "company_id": company_id,
            "fund_id": fund_id,
            "category": category,
            "top_k": top_k,
        }
    )
    return json.loads(payload)


@mcp.tool(name="record_investment_roi")
async def record_investment_roi_mcp(
    fund_id: str,
    company_id: str,
    program_name: str,
    thesis: str,
    invested_amount_mm: float,
    current_value_mm: float | None = None,
    realized_value_mm: float = 0.0,
    expected_value_mm: float | None = None,
    target_org_air: float | None = None,
    current_org_air: float | None = None,
    status: str = "active",
) -> dict[str, Any]:
    logger.info("mcp_tool_call", tool="record_investment_roi", company_id=company_id, fund_id=fund_id)
    payload = await record_investment_roi(
        {
            "fund_id": fund_id,
            "company_id": company_id,
            "program_name": program_name,
            "thesis": thesis,
            "invested_amount_mm": invested_amount_mm,
            "current_value_mm": current_value_mm,
            "realized_value_mm": realized_value_mm,
            "expected_value_mm": expected_value_mm,
            "target_org_air": target_org_air,
            "current_org_air": current_org_air,
            "status": status,
        }
    )
    return json.loads(payload)


@mcp.tool(name="get_investment_tracker_summary")
async def get_investment_tracker_summary_mcp(fund_id: str) -> dict[str, Any]:
    logger.info("mcp_tool_call", tool="get_investment_tracker_summary", fund_id=fund_id)
    payload = await get_investment_tracker_summary({"fund_id": fund_id})
    return json.loads(payload)


@mcp.tool(name="generate_ic_memo")
async def generate_ic_memo_mcp(company_id: str, fund_id: str | None = None) -> dict[str, Any]:
    logger.info("mcp_tool_call", tool="generate_ic_memo", company_id=company_id, fund_id=fund_id)
    payload = await generate_ic_memo({"company_id": company_id, "fund_id": fund_id})
    return json.loads(payload)


@mcp.tool(name="generate_lp_letter")
async def generate_lp_letter_mcp(fund_id: str) -> dict[str, Any]:
    logger.info("mcp_tool_call", tool="generate_lp_letter", fund_id=fund_id)
    payload = await generate_lp_letter({"fund_id": fund_id})
    return json.loads(payload)
# -------------------------
# Resources
# -------------------------
def _make_resource_loader(u: str, n: str, d: str) -> None:
    @mcp.resource(u, name=n, description=d)
    def _resource_loader() -> str:
        return read_resource(u)

for resource_def in list_resource_defs():
    _make_resource_loader(resource_def["uri"], resource_def["name"], resource_def["description"])
# -------------------------
# Prompts
# -------------------------
def _make_prompt_loader(p_name: str, p_desc: str) -> None:
    @mcp.prompt(name=p_name, description=p_desc)
    def _prompt_loader(company_id: str) -> str:
        messages = get_prompt(p_name, {"company_id": company_id})
        return "\n\n".join(
            msg.get("content", "")
            for msg in messages
            if isinstance(msg, dict)
        )

for prompt_def in list_prompt_defs():
    _make_prompt_loader(prompt_def["name"], prompt_def["description"])
# -------------------------
# Compatibility helper
# -------------------------
TOOL_HANDLERS = {
    "calculate_org_air_score": calculate_org_air_score,
    "get_company_evidence": get_company_evidence,
    "generate_justification": generate_justification,
    "project_ebitda_impact": project_ebitda_impact,
    "run_gap_analysis": run_gap_analysis,
    "get_portfolio_summary": get_portfolio_summary,
    "remember_company_memory": remember_company_memory,
    "recall_company_memory": recall_company_memory,
    "record_investment_roi": record_investment_roi,
    "get_investment_tracker_summary": get_investment_tracker_summary,
    "generate_ic_memo": generate_ic_memo,
    "generate_lp_letter": generate_lp_letter,
}
async def call_tool(name: str, arguments: dict) -> str:
    """
    Backward-compatible in-process dispatcher.
    Keeps existing agent code working while the real MCP server is added.
    """
    if name not in TOOL_HANDLERS:
        raise ValueError(f"Unknown MCP tool: {name}")
    logger.info("mcp_dispatch_call", tool=name, arguments=arguments)
    return await TOOL_HANDLERS[name](arguments)
def main() -> None:
    transport = os.getenv("MCP_TRANSPORT", "streamable-http")
    if transport == "stdio":
        mcp.run(transport="stdio")
        return

    logger.info(
        "starting_mcp_server",
        transport=transport,
        host=MCP_HOST,
        port=MCP_PORT,
        path=MCP_PATH,
    )
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
 
