from __future__ import annotations
import structlog
from app.mcp.tools import (
    calculate_org_air_score,
    get_company_evidence,
    generate_justification,
    project_ebitda_impact,
    run_gap_analysis,
    get_portfolio_summary,
)

from app.mcp.resources import list_resource_defs, read_resource
from app.mcp.prompts import list_prompt_defs, get_prompt
logger = structlog.get_logger()
TOOL_HANDLERS = {
    "calculate_org_air_score": calculate_org_air_score,
    "get_company_evidence": get_company_evidence,
    "generate_justification": generate_justification,
    "project_ebitda_impact": project_ebitda_impact,
    "run_gap_analysis": run_gap_analysis,
    "get_portfolio_summary": get_portfolio_summary,
}
async def call_tool(name: str, arguments: dict) -> str:
    if name not in TOOL_HANDLERS:
        raise ValueError(f"Unknown MCP tool: {name}")
    logger.info("mcp_tool_call", tool=name, arguments=arguments)
    return await TOOL_HANDLERS[name](arguments)
 