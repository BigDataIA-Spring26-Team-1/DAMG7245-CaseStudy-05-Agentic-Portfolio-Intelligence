from __future__ import annotations

import importlib
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/v1/platform", tags=["platform"])


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return value


def _tool_handlers() -> dict[str, Callable[..., Any]]:
    module = importlib.import_module("app.mcp.tools")
    return {
        "calculate_org_air_score": module.calculate_org_air_score,
        "get_company_evidence": module.get_company_evidence,
        "generate_justification": module.generate_justification,
        "project_ebitda_impact": module.project_ebitda_impact,
        "run_gap_analysis": module.run_gap_analysis,
        "get_portfolio_summary": module.get_portfolio_summary,
        "remember_company_memory": module.remember_company_memory,
        "recall_company_memory": module.recall_company_memory,
        "record_investment_roi": module.record_investment_roi,
        "get_investment_tracker_summary": module.get_investment_tracker_summary,
        "generate_ic_memo": module.generate_ic_memo,
        "generate_lp_letter": module.generate_lp_letter,
    }


def _resolve_company_id(identifier: str) -> str:
    if not identifier or not identifier.strip():
        raise HTTPException(status_code=400, detail="company identifier is required")

    clean_identifier = identifier.strip()
    client_module = importlib.import_module("app.services.integration.cs1_client")
    client = client_module.CS1Client()
    try:
        company = client.get_company(clean_identifier)
        return str(company.company_id)
    except ValueError:
        pass

    # Fallback for externally-added companies that are not part of CS1 portfolio definitions.
    try:
        snowflake_module = importlib.import_module("app.services.snowflake")
        conn = snowflake_module.get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id
                FROM companies
                WHERE COALESCE(is_deleted, FALSE) = FALSE
                  AND (
                    id = %s
                    OR UPPER(COALESCE(ticker, '')) = UPPER(%s)
                    OR UPPER(COALESCE(name, '')) = UPPER(%s)
                  )
                ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                LIMIT 1
                """,
                (clean_identifier, clean_identifier, clean_identifier),
            )
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
        finally:
            cur.close()
            conn.close()
    except Exception:
        pass

    raise HTTPException(status_code=404, detail=f"Company not found: {clean_identifier}")


async def _run_tool(tool_name: str, arguments: dict[str, Any]) -> Any:
    handler = _tool_handlers().get(tool_name)
    if handler is None:
        raise HTTPException(status_code=404, detail=f"Unknown MCP tool: {tool_name}")

    raw = await handler(arguments)
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {"raw": raw}
    return _jsonable(raw)


class ToolRequest(BaseModel):
    arguments: dict[str, Any] = Field(default_factory=dict)


class AssessmentRecordRequest(BaseModel):
    assessor_id: str = Field(..., min_length=1)
    assessment_type: str = Field(default="full")


class DueDiligenceRequest(BaseModel):
    assessment_type: str = Field(default="full")


class MemoryRememberRequest(BaseModel):
    title: str
    content: str
    company_id: str | None = None
    fund_id: str | None = None
    category: str = "note"
    source: str = "platform_api"
    tags: list[str] | None = None
    metadata: dict[str, Any] | None = None


class MemoryRecallRequest(BaseModel):
    query: str
    company_id: str | None = None
    fund_id: str | None = None
    category: str | None = None
    top_k: int = 5


class InvestmentRecordRequest(BaseModel):
    fund_id: str
    company_id: str
    program_name: str
    thesis: str
    invested_amount_mm: float
    current_value_mm: float | None = None
    realized_value_mm: float = 0.0
    expected_value_mm: float | None = None
    target_org_air: float | None = None
    current_org_air: float | None = None
    status: str = "active"
    start_date: str | None = None
    notes: str = ""
    metadata: dict[str, Any] | None = None


class ICMemoRequest(BaseModel):
    company_id: str
    fund_id: str | None = None


class LPLetterRequest(BaseModel):
    fund_id: str


@router.get("/funds")
def list_funds() -> dict[str, Any]:
    funds: dict[str, str] = {}
    candidate_ids: set[str] = set()

    # Source 1: CS1 configured portfolios (env-backed, no UI hardcoding).
    cs1_module = importlib.import_module("app.services.integration.cs1_client")
    cs1_client = cs1_module.CS1Client()
    try:
        configured = cs1_client._configured_portfolios()  # noqa: SLF001
        for fund_id, portfolio in configured.items():
            clean_id = str(fund_id or "").strip()
            if clean_id:
                funds.setdefault(clean_id, str(getattr(portfolio, "name", clean_id)))
                candidate_ids.add(clean_id)
    except Exception:
        pass

    # Source 2: Persisted CS1 portfolios table in Snowflake.
    try:
        snowflake_module = importlib.import_module("app.services.snowflake")
        conn = snowflake_module.get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, name
                FROM portfolios
                WHERE COALESCE(is_deleted, FALSE) = FALSE
                ORDER BY created_at DESC
                """
            )
            for row in cur.fetchall():
                clean_id = str((row[0] if len(row) > 0 else "") or "").strip()
                if clean_id:
                    clean_name = str((row[1] if len(row) > 1 else clean_id) or clean_id)
                    funds.setdefault(clean_id, clean_name)
                    candidate_ids.add(clean_id)
        finally:
            cur.close()
            conn.close()
    except Exception:
        pass

    # Source 3: Tracked investment records in bonus tracker.
    try:
        bonus_module = importlib.import_module("app.bonus_facade")
        for record in bonus_module.list_investments():
            clean_id = str((record or {}).get("fund_id", "") or "").strip()
            if clean_id:
                funds.setdefault(clean_id, clean_id)
                candidate_ids.add(clean_id)
    except Exception:
        pass

    # Source 4: Probe common CS1 portfolio ids against live holdings.
    # This captures valid funds even when metadata tables are not populated.
    candidate_ids.update({"growth_fund_v", "default"})
    for fund_id in sorted(candidate_ids):
        try:
            holdings = cs1_client.get_portfolio_holdings(fund_id)
            if holdings:
                funds.setdefault(fund_id, funds.get(fund_id, fund_id))
        except Exception:
            continue

    payload = [
        {"fund_id": fund_id, "name": name}
        for fund_id, name in sorted(funds.items(), key=lambda item: item[0].lower())
    ]
    return {"funds": payload}


@router.get("/portfolio/{fund_id}")
async def get_portfolio_overview(fund_id: str) -> dict[str, Any]:
    try:
        payload = await _run_tool("get_portfolio_summary", {"fund_id": fund_id})
    except Exception as exc:
        return {
            "fund_id": fund_id,
            "fund_air": 0.0,
            "company_count": 0,
            "quartile_distribution": {1: 0, 2: 0, 3: 0, 4: 0},
            "sector_hhi": 0.0,
            "avg_delta_since_entry": 0.0,
            "total_ev_mm": 0.0,
            "ai_leaders_count": 0,
            "ai_laggards_count": 0,
            "companies": [],
            "error": str(exc),
        }
    if not isinstance(payload, dict):
        return {
            "fund_id": fund_id,
            "fund_air": 0.0,
            "company_count": 0,
            "quartile_distribution": {1: 0, 2: 0, 3: 0, 4: 0},
            "sector_hhi": 0.0,
            "avg_delta_since_entry": 0.0,
            "total_ev_mm": 0.0,
            "ai_leaders_count": 0,
            "ai_laggards_count": 0,
            "companies": [],
            "error": "Portfolio summary returned invalid payload",
        }
    return payload


@router.get("/mcp/tools")
def list_mcp_tools() -> dict[str, list[str]]:
    return {"tools": sorted(_tool_handlers().keys())}


@router.post("/mcp/tools/{tool_name}")
async def invoke_mcp_tool(tool_name: str, request: ToolRequest) -> Any:
    return await _run_tool(tool_name, request.arguments)


@router.get("/mcp/resources")
def list_mcp_resources() -> dict[str, Any]:
    module = importlib.import_module("app.mcp.resources")
    return {"resources": module.list_resource_defs()}


@router.get("/mcp/resources/read")
def read_mcp_resource(uri: str = Query(..., description="MCP resource URI")) -> Any:
    module = importlib.import_module("app.mcp.resources")
    raw = module.read_resource(uri)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


@router.get("/mcp/prompts")
def list_mcp_prompts() -> dict[str, Any]:
    module = importlib.import_module("app.mcp.prompts")
    return {"prompts": module.list_prompt_defs()}


@router.get("/mcp/prompts/{prompt_name}")
def get_mcp_prompt(
    prompt_name: str,
    company_id: str = Query(..., description="Company identifier for prompt rendering"),
) -> dict[str, Any]:
    module = importlib.import_module("app.mcp.prompts")
    return {"messages": module.get_prompt(prompt_name, {"company_id": company_id})}


@router.post("/history/{company_identifier}/record")
async def record_history_snapshot(
    company_identifier: str,
    request: AssessmentRecordRequest,
) -> Any:
    company_id = _resolve_company_id(company_identifier)
    history_module = importlib.import_module("app.services.tracking.assessment_history")
    cs1_module = importlib.import_module("app.services.integration.cs1_client")
    cs3_module = importlib.import_module("app.services.integration.cs3_client")

    service = history_module.AssessmentHistoryService(cs1_module.CS1Client(), cs3_module.CS3Client())
    snapshot = await service.record_assessment(
        company_id=company_id,
        assessor_id=request.assessor_id,
        assessment_type=request.assessment_type,
    )
    return _jsonable(snapshot)


@router.get("/history/{company_identifier}")
async def get_history(
    company_identifier: str,
    days: int = Query(default=365, ge=1, le=3650),
) -> Any:
    company_id = _resolve_company_id(company_identifier)
    history_module = importlib.import_module("app.services.tracking.assessment_history")
    cs1_module = importlib.import_module("app.services.integration.cs1_client")
    cs3_module = importlib.import_module("app.services.integration.cs3_client")

    service = history_module.AssessmentHistoryService(cs1_module.CS1Client(), cs3_module.CS3Client())
    history = await service.get_history(company_id, days=days)
    return _jsonable(history)


@router.get("/history/{company_identifier}/trend")
async def get_history_trend(company_identifier: str) -> Any:
    company_id = _resolve_company_id(company_identifier)
    history_module = importlib.import_module("app.services.tracking.assessment_history")
    cs1_module = importlib.import_module("app.services.integration.cs1_client")
    cs3_module = importlib.import_module("app.services.integration.cs3_client")

    service = history_module.AssessmentHistoryService(cs1_module.CS1Client(), cs3_module.CS3Client())
    trend = await service.calculate_trend(company_id)
    return _jsonable(trend)


@router.post("/due-diligence/{company_identifier}")
async def execute_due_diligence(
    company_identifier: str,
    request: DueDiligenceRequest,
) -> Any:
    company_id = _resolve_company_id(company_identifier)
    try:
        workflow_module = importlib.import_module("app.agents.run_due_diligence")
    except ModuleNotFoundError as exc:
        missing = getattr(exc, "name", "") or "required dependency"
        raise HTTPException(
            status_code=503,
            detail=(
                f"Due diligence workflow unavailable: missing dependency '{missing}'. "
                "Install project agent dependencies and restart the API server."
            ),
        ) from exc

    try:
        result = await workflow_module.run_due_diligence(company_id, request.assessment_type)
        return _jsonable(result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Due diligence workflow failed: {exc}") from exc


@router.get("/memory/stats")
def get_memory_stats() -> Any:
    module = importlib.import_module("app.bonus_facade")
    return _jsonable(module.memory_stats())


@router.get("/memory")
def list_memory_records(
    company_id: str | None = Query(default=None),
    fund_id: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
) -> Any:
    module = importlib.import_module("app.bonus_facade")
    return _jsonable(module.list_memories(company_id=company_id, fund_id=fund_id, limit=limit))


@router.post("/memory/remember")
def remember_memory(request: MemoryRememberRequest) -> Any:
    module = importlib.import_module("app.bonus_facade")
    return _jsonable(module.remember_company_memory(**request.model_dump()))


@router.post("/memory/recall")
def recall_memory(request: MemoryRecallRequest) -> Any:
    module = importlib.import_module("app.bonus_facade")
    return _jsonable(
        module.recall_company_memory(
            query=request.query,
            company_id=request.company_id,
            fund_id=request.fund_id,
            category=request.category,
            top_k=request.top_k,
        )
    )


@router.get("/investments")
def list_investment_records(
    fund_id: str | None = Query(default=None),
    company_id: str | None = Query(default=None),
) -> Any:
    module = importlib.import_module("app.bonus_facade")
    return _jsonable(module.list_investments(fund_id=fund_id, company_id=company_id))


@router.get("/investments/summary/{fund_id}")
def get_investment_tracker_summary(fund_id: str) -> Any:
    module = importlib.import_module("app.bonus_facade")
    return _jsonable(module.get_investment_summary(fund_id=fund_id))


@router.post("/investments/record")
def record_investment(request: InvestmentRecordRequest) -> Any:
    module = importlib.import_module("app.bonus_facade")
    return _jsonable(module.record_investment(**request.model_dump()))


@router.post("/documents/ic-memo")
def create_ic_memo(request: ICMemoRequest) -> Any:
    module = importlib.import_module("app.bonus_facade")
    return _jsonable(module.generate_ic_memo(request.company_id, fund_id=request.fund_id))


@router.post("/documents/lp-letter")
def create_lp_letter(request: LPLetterRequest) -> Any:
    module = importlib.import_module("app.bonus_facade")
    return _jsonable(module.generate_lp_letter(request.fund_id))
