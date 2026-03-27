from __future__ import annotations
 
import asyncio
import json
from functools import lru_cache
from typing import Any

import structlog
 
from app.bonus_facade import (
    generate_ic_memo as generate_ic_memo_artifact,
    generate_lp_letter as generate_lp_letter_artifact,
    get_investment_summary,
    list_investments,
    list_memories,
    memory_stats,
    recall_company_memory as recall_company_memory_entries,
    record_investment as record_investment_entry,
    remember_company_memory as remember_company_memory_entry,
)
from app.services.integration.cs1_client import CS1Client
from app.services.integration.cs2_client import CS2Client
from app.services.integration.cs3_client import CS3Client, Dimension as CS3Dimension, ScoreLevel
from app.services.integration.portfolio_data_service import portfolio_data_service
from app.services.justification.generator import JustificationGenerator
from app.services.observability.metrics import track_mcp_tool
from app.services.retrieval.dimension_mapper import DimensionMapper
from app.services.value_creation import value_creation_service
 
logger = structlog.get_logger()
 

@lru_cache(maxsize=1)
def get_cs1_client() -> CS1Client:
    return CS1Client()


@lru_cache(maxsize=1)
def get_cs2_client() -> CS2Client:
    return CS2Client()


@lru_cache(maxsize=1)
def get_cs3_client() -> CS3Client:
    return CS3Client()


@lru_cache(maxsize=1)
def get_justification_generator() -> JustificationGenerator:
    return JustificationGenerator()


@lru_cache(maxsize=1)
def get_dimension_mapper() -> DimensionMapper:
    return DimensionMapper()
 

async def _run_sync(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


def _dimension_name(value: Any) -> str:
    return str(getattr(value, "value", value))


def _assessment_dimension_context(assessment: Any) -> dict[str, dict[str, float | int]]:
    out: dict[str, dict[str, float | int]] = {}
    for dimension, score in getattr(assessment, "dimension_scores", {}).items():
        level_value = getattr(getattr(score, "level", None), "value", getattr(score, "level", 1))
        out[_dimension_name(dimension)] = {
            "score": float(getattr(score, "score", 0.0) or 0.0),
            "level": int(level_value or 1),
            "evidence_count": int(getattr(score, "evidence_count", 0) or 0),
        }
    return out


async def _get_assessment_with_backfill(cs3_client: CS3Client, company_id: str):
    try:
        return await _run_sync(cs3_client.get_assessment, company_id)
    except ValueError:
        logger.warning("no_scores_found_triggering_scoring", company_id=company_id)
        await _run_sync(cs3_client.run_scoring, company_id, "v1.0")
        return await _run_sync(cs3_client.get_assessment, company_id)


async def _rubric_targets_for_dimensions(
    cs3_client: CS3Client,
    dimensions: list[str],
    target_level: int,
) -> dict[str, str]:
    rubric_targets: dict[str, str] = {}
    for dim in dimensions:
        try:
            rubric_items = await _run_sync(cs3_client.get_rubric, CS3Dimension(dim), ScoreLevel(target_level))
        except Exception:
            rubric_items = []
        if rubric_items:
            rubric_targets[dim] = str(rubric_items[0].criteria_text)
    return rubric_targets
    

@track_mcp_tool("calculate_org_air_score")
async def calculate_org_air_score(arguments: dict) -> str:
    company_id = arguments["company_id"]
    cs3_client = get_cs3_client()
    assessment = await _get_assessment_with_backfill(cs3_client, company_id)
 
    return json.dumps(
        {
            "company_id": company_id,
            "org_air": assessment.org_air_score,
            "vr_score": assessment.vr_score,
            "hr_score": assessment.hr_score,
            "synergy_score": assessment.synergy_score,
            "confidence_interval": list(assessment.confidence_interval),
            "dimension_scores": {
                getattr(dim, "value", str(dim)): getattr(score, "score", score)
                for dim, score in assessment.dimension_scores.items()
            },
        },
        indent=2,
    )
 
 
@track_mcp_tool("get_company_evidence")
async def get_company_evidence(arguments: dict) -> str:
    company_id = arguments["company_id"]
    dimension = str(arguments.get("dimension", "all") or "all").strip().lower().replace(" ", "_")
    limit = int(arguments.get("limit", 10) or 10)
    cs2_client = get_cs2_client()
    dimension_mapper = get_dimension_mapper()
 
    evidence = await _run_sync(cs2_client.get_evidence, company_id)

    if dimension != "all":
        dimension_aliases = {
            "talent_skills": "talent",
            "leadership_vision": "leadership",
            "culture_change": "culture",
        }
        target_dimension = dimension_aliases.get(dimension, dimension)
        evidence = [
            item
            for item in evidence
            if target_dimension in dimension_mapper.get_all_dimensions_for_evidence(
                signal_category=getattr(item.signal_category, "value", item.signal_category),
                source_type=getattr(item.source_type, "value", item.source_type),
                public_names=True,
            )
        ]

    if limit > 0:
        evidence = evidence[:limit]
 
    return json.dumps(
        [
            {
                "source_type": getattr(e.source_type, "value", str(e.source_type)),
                "content": e.content[:500],
                "confidence": e.confidence,
                "signal_category": getattr(e.signal_category, "value", str(e.signal_category)),
                "source_url": e.source_url,
                "title": e.title,
                "fiscal_year": e.fiscal_year,
            }
            for e in evidence
        ],
        indent=2,
    )
    
 
 
@track_mcp_tool("generate_justification")
async def generate_justification(arguments: dict) -> str:
    company_id = arguments["company_id"]
    dimension = arguments["dimension"]
    justification_generator = get_justification_generator()
 
    result = await _run_sync(
        justification_generator.generate,
        company_id=company_id,
        dimension=dimension,
    )
 
    return json.dumps(result, indent=2, default=str)
 
 
@track_mcp_tool("project_ebitda_impact")
async def project_ebitda_impact(arguments: dict) -> str:
    company_id = arguments["company_id"]
    entry_score = float(arguments["entry_score"])
    target_score = float(arguments["target_score"])
    h_r_score = float(arguments["h_r_score"])
    cs1_client = get_cs1_client()
    cs3_client = get_cs3_client()

    company = await _run_sync(cs1_client.get_company, company_id)
    assessment = await _get_assessment_with_backfill(cs3_client, company_id)

    projection = value_creation_service.project_ebitda(
        company_id=company_id,
        sector=getattr(getattr(company, "sector", None), "value", getattr(company, "sector", "unknown")),
        position_factor=float(getattr(assessment, "position_factor", 0.0) or 0.0),
        current_org_air=float(getattr(assessment, "org_air_score", 0.0) or 0.0),
        current_synergy=float(getattr(assessment, "synergy_score", 0.0) or 0.0),
        entry_score=entry_score,
        target_score=target_score,
        h_r_score=float(getattr(assessment, "hr_score", h_r_score) if getattr(assessment, "hr_score", None) is not None else h_r_score),
    )

    return json.dumps(projection.as_payload(), indent=2)
 
 
@track_mcp_tool("run_gap_analysis")
async def run_gap_analysis(arguments: dict) -> str:
    company_id = arguments["company_id"]
    target_org_air = float(arguments["target_org_air"])
    cs1_client = get_cs1_client()
    cs3_client = get_cs3_client()

    company = await _run_sync(cs1_client.get_company, company_id)
    assessment = await _get_assessment_with_backfill(cs3_client, company_id)
    dimension_scores = _assessment_dimension_context(assessment)
    target_level = 5 if target_org_air >= 80 else 4
    rubric_targets = await _rubric_targets_for_dimensions(
        cs3_client,
        list(dimension_scores.keys()),
        target_level,
    )
    current_org_air = float(getattr(assessment, "org_air_score", 0.0) or 0.0)
    current_hr = float(getattr(assessment, "hr_score", 50.0) or 50.0)
    projection = value_creation_service.project_ebitda(
        company_id=company_id,
        sector=getattr(getattr(company, "sector", None), "value", getattr(company, "sector", "unknown")),
        position_factor=float(getattr(assessment, "position_factor", 0.0) or 0.0),
        current_org_air=current_org_air,
        current_synergy=float(getattr(assessment, "synergy_score", 0.0) or 0.0),
        entry_score=current_org_air,
        target_score=target_org_air,
        h_r_score=current_hr,
    )
    result = value_creation_service.analyze_gap(
        company_id=company_id,
        sector=getattr(getattr(company, "sector", None), "value", getattr(company, "sector", "unknown")),
        current_org_air=current_org_air,
        target_org_air=target_org_air,
        position_factor=float(getattr(assessment, "position_factor", 0.0) or 0.0),
        dimension_scores=dimension_scores,
        rubric_targets=rubric_targets,
        ebitda_projection=projection,
    )

    return json.dumps(result.as_payload(), indent=2)
 
 
@track_mcp_tool("get_portfolio_summary")
async def get_portfolio_summary(arguments: dict) -> str:
    fund_id = arguments["fund_id"]
    portfolio = await portfolio_data_service.get_portfolio_view(fund_id)

    from app.services.analytics.fund_air import fund_air_calculator

    enterprise_values = {
        c.company_id: float(c.enterprise_value_mm)
        for c in portfolio
    }

    metrics = fund_air_calculator.calculate_fund_metrics(
        fund_id=fund_id,
        companies=portfolio,
        enterprise_values=enterprise_values,
    )

    return json.dumps(
        {
            "fund_id": metrics.fund_id,
            "fund_air": metrics.fund_air,
            "company_count": metrics.company_count,
            "quartile_distribution": metrics.quartile_distribution,
            "sector_hhi": metrics.sector_hhi,
            "avg_delta_since_entry": metrics.avg_delta_since_entry,
            "total_ev_mm": metrics.total_ev_mm,
            "ai_leaders_count": metrics.ai_leaders_count,
            "ai_laggards_count": metrics.ai_laggards_count,
            "companies": [
                {
                    "company_id": c.company_id,
                    "ticker": c.ticker,
                    "org_air": c.org_air,
                    "sector": c.sector,
                    "delta_since_entry": c.delta_since_entry,
                    "enterprise_value_mm": c.enterprise_value_mm,
                    "enterprise_value_source": c.enterprise_value_source,
                }
                for c in portfolio
            ],
        },
        indent=2,
    )


@track_mcp_tool("remember_company_memory")
async def remember_company_memory(arguments: dict) -> str:
    payload = await _run_sync(
        remember_company_memory_entry,
        title=str(arguments["title"]),
        content=str(arguments["content"]),
        company_id=arguments.get("company_id"),
        fund_id=arguments.get("fund_id"),
        category=str(arguments.get("category", "note")),
        source=str(arguments.get("source", "mcp_tool")),
        tags=list(arguments.get("tags", []) or []),
        metadata=dict(arguments.get("metadata", {}) or {}),
    )
    return json.dumps(payload, indent=2)


@track_mcp_tool("recall_company_memory")
async def recall_company_memory(arguments: dict) -> str:
    payload = await _run_sync(
        recall_company_memory_entries,
        query=str(arguments["query"]),
        company_id=arguments.get("company_id"),
        fund_id=arguments.get("fund_id"),
        category=arguments.get("category"),
        top_k=int(arguments.get("top_k", 5) or 5),
    )
    return json.dumps(
        {
            "results": payload,
            "stats": memory_stats(),
        },
        indent=2,
    )


@track_mcp_tool("record_investment_roi")
async def record_investment_roi(arguments: dict) -> str:
    payload = await _run_sync(
        record_investment_entry,
        fund_id=str(arguments["fund_id"]),
        company_id=str(arguments["company_id"]),
        program_name=str(arguments["program_name"]),
        thesis=str(arguments["thesis"]),
        invested_amount_mm=float(arguments["invested_amount_mm"]),
        current_value_mm=float(arguments["current_value_mm"]) if arguments.get("current_value_mm") is not None else None,
        realized_value_mm=float(arguments.get("realized_value_mm", 0.0) or 0.0),
        expected_value_mm=float(arguments["expected_value_mm"]) if arguments.get("expected_value_mm") is not None else None,
        target_org_air=float(arguments["target_org_air"]) if arguments.get("target_org_air") is not None else None,
        current_org_air=float(arguments["current_org_air"]) if arguments.get("current_org_air") is not None else None,
        status=str(arguments.get("status", "active")),
        start_date=arguments.get("start_date"),
        notes=str(arguments.get("notes", "")),
        metadata=dict(arguments.get("metadata", {}) or {}),
    )
    return json.dumps(payload, indent=2)


@track_mcp_tool("get_investment_tracker_summary")
async def get_investment_tracker_summary(arguments: dict) -> str:
    fund_id = str(arguments["fund_id"])
    summary = await _run_sync(get_investment_summary, fund_id=fund_id)
    investments = await _run_sync(list_investments, fund_id=fund_id)
    memories = await _run_sync(list_memories, fund_id=fund_id, limit=10)
    return json.dumps(
        {
            **summary,
            "investments": investments,
            "recent_memories": memories,
        },
        indent=2,
    )


@track_mcp_tool("generate_ic_memo")
async def generate_ic_memo(arguments: dict) -> str:
    payload = await _run_sync(
        generate_ic_memo_artifact,
        str(arguments["company_id"]),
        arguments.get("fund_id"),
    )
    return json.dumps(payload, indent=2)


@track_mcp_tool("generate_lp_letter")
async def generate_lp_letter(arguments: dict) -> str:
    payload = await _run_sync(generate_lp_letter_artifact, str(arguments["fund_id"]))
    return json.dumps(payload, indent=2)
