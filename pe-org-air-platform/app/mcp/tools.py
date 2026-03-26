from __future__ import annotations
 
import json
import structlog
 
from app.services.integration.portfolio_data_service import portfolio_data_service
from app.services.integration.cs2_client import CS2Client
from app.services.integration.cs3_client import CS3Client
from app.services.justification.generator import JustificationGenerator
from app.services.retrieval.dimension_mapper import DimensionMapper
import asyncio
 
logger = structlog.get_logger()
 
cs2_client = CS2Client()
cs3_client = CS3Client()
justification_generator = JustificationGenerator()
dimension_mapper = DimensionMapper()
 
async def _run_sync(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)
    
async def calculate_org_air_score(arguments: dict) -> str:
    company_id = arguments["company_id"]
 
    try:
        assessment = await _run_sync(cs3_client.get_assessment, company_id)
    except ValueError:
        logger.warning("no_scores_found_triggering_scoring", company_id=company_id)
 
        await _run_sync(cs3_client.run_scoring, company_id, "v1.0")
 
        assessment = await _run_sync(cs3_client.get_assessment, company_id)
 
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
 
 
async def get_company_evidence(arguments: dict) -> str:
    company_id = arguments["company_id"]
    dimension = str(arguments.get("dimension", "all") or "all").strip().lower().replace(" ", "_")
    limit = int(arguments.get("limit", 10) or 10)
 
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
    
 
 
async def generate_justification(arguments: dict) -> str:
    company_id = arguments["company_id"]
    dimension = arguments["dimension"]
 
    result = await _run_sync(
        justification_generator.generate,
        company_id=company_id,
        dimension=dimension,
    )
 
    return json.dumps(result, indent=2, default=str)
 
 
async def project_ebitda_impact(arguments: dict) -> str:
    """
    Replace internal math here with your CS3/CS4-compatible EBITDA projection logic.
    """
    entry_score = float(arguments["entry_score"])
    target_score = float(arguments["target_score"])
    h_r_score = float(arguments["h_r_score"])
 
    delta_air = target_score - entry_score
    conservative = delta_air * 0.06
    base = delta_air * 0.10
    optimistic = delta_air * 0.14
    risk_adjusted = base * (h_r_score / 100.0)
 
    return json.dumps(
        {
            "delta_air": round(delta_air, 2),
            "scenarios": {
                "conservative": f"{conservative:.2f}%",
                "base": f"{base:.2f}%",
                "optimistic": f"{optimistic:.2f}%",
            },
            "risk_adjusted": f"{risk_adjusted:.2f}%",
            "requires_approval": risk_adjusted > 5.0,
        },
        indent=2,
    )
 
 
async def run_gap_analysis(arguments: dict) -> str:
    company_id = arguments["company_id"]
    target_org_air = float(arguments["target_org_air"])
 
    assessment = await _run_sync(cs3_client.get_assessment, company_id)
 
    current_scores = {
        getattr(dim, "value", str(dim)): getattr(score, "score", score)
        for dim, score in assessment.dimension_scores.items()
    }
 
    gaps = {
        dim: round(max(0.0, target_org_air - float(score)), 2)
        for dim, score in current_scores.items()
    }
 
    return json.dumps(
        {
            "company_id": company_id,
            "target_org_air": target_org_air,
            "current_org_air": assessment.org_air_score,
            "dimension_gaps": gaps,
            "priority_dimensions": sorted(gaps, key=gaps.get, reverse=True)[:3],
        },
        indent=2,
    )
 
 
async def get_portfolio_summary(arguments: dict) -> str:
    fund_id = arguments["fund_id"]
    portfolio = await portfolio_data_service.get_portfolio_view(fund_id)
 
    fund_air = sum(c.org_air for c in portfolio) / len(portfolio) if portfolio else 0.0
 
    return json.dumps(
        {
            "fund_id": fund_id,
            "fund_air": round(fund_air, 1),
            "company_count": len(portfolio),
            "companies": [
                {"ticker": c.ticker, "org_air": c.org_air, "sector": c.sector}
                for c in portfolio
            ],
        },
        indent=2,
    )
