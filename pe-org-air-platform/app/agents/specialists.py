from __future__ import annotations
import json
import os
from datetime import datetime
from typing import Any, Dict
import httpx
import structlog
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI
from app.agents.state import DueDiligenceState
from app.services.observability.metrics import track_agent
logger = structlog.get_logger()
class MultiLLM:
    """
    OpenAI as primary, Gemini as fallback.
    Use this only for reasoning/summarization, not for core factual retrieval.
    """
    def __init__(self) -> None:
        self.primary = None
        self.fallback = None

    def _get_primary(self):
        if self.primary is None and os.getenv("OPENAI_API_KEY"):
            self.primary = ChatOpenAI(
                model="gpt-4o-mini",
                temperature=0.2,
            )
        return self.primary

    def _get_fallback(self):
        if self.fallback is None and os.getenv("GOOGLE_API_KEY"):
            self.fallback = ChatGoogleGenerativeAI(
                model="gemini-1.5-pro",
                temperature=0.2,
            )
        return self.fallback

    def invoke(self, prompt: str):
        primary = self._get_primary()
        fallback = self._get_fallback()

        if primary is None and fallback is None:
            raise RuntimeError(
                "No LLM API key configured. Set OPENAI_API_KEY or GOOGLE_API_KEY."
            )

        try:
            if primary is not None:
                return primary.invoke(prompt)
        except Exception as e:
            logger.warning("openai_failed_fallback_to_gemini", error=str(e))

        try:
            if fallback is not None:
                return fallback.invoke(prompt)
        except Exception as e2:
            logger.error("both_llms_failed", error=str(e2))
            raise

        raise RuntimeError(
            "No available LLM client. Check OPENAI_API_KEY/GOOGLE_API_KEY and connectivity."
        )
class MCPToolCaller:
    """
    Thin async wrapper over the MCP-facing tool interface.
    Replace the URL if you expose the MCP layer through a different route.
    """
    def __init__(self, base_url: str = "http://localhost:3000") -> None:
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=30.0)
    async def call_tool(self, tool_name: str, arguments: dict) -> str:
        response = await self.client.post(
            f"{self.base_url}/tools/{tool_name}",
            json=arguments,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and "result" in payload:
            return payload["result"]
        return json.dumps(payload)
mcp_client = MCPToolCaller()
async def get_org_air_score(company_id: str) -> str:
    return await mcp_client.call_tool(
        "calculate_org_air_score",
        {"company_id": company_id},
    )
async def get_evidence(company_id: str, dimension: str = "all") -> str:
    return await mcp_client.call_tool(
        "get_company_evidence",
        {"company_id": company_id, "dimension": dimension},
    )
async def get_justification(company_id: str, dimension: str) -> str:
    return await mcp_client.call_tool(
        "generate_justification",
        {"company_id": company_id, "dimension": dimension},
    )
async def get_gap_analysis(company_id: str, target: float) -> str:
    return await mcp_client.call_tool(
        "run_gap_analysis",
        {"company_id": company_id, "target_org_air": target},
    )
async def get_ebitda_projection(
    company_id: str,
    entry_score: float,
    target_score: float,
    h_r_score: float,
) -> str:
    return await mcp_client.call_tool(
        "project_ebitda_impact",
        {
            "company_id": company_id,
            "entry_score": entry_score,
            "target_score": target_score,
            "h_r_score": h_r_score,
        },
    )
class SECAnalysisAgent:
    """
    Agent focused on SEC / evidence-led platform assessment.
    """
    def __init__(self) -> None:
        self.llm = MultiLLM()
    @track_agent("sec_analyst")
    async def analyze(self, state: DueDiligenceState) -> Dict[str, Any]:
        company_id = state["company_id"]
        evidence_result = await get_evidence(company_id=company_id, dimension="all")
        findings = json.loads(evidence_result) if evidence_result else []
        summary = self.llm.invoke(
            f"""
            You are analyzing SEC and evidence-led AI readiness signals for {company_id}.
            Summarize the most important findings from the following evidence.
            Focus on:
            - data infrastructure
            - AI governance
            - technology stack
            Keep it concise and factual.
            Evidence:
            {json.dumps(findings[:3], indent=2)}
            """
        ).content
        return {
            "sec_analysis": {
                "company_id": company_id,
                "findings": findings,
                "summary": summary,
                "dimensions_covered": [
                    "data_infrastructure",
                    "ai_governance",
                    "technology_stack",
                ],
            },
            "messages": [
                {
                    "role": "assistant",
                    "content": f"SEC analysis complete for {company_id}",
                    "agent_name": "sec_analyst",
                    "timestamp": datetime.utcnow(),
                }
            ],
        }
class TalentAnalysisAgent:
    """
    Agent focused on talent / external-signal interpretation.
    """
    def __init__(self) -> None:
        self.llm = MultiLLM()
    @track_agent("talent_analyst")
    async def analyze(self, state: DueDiligenceState) -> Dict[str, Any]:
        company_id = state["company_id"]
        evidence_result = await get_evidence(company_id=company_id, dimension="talent")
        findings = json.loads(evidence_result) if evidence_result else []
        summary = self.llm.invoke(
            f"""
            Analyze talent and leadership readiness for {company_id}.
            Summarize what the evidence suggests about:
            - AI talent depth
            - leadership strength
            - organizational culture
            Keep it concise and practical.
            Evidence:
            {json.dumps(findings[:3], indent=2)}
            """
        ).content
        return {
            "talent_analysis": {
                "company_id": company_id,
                "findings": findings,
                "summary": summary,
                "dimensions_covered": ["talent", "leadership", "culture"],
            },
            "messages": [
                {
                    "role": "assistant",
                    "content": f"Talent analysis complete for {company_id}",
                    "agent_name": "talent_analyst",
                    "timestamp": datetime.utcnow(),
                }
            ],
        }
class ScoringAgent:
    """
    Agent responsible for score retrieval and HITL threshold evaluation.
    """
    def __init__(self) -> None:
        self.llm = MultiLLM()
    @track_agent("scorer")
    async def calculate(self, state: DueDiligenceState) -> Dict[str, Any]:
        company_id = state["company_id"]
        score_result = await get_org_air_score(company_id)
        score_data = json.loads(score_result)
        org_air = float(score_data["org_air"])
        requires_approval = org_air > 85 or org_air < 40
        approval_reason = (
            f"Score {org_air:.1f} outside normal range [40, 85]"
            if requires_approval
            else None
        )
        summary = self.llm.invoke(
            f"""
            Summarize this Org-AI-R result for {company_id} in 2-3 sentences.
            Focus on overall interpretation and whether the score appears healthy.
            Score data:
            {json.dumps(score_data, indent=2)}
            """
        ).content
        return {
            "scoring_result": {
                **score_data,
                "summary": summary,
            },
            "requires_approval": requires_approval,
            "approval_reason": approval_reason,
            "approval_status": "pending" if requires_approval else None,
            "messages": [
                {
                    "role": "assistant",
                    "content": (
                        f"Scoring complete: Org-AI-R = {org_air:.1f}"
                        + (" [REQUIRES APPROVAL]" if requires_approval else "")
                    ),
                    "agent_name": "scorer",
                    "timestamp": datetime.utcnow(),
                }
            ],
        }
class EvidenceAgent:
    """
    Agent responsible for grounded CS4 justifications.
    """
    def __init__(self) -> None:
        self.llm = MultiLLM()
    @track_agent("evidence_agent")
    async def justify(self, state: DueDiligenceState) -> Dict[str, Any]:
        company_id = state["company_id"]
        scoring_result = state.get("scoring_result") or {}
        dimension_scores = scoring_result.get("dimension_scores", {})
        if dimension_scores:
            target_dimensions = [
                dim for dim, score in dimension_scores.items() if float(score) < 60
            ]
            target_dimensions = target_dimensions[:3]
        else:
            target_dimensions = [
                "data_infrastructure",
                "talent",
                "use_case_portfolio",
            ]
        justifications = {}
        for dim in target_dimensions:
            result = await get_justification(company_id=company_id, dimension=dim)
            justifications[dim] = json.loads(result)
        summary = self.llm.invoke(
            f"""
            Summarize the most important evidence-backed weaknesses for {company_id}.
            Highlight common gaps and what they imply for AI readiness.
            Justifications:
            {json.dumps(justifications, indent=2)}
            """
        ).content
        return {
            "evidence_justifications": {
                "company_id": company_id,
                "justifications": justifications,
                "summary": summary,
            },
            "messages": [
                {
                    "role": "assistant",
                    "content": f"Generated justifications for {len(justifications)} dimensions",
                    "agent_name": "evidence_agent",
                    "timestamp": datetime.utcnow(),
                }
            ],
        }
class ValueCreationAgent:
    """
    Agent for gap analysis and EBITDA-oriented value creation planning.
    """
    def __init__(self) -> None:
        self.llm = MultiLLM()
    @track_agent("value_creator")
    async def plan(self, state: DueDiligenceState) -> Dict[str, Any]:
        company_id = state["company_id"]
        scoring_result = state.get("scoring_result") or {}
        current_org_air = float(scoring_result.get("org_air", 50.0))
        h_r_score = float(scoring_result.get("hr_score", 50.0))
        target_org_air = max(75.0, current_org_air + 10.0)
        gap_result = await get_gap_analysis(company_id=company_id, target=target_org_air)
        gap_data = json.loads(gap_result)
        ebitda_result = await get_ebitda_projection(
            company_id=company_id,
            entry_score=current_org_air,
            target_score=target_org_air,
            h_r_score=h_r_score,
        )
        ebitda_data = json.loads(ebitda_result)
        risk_adjusted = float(str(ebitda_data.get("risk_adjusted", "0")).replace("%", ""))
        requires_approval = risk_adjusted > 5.0 or state.get("requires_approval", False)
        summary = self.llm.invoke(
            f"""
            Summarize the value creation plan for {company_id}.
            Focus on:
            - target Org-AI-R
            - biggest gaps
            - EBITDA upside
            - whether the case seems attractive
            Gap Analysis:
            {json.dumps(gap_data, indent=2)}
            EBITDA Projection:
            {json.dumps(ebitda_data, indent=2)}
            """
        ).content
        return {
            "value_creation_plan": {
                "company_id": company_id,
                "target_org_air": target_org_air,
                "gap_analysis": gap_data,
                "ebitda_projection": ebitda_data,
                "summary": summary,
            },
            "requires_approval": requires_approval,
            "approval_reason": state.get("approval_reason")
            or (f"EBITDA {risk_adjusted:.2f}% > 5.0%" if risk_adjusted > 5.0 else None),
            "messages": [
                {
                    "role": "assistant",
                    "content": (
                        f"Value creation plan complete. "
                        f"Target Org-AI-R: {target_org_air:.1f}, "
                        f"Risk-adjusted EBITDA: {risk_adjusted:.2f}%"
                    ),
                    "agent_name": "value_creator",
                    "timestamp": datetime.utcnow(),
                }
            ],
        }
sec_agent = SECAnalysisAgent()
talent_agent = TalentAnalysisAgent()
scoring_agent = ScoringAgent()
evidence_agent = EvidenceAgent()
value_agent = ValueCreationAgent()
 
