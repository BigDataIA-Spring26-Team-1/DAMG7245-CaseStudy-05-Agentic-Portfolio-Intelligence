from __future__ import annotations
import asyncio
from datetime import datetime
from typing import Any, Dict
from app.logging_utils import get_logger
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph
from app.agents.state import DueDiligenceState
from app.bonus_facade import remember_due_diligence_state
from app.agents.specialists import (
    evidence_agent,
    scoring_agent,
    sec_agent,
    talent_agent,
    value_agent,
)
from app.services.observability.metrics import HITL_APPROVALS
logger = get_logger(__name__)
async def supervisor_node(state: DueDiligenceState) -> Dict[str, Any]:
    """
    Decide the next agent based on what has already been completed.
    """
    if state.get("requires_approval") and state.get("approval_status") == "pending":
        return {"next_agent": "hitl_approval"}
    if not state.get("sec_analysis"):
        return {"next_agent": "sec_analyst"}
    if not state.get("talent_analysis"):
        return {"next_agent": "talent_analyst"}
    if not state.get("scoring_result"):
        return {"next_agent": "scorer"}
    if not state.get("evidence_justifications"):
        return {"next_agent": "evidence_agent"}
    if not state.get("value_creation_plan") and state["assessment_type"] != "screening":
        return {"next_agent": "value_creator"}
    return {"next_agent": "complete"}
async def sec_analyst_node(state: DueDiligenceState) -> Dict[str, Any]:
    return await sec_agent.analyze(state)
async def talent_analyst_node(state: DueDiligenceState) -> Dict[str, Any]:
    return await talent_agent.analyze(state)
async def scorer_node(state: DueDiligenceState) -> Dict[str, Any]:
    return await scoring_agent.calculate(state)
async def evidence_node(state: DueDiligenceState) -> Dict[str, Any]:
    return await evidence_agent.justify(state)
async def value_creator_node(state: DueDiligenceState) -> Dict[str, Any]:
    return await value_agent.plan(state)
async def hitl_approval_node(state: DueDiligenceState) -> Dict[str, Any]:
    """
    HITL approval gate.
    For coursework/demo, auto-approve after recording the event.
    """
    reason = state.get("approval_reason") or "manual review required"
    logger.warning(
        "hitl_approval_required",
        company_id=state["company_id"],
        reason=reason,
    )
    HITL_APPROVALS.labels(reason=reason, decision="approved").inc()
    return {
        "approval_status": "approved",
        "approved_by": "exercise_auto_approve",
        "messages": [
            {
                "role": "system",
                "content": f"HITL approval granted: {reason}",
                "agent_name": "hitl",
                "timestamp": datetime.utcnow(),
            }
        ],
    }
async def complete_node(state: DueDiligenceState) -> Dict[str, Any]:
    memory_record_id = None
    try:
        memory_payload = await asyncio.to_thread(remember_due_diligence_state, state)
        memory_record_id = memory_payload.get("memory_id")
    except Exception as exc:
        logger.warning("failed_to_persist_due_diligence_memory", error=str(exc))
    return {
        "completed_at": datetime.utcnow(),
        "memory_record_id": memory_record_id,
        "messages": [
            {
                "role": "assistant",
                "content": f"Due diligence complete for {state['company_id']}",
                "agent_name": "supervisor",
                "timestamp": datetime.utcnow(),
            }
        ],
    }
def create_due_diligence_graph():
    workflow = StateGraph(DueDiligenceState)
    workflow.add_node("supervisor", supervisor_node)
    workflow.add_node("sec_analyst", sec_analyst_node)
    workflow.add_node("talent_analyst", talent_analyst_node)
    workflow.add_node("scorer", scorer_node)
    workflow.add_node("evidence_agent", evidence_node)
    workflow.add_node("value_creator", value_creator_node)
    workflow.add_node("hitl_approval", hitl_approval_node)
    workflow.add_node("complete", complete_node)
    workflow.add_conditional_edges(
        "supervisor",
        lambda s: s["next_agent"],
        {
            "sec_analyst": "sec_analyst",
            "talent_analyst": "talent_analyst",
            "scorer": "scorer",
            "evidence_agent": "evidence_agent",
            "value_creator": "value_creator",
            "hitl_approval": "hitl_approval",
            "complete": "complete",
        },
    )
    for agent in [
        "sec_analyst",
        "talent_analyst",
        "scorer",
        "evidence_agent",
        "value_creator",
    ]:
        workflow.add_edge(agent, "supervisor")
    workflow.add_edge("hitl_approval", "supervisor")
    workflow.add_edge("complete", END)
    workflow.set_entry_point("supervisor")
    return workflow.compile(checkpointer=MemorySaver())
dd_graph = create_due_diligence_graph()
 
