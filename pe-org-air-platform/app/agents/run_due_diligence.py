from __future__ import annotations
import asyncio
from datetime import datetime
from app.agents.state import DueDiligenceState
from app.agents.supervisor import dd_graph
async def run_due_diligence(company_id: str, assessment_type: str = "full") -> DueDiligenceState:
    initial_state: DueDiligenceState = {
        "company_id": company_id,
        "assessment_type": assessment_type,
        "requested_by": "analyst",
        "messages": [],
        "sec_analysis": None,
        "talent_analysis": None,
        "scoring_result": None,
        "evidence_justifications": None,
        "value_creation_plan": None,
        "next_agent": None,
        "requires_approval": False,
        "approval_reason": None,
        "approval_status": None,
        "approved_by": None,
        "started_at": datetime.utcnow(),
        "completed_at": None,
        "total_tokens": 0,
        "error": None,
    }
    config = {
        "configurable": {
            "thread_id": f"dd-{company_id}-{datetime.utcnow().isoformat()}"
        }
    }
    return await dd_graph.ainvoke(initial_state, config)
async def main():
    result = await run_due_diligence("NVDA", "full")
    print("=" * 60)
    print("PE OrgAIR Agentic Due Diligence")
    print("=" * 60)
    print(f"Company: {result['company_id']}")
    print(f"Org-AI-R: {result['scoring_result']['org_air'] if result.get('scoring_result') else 'N/A'}")
    print(f"HITL Required: {result.get('requires_approval', False)}")
    print(f"Approval Status: {result.get('approval_status', 'N/A')}")
    print(f"Completed At: {result.get('completed_at')}")
    print("=" * 60)
if __name__ == "__main__":
    asyncio.run(main())
 