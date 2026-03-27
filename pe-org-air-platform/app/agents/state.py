from __future__ import annotations
import operator
from datetime import datetime
from typing import Annotated, Any, Dict, List, Literal, Optional, TypedDict

class AgentMessage(TypedDict):
    role: Literal["user", "assistant", "system", "tool"]
    content: str
    agent_name: Optional[str]
    timestamp: datetime

class DueDiligenceState(TypedDict):
    company_id: str
    assessment_type: Literal["screening", "limited", "full"]
    requested_by: str
    messages: Annotated[List[AgentMessage], operator.add]
    sec_analysis: Optional[Dict[str, Any]]
    talent_analysis: Optional[Dict[str, Any]]
    scoring_result: Optional[Dict[str, Any]]
    evidence_justifications: Optional[Dict[str, Any]]
    value_creation_plan: Optional[Dict[str, Any]]
    next_agent: Optional[str]
    requires_approval: bool
    approval_reason: Optional[str]
    approval_status: Optional[Literal["pending", "approved", "rejected"]]
    approved_by: Optional[str]
    started_at: datetime
    completed_at: Optional[datetime]
    total_tokens: int
    error: Optional[str]
    memory_record_id: Optional[str]
 
