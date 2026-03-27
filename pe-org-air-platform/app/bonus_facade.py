from __future__ import annotations

from functools import lru_cache
from typing import Any

from app.services.extensions.investment_tracker import InvestmentTrackerService
from app.services.extensions.mem0_memory import Mem0SemanticMemoryService
from app.services.extensions.report_generators import ICMemoGenerator, LPLetterGenerator


@lru_cache(maxsize=1)
def get_memory_service() -> Mem0SemanticMemoryService:
    return Mem0SemanticMemoryService()


@lru_cache(maxsize=1)
def get_investment_tracker() -> InvestmentTrackerService:
    return InvestmentTrackerService()


@lru_cache(maxsize=1)
def get_ic_memo_generator() -> ICMemoGenerator:
    return ICMemoGenerator(
        memory_service=get_memory_service(),
        investment_tracker=get_investment_tracker(),
    )


@lru_cache(maxsize=1)
def get_lp_letter_generator() -> LPLetterGenerator:
    return LPLetterGenerator(
        memory_service=get_memory_service(),
        investment_tracker=get_investment_tracker(),
    )


def remember_company_memory(
    *,
    title: str,
    content: str,
    company_id: str | None = None,
    fund_id: str | None = None,
    category: str = "note",
    source: str = "manual",
    tags: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return get_memory_service().remember(
        title=title,
        content=content,
        company_id=company_id,
        fund_id=fund_id,
        category=category,
        source=source,
        tags=tags,
        metadata=metadata,
    )


def remember_due_diligence_state(state: dict[str, Any]) -> dict[str, Any]:
    return get_memory_service().remember_due_diligence_state(state)


def recall_company_memory(
    *,
    query: str,
    company_id: str | None = None,
    fund_id: str | None = None,
    category: str | None = None,
    top_k: int = 5,
) -> list[dict[str, Any]]:
    return get_memory_service().recall(
        query=query,
        company_id=company_id,
        fund_id=fund_id,
        category=category,
        top_k=top_k,
    )


def list_memories(
    *,
    company_id: str | None = None,
    fund_id: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    return get_memory_service().list_memories(company_id=company_id, fund_id=fund_id, limit=limit)


def memory_stats() -> dict[str, Any]:
    return get_memory_service().stats()


def record_investment(
    *,
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
    start_date: str | None = None,
    notes: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return get_investment_tracker().add_investment(
        fund_id=fund_id,
        company_id=company_id,
        program_name=program_name,
        thesis=thesis,
        invested_amount_mm=invested_amount_mm,
        current_value_mm=current_value_mm,
        realized_value_mm=realized_value_mm,
        expected_value_mm=expected_value_mm,
        target_org_air=target_org_air,
        current_org_air=current_org_air,
        status=status,
        start_date=start_date,
        notes=notes,
        metadata=metadata,
    )


def list_investments(*, fund_id: str | None = None, company_id: str | None = None) -> list[dict[str, Any]]:
    return get_investment_tracker().list_investments(fund_id=fund_id, company_id=company_id)


def get_investment_summary(*, fund_id: str) -> dict[str, Any]:
    return get_investment_tracker().summarize(fund_id=fund_id)


def generate_ic_memo(company_id: str, fund_id: str | None = None) -> dict[str, Any]:
    return get_ic_memo_generator().generate(company_id=company_id, fund_id=fund_id)


def generate_lp_letter(fund_id: str) -> dict[str, Any]:
    return get_lp_letter_generator().generate_sync(fund_id=fund_id)

