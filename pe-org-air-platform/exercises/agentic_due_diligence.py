from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from app.agents.run_due_diligence import run_due_diligence


def _build_summary(result: dict[str, Any]) -> dict[str, Any]:
    scoring_result = result.get("scoring_result") or {}
    value_creation_plan = result.get("value_creation_plan") or {}
    return {
        "company_id": result.get("company_id"),
        "assessment_type": result.get("assessment_type"),
        "approval_status": result.get("approval_status"),
        "requires_approval": result.get("requires_approval", False),
        "org_air": scoring_result.get("org_air"),
        "vr_score": scoring_result.get("vr_score"),
        "hr_score": scoring_result.get("hr_score"),
        "target_org_air": value_creation_plan.get("target_org_air"),
        "completed_at": result.get("completed_at"),
        "message_count": len(result.get("messages", [])),
    }


async def _main_async(company_id: str, assessment_type: str, as_json: bool) -> None:
    result = await run_due_diligence(company_id, assessment_type)
    if as_json:
        print(json.dumps(result, indent=2, default=str))
        return

    summary = _build_summary(result)
    print("=" * 72)
    print("PE OrgAIR Agentic Due Diligence")
    print("=" * 72)
    print(f"Company: {summary['company_id']}")
    print(f"Assessment Type: {summary['assessment_type']}")
    print(f"Org-AI-R: {summary['org_air']}")
    print(f"Requires Approval: {summary['requires_approval']}")
    print(f"Approval Status: {summary['approval_status']}")
    print(f"Target Org-AI-R: {summary['target_org_air']}")
    print(f"Completed At: {summary['completed_at']}")
    print(f"Messages: {summary['message_count']}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the CS5 agentic due diligence workflow."
    )
    parser.add_argument(
        "--company-id",
        default="NVDA",
        help="Company identifier or ticker to analyze.",
    )
    parser.add_argument(
        "--assessment-type",
        default="full",
        choices=["screening", "limited", "full"],
        help="Due diligence depth.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full workflow result as JSON.",
    )
    args = parser.parse_args()

    asyncio.run(
        _main_async(
            company_id=args.company_id,
            assessment_type=args.assessment_type,
            as_json=args.json,
        )
    )


if __name__ == "__main__":
    main()
