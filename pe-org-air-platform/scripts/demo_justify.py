from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List, Optional

from app.services.integration.company_client import CompanyClient
from app.services.integration.scoring_client import ScoringClient
from app.services.justification.generator import JustificationGenerator
from app.services.workflows.ic_prep import ICPrepWorkflow
from app.services.workflows.analyst_notes import AnalystNotesCollector


def _print_section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _trim_evidence(evidence: List[Dict[str, Any]], limit: int = 3) -> List[Dict[str, Any]]:
    trimmed: List[Dict[str, Any]] = []
    for item in evidence[:limit]:
        trimmed.append(
            {
                "evidence_id": item.get("evidence_id"),
                "title": item.get("title"),
                "source_type": item.get("source_type"),
                "source_url": item.get("source_url"),
                "confidence": item.get("confidence"),
                "relevance_score": item.get("relevance_score"),
                "matched_keywords": item.get("matched_keywords", []),
                "content": (item.get("content") or "")[:250],
            }
        )
    return trimmed


def run_demo(
    company_id: str,
    dimension: str,
    top_k: int,
    min_confidence: Optional[float],
    include_ic_prep: bool,
    include_analyst_note: bool,
    question: Optional[str],
) -> Dict[str, Any]:
    company_client = CompanyClient()
    scoring_client = ScoringClient()
    generator = JustificationGenerator()

    company = company_client.get_company(company_id)
    scores = scoring_client.get_latest_scores(company_id)

    justification = generator.generate(
        company_id=company_id,
        dimension=dimension,
        question=question or f"Why does this company deserve its {dimension.replace('_', ' ')} score?",
        top_k=top_k,
        min_confidence=min_confidence,
    )

    result: Dict[str, Any] = {
        "mode": "demo_justify",
        "company": company,
        "scores": {
            "company_id": scores.get("company_id"),
            "overall_score": scores.get("overall_score"),
            "score_band": scores.get("score_band"),
            "composite_score": scores.get("composite_score"),
            "scored_at": scores.get("scored_at"),
        },
        "justification": {
            "company_id": justification.get("company_id"),
            "dimension": justification.get("dimension"),
            "score": justification.get("score"),
            "level": justification.get("level"),
            "level_name": justification.get("level_name"),
            "evidence_strength": justification.get("evidence_strength"),
            "query_used": justification.get("query_used"),
            "gaps_identified": justification.get("gaps_identified"),
            "generated_summary": justification.get("generated_summary"),
            "supporting_evidence": _trim_evidence(
                justification.get("supporting_evidence", []),
                limit=3,
            ),
        },
    }

    if include_ic_prep:
        workflow = ICPrepWorkflow()
        result["ic_prep"] = workflow.build_packet(
            company_id=company_id,
            top_k=top_k,
            min_confidence=min_confidence,
        )

    if include_analyst_note:
        collector = AnalystNotesCollector()
        result["analyst_note"] = collector.collect_note(
            company_id=company_id,
            dimension=dimension,
            question=question,
            top_k=top_k,
            min_confidence=min_confidence,
        )

    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Demo the end-to-end CS4 justification workflow."
    )
    parser.add_argument(
        "--company-id",
        required=True,
        help="Company UUID to demo",
    )
    parser.add_argument(
        "--dimension",
        default="leadership",
        help="Dimension to justify (default: leadership)",
    )
    parser.add_argument(
        "--question",
        default=None,
        help="Optional custom question to guide justification retrieval",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Top-k evidence chunks to retrieve (default: 5)",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=None,
        help="Optional minimum evidence confidence threshold",
    )
    parser.add_argument(
        "--include-ic-prep",
        action="store_true",
        help="Include IC prep workflow output",
    )
    parser.add_argument(
        "--include-analyst-note",
        action="store_true",
        help="Include analyst note workflow output",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print full JSON output",
    )

    args = parser.parse_args()

    try:
        result = run_demo(
            company_id=args.company_id,
            dimension=args.dimension.strip().lower().replace(" ", "_"),
            top_k=args.top_k,
            min_confidence=args.min_confidence,
            include_ic_prep=args.include_ic_prep,
            include_analyst_note=args.include_analyst_note,
            question=args.question,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "message": str(exc),
                    "company_id": args.company_id,
                    "dimension": args.dimension,
                },
                indent=2,
                default=str,
            )
        )
        raise SystemExit(1) from exc

    if args.json:
        print(json.dumps(result, indent=2, default=str))
        return

    _print_section("COMPANY")
    print(json.dumps(result["company"], indent=2, default=str))

    _print_section("LATEST SCORE SNAPSHOT")
    print(json.dumps(result["scores"], indent=2, default=str))

    _print_section("JUSTIFICATION")
    print(
        json.dumps(
            result["justification"],
            indent=2,
            default=str,
        )
    )

    if "ic_prep" in result:
        _print_section("IC PREP")
        print(json.dumps(result["ic_prep"], indent=2, default=str))

    if "analyst_note" in result:
        _print_section("ANALYST NOTE")
        print(json.dumps(result["analyst_note"], indent=2, default=str))


if __name__ == "__main__":
    main()