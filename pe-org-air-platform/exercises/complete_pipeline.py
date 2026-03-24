from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from typing import Any, Dict, List, Optional

from app.services.integration.cs1_client import CS1Client
from app.services.integration.cs2_client import CS2Client
from app.services.integration.cs3_client import CS3Client, Dimension
from app.services.justification.generator import JustificationGenerator
from app.services.result_artifacts import write_json_artifact
from app.services.retrieval.dimension_mapper import DimensionMapper
from app.services.retrieval.hybrid import HybridRetriever
from app.services.workflows.ic_prep import ICPrepWorkflow
from scripts.index_evidence import evidence_to_docchunk


def _print_banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def _serialize_dimension_score(score: Any) -> Dict[str, Any]:
    return {
        "dimension": score.dimension.value,
        "score": score.score,
        "level": int(score.level.value),
        "level_name": score.level.name_label,
        "confidence_interval": list(score.confidence_interval),
        "evidence_count": score.evidence_count,
        "last_updated": score.last_updated,
    }


def _serialize_company(company: Any) -> Dict[str, Any]:
    payload = asdict(company)
    payload["sector"] = company.sector.value
    return payload


def _serialize_assessment(assessment: Any) -> Dict[str, Any]:
    return {
        "company_id": assessment.company_id,
        "assessment_date": assessment.assessment_date,
        "vr_score": assessment.vr_score,
        "hr_score": assessment.hr_score,
        "synergy_score": assessment.synergy_score,
        "org_air_score": assessment.org_air_score,
        "confidence_interval": list(assessment.confidence_interval),
        "talent_concentration": assessment.talent_concentration,
        "position_factor": assessment.position_factor,
    }


def _trim_evidence(evidence: List[Dict[str, Any]], limit: int = 3) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in evidence[:limit]:
        out.append(
            {
                "evidence_id": item.get("evidence_id"),
                "source_type": item.get("source_type"),
                "title": item.get("title"),
                "confidence": item.get("confidence"),
                "matched_keywords": item.get("matched_keywords", []),
                "content": (item.get("content") or "")[:180],
            }
        )
    return out


def _serialize_rubric(item: Any) -> Dict[str, Any]:
    payload = asdict(item)
    payload["dimension"] = item.dimension.value
    payload["level"] = int(item.level.value)
    return payload


def run_exercise(
    identifier: str = "NVDA",
    dimension: str = Dimension.DATA_INFRASTRUCTURE.value,
    top_k: int = 5,
    min_confidence: Optional[float] = None,
    reindex: bool = True,
) -> Dict[str, Any]:
    target_dimension = Dimension(dimension)

    cs1 = CS1Client()
    cs2 = CS2Client()
    cs3 = CS3Client()
    retriever = HybridRetriever()
    generator = JustificationGenerator()
    ic_prep = ICPrepWorkflow()

    company = cs1.get_company(identifier)
    assessment = cs3.get_assessment(company.company_id)
    dimension_score = cs3.get_dimension_score(company.company_id, target_dimension)
    rubrics = cs3.get_rubric(target_dimension, dimension_score.level)

    evidence = cs2.get_evidence(
        company_id=company.company_id,
        min_confidence=min_confidence or 0.0,
    )

    mapper = DimensionMapper()
    chunks = [
        evidence_to_docchunk(item, mapper)
        for item in evidence
        if (item.content or "").strip()
    ]

    deleted = 0
    if reindex:
        deleted = retriever.vector_store.delete_by_filter({"company_id": company.company_id})

    indexed = retriever.index_documents(chunks)
    if chunks:
        cs2.mark_indexed([chunk.id for chunk in chunks])

    question = (
        f"Why did {company.name} score {dimension_score.score:.1f} on "
        f"{target_dimension.value.replace('_', ' ')}?"
    )
    justification = generator.generate_justification(
        company_id=company.company_id,
        dimension=target_dimension,
        question=question,
        top_k=top_k,
        min_confidence=min_confidence,
    )
    packet = ic_prep.build_packet(
        company_id=company.company_id,
        dimensions=[target_dimension.value],
        top_k=top_k,
        min_confidence=min_confidence,
    )

    result = {
        "company": _serialize_company(company),
        "assessment": _serialize_assessment(assessment),
        "dimension_score": _serialize_dimension_score(dimension_score),
        "rubric": [_serialize_rubric(item) for item in rubrics],
        "evidence_summary": {
            "fetched": len(evidence),
            "indexed": indexed,
            "deleted_before_reindex": deleted,
        },
        "justification": justification,
        "ic_packet": packet,
    }
    write_json_artifact(
        ticker=(company.ticker or company.company_id),
        category="cs4",
        filename="complete_pipeline_latest.json",
        payload=result,
    )
    return result


def _print_report(result: Dict[str, Any]) -> None:
    company = result["company"]
    dimension_score = result["dimension_score"]
    rubric = result["rubric"][0] if result["rubric"] else {}
    justification = result["justification"]
    evidence_summary = result["evidence_summary"]
    ic_packet = result["ic_packet"]

    _print_banner("STEP 1 - CS1 COMPANY LOOKUP")
    print(f"Company: {company['name']} ({company.get('ticker') or 'n/a'})")
    print(f"Sector: {company['sector']}")
    print(f"Market cap percentile: {company['market_cap_percentile']:.2f}")

    _print_banner("STEP 2 - CS3 DIMENSION SCORE")
    print(f"Dimension: {dimension_score['dimension']}")
    print(f"Score: {dimension_score['score']:.1f}")
    print(f"Level: {dimension_score['level']} ({dimension_score['level_name']})")
    print(
        "Confidence interval: "
        f"[{dimension_score['confidence_interval'][0]:.1f}, {dimension_score['confidence_interval'][1]:.1f}]"
    )

    _print_banner("STEP 3 - RUBRIC")
    if rubric:
        print(f"Level {rubric['level']} rubric:")
        print((rubric["criteria_text"] or "")[:220])
        print(f"Keywords: {', '.join(rubric.get('keywords', [])[:6])}")
    else:
        print("No rubric returned.")

    _print_banner("STEP 4 - CS2 EVIDENCE INDEXING")
    print(f"Evidence fetched: {evidence_summary['fetched']}")
    print(f"Vectors indexed: {evidence_summary['indexed']}")
    print(f"Deleted before reindex: {evidence_summary['deleted_before_reindex']}")

    _print_banner("STEP 5 - JUSTIFICATION")
    print(f"Summary: {justification['generated_summary']}")
    print(f"Evidence strength: {justification['evidence_strength']}")
    print("Supporting evidence:")
    for item in _trim_evidence(justification.get("supporting_evidence", [])):
        print(
            f"- [{item['source_type']}] conf={float(item['confidence'] or 0.0):.2f} "
            f"{item.get('title') or item['evidence_id']}"
        )
        print(f"  {item['content']}")
    if justification.get("gaps_identified"):
        print("Gaps identified:")
        for gap in justification["gaps_identified"]:
            print(f"- {gap}")

    _print_banner("STEP 6 - IC PACKET SNAPSHOT")
    print(f"Recommendation: {ic_packet['recommendation']}")
    print(f"Average evidence strength: {ic_packet['avg_evidence_strength']}")
    print(f"Total evidence count: {ic_packet['total_evidence_count']}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the CS4 complete pipeline exercise for a company and dimension."
    )
    parser.add_argument(
        "--identifier",
        default="NVDA",
        help="Company identifier accepted by the CS1 facade, such as NVDA or a company UUID.",
    )
    parser.add_argument(
        "--dimension",
        default=Dimension.DATA_INFRASTRUCTURE.value,
        choices=[item.value for item in Dimension],
        help="Dimension to justify.",
    )
    parser.add_argument("--top-k", type=int, default=5, help="Number of evidence items to cite.")
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=None,
        help="Optional evidence confidence filter between 0.0 and 1.0.",
    )
    parser.add_argument(
        "--no-reindex",
        action="store_true",
        help="Skip deleting existing vectors for the selected company before indexing.",
    )
    parser.add_argument("--json", action="store_true", help="Print the full result payload as JSON.")
    args = parser.parse_args()

    try:
        result = run_exercise(
            identifier=args.identifier,
            dimension=args.dimension,
            top_k=args.top_k,
            min_confidence=args.min_confidence,
            reindex=not args.no_reindex,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "message": str(exc),
                    "identifier": args.identifier,
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

    _print_report(result)


if __name__ == "__main__":
    main()
