from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict
from uuid import uuid4
from app.scoring_engine.evidence_mapper import (
    EvidenceItem,
    map_evidence_to_dimensions,
    build_source_payloads,
    map_sources_to_dimension_features,
)

from app.scoring_engine.rubric_scorer import DimensionScoreResult, score_dimension_features

@dataclass(frozen=True)
class DimensionPipelineOut:
    company_id: str
    assessment_id: str
    results: List[DimensionScoreResult]
    source_payloads: Dict[str, dict]

def score_dimensions_for_assessment(
    *,
    company_id: str,
    assessment_id: str,
    evidence_items: List[EvidenceItem],
) -> DimensionPipelineOut:
    mapped = map_evidence_to_dimensions(evidence_items)
    payloads = build_source_payloads(mapped)
    features = map_sources_to_dimension_features(payloads)
    results = score_dimension_features(features)
    return DimensionPipelineOut(
        company_id=company_id,
        assessment_id=assessment_id,
        results=results,
        source_payloads=payloads,
    )

def upsert_dimension_scores(cur, assessment_id: str, results: List[DimensionScoreResult]) -> None:
    """
    Idempotent write: MERGE on (assessment_id, dimension)
    """
    for r in results:
        cur.execute(
            """
            MERGE INTO dimension_scores t
            USING (
              SELECT %s AS assessment_id, %s AS dimension
            ) s
            ON t.assessment_id = s.assessment_id AND t.dimension = s.dimension
            WHEN MATCHED THEN UPDATE SET
              score = %s,
              confidence = %s,
              evidence_count = %s,
              created_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
              id, assessment_id, dimension, score, weight, confidence, evidence_count, created_at
            ) VALUES (
              %s, %s, %s, %s, NULL, %s, %s, CURRENT_TIMESTAMP()
            )
            """,
            (
                assessment_id,
                r.dimension,
                float(r.score),
                float(r.confidence),
                int(r.evidence_count),
                str(uuid4()),
                assessment_id,
                r.dimension,
                float(r.score),
                float(r.confidence),
                int(r.evidence_count),
            ),
        )