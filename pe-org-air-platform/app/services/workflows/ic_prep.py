from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from app.services.integration.company_client import CompanyClient
from app.services.integration.scoring_client import ScoringClient
from app.services.justification.generator import JustificationGenerator


@dataclass
class ICDimensionPacket:
    dimension: str
    score: float
    level: int
    level_name: str
    evidence_strength: str
    summary: str
    top_evidence: List[Dict[str, Any]]
    gaps_identified: List[str]


@dataclass
class ICPrepPacket:
    company_id: str
    company_profile: Dict[str, Any]
    overall_score: Optional[float]
    overall_level: Optional[int]
    overall_level_name: Optional[str]
    score_band: Optional[str]
    dimensions: List[ICDimensionPacket]
    strengths: List[str]
    key_gaps: List[str]
    risks: List[str]
    diligence_questions: List[str]
    recommendation: str
    generated_at: str
    total_evidence_count: int
    avg_evidence_strength: str
    generated_by: str


class ICPrepWorkflow:
    """
    Builds an investment-committee-style prep packet by orchestrating:
    - company metadata lookup
    - latest scoring lookup
    - dimension-level justification generation

    This implementation is deterministic and grounded in retrieved evidence.
    """

    JUSTIFICATION_DIMENSIONS: List[str] = [
        "leadership",
        "talent",
        "culture",
        "ai_governance",
        "data_infrastructure",
        "technology_stack",
        "use_case_portfolio",
    ]

    SCORE_TO_JUSTIFICATION_DIMENSION: Dict[str, str] = {
        "leadership_vision": "leadership",
        "talent_skills": "talent",
        "culture_change": "culture",
        "ai_governance": "ai_governance",
        "data_infrastructure": "data_infrastructure",
        "technology_stack": "technology_stack",
        "use_case_portfolio": "use_case_portfolio",
    }

    LEVEL_NAMES: Dict[int, str] = {
        1: "Nascent",
        2: "Developing",
        3: "Adequate",
        4: "Good",
        5: "Excellent",
    }

    def __init__(self) -> None:
        self.company_client = CompanyClient()
        self.scoring_client = ScoringClient()
        self.generator = JustificationGenerator()

    def build_packet(
        self,
        company_id: str,
        dimensions: Optional[List[str]] = None,
        top_k: int = 5,
        min_confidence: Optional[float] = None,
    ) -> Dict[str, Any]:
        if not company_id or not company_id.strip():
            raise ValueError("company_id is required")

        company_profile = self.company_client.get_company(company_id)
        scorecard = self.scoring_client.get_latest_scores(company_id)

        selected_dimensions = self._resolve_dimensions(scorecard, dimensions)

        dimension_packets: List[ICDimensionPacket] = []
        key_gaps: List[str] = []
        total_evidence_count = 0
        strength_values: List[int] = []
        for dimension in selected_dimensions:
            justification = self.generator.generate(
                company_id=company_id,
                dimension=dimension,
                question=f"Why does this company deserve its {dimension.replace('_', ' ')} score?",
                top_k=top_k,
                min_confidence=min_confidence,
            )
            supporting_evidence = list(justification.get("supporting_evidence", []))
            total_evidence_count += len(supporting_evidence)
            strength_values.append(self._strength_value(str(justification.get("evidence_strength", "weak"))))
            if justification.get("gaps_identified"):
                key_gaps.append(str(justification["gaps_identified"][0]))

            dimension_packets.append(
                ICDimensionPacket(
                    dimension=dimension,
                    score=float(justification["score"]),
                    level=int(justification["level"]),
                    level_name=str(justification["level_name"]),
                    evidence_strength=str(justification["evidence_strength"]),
                    summary=str(justification["generated_summary"]),
                    top_evidence=self._trim_evidence(supporting_evidence),
                    gaps_identified=list(justification.get("gaps_identified", [])),
                )
            )

        overall_score = self._extract_overall_score(scorecard, dimension_packets)
        overall_level = self._score_to_level(overall_score) if overall_score is not None else None
        overall_level_name = self.LEVEL_NAMES.get(overall_level) if overall_level is not None else None
        score_band = scorecard.get("score_band")

        strengths = self._derive_strengths(dimension_packets)
        risks = self._derive_risks(dimension_packets)
        diligence_questions = self._derive_diligence_questions(dimension_packets)
        avg_evidence_strength = self._avg_strength_label(strength_values)
        recommendation = self._derive_recommendation(
            overall_score=overall_score,
            strengths=strengths,
            risks=risks,
        )

        packet = ICPrepPacket(
            company_id=company_id,
            company_profile=company_profile,
            overall_score=overall_score,
            overall_level=overall_level,
            overall_level_name=overall_level_name,
            score_band=score_band,
            dimensions=dimension_packets,
            strengths=strengths,
            key_gaps=key_gaps[:6],
            risks=risks,
            diligence_questions=diligence_questions,
            recommendation=recommendation,
            generated_at=self._generated_at(),
            total_evidence_count=total_evidence_count,
            avg_evidence_strength=avg_evidence_strength,
            generated_by="deterministic_ic_prep_workflow",
        )

        payload = asdict(packet)
        payload["dimensions"] = [asdict(d) for d in dimension_packets]
        return payload

    def _resolve_dimensions(
        self,
        scorecard: Dict[str, Any],
        dimensions: Optional[List[str]],
    ) -> List[str]:
        if dimensions:
            normalized = [self._normalize_dimension(d) for d in dimensions]
            return normalized

        breakdown = scorecard.get("breakdown", {}) or {}
        vr = breakdown.get("vr", {}) or {}
        dimension_breakdown = vr.get("dimension_breakdown", []) or []

        mapped: List[str] = []
        for item in dimension_breakdown:
            raw_dimension = str(item.get("dimension", "")).strip().lower()
            if not raw_dimension:
                continue
            mapped_dimension = self.SCORE_TO_JUSTIFICATION_DIMENSION.get(raw_dimension)
            if mapped_dimension and mapped_dimension not in mapped:
                mapped.append(mapped_dimension)

        if mapped:
            return mapped

        return self.JUSTIFICATION_DIMENSIONS.copy()

    def _normalize_dimension(self, dimension: str) -> str:
        dim = (dimension or "").strip().lower().replace(" ", "_")
        return self.SCORE_TO_JUSTIFICATION_DIMENSION.get(dim, dim)

    def _trim_evidence(self, evidence: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        trimmed: List[Dict[str, Any]] = []
        for item in evidence[:3]:
            trimmed.append(
                {
                    "evidence_id": item.get("evidence_id"),
                    "source_type": item.get("source_type"),
                    "source_url": item.get("source_url"),
                    "title": item.get("title"),
                    "confidence": item.get("confidence"),
                    "relevance_score": item.get("relevance_score"),
                    "matched_keywords": item.get("matched_keywords", []),
                    "content": (item.get("content") or "")[:250],
                }
            )
        return trimmed

    def _strength_value(self, label: str) -> int:
        mapping = {"strong": 3, "moderate": 2, "weak": 1}
        return mapping.get(label.strip().lower(), 1)

    def _avg_strength_label(self, values: List[int]) -> str:
        if not values:
            return "weak"
        avg = sum(values) / len(values)
        if avg >= 2.5:
            return "strong"
        if avg >= 1.5:
            return "moderate"
        return "weak"

    def _derive_recommendation(
        self,
        overall_score: Optional[float],
        strengths: List[str],
        risks: List[str],
    ) -> str:
        if overall_score is None:
            return "FURTHER DILIGENCE - No consolidated score available"
        if overall_score >= 70 and len(strengths) >= 2 and len(risks) <= 2:
            return "PROCEED - Strong AI readiness with a credible evidence base"
        if overall_score >= 50:
            return "PROCEED WITH CAUTION - Moderate readiness, but diligence gaps remain"
        return "FURTHER DILIGENCE - Material capability gaps or weak evidence"

    def _generated_at(self) -> str:
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()

    def _extract_overall_score(
        self,
        scorecard: Dict[str, Any],
        dimension_packets: List[ICDimensionPacket],
    ) -> Optional[float]:
        overall = scorecard.get("overall_score")
        if isinstance(overall, (int, float)):
            return round(float(overall), 2)

        composite = scorecard.get("composite_score")
        if isinstance(composite, (int, float)):
            return round(float(composite), 2)

        if not dimension_packets:
            return None

        avg_score = sum(packet.score for packet in dimension_packets) / len(dimension_packets)
        return round(avg_score, 2)

    def _score_to_level(self, score: float) -> int:
        if score >= 80:
            return 5
        if score >= 60:
            return 4
        if score >= 40:
            return 3
        if score >= 20:
            return 2
        return 1

    def _derive_strengths(self, dimension_packets: List[ICDimensionPacket]) -> List[str]:
        strengths: List[str] = []
        strong_dimensions = [
            packet
            for packet in dimension_packets
            if packet.evidence_strength in {"strong", "moderate"} and packet.score >= 60
        ]

        for packet in strong_dimensions[:4]:
            strengths.append(
                f"{packet.dimension.replace('_', ' ').title()} appears to be a relative strength "
                f"(score {packet.score}, {packet.evidence_strength} evidence base)"
            )

        return strengths

    def _derive_risks(self, dimension_packets: List[ICDimensionPacket]) -> List[str]:
        risks: List[str] = []
        weak_dimensions = [
            packet
            for packet in dimension_packets
            if packet.score < 50 or packet.evidence_strength == "weak"
        ]

        for packet in weak_dimensions[:4]:
            if packet.gaps_identified:
                risks.append(
                    f"{packet.dimension.replace('_', ' ').title()} shows execution risk: "
                    f"{packet.gaps_identified[0]}"
                )
            else:
                risks.append(
                    f"{packet.dimension.replace('_', ' ').title()} has limited supporting evidence "
                    f"and should be treated as a diligence risk"
                )

        return risks

    def _derive_diligence_questions(self, dimension_packets: List[ICDimensionPacket]) -> List[str]:
        questions: List[str] = []

        for packet in dimension_packets:
            dimension_name = packet.dimension.replace("_", " ")
            if packet.gaps_identified:
                questions.append(
                    f"What concrete evidence can management provide to address the gap in {dimension_name}: "
                    f"{packet.gaps_identified[0]}?"
                )
            elif packet.score < 60:
                questions.append(
                    f"What near-term actions is management taking to improve {dimension_name} maturity?"
                )

        if not questions:
            questions.append(
                "Which current AI initiatives are already delivering measurable business impact, and how are they governed?"
            )

        return questions[:6]
