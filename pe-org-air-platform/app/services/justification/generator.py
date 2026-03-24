from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional
import re

from app.services.integration.scoring_client import ScoringClient
from app.services.llm.router import LiteLLMRouter, TaskType
from app.services.retrieval.hybrid import HybridRetriever


_WORD_RE = re.compile(r"[A-Za-z0-9_]+")


@dataclass
class CitedEvidence:
    evidence_id: str
    content: str
    source_type: str
    source_url: Optional[str]
    confidence: float
    matched_keywords: List[str]
    relevance_score: float
    title: Optional[str] = None
    published_at: Optional[str] = None
    chunk_index: Optional[int] = None
    fiscal_year: Optional[int] = None


@dataclass
class ScoreJustification:
    company_id: str
    dimension: str
    score: float
    level: int
    level_name: str
    confidence_interval: List[float]
    rubric_criteria: str
    rubric_keywords: List[str]
    supporting_evidence: List[CitedEvidence]
    gaps_identified: List[str]
    generated_summary: str
    evidence_strength: str
    score_context: Dict[str, Any]


class JustificationGenerator:
    """
    CS4 score justification generator.

    Updated behavior:
    - retrieves CS3 dimension context from ScoringClient
    - uses HybridRetriever for grounded evidence retrieval
    - anchors score/level to CS3 scoring output when available
    - still remains deterministic and evidence-grounded
    """

    DIMENSION_KEYWORDS: Dict[str, List[str]] = {
        "data_infrastructure": [
            "data lake", "warehouse", "etl", "pipeline", "accessibility",
            "data quality", "source systems", "api", "integration", "clean data",
        ],
        "ai_governance": [
            "governance", "audit trail", "explainability", "bias", "ethics",
            "risk", "compliance", "model risk", "policy", "controls",
        ],
        "technology_stack": [
            "aws", "azure", "gcp", "cloud", "mlops", "deployment",
            "model registry", "experiment tracking", "streaming", "api",
        ],
        "talent": [
            "data engineer", "ml engineer", "analyst", "hiring", "retention",
            "ai talent", "flight risk", "team", "staffing", "capability",
        ],
        "leadership": [
            "executive", "budget", "sponsor", "vision", "strategy",
            "leadership", "champion", "roadmap", "board", "ownership",
        ],
        "use_case_portfolio": [
            "production ai", "use case", "roi", "revenue impact", "pilot",
            "automation", "deployment", "business value", "pipeline", "initiative",
        ],
        "culture": [
            "innovation", "experimentation", "adoption", "change management",
            "collaboration", "ai literacy", "culture", "training", "risk appetite",
        ],
    }

    LEVEL_NAMES: Dict[int, str] = {
        1: "Nascent",
        2: "Developing",
        3: "Adequate",
        4: "Good",
        5: "Excellent",
    }

    def __init__(self) -> None:
        self.retriever = HybridRetriever()
        self.scoring_client = ScoringClient()
        self.router = LiteLLMRouter()

    def generate(
        self,
        company_id: str,
        dimension: str,
        question: Optional[str] = None,
        top_k: int = 5,
        min_confidence: Optional[float] = None,
    ) -> Dict[str, Any]:
        dimension = self._normalize_dimension(dimension)
        if not company_id:
            raise ValueError("company_id is required")
        if dimension not in self.DIMENSION_KEYWORDS:
            raise ValueError(f"Unsupported dimension: {dimension}")

        rubric_keywords = self._get_rubric_keywords(dimension)
        score_context = self._get_score_context(company_id=company_id, dimension=dimension)
        rubric = self._get_rubric(dimension=dimension, level=score_context.get("level"))
        if rubric.get("keywords"):
            rubric_keywords = list(rubric["keywords"])

        query = self._build_query(
            dimension=dimension,
            question=question,
            rubric_keywords=rubric_keywords,
            score_context=score_context,
        )

        hits = self.retriever.search(
            query=query,
            top_k=max(top_k, 8),
            company_id=company_id,
            dimension=dimension,
            min_confidence=min_confidence,
            use_hyde=True,
        ) or []

        cited = self._match_to_rubric(
            hits=hits,
            rubric_keywords=rubric_keywords,
            top_k=top_k,
        )

        score = self._resolve_score(score_context=score_context, cited=cited, dimension=dimension)
        level = self._resolve_level(score_context=score_context, score=score)
        level_name = self.LEVEL_NAMES[level]
        if not rubric:
            rubric = self._get_rubric(dimension=dimension, level=level)
            if rubric.get("keywords"):
                rubric_keywords = list(rubric["keywords"])

        confidence_interval = self._build_confidence_interval(score_context=score_context, score=score)
        rubric_criteria = self._build_rubric_criteria(
            dimension=dimension,
            level=level,
            score_context=score_context,
            rubric=rubric,
        )
        gaps = self._identify_gaps(dimension=dimension, level=level, evidence=cited)
        strength = self._assess_strength(cited)
        summary = self._build_summary(
            company_id=company_id,
            dimension=dimension,
            score=score,
            level=level,
            level_name=level_name,
            rubric_criteria=rubric_criteria,
            cited=cited,
            gaps=gaps,
            evidence_strength=strength,
            score_context=score_context,
        )

        result = ScoreJustification(
            company_id=company_id,
            dimension=dimension,
            score=score,
            level=level,
            level_name=level_name,
            confidence_interval=confidence_interval,
            rubric_criteria=rubric_criteria,
            rubric_keywords=rubric_keywords,
            supporting_evidence=cited,
            gaps_identified=gaps,
            generated_summary=summary,
            evidence_strength=strength,
            score_context=score_context,
        )

        payload = asdict(result)
        payload["supporting_evidence"] = [asdict(e) for e in cited]
        payload["query_used"] = query
        payload["evidence_count"] = len(cited)
        payload["generation_mode"] = "cs3_context_grounded"
        payload["rubric_source"] = "cs3_rubric" if rubric else "heuristic_keywords"
        return payload

    def generate_justification(
        self,
        company_id: str,
        dimension: Any,
        question: Optional[str] = None,
        top_k: int = 5,
        min_confidence: Optional[float] = None,
    ) -> Dict[str, Any]:
        normalized_dimension = getattr(dimension, "value", dimension)
        return self.generate(
            company_id=company_id,
            dimension=str(normalized_dimension),
            question=question,
            top_k=top_k,
            min_confidence=min_confidence,
        )

    def _normalize_dimension(self, dimension: str) -> str:
        value = getattr(dimension, "value", dimension)
        return str(value or "").strip().lower().replace(" ", "_")

    def _get_rubric_keywords(self, dimension: str) -> List[str]:
        return self.DIMENSION_KEYWORDS.get(dimension, []).copy()

    def _get_rubric(self, dimension: str, level: Optional[int]) -> Dict[str, Any]:
        get_rubric = getattr(self.scoring_client, "get_rubric", None)
        if not callable(get_rubric):
            return {}
        try:
            rubrics = get_rubric(dimension, level=level)
        except Exception:
            return {}
        if not rubrics:
            return {}
        rubric = rubrics[0]
        return rubric if isinstance(rubric, dict) else {}

    def _get_score_context(self, company_id: str, dimension: str) -> Dict[str, Any]:
        try:
            return self.scoring_client.get_dimension_context(company_id, dimension)
        except Exception:
            return {}

    def _build_query(
        self,
        dimension: str,
        question: Optional[str],
        rubric_keywords: List[str],
        score_context: Dict[str, Any],
    ) -> str:
        dimension_text = dimension.replace("_", " ")
        keywords = " ".join(rubric_keywords[:5])

        level_name = score_context.get("level_name")
        raw_score = score_context.get("raw_score")
        score_phrase = ""
        if level_name is not None and raw_score is not None:
            score_phrase = f" current score {raw_score} level {level_name}"

        if question and question.strip():
            return f"{question.strip()} {dimension_text} {keywords}{score_phrase}".strip()

        return f"{dimension_text} {keywords}{score_phrase}".strip()

    def _match_to_rubric(
        self,
        hits: List[Any],
        rubric_keywords: List[str],
        top_k: int,
    ) -> List[CitedEvidence]:
        cited: List[CitedEvidence] = []

        for hit in hits:
            evidence_id = str(getattr(hit, "id", "") or "").strip()
            text = (getattr(hit, "text", "") or "").strip()
            if not evidence_id or not text:
                continue

            metadata = getattr(hit, "metadata", {}) or {}
            matched_keywords = self._keyword_matches(text=text, keywords=rubric_keywords)
            relevance_score = float(getattr(hit, "score", 0.0) or 0.0)
            confidence = self._coerce_confidence(metadata.get("confidence"))
            source_type = metadata.get("source_type") or metadata.get("doc_type") or "unknown"
            fiscal_year = metadata.get("fiscal_year")
            if fiscal_year is not None:
                try:
                    fiscal_year = int(fiscal_year)
                except (TypeError, ValueError):
                    fiscal_year = None

            if matched_keywords or relevance_score >= 0.45:
                cited.append(
                    CitedEvidence(
                        evidence_id=evidence_id,
                        content=text[:500],
                        source_type=str(source_type),
                        source_url=metadata.get("source_url"),
                        confidence=confidence,
                        matched_keywords=matched_keywords,
                        relevance_score=round(relevance_score, 4),
                        title=metadata.get("title"),
                        published_at=metadata.get("published_at"),
                        chunk_index=metadata.get("chunk_index"),
                        fiscal_year=fiscal_year,
                    )
                )

        cited.sort(
            key=lambda e: (len(e.matched_keywords), e.confidence, e.relevance_score),
            reverse=True,
        )
        return cited[:top_k]

    def _keyword_matches(self, text: str, keywords: List[str]) -> List[str]:
        lowered = (text or "").lower()
        matches: List[str] = []
        for kw in keywords:
            if kw.lower() in lowered:
                matches.append(kw)
        return matches

    def _coerce_confidence(self, value: Any) -> float:
        try:
            val = float(value)
        except (TypeError, ValueError):
            val = 0.5
        return max(0.0, min(1.0, val))

    def _estimate_score(self, cited: List[CitedEvidence], dimension: str) -> float:
        if not cited:
            return 20.0

        avg_relevance = sum(e.relevance_score for e in cited) / len(cited)
        avg_conf = sum(e.confidence for e in cited) / len(cited)
        avg_keyword_matches = sum(len(e.matched_keywords) for e in cited) / len(cited)
        coverage_ratio = min(1.0, avg_keyword_matches / max(1.0, len(self.DIMENSION_KEYWORDS[dimension]) / 3))

        score = (
            20
            + (avg_relevance * 35)
            + (avg_conf * 20)
            + (coverage_ratio * 25)
        )
        return round(max(0.0, min(100.0, score)), 2)

    def _resolve_score(
        self,
        score_context: Dict[str, Any],
        cited: List[CitedEvidence],
        dimension: str,
    ) -> float:
        raw_score = score_context.get("raw_score")
        if isinstance(raw_score, (int, float)):
            return round(float(raw_score), 2)

        return self._estimate_score(cited=cited, dimension=dimension)

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

    def _resolve_level(self, score_context: Dict[str, Any], score: float) -> int:
        level = score_context.get("level")
        if isinstance(level, int) and level in self.LEVEL_NAMES:
            return level
        return self._score_to_level(score)

    def _build_confidence_interval(self, score_context: Dict[str, Any], score: float) -> List[float]:
        explicit = score_context.get("confidence_interval")
        if (
            isinstance(explicit, (list, tuple))
            and len(explicit) == 2
            and all(isinstance(x, (int, float)) for x in explicit)
        ):
            return [round(float(explicit[0]), 2), round(float(explicit[1]), 2)]

        overall_score = score_context.get("overall_score")
        if isinstance(overall_score, (int, float)):
            return [
                max(0.0, round(float(score) - 8, 2)),
                min(100.0, round(float(score) + 8, 2)),
            ]
        return [max(0.0, round(score - 8, 2)), min(100.0, round(score + 8, 2))]

    def _build_rubric_criteria(
        self,
        dimension: str,
        level: int,
        score_context: Dict[str, Any],
        rubric: Optional[Dict[str, Any]] = None,
    ) -> str:
        if rubric and rubric.get("criteria_text"):
            return str(rubric["criteria_text"])

        dimension_text = dimension.replace("_", " ")
        base_keywords = self.DIMENSION_KEYWORDS.get(dimension, [])
        keywords_preview = ", ".join(base_keywords[:5])

        score_dimension = score_context.get("score_dimension")
        confidence = score_context.get("confidence")
        evidence_count = score_context.get("evidence_count")

        if level == 5:
            qualifier = "clear, repeated, high-confidence evidence of mature and scalable capability"
        elif level == 4:
            qualifier = "multiple credible signals showing solid capability with some gaps remaining"
        elif level == 3:
            qualifier = "mixed evidence indicating partial adoption and developing maturity"
        elif level == 2:
            qualifier = "limited evidence, early-stage capability, and material gaps"
        else:
            qualifier = "minimal or no reliable evidence of operational capability"

        extra = ""
        if score_dimension or confidence is not None or evidence_count is not None:
            extra = (
                f" CS3 context: score_dimension={score_dimension}, "
                f"confidence={confidence}, evidence_count={evidence_count}."
            )

        return (
            f"{dimension_text.title()} at Level {level} requires {qualifier}. "
            f"Typical signals include: {keywords_preview}.{extra}"
        )

    def _identify_gaps(self, dimension: str, level: int, evidence: List[CitedEvidence]) -> List[str]:
        if level >= 5:
            return []

        present = set()
        for e in evidence:
            for kw in e.matched_keywords:
                present.add(kw.lower())

        expected = self.DIMENSION_KEYWORDS.get(dimension, [])
        next_level_rubric = self._get_rubric(dimension=dimension, level=min(5, level + 1))
        if next_level_rubric.get("keywords"):
            expected = list(next_level_rubric["keywords"])
        missing = [kw for kw in expected if kw.lower() not in present]

        gaps = [f"No strong evidence of '{kw}' for next-level readiness" for kw in missing[:5]]
        return gaps

    def _assess_strength(self, evidence: List[CitedEvidence]) -> str:
        if not evidence:
            return "weak"

        avg_conf = sum(e.confidence for e in evidence) / len(evidence)
        avg_matches = sum(len(e.matched_keywords) for e in evidence) / len(evidence)
        avg_relevance = sum(e.relevance_score for e in evidence) / len(evidence)

        if avg_conf >= 0.75 and avg_matches >= 2 and avg_relevance >= 0.70:
            return "strong"
        if avg_conf >= 0.55 and avg_matches >= 1 and avg_relevance >= 0.50:
            return "moderate"
        return "weak"

    def _llm_summary(
        self,
        company_id: str,
        dimension_text: str,
        score: float,
        level: int,
        level_name: str,
        rubric_criteria: str,
        cited: List[CitedEvidence],
        gaps: List[str],
        evidence_strength: str,
    ) -> Optional[str]:
        router = getattr(self, "router", None)
        if router is None:
            return None

        evidence_lines: List[str] = []
        for evidence in cited[:5]:
            fy = evidence.fiscal_year
            if fy is None and evidence.published_at:
                try:
                    fy = int(str(evidence.published_at)[:4])
                except Exception:
                    fy = None
            citation = f"[{evidence.source_type}, FY {fy}]" if fy else f"[{evidence.source_type}]"
            snippet = (evidence.content or "").replace("\n", " ").strip()
            evidence_lines.append(f"{citation} {snippet[:220]}")

        prompt = (
            f"Company: {company_id}\n"
            f"Dimension: {dimension_text}\n"
            f"Score: {score}/100 (Level {level} - {level_name})\n"
            f"Evidence strength: {evidence_strength}\n"
            f"Rubric: {rubric_criteria}\n"
            f"Supporting evidence:\n- " + "\n- ".join(evidence_lines or ["No evidence retrieved"]) + "\n"
            f"Gaps:\n- " + "\n- ".join(gaps[:5] or ["No explicit gaps identified"]) + "\n\n"
            "Write a concise 150-200 word IC-ready justification. Cite sources inline using the provided brackets."
        )

        try:
            response = router.complete(
                task_type=TaskType.JUSTIFICATION,
                user_prompt=prompt,
                system_prompt="You are a PE analyst writing evidence-backed score justifications.",
                temperature=0.2,
                max_tokens=260,
            )
        except Exception:
            return None

        text = (response.text or "").strip()
        return text or None

    def _build_summary(
        self,
        company_id: str,
        dimension: str,
        score: float,
        level: int,
        level_name: str,
        rubric_criteria: str,
        cited: List[CitedEvidence],
        gaps: List[str],
        evidence_strength: str,
        score_context: Dict[str, Any],
    ) -> str:
        dimension_text = dimension.replace("_", " ").title()
        overall_score = score_context.get("overall_score")
        score_band = score_context.get("score_band")

        context_phrase = ""
        if overall_score is not None or score_band:
            context_phrase = f" Company-level context: overall_score={overall_score}, score_band={score_band}. "

        if not cited:
            return (
                f"{company_id} is assessed at {score}/100 for {dimension_text} "
                f"(Level {level} - {level_name}). No strong supporting evidence was retrieved, "
                f"so this justification should be treated as weak and provisional. "
                f"{context_phrase}"
                f"The current rubric expectation is: {rubric_criteria}"
            )

        llm_summary = self._llm_summary(
            company_id=company_id,
            dimension_text=dimension_text,
            score=score,
            level=level,
            level_name=level_name,
            rubric_criteria=rubric_criteria,
            cited=cited,
            gaps=gaps,
            evidence_strength=evidence_strength,
        )
        if llm_summary:
            return llm_summary

        evidence_lines: List[str] = []
        for e in cited[:3]:
            fy = e.fiscal_year
            if fy is None and e.published_at:
                try:
                    fy = int(str(e.published_at)[:4])
                except Exception:
                    fy = None
            citation = f"[{e.source_type}, FY {fy}]" if fy else f"[{e.source_type}]"
            source_label = f"{citation} {e.title or e.source_type or 'source'}"
            snippet = (e.content or "").replace("\n", " ").strip()
            snippet = snippet[:180] + ("..." if len(snippet) > 180 else "")
            evidence_lines.append(f"{source_label}: {snippet}")

        gaps_text = "; ".join(gaps[:3]) if gaps else "No immediate next-level gaps identified from retrieved evidence."

        return (
            f"{company_id} is assessed at {score}/100 for {dimension_text} "
            f"(Level {level} - {level_name}). The evidence base is {evidence_strength}. "
            f"{context_phrase}"
            f"The current level is supported by retrieved signals aligned to the rubric, including: "
            f"{' | '.join(evidence_lines)}. "
            f"Rubric interpretation: {rubric_criteria} "
            f"Key gaps to reach the next level: {gaps_text}"
        )
