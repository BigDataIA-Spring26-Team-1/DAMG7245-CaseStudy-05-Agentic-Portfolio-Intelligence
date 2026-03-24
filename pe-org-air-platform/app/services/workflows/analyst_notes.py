from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from app.services.integration.company_client import CompanyClient
from app.services.justification.generator import JustificationGenerator
from app.services.search.vector_store import DocumentChunk, VectorStore


@dataclass
class AnalystNoteRecord:
    company_id: str
    company_name: Optional[str]
    dimension: str
    note_title: str
    note_summary: str
    evidence_snapshot: List[Dict[str, Any]]
    key_gaps: List[str]
    confidence_label: str
    created_at: str
    generated_by: str


class NoteType(str, Enum):
    INTERVIEW_TRANSCRIPT = "interview_transcript"
    MANAGEMENT_MEETING = "management_meeting"
    SITE_VISIT = "site_visit"
    DD_FINDING = "dd_finding"
    DATA_ROOM_SUMMARY = "data_room_summary"


@dataclass
class AnalystNote:
    note_id: str
    company_id: str
    note_type: NoteType
    title: str
    content: str
    interviewee: Optional[str] = None
    interviewee_title: Optional[str] = None
    dimensions_discussed: List[str] = field(default_factory=list)
    key_findings: List[str] = field(default_factory=list)
    risk_flags: List[str] = field(default_factory=list)
    assessor: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class AnalystNotesCollector:
    """
    Supports both spec-facing analyst-note ingestion and summary generation.

    The collector can:
    - submit primary-source diligence notes and index them into CS4 retrieval
    - synthesize a dimension note from grounded justification output
    """

    SUPPORTED_DIMENSIONS: List[str] = [
        "leadership",
        "talent",
        "culture",
        "ai_governance",
        "data_infrastructure",
        "technology_stack",
        "use_case_portfolio",
    ]

    def __init__(self) -> None:
        self.company_client = CompanyClient()
        self.generator = JustificationGenerator()
        self.vector_store: Optional[VectorStore] = None

    def collect_note(
        self,
        company_id: str,
        dimension: str,
        note_title: Optional[str] = None,
        question: Optional[str] = None,
        top_k: int = 5,
        min_confidence: Optional[float] = None,
    ) -> Dict[str, Any]:
        if not company_id or not company_id.strip():
            raise ValueError("company_id is required")

        normalized_dimension = self._normalize_dimension(dimension)
        if normalized_dimension not in self.SUPPORTED_DIMENSIONS:
            raise ValueError(f"Unsupported dimension: {normalized_dimension}")

        company = self.company_client.get_company(company_id)
        justification = self.generator.generate(
            company_id=company_id,
            dimension=normalized_dimension,
            question=question
            or f"What should an analyst note emphasize about {normalized_dimension.replace('_', ' ')}?",
            top_k=top_k,
            min_confidence=min_confidence,
        )

        note = AnalystNoteRecord(
            company_id=company_id,
            company_name=company.get("name"),
            dimension=normalized_dimension,
            note_title=note_title or self._default_title(company, normalized_dimension),
            note_summary=self._build_note_summary(company, justification),
            evidence_snapshot=self._trim_evidence(justification.get("supporting_evidence", [])),
            key_gaps=list(justification.get("gaps_identified", []))[:5],
            confidence_label=self._confidence_label(
                justification.get("evidence_strength"),
                float(justification.get("score", 0.0) or 0.0),
            ),
            created_at=datetime.now(timezone.utc).isoformat(),
            generated_by="deterministic_analyst_notes_collector",
        )

        return asdict(note)

    def _get_vector_store(self) -> VectorStore:
        if self.vector_store is None:
            self.vector_store = VectorStore()
        return self.vector_store

    def _normalize_dimensions(self, dimensions: Optional[List[str]]) -> List[str]:
        out: List[str] = []
        for dimension in dimensions or []:
            normalized = self._normalize_dimension(dimension)
            if normalized and normalized not in out:
                out.append(normalized)
        return out

    def _note_to_chunk(self, note: AnalystNote, primary_dimension: str) -> DocumentChunk:
        metadata = {
            "company_id": note.company_id,
            "source_type": note.note_type.value,
            "dimension": primary_dimension,
            "confidence": note.confidence,
            "assessor": note.assessor,
            "title": note.title,
            "created_at": note.created_at.isoformat(),
        }
        metadata.update(note.metadata)
        if note.interviewee:
            metadata["interviewee"] = note.interviewee
        if note.interviewee_title:
            metadata["interviewee_title"] = note.interviewee_title

        return DocumentChunk(
            id=note.note_id,
            text=note.content,
            metadata=metadata,
        )

    def _index_note(self, note: AnalystNote) -> str:
        dimensions = self._normalize_dimensions(note.dimensions_discussed)
        primary_dimension = dimensions[0] if dimensions else "leadership"
        chunk = self._note_to_chunk(note, primary_dimension)
        self._get_vector_store().upsert([chunk])
        return note.note_id

    def submit_interview(
        self,
        company_id: str,
        interviewee: str,
        interviewee_title: str,
        transcript: str,
        assessor: str,
        dimensions_discussed: List[str],
    ) -> str:
        note = AnalystNote(
            note_id=f"interview_{uuid4()}",
            company_id=company_id,
            note_type=NoteType.INTERVIEW_TRANSCRIPT,
            title=f"Interview with {interviewee_title}",
            content=f"Interview: {interviewee_title}\n\n{transcript}",
            interviewee=interviewee,
            interviewee_title=interviewee_title,
            dimensions_discussed=self._normalize_dimensions(dimensions_discussed),
            assessor=assessor,
        )
        return self._index_note(note)

    def submit_dd_finding(
        self,
        company_id: str,
        title: str,
        finding: str,
        dimension: str,
        severity: str,
        assessor: str,
    ) -> str:
        note = AnalystNote(
            note_id=f"dd_{uuid4()}",
            company_id=company_id,
            note_type=NoteType.DD_FINDING,
            title=title,
            content=f"{title}\n\n{finding}",
            dimensions_discussed=[self._normalize_dimension(dimension)],
            assessor=assessor,
            metadata={"severity": severity},
        )
        return self._index_note(note)

    def submit_data_room_summary(
        self,
        company_id: str,
        document_name: str,
        summary: str,
        dimension: str,
        assessor: str,
    ) -> str:
        note = AnalystNote(
            note_id=f"dataroom_{uuid4()}",
            company_id=company_id,
            note_type=NoteType.DATA_ROOM_SUMMARY,
            title=f"Data Room: {document_name}",
            content=f"Data Room: {document_name}\n\n{summary}",
            dimensions_discussed=[self._normalize_dimension(dimension)],
            assessor=assessor,
            metadata={"document_name": document_name},
        )
        return self._index_note(note)

    def collect_notes_for_dimensions(
        self,
        company_id: str,
        dimensions: Optional[List[str]] = None,
        top_k: int = 5,
        min_confidence: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        selected = dimensions or self.SUPPORTED_DIMENSIONS
        out: List[Dict[str, Any]] = []

        for dimension in selected:
            out.append(
                self.collect_note(
                    company_id=company_id,
                    dimension=dimension,
                    top_k=top_k,
                    min_confidence=min_confidence,
                )
            )

        return out

    def _normalize_dimension(self, dimension: str) -> str:
        return (dimension or "").strip().lower().replace(" ", "_")

    def _default_title(self, company: Dict[str, Any], dimension: str) -> str:
        company_name = company.get("name", "Company")
        return f"{company_name} - {dimension.replace('_', ' ').title()} Analyst Note"

    def _trim_evidence(self, evidence: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        trimmed: List[Dict[str, Any]] = []
        for item in evidence[:3]:
            trimmed.append(
                {
                    "evidence_id": item.get("evidence_id"),
                    "title": item.get("title"),
                    "source_type": item.get("source_type"),
                    "source_url": item.get("source_url"),
                    "confidence": item.get("confidence"),
                    "relevance_score": item.get("relevance_score"),
                    "matched_keywords": item.get("matched_keywords", []),
                    "content": (item.get("content") or "")[:220],
                }
            )
        return trimmed

    def _build_note_summary(
        self,
        company: Dict[str, Any],
        justification: Dict[str, Any],
    ) -> str:
        company_name = company.get("name", justification.get("company_id", "Company"))
        dimension = str(justification.get("dimension", "")).replace("_", " ").title()
        score = justification.get("score")
        level = justification.get("level_name")
        evidence_strength = justification.get("evidence_strength", "unknown")
        generated_summary = justification.get("generated_summary", "").strip()

        return (
            f"{company_name} is currently assessed at {score}/100 for {dimension} "
            f"(Level: {level}). The evidence base is {evidence_strength}. "
            f"{generated_summary}"
        ).strip()

    def _confidence_label(self, evidence_strength: Any, score: float) -> str:
        evidence_strength = str(evidence_strength or "").lower()

        if evidence_strength == "strong" and score >= 75:
            return "high"
        if evidence_strength in {"strong", "moderate"} and score >= 50:
            return "medium"
        return "low"
