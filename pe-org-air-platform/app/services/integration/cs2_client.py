from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from app.services.redis_cache import cache_get_json, cache_set_json
from app.services.snowflake import get_snowflake_connection


class SourceType(str, Enum):
    SEC_10K_ITEM_1 = "sec_10k_item_1"
    SEC_10K_ITEM_1A = "sec_10k_item_1a"
    SEC_10K_ITEM_7 = "sec_10k_item_7"
    JOB_POSTING_LINKEDIN = "job_posting_linkedin"
    JOB_POSTING_INDEED = "job_posting_indeed"
    PATENT_USPTO = "patent_uspto"
    PRESS_RELEASE = "press_release"
    GLASSDOOR_REVIEW = "glassdoor_review"
    BOARD_PROXY_DEF14A = "board_proxy_def14a"
    ANALYST_INTERVIEW = "analyst_interview"
    DD_DATA_ROOM = "dd_data_room"


class SignalCategory(str, Enum):
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"
    CULTURE_SIGNALS = "culture_signals"
    GOVERNANCE_SIGNALS = "governance_signals"


@dataclass(frozen=True)
class ExtractedEntity:
    entity_type: str
    text: str
    char_start: int
    char_end: int
    confidence: float
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CS2Evidence:
    evidence_id: str
    company_id: str
    source_type: SourceType
    signal_category: SignalCategory
    content: str
    extracted_at: datetime
    confidence: float
    fiscal_year: Optional[int] = None
    source_url: Optional[str] = None
    page_number: Optional[int] = None
    extracted_entities: List[ExtractedEntity] = field(default_factory=list)
    indexed_in_cs4: bool = False
    indexed_at: Optional[datetime] = None
    title: Optional[str] = None


def _index_cache_key(evidence_id: str) -> str:
    return f"cs4:indexed:{evidence_id}"


class CS2Client:
    def _section_source_type(self, section: Optional[str]) -> SourceType:
        normalized = (section or "").strip().lower()
        if "1a" in normalized:
            return SourceType.SEC_10K_ITEM_1A
        if normalized.endswith("7") or "item 7" in normalized:
            return SourceType.SEC_10K_ITEM_7
        return SourceType.SEC_10K_ITEM_1

    def _signal_category_for_document(self, source_type: SourceType) -> SignalCategory:
        if source_type == SourceType.SEC_10K_ITEM_1A:
            return SignalCategory.GOVERNANCE_SIGNALS
        if source_type == SourceType.SEC_10K_ITEM_7:
            return SignalCategory.LEADERSHIP_SIGNALS
        return SignalCategory.DIGITAL_PRESENCE

    def _source_type_for_signal(self, signal_type: str, source: str) -> SourceType:
        normalized_type = (signal_type or "").strip().lower()
        normalized_source = (source or "").strip().lower()

        if normalized_type in {"jobs", "technology_hiring"}:
            if "indeed" in normalized_source:
                return SourceType.JOB_POSTING_INDEED
            return SourceType.JOB_POSTING_LINKEDIN
        if normalized_type in {"patents", "innovation_activity"}:
            return SourceType.PATENT_USPTO
        if "glassdoor" in normalized_source:
            return SourceType.GLASSDOOR_REVIEW
        if "board" in normalized_source:
            return SourceType.BOARD_PROXY_DEF14A
        return SourceType.PRESS_RELEASE

    def _signal_category_for_signal(self, signal_type: str) -> SignalCategory:
        normalized = (signal_type or "").strip().lower()
        mapping = {
            "jobs": SignalCategory.TECHNOLOGY_HIRING,
            "technology_hiring": SignalCategory.TECHNOLOGY_HIRING,
            "patents": SignalCategory.INNOVATION_ACTIVITY,
            "innovation_activity": SignalCategory.INNOVATION_ACTIVITY,
            "tech": SignalCategory.DIGITAL_PRESENCE,
            "digital_presence": SignalCategory.DIGITAL_PRESENCE,
            "news": SignalCategory.LEADERSHIP_SIGNALS,
            "leadership_signals": SignalCategory.LEADERSHIP_SIGNALS,
            "glassdoor": SignalCategory.CULTURE_SIGNALS,
            "glassdoor_reviews": SignalCategory.CULTURE_SIGNALS,
            "board": SignalCategory.GOVERNANCE_SIGNALS,
            "board_composition": SignalCategory.GOVERNANCE_SIGNALS,
        }
        return mapping.get(normalized, SignalCategory.LEADERSHIP_SIGNALS)

    def _signal_confidence(self, signal_type: str, source: str) -> float:
        source_type = self._source_type_for_signal(signal_type, source)
        defaults = {
            SourceType.JOB_POSTING_LINKEDIN: 0.82,
            SourceType.JOB_POSTING_INDEED: 0.78,
            SourceType.PATENT_USPTO: 0.84,
            SourceType.GLASSDOOR_REVIEW: 0.72,
            SourceType.BOARD_PROXY_DEF14A: 0.90,
            SourceType.PRESS_RELEASE: 0.68,
        }
        return defaults.get(source_type, 0.70)

    def _indexed_state(self, evidence_id: str) -> tuple[bool, Optional[datetime]]:
        payload = cache_get_json(_index_cache_key(evidence_id))
        if not isinstance(payload, dict):
            return False, None

        indexed_at = payload.get("indexed_at")
        if isinstance(indexed_at, str):
            try:
                return True, datetime.fromisoformat(indexed_at)
            except ValueError:
                return True, None
        return True, None

    def _document_evidence(self, company_id: str) -> List[CS2Evidence]:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                    d.id,
                    c.id,
                    d.company_id,
                    c.content,
                    c.section,
                    d.source_url,
                    d.filing_date,
                    d.filing_type,
                    c.created_at
                FROM document_chunks c
                JOIN documents d
                  ON d.id = c.document_id
                WHERE d.company_id = %s
                ORDER BY d.filing_date DESC, c.chunk_index ASC
                """,
                (company_id,),
            )
            rows = cur.fetchall()
        finally:
            cur.close()
            conn.close()

        out: List[CS2Evidence] = []
        for row in rows:
            document_id, chunk_id, company_id, content, section, source_url, filing_date, filing_type, created_at = row
            source_type = self._section_source_type(section)
            indexed, indexed_at = self._indexed_state(f"{document_id}:{chunk_id}")
            extracted_at = created_at or datetime.now(timezone.utc)
            if isinstance(extracted_at, str):
                try:
                    extracted_at = datetime.fromisoformat(extracted_at)
                except ValueError:
                    extracted_at = datetime.now(timezone.utc)

            fiscal_year = None
            if filing_date is not None:
                try:
                    fiscal_year = int(getattr(filing_date, "year", int(str(filing_date)[:4])))
                except Exception:
                    fiscal_year = None

            out.append(
                CS2Evidence(
                    evidence_id=f"{document_id}:{chunk_id}",
                    company_id=str(company_id),
                    source_type=source_type,
                    signal_category=self._signal_category_for_document(source_type),
                    content=str(content or ""),
                    extracted_at=extracted_at,
                    confidence=0.90,
                    fiscal_year=fiscal_year,
                    source_url=str(source_url) if source_url else None,
                    extracted_entities=[],
                    indexed_in_cs4=indexed,
                    indexed_at=indexed_at,
                    title=str(filing_type) if filing_type else None,
                )
            )
        return out

    def _signal_evidence(self, company_id: str) -> List[CS2Evidence]:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                    id,
                    company_id,
                    signal_type,
                    source,
                    title,
                    url,
                    published_at,
                    collected_at,
                    content_text
                FROM external_signals
                WHERE company_id = %s
                ORDER BY collected_at DESC
                """,
                (company_id,),
            )
            rows = cur.fetchall()
        finally:
            cur.close()
            conn.close()

        out: List[CS2Evidence] = []
        for row in rows:
            signal_id, company_id, signal_type, source, title, url, published_at, collected_at, content_text = row
            source_type = self._source_type_for_signal(str(signal_type or ""), str(source or ""))
            indexed, indexed_at = self._indexed_state(str(signal_id))
            extracted_at = collected_at or published_at or datetime.now(timezone.utc)
            if isinstance(extracted_at, str):
                try:
                    extracted_at = datetime.fromisoformat(extracted_at)
                except ValueError:
                    extracted_at = datetime.now(timezone.utc)

            out.append(
                CS2Evidence(
                    evidence_id=str(signal_id),
                    company_id=str(company_id),
                    source_type=source_type,
                    signal_category=self._signal_category_for_signal(str(signal_type or "")),
                    content="\n\n".join(
                        [part for part in [str(title or "").strip(), str(content_text or "").strip()] if part]
                    ),
                    extracted_at=extracted_at,
                    confidence=self._signal_confidence(str(signal_type or ""), str(source or "")),
                    source_url=str(url) if url else None,
                    extracted_entities=[],
                    indexed_in_cs4=indexed,
                    indexed_at=indexed_at,
                    title=str(title) if title else None,
                )
            )
        return out

    def get_evidence(
        self,
        company_id: str,
        source_types: Optional[List[SourceType]] = None,
        signal_categories: Optional[List[SignalCategory]] = None,
        min_confidence: float = 0.0,
        indexed: Optional[bool] = None,
        since: Optional[datetime] = None,
    ) -> List[CS2Evidence]:
        evidence = self._document_evidence(company_id) + self._signal_evidence(company_id)

        if source_types:
            allowed_sources = {item.value for item in source_types}
            evidence = [item for item in evidence if item.source_type.value in allowed_sources]

        if signal_categories:
            allowed_categories = {item.value for item in signal_categories}
            evidence = [item for item in evidence if item.signal_category.value in allowed_categories]

        if min_confidence > 0:
            evidence = [item for item in evidence if item.confidence >= float(min_confidence)]

        if indexed is not None:
            evidence = [item for item in evidence if item.indexed_in_cs4 is indexed]

        if since is not None:
            evidence = [item for item in evidence if item.extracted_at >= since]

        evidence.sort(key=lambda item: item.extracted_at, reverse=True)
        return evidence

    def mark_indexed(self, evidence_ids: List[str]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        updated = 0
        for evidence_id in evidence_ids:
            cache_set_json(
                _index_cache_key(evidence_id),
                {"indexed_at": now},
                ttl_seconds=60 * 60 * 24 * 30,
            )
            updated += 1
        return updated

    def close(self) -> None:
        return None
