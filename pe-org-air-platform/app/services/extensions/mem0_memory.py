from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from app.config import ROOT_DIR, settings


STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "with",
}

SYNONYM_GROUPS = {
    "governance": {"governance", "controls", "policy", "audit", "risk", "compliance"},
    "data": {"data", "platform", "warehouse", "pipeline", "infrastructure", "lake"},
    "talent": {"talent", "skills", "hiring", "training", "workforce", "leadership"},
    "value": {"roi", "return", "ebitda", "value", "impact", "uplift"},
    "productivity": {"automation", "copilot", "workflow", "efficiency", "productivity"},
    "portfolio": {"portfolio", "fund", "holdings", "platform"},
}


@dataclass
class MemoryRecord:
    memory_id: str
    company_id: str | None
    fund_id: str | None
    category: str
    title: str
    content: str
    summary: str
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    source: str = "manual"


class Mem0SemanticMemoryService:
    """
    Lightweight semantic memory store persisted into the repository results tree.

    This is deliberately local and deterministic so it works without remote model
    dependencies, but it still supports semantic-ish recall through token
    expansion, tag extraction, and similarity scoring across prior notes.
    """

    def __init__(self, storage_path: str | Path | None = None) -> None:
        default_path = ROOT_DIR / settings.results_dir / "bonus" / "mem0_memory.json"
        self.storage_path = Path(storage_path or default_path)

    def _ensure_parent(self) -> None:
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

    def _load_records(self) -> list[MemoryRecord]:
        if not self.storage_path.exists():
            return []
        raw = json.loads(self.storage_path.read_text(encoding="utf-8"))
        return [MemoryRecord(**item) for item in raw]

    def _save_records(self, records: list[MemoryRecord]) -> None:
        self._ensure_parent()
        self.storage_path.write_text(
            json.dumps([asdict(record) for record in records], indent=2),
            encoding="utf-8",
        )

    def _tokenize(self, text: str) -> set[str]:
        tokens = {
            token
            for token in re.findall(r"[a-z0-9_]+", (text or "").lower())
            if token and token not in STOPWORDS and len(token) > 2
        }
        expanded = set(tokens)
        for token in list(tokens):
            for group in SYNONYM_GROUPS.values():
                if token in group:
                    expanded.update(group)
        return expanded

    def _auto_tags(self, title: str, content: str) -> list[str]:
        text = f"{title}\n{content}"
        tags: list[str] = []
        tokens = self._tokenize(text)
        for canonical, group in SYNONYM_GROUPS.items():
            if tokens.intersection(group):
                tags.append(canonical)
        if "org_air" in tokens or "orgair" in tokens:
            tags.append("org-air")
        return sorted(set(tags))

    def _summary(self, content: str, limit: int = 220) -> str:
        normalized = " ".join((content or "").split()).strip()
        if len(normalized) <= limit:
            return normalized
        return normalized[: limit - 3].rstrip() + "..."

    def remember(
        self,
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
        records = self._load_records()
        record = MemoryRecord(
            memory_id=f"mem_{uuid4().hex[:12]}",
            company_id=company_id,
            fund_id=fund_id,
            category=category,
            title=title.strip() or "Untitled Memory",
            content=content.strip(),
            summary=self._summary(content),
            tags=sorted(set((tags or []) + self._auto_tags(title, content))),
            metadata=metadata or {},
            source=source,
        )
        records.append(record)
        self._save_records(records)
        return asdict(record)

    def remember_due_diligence_state(self, state: dict[str, Any]) -> dict[str, Any]:
        company_id = str(state.get("company_id") or "").strip() or None
        score = state.get("scoring_result") or {}
        value_plan = state.get("value_creation_plan") or {}
        evidence = (state.get("evidence_justifications") or {}).get("summary", "")
        summary_lines = [
            f"Org-AI-R: {float(score.get('org_air', 0.0) or 0.0):.1f}",
            f"V^R: {float(score.get('vr_score', 0.0) or 0.0):.1f}",
            f"H^R: {float(score.get('hr_score', 0.0) or 0.0):.1f}",
        ]
        if value_plan:
            ebitda = (value_plan.get("ebitda_projection") or {}).get("risk_adjusted", 0.0)
            summary_lines.append(f"Projected EBITDA impact: {float(ebitda or 0.0):.2f}%")
        if evidence:
            summary_lines.append(f"Evidence summary: {evidence}")
        return self.remember(
            title=f"Due diligence memory for {company_id or 'unknown company'}",
            content="\n".join(summary_lines),
            company_id=company_id,
            category="due_diligence",
            source="langgraph_workflow",
            metadata={
                "assessment_type": state.get("assessment_type"),
                "approval_status": state.get("approval_status"),
            },
        )

    def _similarity(self, query_tokens: set[str], record: MemoryRecord) -> float:
        record_tokens = self._tokenize(f"{record.title} {record.content} {' '.join(record.tags)}")
        if not record_tokens or not query_tokens:
            return 0.0
        overlap = len(query_tokens.intersection(record_tokens))
        cosine = overlap / math.sqrt(len(query_tokens) * len(record_tokens))
        tag_overlap = len(query_tokens.intersection(set(record.tags))) / max(1, len(record.tags))
        exact_bonus = 0.15 if " ".join(query_tokens) in record.content.lower() else 0.0
        return round((0.7 * cosine) + (0.2 * tag_overlap) + exact_bonus, 4)

    def recall(
        self,
        *,
        query: str,
        company_id: str | None = None,
        fund_id: str | None = None,
        category: str | None = None,
        top_k: int = 5,
    ) -> list[dict[str, Any]]:
        query_tokens = self._tokenize(query)
        matches: list[tuple[float, MemoryRecord]] = []

        for record in self._load_records():
            if company_id and record.company_id not in {None, company_id}:
                continue
            if fund_id and record.fund_id not in {None, fund_id}:
                continue
            if category and record.category != category:
                continue
            score = self._similarity(query_tokens, record)
            if score <= 0:
                continue
            matches.append((score, record))

        matches.sort(key=lambda item: (item[0], item[1].created_at), reverse=True)
        return [
            {
                **asdict(record),
                "similarity": score,
            }
            for score, record in matches[: max(1, top_k)]
        ]

    def list_memories(
        self,
        *,
        company_id: str | None = None,
        fund_id: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        records = self._load_records()
        filtered = [
            record
            for record in records
            if (not company_id or record.company_id == company_id)
            and (not fund_id or record.fund_id == fund_id)
        ]
        filtered.sort(key=lambda record: record.created_at, reverse=True)
        return [asdict(record) for record in filtered[: max(1, limit)]]

    def stats(self) -> dict[str, Any]:
        records = self._load_records()
        company_ids = {record.company_id for record in records if record.company_id}
        fund_ids = {record.fund_id for record in records if record.fund_id}
        return {
            "memory_count": len(records),
            "companies_covered": len(company_ids),
            "funds_covered": len(fund_ids),
            "storage_path": str(self.storage_path),
        }

