from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from app.config import ROOT_DIR, settings


@dataclass
class InvestmentRecord:
    investment_id: str
    fund_id: str
    company_id: str
    program_name: str
    thesis: str
    invested_amount_mm: float
    current_value_mm: float
    realized_value_mm: float
    expected_value_mm: float
    target_org_air: float | None
    current_org_air: float | None
    status: str
    start_date: str
    notes: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    metadata: dict[str, Any] = field(default_factory=dict)


class InvestmentTrackerService:
    def __init__(self, storage_path: str | Path | None = None) -> None:
        default_path = ROOT_DIR / settings.results_dir / "bonus" / "investment_tracker.json"
        self.storage_path = Path(storage_path or default_path)

    def _ensure_parent(self) -> None:
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

    def _load_records(self) -> list[InvestmentRecord]:
        if not self.storage_path.exists():
            return []
        raw = json.loads(self.storage_path.read_text(encoding="utf-8"))
        return [InvestmentRecord(**item) for item in raw]

    def _save_records(self, records: list[InvestmentRecord]) -> None:
        self._ensure_parent()
        self.storage_path.write_text(
            json.dumps([asdict(record) for record in records], indent=2),
            encoding="utf-8",
        )

    def add_investment(
        self,
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
        records = self._load_records()
        invested = round(float(invested_amount_mm), 2)
        current_value = round(float(current_value_mm if current_value_mm is not None else invested), 2)
        expected_value = round(float(expected_value_mm if expected_value_mm is not None else current_value), 2)
        realized_value = round(float(realized_value_mm), 2)
        today = (start_date or date.today().isoformat()).strip()
        timestamp = datetime.now(timezone.utc).isoformat()

        record = InvestmentRecord(
            investment_id=f"inv_{uuid4().hex[:12]}",
            fund_id=fund_id.strip(),
            company_id=company_id.strip(),
            program_name=program_name.strip() or "AI Value Creation Program",
            thesis=thesis.strip(),
            invested_amount_mm=invested,
            current_value_mm=current_value,
            realized_value_mm=realized_value,
            expected_value_mm=expected_value,
            target_org_air=round(float(target_org_air), 2) if target_org_air is not None else None,
            current_org_air=round(float(current_org_air), 2) if current_org_air is not None else None,
            status=(status or "active").strip().lower(),
            start_date=today,
            notes=notes.strip(),
            created_at=timestamp,
            updated_at=timestamp,
            metadata=metadata or {},
        )
        records.append(record)
        self._save_records(records)
        return asdict(record)

    def list_investments(
        self,
        *,
        fund_id: str | None = None,
        company_id: str | None = None,
    ) -> list[dict[str, Any]]:
        records = [
            record
            for record in self._load_records()
            if (not fund_id or record.fund_id == fund_id)
            and (not company_id or record.company_id == company_id)
        ]
        records.sort(key=lambda record: record.updated_at, reverse=True)
        return [asdict(record) for record in records]

    def summarize(self, *, fund_id: str) -> dict[str, Any]:
        records = [
            record
            for record in self._load_records()
            if record.fund_id == fund_id
        ]
        invested = round(sum(record.invested_amount_mm for record in records), 2)
        current_value = round(sum(record.current_value_mm for record in records), 2)
        realized = round(sum(record.realized_value_mm for record in records), 2)
        expected = round(sum(record.expected_value_mm for record in records), 2)
        total_value = round(current_value + realized, 2)
        net_gain = round(total_value - invested, 2)
        projected_gain = round(expected - invested, 2)
        roi_pct = round((net_gain / invested) * 100.0, 2) if invested else 0.0
        projected_roi_pct = round((projected_gain / invested) * 100.0, 2) if invested else 0.0
        moic = round(total_value / invested, 2) if invested else 0.0
        status_counts: dict[str, int] = {}
        for record in records:
            status_counts[record.status] = status_counts.get(record.status, 0) + 1

        company_breakdown = [
            {
                "company_id": record.company_id,
                "program_name": record.program_name,
                "invested_amount_mm": record.invested_amount_mm,
                "current_value_mm": record.current_value_mm,
                "realized_value_mm": record.realized_value_mm,
                "expected_value_mm": record.expected_value_mm,
                "roi_pct": round(
                    ((record.current_value_mm + record.realized_value_mm - record.invested_amount_mm) / record.invested_amount_mm) * 100.0,
                    2,
                )
                if record.invested_amount_mm
                else 0.0,
                "status": record.status,
            }
            for record in records
        ]
        company_breakdown.sort(key=lambda item: item["roi_pct"], reverse=True)

        return {
            "fund_id": fund_id,
            "investment_count": len(records),
            "invested_amount_mm": invested,
            "current_value_mm": current_value,
            "realized_value_mm": realized,
            "expected_value_mm": expected,
            "total_value_mm": total_value,
            "net_gain_mm": net_gain,
            "projected_gain_mm": projected_gain,
            "roi_pct": roi_pct,
            "projected_roi_pct": projected_roi_pct,
            "moic": moic,
            "status_counts": status_counts,
            "companies": company_breakdown,
            "storage_path": str(self.storage_path),
        }

