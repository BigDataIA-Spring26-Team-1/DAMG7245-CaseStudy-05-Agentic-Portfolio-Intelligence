from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import ROOT_DIR, settings
from app.services.analytics.fund_air import fund_air_calculator
from app.services.extensions.docx_export import DocParagraph, write_docx
from app.services.extensions.investment_tracker import InvestmentTrackerService
from app.services.extensions.mem0_memory import Mem0SemanticMemoryService
from app.services.integration.portfolio_data_service import portfolio_data_service
from app.services.workflows.ic_prep import ICPrepWorkflow


def _slugify(value: str) -> str:
    clean = "".join(ch.lower() if ch.isalnum() else "_" for ch in (value or "").strip())
    while "__" in clean:
        clean = clean.replace("__", "_")
    return clean.strip("_") or "artifact"


def _artifact_root() -> Path:
    out = ROOT_DIR / settings.results_dir / "bonus" / "documents"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


@dataclass(frozen=True)
class GeneratedReport:
    title: str
    markdown: str
    markdown_path: str
    docx_path: str
    metadata: dict[str, Any]

    def as_payload(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "preview_markdown": self.markdown,
            "markdown_path": self.markdown_path,
            "docx_path": self.docx_path,
            "metadata": self.metadata,
        }


class ICMemoGenerator:
    def __init__(
        self,
        *,
        workflow: ICPrepWorkflow | None = None,
        memory_service: Mem0SemanticMemoryService | None = None,
        investment_tracker: InvestmentTrackerService | None = None,
    ) -> None:
        self.workflow = workflow or ICPrepWorkflow()
        self.memory_service = memory_service or Mem0SemanticMemoryService()
        self.investment_tracker = investment_tracker or InvestmentTrackerService()

    def generate(self, company_id: str, fund_id: str | None = None) -> dict[str, Any]:
        packet = self.workflow.build_packet(company_id=company_id)
        company_profile = packet.get("company_profile", {}) or {}
        company_name = str(company_profile.get("name") or company_id)
        memories = self.memory_service.recall(
            query=f"{company_name} due diligence org-air value creation",
            company_id=company_id,
            top_k=5,
        )
        investments = self.investment_tracker.list_investments(company_id=company_id)
        title = f"IC Memo - {company_name}"

        lines: list[str] = [
            f"# {title}",
            "",
            "## Executive Summary",
            f"- Recommendation: {packet.get('recommendation', 'N/A')}",
            f"- Overall Org-AI-R: {float(packet.get('overall_score', 0.0) or 0.0):.1f}",
            f"- Level: {packet.get('overall_level_name', 'N/A')}",
            f"- Evidence count: {int(packet.get('total_evidence_count', 0) or 0)}",
            "",
            "## Company Profile",
            f"- Company ID: {company_id}",
            f"- Company Name: {company_name}",
            f"- Ticker: {company_profile.get('ticker', 'N/A')}",
            f"- Industry: {company_profile.get('industry_id', 'N/A')}",
            "",
            "## Strengths",
        ]
        lines.extend(f"- {item}" for item in packet.get("strengths", [])[:5] or ["No material strengths recorded"])
        lines.extend(["", "## Key Gaps"])
        lines.extend(f"- {item}" for item in packet.get("key_gaps", [])[:6] or ["No key gaps recorded"])
        lines.extend(["", "## Risks"])
        lines.extend(f"- {item}" for item in packet.get("risks", [])[:6] or ["No major risks recorded"])
        lines.extend(["", "## Diligence Questions"])
        lines.extend(f"- {item}" for item in packet.get("diligence_questions", [])[:6] or ["No further diligence questions recorded"])

        if memories:
            lines.extend(["", "## Semantic Memory Recall"])
            lines.extend(
                f"- {item['title']}: {item['summary']} (similarity {float(item['similarity']):.2f})"
                for item in memories
            )

        if investments:
            lines.extend(["", "## Active AI Investment Programs"])
            lines.extend(
                f"- {item['program_name']}: invested ${float(item['invested_amount_mm']):.2f}M, "
                f"current value ${float(item['current_value_mm']):.2f}M, status {item['status']}"
                for item in investments[:6]
            )

        lines.extend(["", "## Dimension Breakdown"])
        for item in packet.get("dimensions", [])[:7]:
            lines.append(
                f"- {str(item.get('dimension', 'unknown')).replace('_', ' ').title()}: "
                f"{float(item.get('score', 0.0) or 0.0):.1f} ({item.get('level_name', 'N/A')})"
            )
            lines.append(f"- Evidence strength: {item.get('evidence_strength', 'N/A')}")
            lines.append(f"- Summary: {item.get('summary', '')}")

        markdown = "\n".join(lines).strip() + "\n"
        artifact_dir = _artifact_root()
        stem = f"ic_memo_{_slugify(company_name)}_{_timestamp_slug()}"
        markdown_path = artifact_dir / f"{stem}.md"
        docx_path = artifact_dir / f"{stem}.docx"
        markdown_path.write_text(markdown, encoding="utf-8")

        blocks = [
            DocParagraph("title", title),
            DocParagraph("heading1", "Executive Summary"),
            DocParagraph("body", f"Recommendation: {packet.get('recommendation', 'N/A')}"),
            DocParagraph("body", f"Overall Org-AI-R: {float(packet.get('overall_score', 0.0) or 0.0):.1f}"),
            DocParagraph("heading1", "Strengths"),
        ]
        blocks.extend(DocParagraph("bullet", item) for item in packet.get("strengths", [])[:5] or ["No material strengths recorded"])
        blocks.append(DocParagraph("heading1", "Key Gaps"))
        blocks.extend(DocParagraph("bullet", item) for item in packet.get("key_gaps", [])[:6] or ["No key gaps recorded"])
        blocks.append(DocParagraph("heading1", "Risks"))
        blocks.extend(DocParagraph("bullet", item) for item in packet.get("risks", [])[:6] or ["No major risks recorded"])
        blocks.append(DocParagraph("heading1", "Diligence Questions"))
        blocks.extend(DocParagraph("bullet", item) for item in packet.get("diligence_questions", [])[:6] or ["No further diligence questions recorded"])
        if memories:
            blocks.append(DocParagraph("heading1", "Semantic Memory Recall"))
            blocks.extend(
                DocParagraph("bullet", f"{item['title']}: {item['summary']}")
                for item in memories
            )
        if investments:
            blocks.append(DocParagraph("heading1", "Active AI Investment Programs"))
            blocks.extend(
                DocParagraph(
                    "bullet",
                    f"{item['program_name']}: invested ${float(item['invested_amount_mm']):.2f}M; "
                    f"current value ${float(item['current_value_mm']):.2f}M; status {item['status']}",
                )
                for item in investments[:6]
            )
        write_docx(docx_path, title=title, paragraphs=blocks, subject="Investment Committee Memo")

        return GeneratedReport(
            title=title,
            markdown=markdown,
            markdown_path=str(markdown_path),
            docx_path=str(docx_path),
            metadata={
                "company_id": company_id,
                "fund_id": fund_id,
                "memory_count": len(memories),
                "investment_count": len(investments),
            },
        ).as_payload()


class LPLetterGenerator:
    def __init__(
        self,
        *,
        memory_service: Mem0SemanticMemoryService | None = None,
        investment_tracker: InvestmentTrackerService | None = None,
    ) -> None:
        self.memory_service = memory_service or Mem0SemanticMemoryService()
        self.investment_tracker = investment_tracker or InvestmentTrackerService()

    async def generate(self, fund_id: str) -> dict[str, Any]:
        portfolio = await portfolio_data_service.get_portfolio_view(fund_id)
        enterprise_values = {company.company_id: float(company.enterprise_value_mm) for company in portfolio}
        metrics = fund_air_calculator.calculate_fund_metrics(
            fund_id=fund_id,
            companies=portfolio,
            enterprise_values=enterprise_values,
        )
        investments = self.investment_tracker.summarize(fund_id=fund_id)
        memories = self.memory_service.recall(
            query=f"{fund_id} portfolio quarterly update ai readiness",
            fund_id=fund_id,
            top_k=5,
        )
        leaders = sorted(portfolio, key=lambda company: company.org_air, reverse=True)[:3]
        improvers = sorted(portfolio, key=lambda company: company.delta_since_entry, reverse=True)[:3]
        title = f"LP Letter - {fund_id}"

        lines: list[str] = [
            f"# {title}",
            "",
            "Dear Limited Partners,",
            "",
            (
                f"We are writing with an update on {fund_id}. The current EV-weighted Fund-AI-R is "
                f"{float(metrics.fund_air):.1f} across {int(metrics.company_count)} portfolio companies, "
                f"with an average delta since entry of {float(metrics.avg_delta_since_entry):+.1f} points."
            ),
            "",
            "## Portfolio Highlights",
        ]
        lines.extend(
            f"- {company.name} ({company.ticker}) leads the portfolio at Org-AI-R {float(company.org_air):.1f}."
            for company in leaders
        )
        lines.extend(["", "## Biggest Improvements"])
        lines.extend(
            f"- {company.name} improved {float(company.delta_since_entry):+.1f} points versus entry."
            for company in improvers
        )
        lines.extend(
            [
                "",
                "## Investment Tracker",
                f"- Active programs: {int(investments['investment_count'])}",
                f"- Capital invested: ${float(investments['invested_amount_mm']):.2f}M",
                f"- Current plus realized value: ${float(investments['total_value_mm']):.2f}M",
                f"- ROI: {float(investments['roi_pct']):.2f}%",
                f"- Projected ROI: {float(investments['projected_roi_pct']):.2f}%",
            ]
        )

        if memories:
            lines.extend(["", "## Semantic Memory Signals"])
            lines.extend(
                f"- {item['title']}: {item['summary']}"
                for item in memories
            )

        lines.extend(["", "## Next Steps"])
        lines.extend(
            [
                "- Continue evidence-backed value creation on the lowest-scoring dimensions in each company.",
                "- Track realized ROI against active AI investment programs each quarter.",
                "- Re-run the agentic due diligence workflow for material score changes or major diligence events.",
                "",
                "Sincerely,",
                "PE OrgAIR Platform",
            ]
        )

        markdown = "\n".join(lines).strip() + "\n"
        artifact_dir = _artifact_root()
        stem = f"lp_letter_{_slugify(fund_id)}_{_timestamp_slug()}"
        markdown_path = artifact_dir / f"{stem}.md"
        docx_path = artifact_dir / f"{stem}.docx"
        markdown_path.write_text(markdown, encoding="utf-8")

        blocks = [
            DocParagraph("title", title),
            DocParagraph("body", f"Fund-AI-R: {float(metrics.fund_air):.1f}"),
            DocParagraph("body", f"Portfolio companies: {int(metrics.company_count)}"),
            DocParagraph("body", f"Average delta since entry: {float(metrics.avg_delta_since_entry):+.1f}"),
            DocParagraph("heading1", "Portfolio Highlights"),
        ]
        blocks.extend(
            DocParagraph("bullet", f"{company.name} ({company.ticker}) at Org-AI-R {float(company.org_air):.1f}")
            for company in leaders
        )
        blocks.append(DocParagraph("heading1", "Investment Tracker"))
        blocks.extend(
            [
                DocParagraph("bullet", f"Active programs: {int(investments['investment_count'])}"),
                DocParagraph("bullet", f"Capital invested: ${float(investments['invested_amount_mm']):.2f}M"),
                DocParagraph("bullet", f"Current plus realized value: ${float(investments['total_value_mm']):.2f}M"),
                DocParagraph("bullet", f"ROI: {float(investments['roi_pct']):.2f}%"),
            ]
        )
        if memories:
            blocks.append(DocParagraph("heading1", "Semantic Memory Signals"))
            blocks.extend(
                DocParagraph("bullet", f"{item['title']}: {item['summary']}")
                for item in memories
            )
        blocks.append(DocParagraph("heading1", "Next Steps"))
        blocks.extend(
            [
                DocParagraph("bullet", "Continue evidence-backed value creation on the lowest-scoring dimensions."),
                DocParagraph("bullet", "Track realized ROI against active AI investment programs each quarter."),
                DocParagraph("bullet", "Re-run agentic due diligence when major score movements occur."),
            ]
        )
        write_docx(docx_path, title=title, paragraphs=blocks, subject="LP Update Letter")

        return GeneratedReport(
            title=title,
            markdown=markdown,
            markdown_path=str(markdown_path),
            docx_path=str(docx_path),
            metadata={
                "fund_id": fund_id,
                "company_count": int(metrics.company_count),
                "investment_count": int(investments["investment_count"]),
                "memory_count": len(memories),
            },
        ).as_payload()

    def generate_sync(self, fund_id: str) -> dict[str, Any]:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(self.generate(fund_id))
        finally:
            loop.close()

