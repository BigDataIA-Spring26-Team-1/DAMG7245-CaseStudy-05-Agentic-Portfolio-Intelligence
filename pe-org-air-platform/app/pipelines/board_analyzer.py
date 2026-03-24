from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import re
from typing import List

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None


@dataclass
class BoardMember:
    name: str
    title: str
    committees: List[str]
    bio: str
    is_independent: bool
    tenure_years: int


@dataclass
class GovernanceSignal:
    company_id: str
    ticker: str
    has_tech_committee: bool
    has_ai_expertise: bool
    has_data_officer: bool
    has_risk_tech_oversight: bool
    has_ai_in_strategy: bool
    tech_expertise_count: int
    independent_ratio: Decimal
    governance_score: Decimal
    confidence: Decimal
    ai_experts: List[str] = field(default_factory=list)
    relevant_committees: List[str] = field(default_factory=list)


class BoardCompositionAnalyzer:
    AI_EXPERTISE_KEYWORDS = [
        "artificial intelligence", "machine learning", "chief data officer", "cdo", "caio", "chief ai",
        "chief technology", "cto", "chief digital", "data science", "analytics", "digital transformation",
    ]
    TECH_COMMITTEE_NAMES = [
        "technology committee", "digital committee", "innovation committee", "it committee", "technology and cybersecurity",
    ]
    DATA_OFFICER_TITLES = [
        "chief data officer", "cdo", "chief ai officer", "caio", "chief analytics officer", "cao", "chief digital officer",
    ]

    @staticmethod
    def _contains_keyword(text: str, keyword: str) -> bool:
        return re.search(r"\b" + re.escape(keyword.lower()) + r"\b", (text or "").lower()) is not None

    def analyze_board(
        self,
        company_id: str,
        ticker: str,
        members: List[BoardMember],
        committees: List[str],
        strategy_text: str = "",
    ) -> GovernanceSignal:
        score = Decimal("20")

        committees_lower = [c.lower() for c in committees]
        strategy_lower = (strategy_text or "").lower()

        has_tech = any(any(tc in c for tc in self.TECH_COMMITTEE_NAMES) for c in committees_lower)
        if has_tech:
            score += Decimal("15")

        ai_experts: List[str] = []
        for member in members:
            bio_lower = (member.bio or "").lower()
            title_lower = (member.title or "").lower()
            if any(
                self._contains_keyword(bio_lower, kw) or self._contains_keyword(title_lower, kw)
                for kw in self.AI_EXPERTISE_KEYWORDS
            ):
                ai_experts.append(member.name)
        has_ai_expertise = len(ai_experts) > 0
        if has_ai_expertise:
            score += Decimal("20")

        has_data_officer = any(any(t in (m.title or "").lower() for t in self.DATA_OFFICER_TITLES) for m in members)
        if has_data_officer:
            score += Decimal("15")

        independent_count = sum(1 for m in members if m.is_independent)
        independent_ratio = Decimal(str(round(independent_count / max(1, len(members)), 3)))
        if independent_ratio > Decimal("0.5"):
            score += Decimal("10")

        has_risk_tech_oversight = any(
            "risk" in c and ("tech" in c or "cyber" in c or "digital" in c)
            for c in committees_lower
        )
        if has_risk_tech_oversight:
            score += Decimal("10")

        has_ai_in_strategy = any(k in strategy_lower for k in ["ai", "artificial intelligence", "machine learning", "automation", "data science"])
        if has_ai_in_strategy:
            score += Decimal("10")

        score = min(score, Decimal("100"))
        confidence = min(Decimal("0.5") + Decimal(str(len(members) / 20.0)), Decimal("0.95"))

        relevant_committees = [
            c for c in committees
            if any(tc in c.lower() for tc in self.TECH_COMMITTEE_NAMES) or "risk" in c.lower()
        ]

        return GovernanceSignal(
            company_id=company_id,
            ticker=ticker,
            has_tech_committee=has_tech,
            has_ai_expertise=has_ai_expertise,
            has_data_officer=has_data_officer,
            has_risk_tech_oversight=has_risk_tech_oversight,
            has_ai_in_strategy=has_ai_in_strategy,
            tech_expertise_count=len(ai_experts),
            independent_ratio=independent_ratio,
            governance_score=score,
            confidence=confidence.quantize(Decimal("0.001")),
            ai_experts=ai_experts,
            relevant_committees=relevant_committees,
        )

    def extract_from_proxy(self, proxy_html: str) -> tuple[List[BoardMember], List[str]]:
        if BeautifulSoup is not None:
            soup = BeautifulSoup(proxy_html or "", "html.parser")
            text = soup.get_text(" ", strip=True)
        else:
            text = re.sub(r"<[^>]+>", " ", proxy_html or "")
            text = re.sub(r"\s+", " ", text).strip()

        committee_pattern = re.compile(
            r"(technology committee|digital committee|innovation committee|it committee|risk committee|cybersecurity committee)",
            re.IGNORECASE,
        )
        committees = sorted(set(m.group(1).strip() for m in committee_pattern.finditer(text)))

        name_pattern = re.compile(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}\b")
        members: List[BoardMember] = []
        seen: set[str] = set()
        for name in name_pattern.findall(text):
            if name in seen:
                continue
            seen.add(name)
            members.append(
                BoardMember(
                    name=name,
                    title="Director",
                    committees=[],
                    bio="",
                    is_independent=True,
                    tenure_years=0,
                )
            )

        return members, committees
