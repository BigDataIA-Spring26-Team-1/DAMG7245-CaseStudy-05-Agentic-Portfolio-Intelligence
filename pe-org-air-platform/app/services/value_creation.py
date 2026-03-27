from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List


ORG_AIR_PARAMETERS_V2: dict[str, float | str] = {
    "version": "2.0",
    "alpha": 0.60,
    "beta": 0.12,
    "gamma_0": 0.0025,
    "gamma_1": 0.05,
    "gamma_2": 0.025,
    "gamma_3": 0.01,
}


SECTOR_DEFINITIONS: dict[str, dict[str, float]] = {
    "technology": {
        "h_r_base": 85.0,
        "weight_talent": 0.18,
        "weight_governance": 0.12,
        "ebitda_multiplier": 1.20,
        "enterprise_value_base_mm": 120000.0,
    },
    "healthcare": {
        "h_r_base": 75.0,
        "weight_talent": 0.14,
        "weight_governance": 0.18,
        "ebitda_multiplier": 1.00,
        "enterprise_value_base_mm": 70000.0,
    },
    "financial_services": {
        "h_r_base": 78.0,
        "weight_talent": 0.12,
        "weight_governance": 0.18,
        "ebitda_multiplier": 0.95,
        "enterprise_value_base_mm": 90000.0,
    },
    "manufacturing": {
        "h_r_base": 68.0,
        "weight_talent": 0.10,
        "weight_governance": 0.12,
        "ebitda_multiplier": 0.90,
        "enterprise_value_base_mm": 55000.0,
    },
    "retail": {
        "h_r_base": 72.0,
        "weight_talent": 0.11,
        "weight_governance": 0.12,
        "ebitda_multiplier": 0.88,
        "enterprise_value_base_mm": 50000.0,
    },
    "business_services": {
        "h_r_base": 74.0,
        "weight_talent": 0.15,
        "weight_governance": 0.12,
        "ebitda_multiplier": 1.02,
        "enterprise_value_base_mm": 60000.0,
    },
    "consumer": {
        "h_r_base": 70.0,
        "weight_talent": 0.10,
        "weight_governance": 0.12,
        "ebitda_multiplier": 0.90,
        "enterprise_value_base_mm": 48000.0,
    },
    "industrials": {
        "h_r_base": 69.0,
        "weight_talent": 0.10,
        "weight_governance": 0.13,
        "ebitda_multiplier": 0.92,
        "enterprise_value_base_mm": 52000.0,
    },
    "services": {
        "h_r_base": 73.0,
        "weight_talent": 0.14,
        "weight_governance": 0.12,
        "ebitda_multiplier": 0.98,
        "enterprise_value_base_mm": 58000.0,
    },
    "energy": {
        "h_r_base": 60.0,
        "weight_talent": 0.09,
        "weight_governance": 0.14,
        "ebitda_multiplier": 0.84,
        "enterprise_value_base_mm": 65000.0,
    },
    "unknown": {
        "h_r_base": 70.0,
        "weight_talent": 0.12,
        "weight_governance": 0.12,
        "ebitda_multiplier": 0.95,
        "enterprise_value_base_mm": 50000.0,
    },
}


DIMENSION_PLAYBOOKS: dict[str, dict[str, Any]] = {
    "data_infrastructure": {
        "initiative": "Modernize the governed data foundation",
        "owner": "CTO / Data Platform Lead",
        "base_investment_mm": 3.0,
        "actions": [
            "Consolidate critical data sources into a governed warehouse or lakehouse",
            "Implement data quality controls, lineage, and service-level monitoring",
            "Expose reusable APIs and curated data products for analysts and model teams",
        ],
    },
    "ai_governance": {
        "initiative": "Establish AI governance and control frameworks",
        "owner": "Chief Risk Officer / CIO",
        "base_investment_mm": 1.6,
        "actions": [
            "Define model risk ownership, approval workflows, and audit logging",
            "Publish policies for acceptable use, bias review, and monitoring",
            "Stand up a lightweight governance council with business and legal representation",
        ],
    },
    "technology_stack": {
        "initiative": "Standardize the model-development and deployment stack",
        "owner": "Head of Engineering",
        "base_investment_mm": 2.4,
        "actions": [
            "Adopt a consistent experimentation, registry, and deployment toolchain",
            "Instrument production workflows for observability and rollback",
            "Reduce bespoke point solutions that slow scaling across business units",
        ],
    },
    "talent": {
        "initiative": "Close critical AI talent gaps",
        "owner": "Chief People Officer",
        "base_investment_mm": 2.2,
        "actions": [
            "Hire or retain senior ML, data engineering, and platform leadership roles",
            "Create role-specific enablement plans for operators and analysts",
            "Align compensation and recruiting with the target AI operating model",
        ],
    },
    "leadership": {
        "initiative": "Create executive ownership for AI value creation",
        "owner": "CEO / Business Sponsor",
        "base_investment_mm": 1.1,
        "actions": [
            "Name an executive sponsor with explicit transformation accountability",
            "Tie the roadmap to measurable operating and financial outcomes",
            "Review AI initiatives in the same cadence as major operational priorities",
        ],
    },
    "use_case_portfolio": {
        "initiative": "Prioritize a scaled AI use-case portfolio",
        "owner": "Business Transformation Lead",
        "base_investment_mm": 2.8,
        "actions": [
            "Rank use cases by EBITDA leverage, implementation friction, and data readiness",
            "Move the top use cases from pilot to production with named owners",
            "Retire low-value experiments and concentrate capital behind the best wedge cases",
        ],
    },
    "culture": {
        "initiative": "Improve adoption, literacy, and operating cadence",
        "owner": "COO / Change Management Lead",
        "base_investment_mm": 1.3,
        "actions": [
            "Launch manager-level AI literacy and workflow redesign training",
            "Create a visible adoption scoreboard for operating teams",
            "Reward experimentation that improves cycle time, quality, or revenue conversion",
        ],
    },
}


def normalize_sector(sector: str | None) -> str:
    value = str(sector or "").strip().lower().replace(" ", "_")
    if value in SECTOR_DEFINITIONS:
        return value
    return "unknown"


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def score_to_level(score: float) -> int:
    if score >= 80:
        return 5
    if score >= 60:
        return 4
    if score >= 40:
        return 3
    if score >= 20:
        return 2
    return 1


def estimate_enterprise_value_mm(
    sector: str | None,
    market_cap_percentile: float,
    position_factor: float,
) -> float:
    sector_key = normalize_sector(sector)
    sector_def = SECTOR_DEFINITIONS[sector_key]
    base_value = float(sector_def["enterprise_value_base_mm"])
    percentile_multiplier = 0.70 + clamp01(market_cap_percentile)
    normalized_position = clamp01((float(position_factor) + 1.0) / 2.0)
    position_multiplier = 0.85 + (0.50 * normalized_position)
    return round(base_value * percentile_multiplier * position_multiplier, 1)


@dataclass(frozen=True)
class EBITDAProjection:
    company_id: str
    sector: str
    delta_air: float
    current_org_air: float
    target_org_air: float
    scenarios: Dict[str, str]
    scenario_values: Dict[str, float]
    risk_adjusted: str
    risk_adjusted_pct: float
    expected_payback_months: int
    requires_approval: bool
    inputs_used: Dict[str, float | str]

    def as_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GapInitiative:
    dimension: str
    initiative: str
    priority_rank: int
    current_score: float
    target_score: float
    gap: float
    evidence_count: int
    owner: str
    timeline_days: int
    rationale: str
    rubric_target: str
    recommended_actions: List[str]
    estimated_investment_mm: Dict[str, float]
    expected_score_lift: float

    def as_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GapAnalysisResult:
    company_id: str
    sector: str
    current_org_air: float
    target_org_air: float
    current_vs_target_gap: float
    dimension_gaps: Dict[str, float]
    priority_dimensions: List[str]
    initiatives: List[GapInitiative]
    investment_estimate_mm: Dict[str, float]
    projected_ebitda_pct: float
    expected_payback_months: int
    notes: List[str]

    def as_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["initiatives"] = [initiative.as_payload() for initiative in self.initiatives]
        return payload


class ValueCreationService:
    def project_ebitda(
        self,
        *,
        company_id: str,
        sector: str | None,
        position_factor: float,
        current_org_air: float,
        current_synergy: float,
        entry_score: float,
        target_score: float,
        h_r_score: float,
    ) -> EBITDAProjection:
        sector_key = normalize_sector(sector)
        params = ORG_AIR_PARAMETERS_V2
        sector_def = SECTOR_DEFINITIONS[sector_key]

        delta_air = max(0.0, float(target_score) - float(entry_score))
        delta_norm = delta_air / 100.0
        hr_norm = clamp01(float(h_r_score) / 100.0)
        synergy_norm = clamp01(float(current_synergy) / 100.0)
        position_norm = clamp01((float(position_factor) + 1.0) / 2.0)

        base_ratio = (
            float(params["gamma_0"])
            + (float(params["gamma_1"]) * delta_norm)
            + (float(params["gamma_2"]) * hr_norm)
            + (float(params["gamma_3"]) * ((0.60 * synergy_norm) + (0.40 * position_norm)))
        )

        sector_multiplier = float(sector_def["ebitda_multiplier"])
        base_pct = round(base_ratio * sector_multiplier * 100.0, 2)
        conservative = round(base_pct * 0.75, 2)
        optimistic = round(base_pct * 1.30, 2)
        risk_adjustment_factor = 0.70 + (0.30 * hr_norm)
        risk_adjusted_pct = round(base_pct * risk_adjustment_factor, 2)

        payback_months = int(
            max(
                12,
                min(
                    48,
                    round(30.0 / max(1.0, risk_adjusted_pct)),
                ),
            )
        )

        return EBITDAProjection(
            company_id=company_id,
            sector=sector_key,
            delta_air=round(delta_air, 2),
            current_org_air=round(float(current_org_air), 2),
            target_org_air=round(float(target_score), 2),
            scenarios={
                "conservative": f"{conservative:.2f}%",
                "base": f"{base_pct:.2f}%",
                "optimistic": f"{optimistic:.2f}%",
            },
            scenario_values={
                "conservative": conservative,
                "base": base_pct,
                "optimistic": optimistic,
            },
            risk_adjusted=f"{risk_adjusted_pct:.2f}%",
            risk_adjusted_pct=risk_adjusted_pct,
            expected_payback_months=payback_months,
            requires_approval=risk_adjusted_pct > 5.0,
            inputs_used={
                "parameter_version": str(params["version"]),
                "sector_multiplier": sector_multiplier,
                "position_factor": round(float(position_factor), 4),
                "hr_score": round(float(h_r_score), 2),
                "synergy_score": round(float(current_synergy), 2),
            },
        )

    def analyze_gap(
        self,
        *,
        company_id: str,
        sector: str | None,
        current_org_air: float,
        target_org_air: float,
        position_factor: float,
        dimension_scores: Dict[str, Dict[str, float | int | str]],
        rubric_targets: Dict[str, str],
        ebitda_projection: EBITDAProjection,
    ) -> GapAnalysisResult:
        sector_key = normalize_sector(sector)
        sector_multiplier = float(SECTOR_DEFINITIONS[sector_key]["ebitda_multiplier"])
        current_vs_target_gap = round(max(0.0, float(target_org_air) - float(current_org_air)), 2)

        dimension_gaps = {
            dimension: round(
                max(0.0, float(target_org_air) - float(context.get("score", 0.0) or 0.0)),
                2,
            )
            for dimension, context in dimension_scores.items()
        }

        sorted_dimensions = sorted(
            dimension_gaps.items(),
            key=lambda item: item[1],
            reverse=True,
        )
        priority_dimensions = [
            dimension
            for dimension, gap in sorted_dimensions
            if gap > 0
        ][:3]

        if not priority_dimensions:
            priority_dimensions = [
                dimension
                for dimension, _ in sorted(
                    (
                        (dimension, float(context.get("score", 0.0) or 0.0))
                        for dimension, context in dimension_scores.items()
                    ),
                    key=lambda item: item[1],
                )
            ][:3]

        initiatives: list[GapInitiative] = []
        total_low = 0.0
        total_base = 0.0
        total_high = 0.0

        for index, dimension in enumerate(priority_dimensions, start=1):
            playbook = DIMENSION_PLAYBOOKS.get(dimension, DIMENSION_PLAYBOOKS["technology_stack"])
            context = dimension_scores.get(dimension, {})
            current_score = round(float(context.get("score", 0.0) or 0.0), 1)
            gap = round(max(0.0, float(target_org_air) - current_score), 1)
            evidence_count = int(context.get("evidence_count", 0) or 0)
            target_score = round(min(85.0, max(current_score, current_score + min(gap, 18.0))), 1)

            severity = max(0.60, min(1.80, (gap / 15.0) if gap > 0 else 0.60))
            evidence_factor = 1.10 if evidence_count < 3 else 1.0
            position_factor_multiplier = 0.90 + (0.20 * clamp01((float(position_factor) + 1.0) / 2.0))
            base_investment = round(
                float(playbook["base_investment_mm"]) * severity * evidence_factor * sector_multiplier * position_factor_multiplier,
                1,
            )
            low_investment = round(base_investment * 0.80, 1)
            high_investment = round(base_investment * 1.30, 1)
            expected_score_lift = round(min(gap if gap > 0 else 5.0, 4.0 + (base_investment * 2.0)), 1)
            timeline_days = 100 if index == 1 else (120 if index == 2 else 150)
            rubric_target = rubric_targets.get(dimension, "Elevate evidence quality and operating maturity for this dimension.")

            initiatives.append(
                GapInitiative(
                    dimension=dimension,
                    initiative=str(playbook["initiative"]),
                    priority_rank=index,
                    current_score=current_score,
                    target_score=target_score,
                    gap=gap,
                    evidence_count=evidence_count,
                    owner=str(playbook["owner"]),
                    timeline_days=timeline_days,
                    rationale=(
                        f"{dimension.replace('_', ' ').title()} is {gap:.1f} points below the target operating posture. "
                        f"Evidence coverage is {evidence_count} items, so the plan emphasizes both execution and proof."
                    ),
                    rubric_target=rubric_target,
                    recommended_actions=list(playbook["actions"]),
                    estimated_investment_mm={
                        "low": low_investment,
                        "base": base_investment,
                        "high": high_investment,
                    },
                    expected_score_lift=expected_score_lift,
                )
            )
            total_low += low_investment
            total_base += base_investment
            total_high += high_investment

        notes = [
            "Priority dimensions are ranked by current score gap to the target Org-AI-R state.",
            "Investment estimates are directional and should be replaced with operator-validated budgets.",
            "Projected EBITDA impact is derived from the v2.0 value-creation model parameters and current CS3 state.",
        ]

        return GapAnalysisResult(
            company_id=company_id,
            sector=sector_key,
            current_org_air=round(float(current_org_air), 2),
            target_org_air=round(float(target_org_air), 2),
            current_vs_target_gap=current_vs_target_gap,
            dimension_gaps=dimension_gaps,
            priority_dimensions=priority_dimensions,
            initiatives=initiatives,
            investment_estimate_mm={
                "low": round(total_low, 1),
                "base": round(total_base, 1),
                "high": round(total_high, 1),
            },
            projected_ebitda_pct=ebitda_projection.risk_adjusted_pct,
            expected_payback_months=ebitda_projection.expected_payback_months,
            notes=notes,
        )


value_creation_service = ValueCreationService()
