from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from app.config import settings
from app.services.redis_cache import cache_get_json, cache_set_json
from app.services.snowflake import get_snowflake_connection
from app.scoring_engine.rubric_scorer import DIMENSION_RUBRICS, FEATURE_TO_RUBRIC_DIM


@dataclass(frozen=True)
class ScoringRecord:
    company_id: str
    assessment_id: Optional[str]
    scoring_run_id: Optional[str]
    vr_score: float
    synergy_bonus: float
    talent_penalty: float
    sem_lower: Optional[float]
    sem_upper: Optional[float]
    composite_score: float
    score_band: str
    breakdown: Dict[str, Any]
    scored_at: Any


class ScoringClient:
    """
    Read-only scoring integration client for internal service usage.

    Current responsibilities:
    - fetch latest company-level scoring payload
    - list latest scored companies
    - expose dimension-level score context from CS3 breakdown JSON
    - normalize dimension aliases between CS3 naming and CS4 workflow naming
    """

    JUSTIFICATION_TO_SCORE_DIMENSION: Dict[str, str] = {
        "leadership": "leadership_vision",
        "talent": "talent_skills",
        "culture": "culture_change",
        "ai_governance": "ai_governance",
        "data_infrastructure": "data_infrastructure",
        "technology_stack": "technology_stack",
        "use_case_portfolio": "use_case_portfolio",
    }

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

    LEVEL_RANGES: Dict[int, tuple[int, int]] = {
        1: (0, 19),
        2: (20, 39),
        3: (40, 59),
        4: (60, 79),
        5: (80, 100),
    }

    @staticmethod
    def _coerce_numeric(value: Any, default: float = 0.0) -> float:
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return default
        if isinstance(value, dict):
            for key in ("value", "score", "position_factor", "factor", "raw", "final"):
                if key in value:
                    return ScoringClient._coerce_numeric(value.get(key), default=default)
            return default
        return default

    def _parse_breakdown(self, row_variant: Any) -> Dict[str, Any]:
        if row_variant is None:
            return {}
        if isinstance(row_variant, dict):
            return row_variant
        if isinstance(row_variant, list):
            return {"_": row_variant}
        try:
            return json.loads(row_variant)
        except Exception:
            return {}

    def _normalize_dimension_for_score(self, dimension: str) -> str:
        normalized = (dimension or "").strip().lower().replace(" ", "_")
        return self.JUSTIFICATION_TO_SCORE_DIMENSION.get(normalized, normalized)

    def _normalize_dimension_for_justify(self, dimension: str) -> str:
        normalized = (dimension or "").strip().lower().replace(" ", "_")
        return self.SCORE_TO_JUSTIFICATION_DIMENSION.get(normalized, normalized)

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

    def _rubric_dimension(self, dimension: str) -> str:
        score_dimension = self._normalize_dimension_for_score(dimension)
        return FEATURE_TO_RUBRIC_DIM.get(score_dimension, self._normalize_dimension_for_justify(score_dimension))

    def _dimension_confidence_interval(
        self,
        score: float,
        confidence: Optional[float],
    ) -> tuple[float, float]:
        conf = float(confidence if confidence is not None else 0.5)
        conf = max(0.0, min(1.0, conf))
        half_width = max(4.0, round(12.0 - (conf * 8.0), 2))
        return (
            max(0.0, round(float(score) - half_width, 2)),
            min(100.0, round(float(score) + half_width, 2)),
        )

    def _hr_score_from_payload(self, payload: Dict[str, Any]) -> Optional[float]:
        breakdown = payload.get("breakdown", {}) or {}
        hr = breakdown.get("hr")
        if isinstance(hr, dict):
            score = hr.get("score")
            if isinstance(score, (int, float)):
                return round(float(score), 2)
        return None

    def _latest_score_row(self, company_id: str) -> ScoringRecord:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                  company_id,
                  assessment_id,
                  scoring_run_id,
                  vr_score,
                  synergy_bonus,
                  talent_penalty,
                  sem_lower,
                  sem_upper,
                  composite_score,
                  score_band,
                  dimension_breakdown_json,
                  scored_at
                FROM org_air_scores
                WHERE company_id = %s
                ORDER BY scored_at DESC, created_at DESC
                LIMIT 1
                """,
                (company_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError("No scores found for company")

            return ScoringRecord(
                company_id=str(row[0]),
                assessment_id=str(row[1]) if row[1] else None,
                scoring_run_id=str(row[2]) if row[2] else None,
                vr_score=float(row[3] or 0),
                synergy_bonus=float(row[4] or 0),
                talent_penalty=float(row[5] or 0),
                sem_lower=float(row[6]) if row[6] is not None else None,
                sem_upper=float(row[7]) if row[7] is not None else None,
                composite_score=float(row[8] or 0),
                score_band=str(row[9] or ""),
                breakdown=self._parse_breakdown(row[10]),
                scored_at=row[11],
            )
        finally:
            cur.close()
            conn.close()

    def _build_payload(self, record: ScoringRecord) -> Dict[str, Any]:
        payload = asdict(record)
        payload["overall_score"] = payload["composite_score"]
        payload["dimension_scores"] = self.get_dimension_scores_from_payload(payload)
        return payload

    def _extract_dimension_breakdown(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        breakdown = payload.get("breakdown", {}) or {}
        vr = breakdown.get("vr", {}) or {}
        dimension_breakdown = vr.get("dimension_breakdown", []) or []
        return [item for item in dimension_breakdown if isinstance(item, dict)]

    def _find_dimension_entry(
        self,
        payload: Dict[str, Any],
        dimension: str,
    ) -> Optional[Dict[str, Any]]:
        target_score_dimension = self._normalize_dimension_for_score(dimension)

        for item in self._extract_dimension_breakdown(payload):
            raw_dimension = str(item.get("dimension", "")).strip().lower()
            if raw_dimension == target_score_dimension:
                return item

        return None

    def get_latest_scores(self, company_id: str) -> Dict[str, Any]:
        if not company_id or not company_id.strip():
            raise ValueError("company_id is required")

        cache_key = f"scoring:results:company:{company_id}"
        cached = cache_get_json(cache_key)
        if cached is not None:
            payload = cached if isinstance(cached, dict) else dict(cached)
            if "dimension_scores" not in payload:
                payload["dimension_scores"] = self.get_dimension_scores_from_payload(payload)
            return payload

        record = self._latest_score_row(company_id)
        payload = self._build_payload(record)

        cache_set_json(
            cache_key,
            payload,
            settings.redis_ttl_seconds,
        )
        return payload

    def get_dimension_scores_from_payload(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for item in self._extract_dimension_breakdown(payload):
            raw_dimension = str(item.get("dimension", "")).strip().lower()
            raw_score = float(item.get("raw_score", 0.0) or 0.0)
            level = self._score_to_level(raw_score)

            out.append(
                {
                    "score_dimension": raw_dimension,
                    "dimension": self._normalize_dimension_for_justify(raw_dimension),
                    "raw_score": raw_score,
                    "weighted_score": float(item.get("weighted_score", item.get("weighted_contribution", 0.0)) or 0.0),
                    "sector_weight": float(item.get("sector_weight", item.get("weight", 0.0)) or 0.0),
                    "confidence": float(item.get("confidence_used", item.get("confidence", 0.0)) or 0.0),
                    "evidence_count": int(item.get("evidence_count", 0) or 0),
                    "level": level,
                    "level_name": self.LEVEL_NAMES[level],
                }
            )

        return out

    def get_dimension_score(self, company_id: str, dimension: str) -> Dict[str, Any]:
        payload = self.get_latest_scores(company_id)
        entry = self._find_dimension_entry(payload, dimension)
        if entry is None:
            raise ValueError(f"No dimension score found for dimension: {dimension}")

        score_dimension = str(entry.get("dimension", "")).strip().lower()
        raw_score = float(entry.get("raw_score", 0.0) or 0.0)
        level = self._score_to_level(raw_score)

        return {
            "company_id": payload.get("company_id"),
            "assessment_id": payload.get("assessment_id"),
            "scoring_run_id": payload.get("scoring_run_id"),
            "dimension": self._normalize_dimension_for_justify(score_dimension),
            "score_dimension": score_dimension,
            "raw_score": raw_score,
            "weighted_score": float(entry.get("weighted_score", entry.get("weighted_contribution", 0.0)) or 0.0),
            "sector_weight": float(entry.get("sector_weight", entry.get("weight", 0.0)) or 0.0),
            "confidence": float(entry.get("confidence_used", entry.get("confidence", 0.0)) or 0.0),
            "evidence_count": int(entry.get("evidence_count", 0) or 0),
            "level": level,
            "level_name": self.LEVEL_NAMES[level],
            "confidence_interval": self._dimension_confidence_interval(
                raw_score,
                float(entry.get("confidence_used", entry.get("confidence", 0.0)) or 0.0),
            ),
            "score_band": payload.get("score_band"),
            "overall_score": payload.get("overall_score"),
            "scored_at": payload.get("scored_at"),
        }

    def get_dimension_context(self, company_id: str, dimension: str) -> Dict[str, Any]:
        """
        Returns dimension-level score context for downstream justification logic.

        This is not a separate rubric table lookup, because the current repository
        does not expose rubric rows from Snowflake. Instead it exposes the CS3
        dimension score context derived from the latest scoring breakdown.
        """
        dimension_score = self.get_dimension_score(company_id, dimension)
        return {
            "company_id": dimension_score["company_id"],
            "dimension": dimension_score["dimension"],
            "score_dimension": dimension_score["score_dimension"],
            "raw_score": dimension_score["raw_score"],
            "weighted_score": dimension_score["weighted_score"],
            "sector_weight": dimension_score["sector_weight"],
            "confidence": dimension_score["confidence"],
            "confidence_interval": dimension_score["confidence_interval"],
            "evidence_count": dimension_score["evidence_count"],
            "level": dimension_score["level"],
            "level_name": dimension_score["level_name"],
            "overall_score": dimension_score["overall_score"],
            "score_band": dimension_score["score_band"],
            "scored_at": dimension_score["scored_at"],
        }

    def get_rubric(
        self,
        dimension: str,
        level: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        rubric_dimension = self._rubric_dimension(dimension)
        rubrics = DIMENSION_RUBRICS.get(rubric_dimension, {})
        out: List[Dict[str, Any]] = []

        for level_enum, criteria in rubrics.items():
            level_value = int(level_enum.name.split("_")[-1])
            if level is not None and level_value != int(level):
                continue

            score_range = self.LEVEL_RANGES[level_value]
            criteria_text = (
                f"{rubric_dimension.replace('_', ' ').title()} Level {level_value} "
                f"({self.LEVEL_NAMES[level_value]}) expects keyword evidence such as "
                f"{', '.join(criteria.keywords[:5])} and a quantitative threshold "
                f"of at least {criteria.quantitative_threshold:.2f}."
            )

            out.append(
                {
                    "dimension": rubric_dimension,
                    "level": level_value,
                    "level_name": self.LEVEL_NAMES[level_value],
                    "criteria_text": criteria_text,
                    "keywords": list(criteria.keywords),
                    "quantitative_thresholds": {
                        "threshold": float(criteria.quantitative_threshold),
                        "min_keyword_matches": int(criteria.min_keyword_matches),
                        "score_min": score_range[0],
                        "score_max": score_range[1],
                    },
                }
            )

        out.sort(key=lambda item: int(item["level"]), reverse=True)
        return out

    def get_assessment(self, company_id: str) -> Dict[str, Any]:
        payload = self.get_latest_scores(company_id)
        dimension_scores = {
            item["dimension"]: {
                "dimension": item["dimension"],
                "score": item["raw_score"],
                "level": item["level"],
                "level_name": item["level_name"],
                "confidence": item["confidence"],
                "confidence_interval": self._dimension_confidence_interval(
                    float(item["raw_score"]),
                    float(item["confidence"]),
                ),
                "evidence_count": item["evidence_count"],
                "sector_weight": item["sector_weight"],
                "weighted_score": item["weighted_score"],
            }
            for item in self.get_dimension_scores_from_payload(payload)
        }

        lower = payload.get("sem_lower")
        upper = payload.get("sem_upper")
        if not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
            overall_score = float(payload.get("overall_score", payload.get("composite_score", 0.0)) or 0.0)
            lower = max(0.0, round(overall_score - 8.0, 2))
            upper = min(100.0, round(overall_score + 8.0, 2))

        breakdown = payload.get("breakdown", {}) or {}
        talent_penalty = breakdown.get("talent_penalty", {}) if isinstance(breakdown, dict) else {}

        return {
            "company_id": payload.get("company_id"),
            "assessment_id": payload.get("assessment_id"),
            "assessment_date": str(payload.get("scored_at") or ""),
            "vr_score": self._coerce_numeric(payload.get("vr_score", 0.0), default=0.0),
            "hr_score": self._hr_score_from_payload(payload),
            "synergy_score": self._coerce_numeric(payload.get("synergy_bonus", 0.0), default=0.0),
            "org_air_score": self._coerce_numeric(
                payload.get("overall_score", payload.get("composite_score", 0.0)),
                default=0.0,
            ),
            "confidence_interval": (float(lower), float(upper)),
            "dimension_scores": dimension_scores,
            "talent_concentration": self._coerce_numeric(talent_penalty.get("hhi_value", 0.0), default=0.0),
            "position_factor": self._coerce_numeric(
                breakdown.get("position_factor") if isinstance(breakdown, dict) else 0.0,
                default=0.0,
            ),
            "score_band": payload.get("score_band"),
            "scored_at": payload.get("scored_at"),
        }

    def list_latest_scores(self, limit: int = 50) -> List[Dict[str, Any]]:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                WITH latest AS (
                  SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY scored_at DESC, created_at DESC) AS rn
                  FROM org_air_scores
                )
                SELECT
                  company_id,
                  assessment_id,
                  scoring_run_id,
                  vr_score,
                  synergy_bonus,
                  talent_penalty,
                  sem_lower,
                  sem_upper,
                  composite_score,
                  score_band,
                  dimension_breakdown_json,
                  scored_at
                FROM latest
                WHERE rn = 1
                ORDER BY composite_score DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

            out: List[Dict[str, Any]] = []
            for row in rows:
                record = ScoringRecord(
                    company_id=str(row[0]),
                    assessment_id=str(row[1]) if row[1] else None,
                    scoring_run_id=str(row[2]) if row[2] else None,
                    vr_score=float(row[3] or 0),
                    synergy_bonus=float(row[4] or 0),
                    talent_penalty=float(row[5] or 0),
                    sem_lower=float(row[6]) if row[6] is not None else None,
                    sem_upper=float(row[7]) if row[7] is not None else None,
                    composite_score=float(row[8] or 0),
                    score_band=str(row[9] or ""),
                    breakdown=self._parse_breakdown(row[10]),
                    scored_at=row[11],
                )
                out.append(self._build_payload(record))

            return out
        finally:
            cur.close()
            conn.close()
