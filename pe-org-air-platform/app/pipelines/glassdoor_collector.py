from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Optional

import httpx
from app.config import settings


@dataclass
class GlassdoorReview:
    review_id: str
    rating: float
    title: str
    pros: str
    cons: str
    advice_to_management: Optional[str]
    is_current_employee: bool
    job_title: str
    review_date: datetime


@dataclass
class CultureSignal:
    company_id: str
    ticker: str
    innovation_score: Decimal
    data_driven_score: Decimal
    change_readiness_score: Decimal
    ai_awareness_score: Decimal
    overall_score: Decimal
    review_count: int
    avg_rating: Decimal
    current_employee_ratio: Decimal
    confidence: Decimal
    positive_keywords_found: List[str] = field(default_factory=list)
    negative_keywords_found: List[str] = field(default_factory=list)


class GlassdoorCultureCollector:
    INNOVATION_POSITIVE = [
        "innovative", "cutting-edge", "forward-thinking", "encourages new ideas",
        "experimental", "creative freedom", "startup mentality", "move fast", "disruptive",
    ]
    INNOVATION_NEGATIVE = [
        "bureaucratic", "slow to change", "resistant", "outdated", "stuck in old ways",
        "red tape", "politics", "siloed", "hierarchical",
    ]
    DATA_DRIVEN_KEYWORDS = [
        "data-driven", "metrics", "evidence-based", "analytical", "kpis", "dashboards",
        "data culture", "measurement", "quantitative",
    ]
    AI_AWARENESS_KEYWORDS = [
        "ai", "artificial intelligence", "machine learning", "automation", "data science",
        "ml", "algorithms", "predictive", "neural network",
    ]
    CHANGE_POSITIVE = ["agile", "adaptive", "fast-paced", "embraces change", "continuous improvement", "growth mindset"]
    CHANGE_NEGATIVE = ["rigid", "traditional", "slow", "risk-averse", "change resistant", "old school"]
    DEFAULT_RAPIDAPI_HOST = "glassdoor-real-time.p.rapidapi.com"
    DEFAULT_COMPANY_SEARCH_PATH = "/companies/search"
    DEFAULT_REVIEWS_PATH = "/companies/reviews"
    ENV_TO_SETTING: Dict[str, str] = {
        "RAPIDAPI_KEY": "rapidapi_key",
        "GLASSDOOR_RAPIDAPI_KEY": "glassdoor_rapidapi_key",
        "GLASSDOOR_RAPIDAPI_HOST": "glassdoor_rapidapi_host",
        "GLASSDOOR_COMPANY_SEARCH_PATH": "glassdoor_company_search_path",
        "GLASSDOOR_REVIEWS_PATH": "glassdoor_reviews_path",
        "GLASSDOOR_REVIEWS_PAGE_SIZE": "glassdoor_reviews_page_size",
        "GLASSDOOR_CACHE_TO_DISK": "glassdoor_cache_to_disk",
        "GLASSDOOR_DISABLE_DISCOVERY_FALLBACK": "glassdoor_disable_discovery_fallback",
        "GLASSDOOR_REVIEWS_COMPANY_ID_PARAM": "glassdoor_reviews_company_id_param",
        "GLASSDOOR_COMPANY_ID_MAP": "glassdoor_company_id_map",
    }

    def __init__(
        self,
        *,
        rapidapi_key: Optional[str] = None,
        rapidapi_host: Optional[str] = None,
        company_search_path: Optional[str] = None,
        reviews_path: Optional[str] = None,
        timeout_seconds: float = 20.0,
        data_root: Optional[Path] = None,
    ) -> None:
        self.rapidapi_key = (
            rapidapi_key
            or self._env("RAPIDAPI_KEY")
            or self._env("GLASSDOOR_RAPIDAPI_KEY")
            or ""
        ).strip()
        self.rapidapi_host = (rapidapi_host or self._env("GLASSDOOR_RAPIDAPI_HOST") or self.DEFAULT_RAPIDAPI_HOST).strip()
        self.company_search_path = self._normalize_api_path(
            company_search_path or self._env("GLASSDOOR_COMPANY_SEARCH_PATH") or self.DEFAULT_COMPANY_SEARCH_PATH
        )
        self.reviews_path = self._normalize_api_path(
            reviews_path or self._env("GLASSDOOR_REVIEWS_PATH") or self.DEFAULT_REVIEWS_PATH
        )
        self.timeout_seconds = float(timeout_seconds)
        self.data_root = data_root
        self.page_size = self._safe_int(self._env("GLASSDOOR_REVIEWS_PAGE_SIZE"), default=50, lo=1, hi=100)
        self.cache_to_disk = (str(self._env("GLASSDOOR_CACHE_TO_DISK") or "true")).strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
        }
        self.disable_discovery_fallback = (str(self._env("GLASSDOOR_DISABLE_DISCOVERY_FALLBACK") or "false")).strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
        }
        self.reviews_company_id_param = (
            str(self._env("GLASSDOOR_REVIEWS_COMPANY_ID_PARAM") or "companyId").strip() or "companyId"
        )
        self.company_id_map = self._load_company_id_map()

    @staticmethod
    def _clamp(x: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, x))

    def analyze_reviews(self, company_id: str, ticker: str, reviews: List[GlassdoorReview]) -> CultureSignal:
        if not reviews:
            return CultureSignal(
                company_id=company_id,
                ticker=ticker,
                innovation_score=Decimal("50.00"),
                data_driven_score=Decimal("50.00"),
                change_readiness_score=Decimal("50.00"),
                ai_awareness_score=Decimal("50.00"),
                overall_score=Decimal("50.00"),
                review_count=0,
                avg_rating=Decimal("0.00"),
                current_employee_ratio=Decimal("0.00"),
                confidence=Decimal("0.30"),
            )

        now = datetime.now(timezone.utc)
        innovation_pos = innovation_neg = 0.0
        data_mentions = ai_mentions = 0.0
        change_pos = change_neg = 0.0
        total_weight = 0.0

        positive_hits: set[str] = set()
        negative_hits: set[str] = set()

        ratings: List[float] = []
        current_employees = 0

        for review in reviews:
            text = f"{review.title} {review.pros} {review.cons} {review.advice_to_management or ''}".lower()
            review_dt = review.review_date
            if review_dt.tzinfo is None:
                review_dt = review_dt.replace(tzinfo=timezone.utc)

            days_old = (now - review_dt).days
            recency_weight = 1.0 if days_old < 730 else 0.5
            employee_weight = 1.2 if review.is_current_employee else 1.0
            weight = recency_weight * employee_weight
            total_weight += weight

            ratings.append(float(review.rating))
            if review.is_current_employee:
                current_employees += 1

            for kw in self.INNOVATION_POSITIVE:
                if kw in text:
                    innovation_pos += weight
                    positive_hits.add(kw)
            for kw in self.INNOVATION_NEGATIVE:
                if kw in text:
                    innovation_neg += weight
                    negative_hits.add(kw)
            for kw in self.DATA_DRIVEN_KEYWORDS:
                if kw in text:
                    data_mentions += weight
                    positive_hits.add(kw)
            for kw in self.AI_AWARENESS_KEYWORDS:
                if kw in text:
                    ai_mentions += weight
                    positive_hits.add(kw)
            for kw in self.CHANGE_POSITIVE:
                if kw in text:
                    change_pos += weight
                    positive_hits.add(kw)
            for kw in self.CHANGE_NEGATIVE:
                if kw in text:
                    change_neg += weight
                    negative_hits.add(kw)

        denom = max(1.0, total_weight)

        innovation = self._clamp(((innovation_pos - innovation_neg) / denom) * 50.0 + 50.0, 0.0, 100.0)
        data_driven = self._clamp((data_mentions / denom) * 100.0, 0.0, 100.0)
        ai_awareness = self._clamp((ai_mentions / denom) * 100.0, 0.0, 100.0)
        change_readiness = self._clamp(((change_pos - change_neg) / denom) * 50.0 + 50.0, 0.0, 100.0)

        overall = 0.30 * innovation + 0.25 * data_driven + 0.25 * ai_awareness + 0.20 * change_readiness
        confidence = self._clamp(0.40 + min(len(reviews), 100) / 100.0 * 0.45, 0.40, 0.95)

        return CultureSignal(
            company_id=company_id,
            ticker=ticker,
            innovation_score=Decimal(str(round(innovation, 2))),
            data_driven_score=Decimal(str(round(data_driven, 2))),
            change_readiness_score=Decimal(str(round(change_readiness, 2))),
            ai_awareness_score=Decimal(str(round(ai_awareness, 2))),
            overall_score=Decimal(str(round(overall, 2))),
            review_count=len(reviews),
            avg_rating=Decimal(str(round(sum(ratings) / max(1, len(ratings)), 2))),
            current_employee_ratio=Decimal(str(round(current_employees / max(1, len(reviews)), 3))),
            confidence=Decimal(str(round(confidence, 3))),
            positive_keywords_found=sorted(positive_hits),
            negative_keywords_found=sorted(negative_hits),
        )

    def fetch_reviews(self, ticker: str, limit: int = 100) -> List[GlassdoorReview]:
        ticker_norm = str(ticker or "").strip().upper()
        if not ticker_norm:
            return []

        if self.rapidapi_key:
            api_reviews = self._fetch_reviews_from_rapidapi(ticker=ticker_norm, limit=limit)
            if api_reviews:
                if self.cache_to_disk:
                    self._write_reviews_cache(ticker=ticker_norm, reviews=api_reviews)
                return api_reviews

        return self._load_reviews_from_disk(ticker=ticker_norm, limit=limit)

    @staticmethod
    def _safe_int(raw: Optional[str], default: int, lo: int, hi: int) -> int:
        try:
            return max(lo, min(hi, int(str(raw or "").strip())))
        except Exception:
            return default

    @classmethod
    def _env(cls, key: str) -> Optional[str]:
        direct = os.getenv(key)
        if direct not in (None, ""):
            return str(direct)
        setting_name = cls.ENV_TO_SETTING.get(key)
        if setting_name and hasattr(settings, setting_name):
            value = getattr(settings, setting_name)
            if value is not None and str(value).strip() != "":
                return str(value)
        return None

    @staticmethod
    def _normalize_api_path(path: str) -> str:
        p = str(path or "").strip()
        if not p:
            return "/"
        return p if p.startswith("/") else f"/{p}"

    def _fetch_reviews_from_rapidapi(self, ticker: str, limit: int) -> List[GlassdoorReview]:
        headers = {
            "x-rapidapi-key": self.rapidapi_key,
            "x-rapidapi-host": self.rapidapi_host,
        }
        base_url = f"https://{self.rapidapi_host}"
        out: List[GlassdoorReview] = []
        try:
            with httpx.Client(base_url=base_url, headers=headers, timeout=self.timeout_seconds, follow_redirects=True) as client:
                company_id = self._configured_company_id(ticker=ticker)
                if not company_id and not self.disable_discovery_fallback:
                    company_id = self._resolve_company_id(client=client, ticker=ticker)
                if company_id:
                    out.extend(self._fetch_reviews_by_company_id(client=client, company_id=company_id, ticker=ticker, limit=limit))
                if not out and not self.disable_discovery_fallback:
                    out.extend(self._fetch_reviews_by_query(client=client, ticker=ticker, limit=limit))
        except Exception:
            return []

        deduped = self._dedupe_reviews(out)
        return deduped[: max(1, int(limit))]

    def _resolve_company_id(self, client: httpx.Client, ticker: str) -> Optional[str]:
        for query_param in ("query", "keyword", "q", "ticker", "symbol", "company"):
            payload = self._safe_get_json(client=client, path=self.company_search_path, params={query_param: ticker})
            company_id = self._extract_company_id(payload=payload, ticker=ticker)
            if company_id:
                return company_id
        return None

    def _configured_company_id(self, ticker: str) -> Optional[str]:
        return self.company_id_map.get(str(ticker or "").strip().upper())

    def _load_company_id_map(self) -> Dict[str, str]:
        merged: Dict[str, str] = {}

        env_map = self._parse_company_id_map_json(self._env("GLASSDOOR_COMPANY_ID_MAP"))
        merged.update(env_map)

        for path in self._candidate_company_id_map_paths():
            if not path.exists():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            file_map = self._normalize_company_id_map(payload)
            for k, v in file_map.items():
                merged.setdefault(k, v)

        return merged

    @staticmethod
    def _parse_company_id_map_json(raw: Optional[str]) -> Dict[str, str]:
        s = str(raw or "").strip()
        if not s:
            return {}
        try:
            payload = json.loads(s)
        except Exception:
            return {}
        return GlassdoorCultureCollector._normalize_company_id_map(payload)

    @staticmethod
    def _normalize_company_id_map(payload: Any) -> Dict[str, str]:
        if not isinstance(payload, dict):
            return {}
        out: Dict[str, str] = {}
        for k, v in payload.items():
            ticker = str(k or "").strip().upper()
            company_id = str(v or "").strip()
            if ticker and company_id:
                out[ticker] = company_id
        return out

    def _candidate_company_id_map_paths(self) -> List[Path]:
        paths: List[Path] = []
        if self.data_root is not None:
            paths.append(self.data_root / "glassdoor" / "company_ids.json")
        paths.append(Path("data") / "glassdoor" / "company_ids.json")
        paths.append(Path(__file__).resolve().parents[2] / "data" / "glassdoor" / "company_ids.json")
        return paths

    def _fetch_reviews_by_company_id(
        self,
        *,
        client: httpx.Client,
        company_id: str,
        ticker: str,
        limit: int,
    ) -> List[GlassdoorReview]:
        id_params = [self.reviews_company_id_param]
        if not self.disable_discovery_fallback:
            id_params.extend(("companyId", "employerId", "company_id", "employer_id", "id"))
        seen: set[str] = set()
        normalized_params: List[str] = []
        for p in id_params:
            key = str(p or "").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            normalized_params.append(key)

        for id_param in normalized_params:
            payload = self._safe_get_json(
                client=client,
                path=self.reviews_path,
                params={id_param: company_id, "limit": min(self.page_size, max(1, int(limit)))},
            )
            rows = self._parse_reviews_payload(payload=payload, ticker=ticker)
            if rows:
                return rows[: max(1, int(limit))]
        return []

    def _fetch_reviews_by_query(self, *, client: httpx.Client, ticker: str, limit: int) -> List[GlassdoorReview]:
        for query_param in ("query", "keyword", "q", "ticker", "symbol", "company"):
            payload = self._safe_get_json(
                client=client,
                path=self.reviews_path,
                params={query_param: ticker, "limit": min(self.page_size, max(1, int(limit)))},
            )
            rows = self._parse_reviews_payload(payload=payload, ticker=ticker)
            if rows:
                return rows[: max(1, int(limit))]
        return []

    @staticmethod
    def _safe_get_json(client: httpx.Client, path: str, params: Dict[str, Any]) -> Optional[Any]:
        try:
            resp = client.get(path, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    def _extract_company_id(self, payload: Any, ticker: str) -> Optional[str]:
        if payload is None:
            return None

        ticker_l = ticker.lower()
        candidates: List[tuple[int, str]] = []
        for row in self._iter_dicts(payload):
            company_id = self._first_present(row, ("companyId", "company_id", "employerId", "employer_id", "id"))
            if company_id in (None, ""):
                continue

            score = 0
            symbol = str(self._first_present(row, ("ticker", "tickerSymbol", "symbol")) or "").lower()
            name = str(self._first_present(row, ("name", "companyName", "employerName", "shortName")) or "").lower()
            if symbol == ticker_l:
                score += 3
            elif ticker_l and ticker_l in symbol:
                score += 2
            if ticker_l and ticker_l in name:
                score += 1
            candidates.append((score, str(company_id)))

        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    def _parse_reviews_payload(self, payload: Any, ticker: str) -> List[GlassdoorReview]:
        if payload is None:
            return []

        out: List[GlassdoorReview] = []
        for row in self._iter_dicts(payload):
            if not self._looks_like_review(row):
                continue
            parsed = self._parse_review_row(row=row, ticker=ticker)
            if parsed is not None:
                out.append(parsed)

        return out

    def _looks_like_review(self, row: Dict[str, Any]) -> bool:
        rating = self._normalize_rating(
            self._first_present(row, ("rating", "overallRating", "overall_rating", "ratingValue", "score"))
        )
        if rating is None:
            return False

        has_text = any(
            bool(str(self._first_present(row, keys) or "").strip())
            for keys in (
                ("title", "reviewTitle", "headline", "summary"),
                ("pros", "prosText", "advantages"),
                ("cons", "consText", "disadvantages"),
                ("adviceToManagement", "advice_to_management"),
            )
        )
        has_date = self._first_present(row, ("reviewDate", "review_date", "date", "createdAt", "created_at")) is not None
        return has_text or has_date

    def _parse_review_row(self, row: Dict[str, Any], ticker: str) -> Optional[GlassdoorReview]:
        rating = self._normalize_rating(
            self._first_present(row, ("rating", "overallRating", "overall_rating", "ratingValue", "score"))
        )
        if rating is None:
            return None

        title = str(self._first_present(row, ("title", "reviewTitle", "headline", "summary")) or "")
        pros = str(self._first_present(row, ("pros", "prosText", "advantages")) or "")
        cons = str(self._first_present(row, ("cons", "consText", "disadvantages")) or "")
        advice_raw = self._first_present(row, ("adviceToManagement", "advice_to_management"))
        advice = str(advice_raw).strip() if advice_raw is not None and str(advice_raw).strip() else None
        job_title = str(self._first_present(row, ("jobTitle", "job_title", "position", "role")) or "")

        dt = self._parse_datetime(
            self._first_present(row, ("reviewDate", "review_date", "date", "createdAt", "created_at", "timestamp"))
        ) or datetime.now(timezone.utc)

        is_current_employee = self._parse_current_employee(
            self._first_present(
                row,
                (
                    "isCurrentEmployee",
                    "is_current_employee",
                    "currentEmployee",
                    "employmentStatus",
                    "employeeStatus",
                ),
            )
        )

        review_id = str(
            self._first_present(row, ("reviewId", "review_id", "id", "uuid", "reviewUUID"))
            or self._synthetic_review_id(ticker=ticker, dt=dt, title=title, pros=pros, cons=cons)
        )

        return GlassdoorReview(
            review_id=review_id,
            rating=rating,
            title=title,
            pros=pros,
            cons=cons,
            advice_to_management=advice,
            is_current_employee=is_current_employee,
            job_title=job_title,
            review_date=dt,
        )

    @staticmethod
    def _first_present(row: Dict[str, Any], keys: Iterable[str]) -> Any:
        for key in keys:
            if key in row and row[key] is not None:
                return row[key]
        return None

    @classmethod
    def _iter_dicts(cls, node: Any) -> Iterable[Dict[str, Any]]:
        if isinstance(node, dict):
            yield node
            for v in node.values():
                yield from cls._iter_dicts(v)
        elif isinstance(node, list):
            for item in node:
                yield from cls._iter_dicts(item)

    @staticmethod
    def _normalize_rating(raw: Any) -> Optional[float]:
        value = GlassdoorCultureCollector._parse_float(raw)
        if value is None:
            return None
        if value > 5.0 and value <= 10.0:
            value = value / 2.0
        if value < 0:
            return None
        if value > 5.0:
            value = 5.0
        return float(round(value, 3))

    @staticmethod
    def _parse_float(raw: Any) -> Optional[float]:
        if raw is None:
            return None
        if isinstance(raw, (int, float)):
            return float(raw)
        s = str(raw).strip()
        if not s:
            return None
        if "/" in s:
            s = s.split("/", 1)[0].strip()
        s = s.replace(",", "")
        try:
            return float(s)
        except Exception:
            match = re.search(r"-?\d+(?:\.\d+)?", s)
            if match:
                try:
                    return float(match.group(0))
                except Exception:
                    return None
            return None

    @staticmethod
    def _parse_datetime(raw: Any) -> Optional[datetime]:
        if raw is None:
            return None
        if isinstance(raw, datetime):
            return raw if raw.tzinfo is not None else raw.replace(tzinfo=timezone.utc)
        if isinstance(raw, (int, float)):
            ts = float(raw)
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            try:
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                return None

        s = str(raw).strip()
        if not s:
            return None
        candidates = (s, s.replace("Z", "+00:00"), s.replace(" UTC", "+00:00"))
        for val in candidates:
            try:
                dt = datetime.fromisoformat(val)
                return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
        return None

    @staticmethod
    def _parse_current_employee(raw: Any) -> bool:
        if isinstance(raw, bool):
            return raw
        if raw is None:
            return False
        s = str(raw).strip().lower()
        if not s:
            return False
        if s in {"1", "true", "yes", "y"}:
            return True
        if "former" in s:
            return False
        if "current" in s:
            return True
        return False

    @staticmethod
    def _synthetic_review_id(ticker: str, dt: datetime, title: str, pros: str, cons: str) -> str:
        payload = f"{ticker}|{dt.isoformat()}|{title}|{pros}|{cons}"
        return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()[:24]

    @staticmethod
    def _dedupe_reviews(reviews: List[GlassdoorReview]) -> List[GlassdoorReview]:
        out: List[GlassdoorReview] = []
        seen: set[str] = set()
        for r in reviews:
            key = r.review_id or f"{r.review_date.isoformat()}|{r.title}|{r.pros}|{r.cons}"
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
        return out

    def _load_reviews_from_disk(self, ticker: str, limit: int) -> List[GlassdoorReview]:
        for path in self._candidate_disk_paths(ticker=ticker):
            if not path.exists():
                continue
            try:
                rows = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue

            if not isinstance(rows, list):
                continue

            out: List[GlassdoorReview] = []
            for row in rows[: max(1, int(limit))]:
                if not isinstance(row, dict):
                    continue
                parsed = self._parse_review_row(row=row, ticker=ticker)
                if parsed is not None:
                    out.append(parsed)
            if out:
                return out
        return []

    def _candidate_disk_paths(self, ticker: str) -> List[Path]:
        file_name = f"{ticker.lower()}.json"
        paths: List[Path] = []
        if self.data_root is not None:
            paths.append(self.data_root / "glassdoor" / file_name)
        paths.append(Path("data") / "glassdoor" / file_name)
        paths.append(Path(__file__).resolve().parents[2] / "data" / "glassdoor" / file_name)

        # Preserve order and avoid duplicates after path normalization.
        seen: set[str] = set()
        unique: List[Path] = []
        for p in paths:
            key = str(p.resolve()) if p.exists() else str(p)
            if key in seen:
                continue
            seen.add(key)
            unique.append(p)
        return unique

    def _write_reviews_cache(self, ticker: str, reviews: List[GlassdoorReview]) -> None:
        try:
            target = self._cache_path(ticker=ticker)
            target.parent.mkdir(parents=True, exist_ok=True)
            payload = [
                {
                    "review_id": r.review_id,
                    "rating": r.rating,
                    "title": r.title,
                    "pros": r.pros,
                    "cons": r.cons,
                    "advice_to_management": r.advice_to_management,
                    "is_current_employee": r.is_current_employee,
                    "job_title": r.job_title,
                    "review_date": r.review_date.isoformat(),
                }
                for r in reviews
            ]
            target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception:
            # Cache failures should never break scoring.
            pass

    def _cache_path(self, ticker: str) -> Path:
        if self.data_root is not None:
            return self.data_root / "glassdoor" / f"{ticker.lower()}.json"
        return Path(__file__).resolve().parents[2] / "data" / "glassdoor" / f"{ticker.lower()}.json"
