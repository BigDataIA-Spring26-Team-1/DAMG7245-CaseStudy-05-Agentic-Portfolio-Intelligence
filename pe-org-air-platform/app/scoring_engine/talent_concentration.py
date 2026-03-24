from __future__ import annotations
 
from dataclasses import dataclass
from decimal import Decimal
import re
from typing import List, Set
 
 
@dataclass
class JobAnalysis:
    total_ai_jobs: int
    senior_ai_jobs: int
    mid_ai_jobs: int
    entry_ai_jobs: int
    unique_skills: Set[str]
 
 
class TalentConcentrationCalculator:
    @staticmethod
    def calculate_tc(
        job_analysis: JobAnalysis,
        glassdoor_individual_mentions: int = 0,
        glassdoor_review_count: int = 1,
    ) -> Decimal:
        if job_analysis.total_ai_jobs > 0:
            leadership_ratio = job_analysis.senior_ai_jobs / job_analysis.total_ai_jobs
        else:
            leadership_ratio = 0.5
 
        team_size_factor = min(1.0, 1.0 / (job_analysis.total_ai_jobs ** 0.5 + 0.1))
        skill_concentration = max(0.0, 1.0 - len(job_analysis.unique_skills) / 15.0)
 
        if glassdoor_review_count > 0:
            individual_factor = min(1.0, glassdoor_individual_mentions / glassdoor_review_count)
        else:
            individual_factor = 0.5
 
        tc = (
            0.4 * leadership_ratio
            + 0.3 * team_size_factor
            + 0.2 * skill_concentration
            + 0.1 * individual_factor
        )
        tc = max(0.0, min(1.0, tc))
        return Decimal(str(tc)).quantize(Decimal("0.0001"))
 
    @staticmethod
    def analyze_job_postings(postings: List[dict]) -> JobAnalysis:
        senior_keywords = ["principal", "staff", "director", "vp", "head", "chief"]
        mid_keywords = ["senior", "lead", "manager"]
        entry_keywords = ["junior", "associate", "entry", "intern"]
 
        skill_vocab = {
            "python", "sql", "pytorch", "tensorflow", "spark", "databricks",
            "aws", "azure", "gcp", "mlops", "kubernetes", "airflow", "dbt",
            "nlp", "llm", "computer vision", "statistics",
        }
 
        total = senior = mid = entry = 0
        unique_skills: Set[str] = set()
 
        for posting in postings:
            title = str(posting.get("title", "")).lower()
            description = str(posting.get("description", posting.get("content_text", ""))).lower()
            text = f"{title} {description}"
 
            if not any(x in text for x in ["ai", "ml", "machine learning", "data science", "llm"]):
                continue
 
            total += 1
            if any(kw in title for kw in senior_keywords):
                senior += 1
            elif any(kw in title for kw in mid_keywords):
                mid += 1
            elif any(kw in title for kw in entry_keywords):
                entry += 1
            else:
                mid += 1
 
            for skill in skill_vocab:
                if re.search(r"\b" + re.escape(skill) + r"\b", text):
                    unique_skills.add(skill)
 
        return JobAnalysis(
            total_ai_jobs=total,
            senior_ai_jobs=senior,
            mid_ai_jobs=mid,
            entry_ai_jobs=entry,
            unique_skills=unique_skills,
        )
 
 
def talent_risk_adjustment(tc: float) -> Decimal:
    """
    Talent risk adjustment from the CS3 formula:
      1 - 0.15 * max(0, TC - 0.25)
    """
    value = 1.0 - 0.15 * max(0.0, float(tc) - 0.25)
    value = max(0.0, min(1.0, value))
    return Decimal(str(value)).quantize(Decimal("0.0001"))
 
 