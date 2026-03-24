from __future__ import annotations

from app.scoring_engine.talent_concentration import (
    JobAnalysis,
    TalentConcentrationCalculator,
    talent_risk_adjustment,
)


def test_analyze_job_postings_classifies_roles_and_skills():
    postings = [
        {
            "title": "Principal ML Engineer",
            "description": "AI platform with Python, AWS, MLOps and Kubernetes",
        },
        {
            "title": "Senior Data Scientist",
            "description": "Machine learning, NLP and statistics",
        },
        {
            "title": "Junior AI Analyst",
            "description": "Entry role for data science and SQL",
        },
        {
            "title": "Finance Manager",
            "description": "Corporate accounting role",
        },
    ]

    out = TalentConcentrationCalculator.analyze_job_postings(postings)
    assert out.total_ai_jobs == 3
    assert out.senior_ai_jobs == 1
    assert out.mid_ai_jobs == 1
    assert out.entry_ai_jobs == 1
    assert "python" in out.unique_skills
    assert "sql" in out.unique_skills


def test_calculate_tc_handles_zero_jobs_and_reviews():
    analysis = JobAnalysis(
        total_ai_jobs=0,
        senior_ai_jobs=0,
        mid_ai_jobs=0,
        entry_ai_jobs=0,
        unique_skills=set(),
    )
    tc = TalentConcentrationCalculator.calculate_tc(
        analysis,
        glassdoor_individual_mentions=0,
        glassdoor_review_count=0,
    )
    assert 0.0 <= float(tc) <= 1.0


def test_calculate_tc_clamps_to_unit_interval():
    analysis = JobAnalysis(
        total_ai_jobs=1,
        senior_ai_jobs=10,
        mid_ai_jobs=0,
        entry_ai_jobs=0,
        unique_skills=set(),
    )
    tc = TalentConcentrationCalculator.calculate_tc(
        analysis,
        glassdoor_individual_mentions=100,
        glassdoor_review_count=1,
    )
    assert 0.0 <= float(tc) <= 1.0


def test_talent_risk_adjustment_formula_behavior():
    low = float(talent_risk_adjustment(0.10))
    high = float(talent_risk_adjustment(0.90))
    assert 0.0 <= high <= low <= 1.0
