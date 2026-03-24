from __future__ import annotations
 
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Tuple
 
 
@dataclass(frozen=True)
class HRResult:
    hr_factor: float                 # 0.0–2.0
    sector_name: str
    baseline_value: float            # 0–100 baseline (from config)
    jobs_signal_count: int           # jobs signals used
    method: str                      # "jobs_proxy" or "neutral"
    window_days: int
 
 
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))
 
 
def _get_sector_hr_baseline(cur, sector_name: str, version: str = "v1.0") -> float:
    """
    Pull sector HR baseline value from sector_baselines.
    We store hr_baseline_value redundantly per dimension; we just take MAX/AVG.
    """
    cur.execute(
        """
        SELECT AVG(hr_baseline_value)
        FROM sector_baselines
        WHERE sector_name = %s AND version = %s
        """,
        (sector_name, version),
    )
    row = cur.fetchone()
    if row and row[0] is not None:
        return float(row[0])
 
    # Fallback: industries.hr_base (since industries table also has hr_base)
    cur.execute(
        """
        SELECT AVG(hr_base)
        FROM industries
        WHERE sector = %s
        """,
        (sector_name,),
    )
    row2 = cur.fetchone()
    if row2 and row2[0] is not None:
        return float(row2[0])
 
    # Final fallback if baseline missing
    return 75.0
 
 
def _count_jobs_signals(cur, company_id: str, window_days: int = 365) -> int:
    """
    Counts job-related signals for a company within a time window.
    Uses external_signals.signal_type='jobs' as the proxy for AI talent demand.
    """
    # We use collected_at since published_at can be null for some sources.
    cur.execute(
        """
        SELECT COUNT(*)
        FROM external_signals
        WHERE company_id = %s
          AND signal_type = 'jobs'
          AND collected_at >= DATEADD(day, -%s, CURRENT_TIMESTAMP())
        """,
        (company_id, window_days),
    )
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0
 
 
def _portfolio_avg_jobs_signals(cur, window_days: int = 365) -> float:
    """
    Computes average jobs signal count across all companies in the portfolio.
    This normalizes job signals into a relative intensity measure.
    """
    cur.execute(
        """
        WITH company_counts AS (
          SELECT company_id, COUNT(*) AS jobs_count
          FROM external_signals
          WHERE signal_type = 'jobs'
            AND collected_at >= DATEADD(day, -%s, CURRENT_TIMESTAMP())
          GROUP BY company_id
        )
        SELECT AVG(jobs_count)
        FROM company_counts
        """,
        (window_days,),
    )
    row = cur.fetchone()
    if row and row[0] is not None:
        return float(row[0])
    # If there are zero job signals across portfolio, return 0 so we can fall back to neutral
    return 0.0
 
 
def compute_hr_factor(
    cur,
    *,
    company_id: str,
    sector_name: str,
    version: str = "v1.0",
    window_days: int = 365,
    min_jobs_for_non_neutral: int = 3,
) -> HRResult:
    """
    Computes HR adjustment factor (0.0–2.0) used to adjust talent_skills dimension.
 
    Practical proxy approach (CS3-ready):
    - Use number of job signals (external_signals.signal_type='jobs') as a proxy
      for AI talent demand/supply signals.
    - Compare company jobs signal intensity to portfolio average.
    - Use sector HR baseline as a stabilizer (higher baseline -> more expected).
 
    Rules:
    - If company has no/low jobs signals, return neutral 1.0 (do not penalize).
    - Bound multiplier to [0.0, 2.0].
    """
    baseline = _get_sector_hr_baseline(cur, sector_name, version=version)
    company_jobs = _count_jobs_signals(cur, company_id, window_days=window_days)
 
    if company_jobs < min_jobs_for_non_neutral:
        return HRResult(
            hr_factor=1.0,
            sector_name=sector_name,
            baseline_value=baseline,
            jobs_signal_count=company_jobs,
            method="neutral",
            window_days=window_days,
        )
 
    portfolio_avg = _portfolio_avg_jobs_signals(cur, window_days=window_days)
 
    # If portfolio average is unavailable (0), avoid divide-by-zero and return neutral.
    if portfolio_avg <= 0.0:
        return HRResult(
            hr_factor=1.0,
            sector_name=sector_name,
            baseline_value=baseline,
            jobs_signal_count=company_jobs,
            method="neutral",
            window_days=window_days,
        )
 
    # Relative intensity (e.g., 1.2 means 20% above portfolio avg jobs signals)
    intensity = company_jobs / portfolio_avg
 
    # Baseline normalization: sectors with higher hr baseline expect more talent,
    # so the same intensity should map to slightly smaller boosts.
    baseline_norm = baseline / 75.0  # 75 is a neutral anchor in your seed data range
    baseline_norm = clamp(baseline_norm, 0.7, 1.3)
 
    # HR factor formula (simple + explainable):
    # - starts at 1.0
    # - adds/subtracts based on intensity
    # - stabilized by baseline_norm
    #
    # Example:
    # intensity=1.5, baseline_norm=1.0 => factor ~ 1.25
    # intensity=0.8, baseline_norm=1.0 => factor ~ 0.90
    raw_factor = 1.0 + 0.5 * (intensity - 1.0) / baseline_norm
 
    hr_factor = clamp(raw_factor, 0.0, 2.0)
 
    return HRResult(
        hr_factor=hr_factor,
        sector_name=sector_name,
        baseline_value=baseline,
        jobs_signal_count=company_jobs,
        method="jobs_proxy",
        window_days=window_days,
    )
 
 
def apply_hr_adjustment_to_talent(
    *,
    dimension: str,
    raw_score: float,
    hr_factor: float,
) -> float:
    """
    Applies HR factor only to the talent_skills dimension, as per requirement A2.4.
    """
    if dimension != "talent_skills":
        return raw_score
    return clamp(raw_score * hr_factor, 0.0, 100.0)