from __future__ import annotations
 
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
import math
import random
 
import numpy as np
 
 
DIMENSIONS = [
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent_skills",
    "leadership_vision",
    "use_case_portfolio",
    "culture_change",
]
 
 
@dataclass(frozen=True)
class SEMResult:
    lower: float
    upper: float
    standard_error: float
    model_fit_index: Dict[str, float]
    method_used: str  # "sem_simplified" | "bootstrap"
 
 
def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))
 
 
def _pca_one_factor_loadings(X: np.ndarray) -> np.ndarray:
    """
    Returns normalized 1-factor loadings using the first principal component.
    X is shape (n_samples, n_features).
    """
    # standardize columns
    mu = X.mean(axis=0)
    sd = X.std(axis=0, ddof=1)
    sd = np.where(sd == 0, 1.0, sd)
    Z = (X - mu) / sd
 
    # covariance of standardized data == correlation matrix
    C = np.cov(Z, rowvar=False, ddof=1)
 
    # eigendecomposition
    vals, vecs = np.linalg.eigh(C)
    v1 = vecs[:, np.argmax(vals)]  # first principal component direction
 
    # Make sign consistent (optional)
    if v1.sum() < 0:
        v1 = -v1
 
    # Convert to non-negative weights (still defensible as "importance weights")
    w = np.abs(v1)
 
    # Normalize to sum to 1
    s = w.sum()
    if s == 0:
        # fallback uniform
        return np.ones(X.shape[1]) / X.shape[1]
    return w / s
 
 
def _ols_fit(x: np.ndarray, y: np.ndarray) -> Tuple[float, float, float, float]:
    """
    Fit y = a + b x using OLS.
    Returns (a, b, sigma, r2) where sigma is residual std (ddof=2).
    """
    n = len(x)
    x_mean = float(x.mean())
    y_mean = float(y.mean())
    Sxx = float(((x - x_mean) ** 2).sum())
    if Sxx <= 1e-12:
        raise ValueError("OLS ill-conditioned: Sxx too small")
 
    b = float(((x - x_mean) * (y - y_mean)).sum() / Sxx)
    a = y_mean - b * x_mean
 
    y_hat = a + b * x
    resid = y - y_hat
 
    # residual std
    dof = max(1, n - 2)
    sigma = float(math.sqrt(float((resid**2).sum()) / dof))
 
    # R^2
    SStot = float(((y - y_mean) ** 2).sum())
    SSres = float((resid**2).sum())
    r2 = 1.0 - (SSres / SStot) if SStot > 1e-12 else 0.0
 
    return a, b, sigma, r2
 
 
def _prediction_se(x: np.ndarray, x0: float, sigma: float) -> float:
    """
    Standard error of predicted mean at x0 in simple linear regression.
    """
    n = len(x)
    x_mean = float(x.mean())
    Sxx = float(((x - x_mean) ** 2).sum())
    if Sxx <= 1e-12:
        return float("inf")
 
    # SE of mean prediction (not full prediction interval)
    se = sigma * math.sqrt((1.0 / n) + ((x0 - x_mean) ** 2) / Sxx)
    return se
 
 
def _compute_eta(X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Compute latent readiness score eta for each row.
    Returns (eta, loadings)
    """
    loadings = _pca_one_factor_loadings(X)
    eta = X @ loadings
    return eta, loadings
 
 
def compute_sem_confidence_intervals(
    *,
    X: np.ndarray,
    y: np.ndarray,
    alpha: float = 0.05,
    bootstrap_samples: int = 400,
    seed: int = 42,
) -> Tuple[List[SEMResult], Dict[str, float]]:
    """
    X: (n, 7) matrix of dimension scores (0-100)
    y: (n,) composite scores (0-100)
 
    Returns:
      - list of SEMResult in row order
      - global fit dict
    """
    n = X.shape[0]
    if n < 5:
        # too few for any stable SEM-like estimate
        return _bootstrap_ci(X, y, bootstrap_samples=bootstrap_samples, seed=seed, alpha=alpha)
 
    # Attempt simplified SEM
    try:
        eta, loadings = _compute_eta(X)
        a, b, sigma, r2 = _ols_fit(eta, y)
 
        z = 1.96  # 95% approx; good enough for CS3
        results: List[SEMResult] = []
        for i in range(n):
            y_hat = a + b * eta[i]
            se = _prediction_se(eta, float(eta[i]), sigma)
            lo = float(y_hat - z * se)
            hi = float(y_hat + z * se)
            lo = _clamp(lo, 0.0, 100.0)
            hi = _clamp(hi, 0.0, 100.0)
 
            results.append(
                SEMResult(
                    lower=round(lo, 2),
                    upper=round(hi, 2),
                    standard_error=round(se, 4),
                    model_fit_index={
                        "r2": round(float(r2), 4),
                        "rmse": round(float(sigma), 4),
                    },
                    method_used="sem_simplified",
                )
            )
 
        fit = {
            "r2": round(float(r2), 4),
            "rmse": round(float(sigma), 4),
        }
        # include loadings as a diagnostic summary (optional)
        # but keep fit dict numeric only (easy to store)
        return results, fit
 
    except Exception:
        # fallback to bootstrap if SEM-like fitting fails
        return _bootstrap_ci(X, y, bootstrap_samples=bootstrap_samples, seed=seed, alpha=alpha)
 
 
def _bootstrap_ci(
    X: np.ndarray,
    y: np.ndarray,
    *,
    bootstrap_samples: int,
    seed: int,
    alpha: float,
) -> Tuple[List[SEMResult], Dict[str, float]]:
    """
    Bootstrap refitting of simplified SEM:
      resample companies with replacement, refit PCA loadings + OLS,
      collect predicted score distribution per company.
    """
    n = X.shape[0]
    rng = random.Random(seed)
 
    preds = [[] for _ in range(n)]
    sigmas = []
 
    for _ in range(bootstrap_samples):
        idx = [rng.randrange(n) for _ in range(n)]
        Xb = X[idx, :]
        yb = y[idx]
 
        try:
            eta_b, _ = _compute_eta(Xb)
            a, b, sigma, r2 = _ols_fit(eta_b, yb)
            sigmas.append(sigma)
 
            # predict for original companies using model fit on bootstrap sample
            eta0, _ = _compute_eta(X)  # compute eta on original X (consistent mapping)
            y_hat0 = a + b * eta0
            for i in range(n):
                preds[i].append(float(y_hat0[i]))
 
        except Exception:
            continue
 
    if not sigmas or any(len(p) == 0 for p in preds):
        # as a last resort, return degenerate CI around observed y
        results = [
            SEMResult(
                lower=round(_clamp(float(y[i] - 5.0), 0.0, 100.0), 2),
                upper=round(_clamp(float(y[i] + 5.0), 0.0, 100.0), 2),
                standard_error=5.0,
                model_fit_index={"rmse": 0.0},
                method_used="bootstrap",
            )
            for i in range(n)
        ]
        return results, {"rmse": 0.0}
 
    lo_q = 100.0 * (alpha / 2.0)
    hi_q = 100.0 * (1.0 - alpha / 2.0)
 
    results: List[SEMResult] = []
    for i in range(n):
        arr = np.array(preds[i], dtype=float)
        lo = float(np.percentile(arr, lo_q))
        hi = float(np.percentile(arr, hi_q))
        se = float(arr.std(ddof=1)) if len(arr) > 1 else 0.0
 
        lo = _clamp(lo, 0.0, 100.0)
        hi = _clamp(hi, 0.0, 100.0)
 
        results.append(
            SEMResult(
                lower=round(lo, 2),
                upper=round(hi, 2),
                standard_error=round(se, 4),
                model_fit_index={"rmse": round(float(np.mean(sigmas)), 4)},
                method_used="bootstrap",
            )
        )
 
    fit = {"rmse": round(float(np.mean(sigmas)), 4)}
    return results, fit
 
 
def _fetch_dimension_vector(cur, assessment_id: str) -> list[float]:
    cur.execute(
        """
        SELECT dimension, score
        FROM dimension_scores
        WHERE assessment_id = %s
        """,
        (assessment_id,),
    )
    rows = cur.fetchall() or []
    by_dim = {str(dim): float(score) for dim, score in rows}
    return [float(by_dim.get(dim, 0.0)) for dim in DIMENSIONS]
 
 
def _fetch_training_rows(cur, company_id: str, version: str, limit: int = 50) -> tuple[np.ndarray, np.ndarray]:
    """
    Build training rows from recent scored runs in the same model/version family.
    """
    cur.execute(
        """
        SELECT
          o.company_id,
          o.assessment_id,
          o.composite_score
        FROM org_air_scores o
        JOIN scoring_runs r
          ON o.scoring_run_id = r.id
        WHERE r.model_version LIKE %s
          AND o.composite_score IS NOT NULL
          AND o.company_id != %s
        ORDER BY o.scored_at DESC
        LIMIT %s
        """,
        (f"{version}%", company_id, limit),
    )
 
    X_rows: list[list[float]] = []
    y_vals: list[float] = []
 
    for _cid, assessment_id, composite_score in cur.fetchall() or []:
        if not assessment_id:
            continue
        vec = _fetch_dimension_vector(cur, str(assessment_id))
        X_rows.append(vec)
        y_vals.append(float(composite_score))
 
    if not X_rows:
        return np.empty((0, len(DIMENSIONS))), np.empty((0,))
 
    return np.array(X_rows, dtype=float), np.array(y_vals, dtype=float)
 
 
def compute_sem_confidence(
    cur,
    *,
    company_id: str,
    assessment_id: str,
    composite_score: float,
    version: str,
    bootstrap_samples: int = 400,
) -> dict:
    """
    Return SEM confidence details for one company score using a
    Spearman-Brown reliability adjustment:
      rho = (n * r) / (1 + (n - 1) * r)
      SEM = sigma * sqrt(1 - rho)
    """
    X_hist, y_hist = _fetch_training_rows(cur, company_id=company_id, version=version, limit=50)
    if X_hist.size == 0 or len(y_hist) < 3:
        lower = round(_clamp(float(composite_score) - 5.0, 0.0, 100.0), 2)
        upper = round(_clamp(float(composite_score) + 5.0, 0.0, 100.0), 2)
        return {
            "lower": lower,
            "upper": upper,
            "standard_error": 5.0,
            "method_used": "fallback_constant_band",
            "model_fit_index": {},
            "global_fit": {},
        }
 
    sigma = float(np.std(y_hist, ddof=1)) if len(y_hist) > 1 else 5.0
    sigma = max(1.0, sigma)
 
    corr = np.corrcoef(X_hist, rowvar=False)
    valid_corrs: list[float] = []
    if corr.ndim == 2:
        n_dim = corr.shape[0]
        for i in range(n_dim):
            for j in range(i + 1, n_dim):
                v = float(corr[i, j])
                if not math.isnan(v) and math.isfinite(v):
                    valid_corrs.append(v)
 
    avg_r = float(np.mean(valid_corrs)) if valid_corrs else 0.50
    avg_r = _clamp(avg_r, 0.0, 0.99)
 
    n_items = len(DIMENSIONS)
    rho = (n_items * avg_r) / (1.0 + (n_items - 1.0) * avg_r)
    rho = _clamp(rho, 0.0, 0.99)
 
    sem = sigma * math.sqrt(max(0.0, 1.0 - rho))
    sem = max(0.5, sem)
    z = 1.96
    lower = round(_clamp(float(composite_score) - z * sem, 0.0, 100.0), 2)
    upper = round(_clamp(float(composite_score) + z * sem, 0.0, 100.0), 2)
 
    return {
        "lower": lower,
        "upper": upper,
        "standard_error": round(float(sem), 4),
        "method_used": "spearman_brown_sem",
        "model_fit_index": {
            "avg_inter_item_correlation": round(avg_r, 4),
            "rho": round(rho, 4),
            "sigma": round(sigma, 4),
        },
        "global_fit": {
            "training_rows": int(len(y_hist)),
        },
    }
 
 