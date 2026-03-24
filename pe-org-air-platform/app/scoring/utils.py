from __future__ import annotations
from typing import Iterable

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))

def safe_div(num: float, den: float, default: float = 0.0) -> float:
    den = float(den)
    return float(default) if den == 0.0 else float(num) / den

def weighted_mean(values: Iterable[float], weights: Iterable[float]) -> float:
    v = list(values)
    w = list(weights)
    return safe_div(sum(x * y for x, y in zip(v, w)), sum(w), 0.0)

def weighted_std_dev(values: Iterable[float], weights: Iterable[float]) -> float:
    v = list(values)
    w = list(weights)
    mu = weighted_mean(v, w)
    var = safe_div(sum(wi * (xi - mu) ** 2 for xi, wi in zip(v, w)), sum(w), 0.0)
    return var ** 0.5

def coefficient_of_variation(values: Iterable[float], weights: Iterable[float]) -> float:
    v = list(values)
    w = list(weights)
    mu = weighted_mean(v, w)
    if abs(mu) < 1e-9:
        return 0.0
    return weighted_std_dev(v, w) / abs(mu)