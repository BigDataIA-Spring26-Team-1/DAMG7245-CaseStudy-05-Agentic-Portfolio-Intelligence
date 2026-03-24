from __future__ import annotations
import math

def compute_sem(std_dev: float, n: int) -> float:
    n = int(n)
    if n <= 0:
        return 0.0
    return float(std_dev) / math.sqrt(n)

def confidence_interval(mean: float, sem: float, z: float = 1.96) -> tuple[float, float]:
    return (float(mean) - z * float(sem), float(mean) + z * float(sem))