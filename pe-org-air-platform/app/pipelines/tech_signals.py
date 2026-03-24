from __future__ import annotations
 
from collections import Counter
from dataclasses import dataclass
from typing import Dict
 
from app.pipelines.external_signals import TechStackCollector, score_tech_stack
 
 
@dataclass(frozen=True)
class TechSignalSummary:
    keyword_counts: Dict[str, int]
    unique_keywords: int
    cloud_ml_count: int
    ml_framework_count: int
    data_platform_count: int
    ai_api_count: int
    score: float
 
 
def extract_tech_counts(text: str) -> Dict[str, int]:
    collector = TechStackCollector()
    return collector.extract(text or "")
 
 
def summarize_tech_signals(counts: Dict[str, int]) -> TechSignalSummary:
    collector = TechStackCollector()
    tech_map = collector.AI_TECHNOLOGIES
 
    categories = Counter()
    for kw, cnt in counts.items():
        cat = tech_map.get(kw)
        if cat:
            categories[cat] += int(cnt)
 
    return TechSignalSummary(
        keyword_counts=dict(counts),
        unique_keywords=len([k for k, v in counts.items() if v > 0]),
        cloud_ml_count=int(categories.get("cloud_ml", 0)),
        ml_framework_count=int(categories.get("ml_framework", 0)),
        data_platform_count=int(categories.get("data_platform", 0)),
        ai_api_count=int(categories.get("ai_api", 0)),
        score=round(score_tech_stack(counts), 2),
    )
 
 
def score_digital_presence_technology(text: str) -> float:
    counts = extract_tech_counts(text)
    return summarize_tech_signals(counts).score
 
 