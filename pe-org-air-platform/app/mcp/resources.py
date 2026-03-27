from __future__ import annotations
 
import json

from app.bonus_facade import get_investment_summary, memory_stats
from app.services.value_creation import ORG_AIR_PARAMETERS_V2, SECTOR_DEFINITIONS
 
 
def list_resource_defs() -> list[dict]:
    return [
        {
            "uri": "orgair://parameters/v2.0",
            "name": "Org-AI-R Scoring Parameters v2.0",
            "description": "Current scoring parameters and weights",
        },
        {
            "uri": "orgair://sectors",
            "name": "Sector Definitions",
            "description": "Sector baselines and sector-specific reference values",
        },
        {
            "uri": "orgair://extensions/bonus",
            "name": "Bonus Deliverables Overview",
            "description": "Mem0 memory stats and investment tracker snapshot",
        },
    ]
 
 
def read_resource(uri: str) -> str:
    if uri == "orgair://parameters/v2.0":
        return json.dumps(ORG_AIR_PARAMETERS_V2, indent=2)
 
    if uri == "orgair://sectors":
        return json.dumps(SECTOR_DEFINITIONS, indent=2)

    if uri == "orgair://extensions/bonus":
        return json.dumps(
            {
                "memory": memory_stats(),
                "default_fund_tracker": get_investment_summary(fund_id="growth_fund_v"),
            },
            indent=2,
        )
 
    return "{}"
