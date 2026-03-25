from __future__ import annotations
 
import json
 
 
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
    ]
 
 
def read_resource(uri: str) -> str:
    if uri == "orgair://parameters/v2.0":
        return json.dumps(
            {
                "version": "2.0",
                "alpha": 0.60,
                "beta": 0.12,
                "delta": 0.15,
            },
            indent=2,
        )
 
    if uri == "orgair://sectors":
        return json.dumps(
            {
                "technology": {"h_r_base": 85},
                "healthcare": {"h_r_base": 75},
                "financial_services": {"h_r_base": 78},
                "manufacturing": {"h_r_base": 68},
                "retail": {"h_r_base": 72},
                "energy": {"h_r_base": 60},
            },
            indent=2,
        )
 
    return "{}"