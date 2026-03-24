from __future__ import annotations

from decimal import Decimal
from typing import Dict


class PositionFactorCalculator:
    SECTOR_AVG_VR: Dict[str, float] = {
        "technology": 65.0,
        "financial_services": 55.0,
        "healthcare": 52.0,
        "business_services": 50.0,
        "retail": 48.0,
        "manufacturing": 45.0,
    }

    @staticmethod
    def calculate_position_factor(vr_score: float, sector: str, market_cap_percentile: float) -> Decimal:
        sector_avg = PositionFactorCalculator.SECTOR_AVG_VR.get((sector or "").lower(), 50.0)

        vr_diff = vr_score - sector_avg
        vr_component = max(-1.0, min(1.0, vr_diff / 50.0))

        mcap_component = (market_cap_percentile - 0.5) * 2.0
        mcap_component = max(-1.0, min(1.0, mcap_component))

        pf = 0.6 * vr_component + 0.4 * mcap_component
        pf = max(-1.0, min(1.0, pf))
        return Decimal(str(round(pf, 4)))
