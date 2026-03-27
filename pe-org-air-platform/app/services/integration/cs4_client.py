from __future__ import annotations

import asyncio
from typing import Any

from app.services.justification.generator import JustificationGenerator
from app.services.observability.metrics import track_cs_client


class CS4Client:
    """
    Thin facade over the CS4 justification path.
    This makes CS4 explicit in the CS5 integration layer.
    """

    def __init__(self) -> None:
        self.generator = JustificationGenerator()

    @track_cs_client("cs4", "generate_justification")
    async def generate_justification(self, company_id: str, dimension: str) -> dict[str, Any]:
        return await asyncio.to_thread(
            self.generator.generate,
            company_id=company_id,
            dimension=dimension,
        )
