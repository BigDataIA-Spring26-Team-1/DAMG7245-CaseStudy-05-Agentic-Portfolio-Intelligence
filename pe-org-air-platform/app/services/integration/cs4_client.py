from __future__ import annotations

from typing import Any

from app.services.justification.generator import JustificationGenerator


class CS4Client:
    """
    Thin facade over the CS4 justification path.
    This makes CS4 explicit in the CS5 integration layer.
    """

    def __init__(self) -> None:
        self.generator = JustificationGenerator()

    async def generate_justification(self, company_id: str, dimension: str) -> dict[str, Any]:
        return await self.generator.generate(company_id=company_id, dimension=dimension)