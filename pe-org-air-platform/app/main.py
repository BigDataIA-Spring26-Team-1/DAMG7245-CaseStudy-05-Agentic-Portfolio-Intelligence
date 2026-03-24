from __future__ import annotations

from fastapi import FastAPI

from app.config import settings
from app.routers.health import router as health_router
from app.routers.companies import router as companies_router
from app.routers.assessments import router as assessments_router

# CS2 routers
from app.routers.documents import router as documents_router
from app.routers.signals import router as signals_router
from app.routers.evidence import router as evidence_router
from app.routers.collection import router as collection_router
from app.routers.signal_summaries import router as signal_summaries_router
from app.routers import chunk

# CS3 router
from app.routers.scoring import router as scoring_router

from app.routers.search import router as search_router
from app.routers.justifications import router as justifications_router

app = FastAPI(title=settings.app_name)

# Health (usually no prefix)
app.include_router(health_router)

# CS1 endpoints
app.include_router(companies_router, prefix=settings.api_prefix, tags=["companies"])
app.include_router(assessments_router, prefix=settings.api_prefix, tags=["assessments"])

# CS2 endpoints
app.include_router(documents_router, prefix=settings.api_prefix, tags=["documents"])
app.include_router(signals_router, prefix=settings.api_prefix, tags=["signals"])
app.include_router(evidence_router, prefix=settings.api_prefix, tags=["evidence"])
app.include_router(collection_router, prefix=settings.api_prefix, tags=["collection"])
app.include_router(signal_summaries_router, prefix=settings.api_prefix, tags=["signal-summaries"])
app.include_router(chunk.router, prefix=settings.api_prefix, tags=["chunks"])

# CS3 scoring endpoints
app.include_router(scoring_router, tags=["scoring"])

app.include_router(search_router, tags=["search"])
app.include_router(justifications_router, tags=["justifications"])
