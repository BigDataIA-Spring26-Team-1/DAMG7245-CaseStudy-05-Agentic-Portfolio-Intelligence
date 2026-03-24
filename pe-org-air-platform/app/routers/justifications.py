from __future__ import annotations
 
from typing import Optional
 
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
 
from app.services.justification.generator import JustificationGenerator
 
router = APIRouter(prefix="/api/v1/justify", tags=["justifications"])
 
_generator: Optional[JustificationGenerator] = None
 
 
def get_generator() -> JustificationGenerator:
    global _generator
    if _generator is None:
        _generator = JustificationGenerator()
    return _generator
 
 
class JustificationRequest(BaseModel):
    company_id: str = Field(..., description="Portfolio company identifier")
    dimension: str = Field(..., description="Dimension to justify, e.g. leadership, talent, digital")
    question: Optional[str] = Field(
        default=None,
        description="Optional user question to guide retrieval and explanation",
    )
    top_k: int = Field(default=5, ge=1, le=20, description="Number of evidence chunks to retrieve")
    min_confidence: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Optional minimum evidence confidence threshold",
    )
 
 
@router.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "message": "Justification endpoint ready",
    }
 
 
@router.post("/")
def justify(req: JustificationRequest) -> dict:
    """
    Delegates all business logic to JustificationGenerator.
 
    Router responsibilities only:
    - validate request
    - call service
    - return result
    - handle service errors cleanly
    """
    try:
        result = get_generator().generate(
            company_id=req.company_id,
            dimension=req.dimension,
            question=req.question,
            top_k=req.top_k,
            min_confidence=req.min_confidence,
        )
        return result
 
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate justification: {str(exc)}",
        ) from exc