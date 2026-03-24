import pytest
from datetime import date
 
from pydantic import ValidationError
 
from app.models.company import CompanyCreate, CompanyUpdate
from app.models.assessment import AssessmentCreate, AssessmentUpdate
from app.models.dimension import DimensionScoreCreate
from app.models.pagination import Page
 
COMPANY_ID = "550e8400-e29b-41d4-a716-446655440001"
INDUSTRY_ID = "550e8400-e29b-41d4-a716-446655440002"
ASSESSMENT_ID = "550e8400-e29b-41d4-a716-446655440003"
 
 
def test_company_ticker_uppercase_ok():
    m = CompanyCreate(name="Test", ticker="ABC", industry_id=INDUSTRY_ID)
    assert m.ticker == "ABC"
 
 
def test_company_ticker_rejects_lowercase_if_you_validate():
    with pytest.raises(ValidationError):
        CompanyCreate(name="Test", ticker="abc", industry_id=INDUSTRY_ID)
 
 
def test_company_update_allows_none_ticker():
    m = CompanyUpdate(name="Test Co", ticker=None)
    assert m.ticker is None
 
 
def test_company_update_rejects_lowercase_ticker():
    with pytest.raises(ValidationError):
        CompanyUpdate(name="Test Co", ticker="abc")
 
 
def test_company_position_factor_bounds():
    with pytest.raises(ValidationError):
        CompanyCreate(name="Test", ticker="ABC", industry_id=INDUSTRY_ID, position_factor=2.0)
 
 
def test_assessment_create_valid():
    m = AssessmentCreate(
        company_id=COMPANY_ID,
        assessment_type="screening",
        assessment_date=date.today(),
        primary_assessor="A",
        secondary_assessor="B",
    )
    assert m.assessment_type == "screening"
 
 
def test_assessment_invalid_enum():
    with pytest.raises(ValidationError):
        AssessmentCreate(
            company_id=COMPANY_ID,
            assessment_type="not-a-real-type",
            assessment_date=date.today(),
            primary_assessor="A",
            secondary_assessor="B",
        )
 
 
def test_assessment_update_rejects_invalid_status():
    with pytest.raises(ValidationError):
        AssessmentUpdate(status="nope")
 
 
def test_dimension_score_create_valid():
    m = DimensionScoreCreate(
        assessment_id=ASSESSMENT_ID,
        dimension="ai_governance",
        score=75,
        weight=0.5,
        confidence=0.9,
        evidence_count=2,
    )
    assert m.score == 75
 
 
def test_dimension_score_create_defaults():
    m = DimensionScoreCreate(
        assessment_id=ASSESSMENT_ID,
        dimension="ai_governance",
        score=80,
    )
    assert m.confidence == 0.8
    assert m.evidence_count == 0
    assert m.weight == 0.20
 
 
def test_dimension_score_create_rejects_score_over_100():
    with pytest.raises(ValidationError):
        DimensionScoreCreate(
            assessment_id=ASSESSMENT_ID,
            dimension="ai_governance",
            score=101,
            weight=0.5,
            confidence=0.9,
            evidence_count=2,
        )
 
 
def test_dimension_score_create_rejects_confidence_over_1():
    with pytest.raises(ValidationError):
        DimensionScoreCreate(
            assessment_id=ASSESSMENT_ID,
            dimension="ai_governance",
            score=80,
            weight=0.5,
            confidence=1.1,
            evidence_count=2,
        )
 
 
def test_page_model_shapes_items():
    page = Page[str](page=1, page_size=10, total=0, total_pages=1, items=["a", "b"])
    assert page.items == ["a", "b"]