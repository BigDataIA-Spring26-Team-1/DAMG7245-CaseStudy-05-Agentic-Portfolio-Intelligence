from __future__ import annotations

import app.pipelines.board_analyzer as board_module
from app.pipelines.board_analyzer import BoardCompositionAnalyzer, BoardMember


def test_analyze_board_detects_all_key_signals():
    analyzer = BoardCompositionAnalyzer()
    members = [
        BoardMember(
            name="Alice Smith",
            title="Chief Data Officer",
            committees=["Technology Committee"],
            bio="Leads machine learning and digital transformation initiatives.",
            is_independent=True,
            tenure_years=5,
        ),
        BoardMember(
            name="Bob Lee",
            title="Independent Director",
            committees=["Risk Committee"],
            bio="Oversees enterprise risk governance.",
            is_independent=True,
            tenure_years=3,
        ),
    ]
    committees = ["Technology Committee", "Risk and Cyber Committee"]
    strategy_text = "The board AI strategy includes machine learning and automation oversight."

    sig = analyzer.analyze_board(
        company_id="cid-1",
        ticker="NVDA",
        members=members,
        committees=committees,
        strategy_text=strategy_text,
    )

    assert sig.has_tech_committee is True
    assert sig.has_ai_expertise is True
    assert sig.has_data_officer is True
    assert sig.has_risk_tech_oversight is True
    assert sig.has_ai_in_strategy is True
    assert sig.tech_expertise_count >= 1
    assert float(sig.independent_ratio) > 0.5
    assert 0.0 <= float(sig.governance_score) <= 100.0
    assert 0.0 <= float(sig.confidence) <= 0.95
    assert sig.relevant_committees


def test_analyze_board_handles_sparse_inputs():
    analyzer = BoardCompositionAnalyzer()
    members = [
        BoardMember(
            name="Chris Doe",
            title="Director",
            committees=[],
            bio="General operations leadership.",
            is_independent=False,
            tenure_years=1,
        )
    ]

    sig = analyzer.analyze_board(
        company_id="cid-2",
        ticker="DG",
        members=members,
        committees=[],
        strategy_text="Traditional strategy text without technology buzzwords.",
    )

    assert sig.has_tech_committee is False
    assert sig.has_ai_expertise is False
    assert sig.has_data_officer is False
    assert sig.has_risk_tech_oversight is False
    assert sig.has_ai_in_strategy is False
    assert sig.tech_expertise_count == 0
    assert float(sig.independent_ratio) == 0.0
    assert float(sig.governance_score) >= 20.0


def test_extract_from_proxy_fallback_without_bs4(monkeypatch):
    analyzer = BoardCompositionAnalyzer()
    monkeypatch.setattr(board_module, "BeautifulSoup", None)

    html = """
    <html><body>
      <h1>Board Committees</h1>
      <p>Technology Committee and Risk Committee</p>
      <p>Jane Miller John Carter</p>
    </body></html>
    """
    members, committees = analyzer.extract_from_proxy(html)

    assert any("Technology Committee" in c for c in committees)
    assert any("Risk Committee" in c for c in committees)
    assert len(members) >= 1
