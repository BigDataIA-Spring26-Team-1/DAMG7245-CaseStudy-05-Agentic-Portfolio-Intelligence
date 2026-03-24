from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scoring_engine.rubric_scorer import score_dimension_features
from app.scoring_engine.evidence_mapper import DIMENSIONS, DimensionFeature
 
def test_score_dimension_features_returns_7():
    feats = {d: DimensionFeature(d, 10.0, 50.0, 0.8, ["k"]) for d in DIMENSIONS}
    out = score_dimension_features(feats)
    assert len(out) == 7
    assert sorted([r.dimension for r in out]) == sorted(DIMENSIONS)
