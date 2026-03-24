from __future__ import annotations

import runpy
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = PROJECT_ROOT / "pe-org-air-platform"
TARGET = APP_ROOT / "exercises" / "complete_pipeline.py"

if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

runpy.run_path(str(TARGET), run_name="__main__")
