from __future__ import annotations

import importlib
import os
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List

try:
    pendulum = importlib.import_module("pendulum")
except ImportError:
    pendulum = None

try:
    airflow_decorators = importlib.import_module("airflow.decorators")
    airflow_exceptions = importlib.import_module("airflow.exceptions")
    airflow_python_operators = importlib.import_module("airflow.operators.python")
    dag: Callable[..., Any] = airflow_decorators.dag
    task: Callable[..., Any] = airflow_decorators.task
    AirflowSkipException = airflow_exceptions.AirflowSkipException
    get_current_context: Callable[[], Dict[str, Any]] = (
        airflow_python_operators.get_current_context
    )
    AIRFLOW_AVAILABLE = True
except ImportError:
    dag = None
    task = None
    AIRFLOW_AVAILABLE = False

    class AirflowSkipException(RuntimeError):
        pass

    def get_current_context() -> Dict[str, Any]:
        raise RuntimeError("Airflow is required to execute tasks in full_platform_pipeline.py")


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"


def _run_python_script(script_name: str, args: List[str] | None = None) -> None:
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Pipeline script not found: {script_path}")

    cmd = [sys.executable, str(script_path)]
    if args:
        cmd.extend(args)

    env = os.environ.copy()
    py_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{REPO_ROOT}{os.pathsep}{py_path}" if py_path else str(REPO_ROOT)
    )

    rendered_cmd = " ".join(shlex.quote(part) for part in cmd)
    print(f"[airflow] running: {rendered_cmd}")
    subprocess.run(cmd, cwd=str(REPO_ROOT), env=env, check=True)


def _param_bool(context: Dict[str, Any], key: str, default: bool = True) -> bool:
    raw = context.get("params", {}).get(key, default)
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y"}


def _param_string(context: Dict[str, Any], key: str, default: str = "") -> str:
    raw = context.get("params", {}).get(key, default)
    if raw is None:
        return default
    return str(raw).strip()


if AIRFLOW_AVAILABLE:
    start_date = (
        pendulum.datetime(2026, 3, 1, tz="UTC")
        if pendulum is not None
        else datetime(2026, 3, 1, tzinfo=timezone.utc)
    )

    @dag(
        dag_id="org_air_full_platform_pipeline",
        description=(
            "End-to-end Airflow integration for CS1/CS2/CS3 pipelines and CS4 evidence indexing"
        ),
        start_date=start_date,
        schedule=None,
        catchup=False,
        tags=["org-air", "cs1", "cs2", "cs3", "cs4", "pipeline", "airflow"],
        params={
            "companies": "all",
            "company_id": "f7a9f167-c3e7-4adc-899b-3c81fc4110ae",
            "tickers": "",
            "scoring_version": "v1.0",
            "model_version": "cs3-scoring-v2",
            "as_of_date": "today",
            "chunk_limit": 200,
            "index_batch_size": 500,
            "min_confidence": None,
            "reindex": True,
            "score_batch": True,
            "run_backfill_companies": True,
            "run_collect_evidence": True,
            "run_collect_signals": True,
            "run_compute_signal_scores": True,
            "run_compute_company_signal_summaries": True,
            "run_compute_dimension_scores": True,
            "run_scoring_engine": True,
            "run_index_evidence": True,
        },
    )
    def org_air_full_platform_pipeline():
        @task(trigger_rule="none_failed")
        def backfill_companies() -> None:
            ctx = get_current_context()
            if not _param_bool(ctx, "run_backfill_companies", True):
                raise AirflowSkipException("Skipping backfill_companies.py")

            companies = _param_string(ctx, "companies", "all")
            args: List[str] = []
            # backfill_companies.py treats omitted --companies as "seed defaults".
            if companies and companies.lower() != "all":
                args.extend(["--companies", companies])
            _run_python_script("backfill_companies.py", args)

        @task(trigger_rule="none_failed")
        def collect_evidence() -> None:
            ctx = get_current_context()
            if not _param_bool(ctx, "run_collect_evidence", True):
                raise AirflowSkipException("Skipping collect_evidence.py")

            companies = _param_string(ctx, "companies", "all")
            _run_python_script("collect_evidence.py", ["--companies", companies])

        @task(trigger_rule="none_failed")
        def collect_signals() -> None:
            ctx = get_current_context()
            if not _param_bool(ctx, "run_collect_signals", True):
                raise AirflowSkipException("Skipping collect_signals.py")

            companies = _param_string(ctx, "companies", "all")
            _run_python_script("collect_signals.py", ["--companies", companies])

        @task(trigger_rule="none_failed")
        def compute_signal_scores() -> None:
            ctx = get_current_context()
            if not _param_bool(ctx, "run_compute_signal_scores", True):
                raise AirflowSkipException("Skipping compute_signal_scores.py")
            _run_python_script("compute_signal_scores.py")

        @task(trigger_rule="none_failed")
        def compute_company_signal_summaries() -> None:
            ctx = get_current_context()
            if not _param_bool(ctx, "run_compute_company_signal_summaries", True):
                raise AirflowSkipException("Skipping compute_company_signal_summaries.py")

            as_of_date = _param_string(ctx, "as_of_date", "today")
            if not as_of_date or as_of_date.lower() == "today":
                as_of_date = datetime.now(timezone.utc).date().isoformat()
            _run_python_script("compute_company_signal_summaries.py", ["--as_of_date", as_of_date])

        @task(trigger_rule="none_failed")
        def compute_dimension_scores() -> None:
            ctx = get_current_context()
            if not _param_bool(ctx, "run_compute_dimension_scores", True):
                raise AirflowSkipException("Skipping compute_dimension_scores.py")

            company_id = _param_string(ctx, "company_id")
            if not company_id:
                raise ValueError("params.company_id is required for compute_dimension_scores.py")

            chunk_limit = int(ctx["params"].get("chunk_limit", 200))
            _run_python_script(
                "compute_dimension_scores.py",
                ["--company-id", company_id, "--chunk-limit", str(chunk_limit)],
            )

        @task(trigger_rule="none_failed")
        def run_scoring_engine() -> None:
            ctx = get_current_context()
            if not _param_bool(ctx, "run_scoring_engine", True):
                raise AirflowSkipException("Skipping run_scoring_engine.py")

            scoring_version = _param_string(ctx, "scoring_version", "v1.0")
            model_version = _param_string(ctx, "model_version", "cs3-scoring-v2")
            score_batch = _param_bool(ctx, "score_batch", True)
            tickers = _param_string(ctx, "tickers", "")

            if score_batch:
                args = ["--batch", "--version", scoring_version, "--model-version", model_version]
                if tickers:
                    args.extend(["--tickers", tickers])
                _run_python_script("run_scoring_engine.py", args)
                return

            company_id = _param_string(ctx, "company_id")
            if not company_id:
                raise ValueError("params.company_id is required when params.score_batch is false")
            _run_python_script(
                "run_scoring_engine.py",
                [
                    "--company-id",
                    company_id,
                    "--version",
                    scoring_version,
                    "--model-version",
                    model_version,
                ],
            )

        @task(trigger_rule="none_failed")
        def index_evidence() -> None:
            ctx = get_current_context()
            if not _param_bool(ctx, "run_index_evidence", True):
                raise AirflowSkipException("Skipping index_evidence.py")

            company_id = _param_string(ctx, "company_id")
            if not company_id:
                raise ValueError("params.company_id is required for index_evidence.py")

            batch_size = int(ctx["params"].get("index_batch_size", 500))
            min_conf = ctx["params"].get("min_confidence")
            reindex = _param_bool(ctx, "reindex", True)

            args = ["--company-id", company_id, "--batch-size", str(batch_size)]
            if min_conf not in (None, "", "null", "None"):
                args.extend(["--min-confidence", str(min_conf)])
            if reindex:
                args.append("--reindex")
            _run_python_script("index_evidence.py", args)

        t_backfill = backfill_companies()
        t_collect_evidence = collect_evidence()
        t_collect_signals = collect_signals()
        t_signal_scores = compute_signal_scores()
        t_company_summaries = compute_company_signal_summaries()
        t_dimension_scores = compute_dimension_scores()
        t_scoring = run_scoring_engine()
        t_index = index_evidence()

        (
            t_backfill
            >> t_collect_evidence
            >> t_collect_signals
            >> t_signal_scores
            >> t_company_summaries
            >> t_dimension_scores
            >> t_scoring
            >> t_index
        )

    dag_instance = org_air_full_platform_pipeline()
else:
    dag_instance = None
