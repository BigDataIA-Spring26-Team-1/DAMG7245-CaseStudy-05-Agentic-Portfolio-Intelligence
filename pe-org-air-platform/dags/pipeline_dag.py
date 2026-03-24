from __future__ import annotations

import json
import importlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict

try:
    pendulum = importlib.import_module("pendulum")
except ImportError:
    pendulum = None

try:
    airflow_decorators = importlib.import_module("airflow.decorators")
    airflow_python_operators = importlib.import_module("airflow.operators.python")
    dag: Callable[..., Any] = airflow_decorators.dag
    task: Callable[..., Any] = airflow_decorators.task
    get_current_context: Callable[[], Dict[str, Any]] = (
        airflow_python_operators.get_current_context
    )

    AIRFLOW_AVAILABLE = True
except ImportError:
    dag = None
    task = None
    AIRFLOW_AVAILABLE = False

    def get_current_context() -> Dict[str, Any]:
        raise RuntimeError("Airflow is required to execute tasks in pipeline_dag.py")


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

ARTIFACT_DIR = REPO_ROOT / "artifacts" / "airflow_runs"
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)


def _write_json(name: str, payload: Dict[str, Any]) -> str:
    path = ARTIFACT_DIR / f"{name}.json"
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return str(path)


if AIRFLOW_AVAILABLE:
    start_date = (
        pendulum.datetime(2026, 3, 1, tz="UTC")
        if pendulum is not None
        else datetime(2026, 3, 1, tzinfo=timezone.utc)
    )

    @dag(
        dag_id="cs4_pipeline_dag",
        description="CS4 Airflow DAG for score context, justification, IC prep, and analyst notes",
        start_date=start_date,
        schedule=None,
        catchup=False,
        tags=["cs4", "rag", "org-air", "airflow"],
        params={
            "company_id": "f7a9f167-c3e7-4adc-899b-3c81fc4110ae",
            "dimension": "leadership",
            "top_k": 5,
            "min_confidence": None,
        },
    )
    def cs4_pipeline_dag():
        @task
        def load_company() -> Dict[str, Any]:
            from app.services.integration.company_client import CompanyClient

            ctx = get_current_context()
            company_id = ctx["params"]["company_id"]

            client = CompanyClient()
            payload = client.get_company(company_id)
            _write_json("company_profile", payload)
            return payload

        @task
        def load_scores() -> Dict[str, Any]:
            from app.services.integration.scoring_client import ScoringClient

            ctx = get_current_context()
            company_id = ctx["params"]["company_id"]

            client = ScoringClient()
            payload = client.get_latest_scores(company_id)
            _write_json("latest_scores", payload)
            return payload

        @task
        def generate_justification() -> Dict[str, Any]:
            from app.services.justification.generator import JustificationGenerator

            ctx = get_current_context()
            company_id = ctx["params"]["company_id"]
            dimension = str(ctx["params"]["dimension"]).strip().lower().replace(" ", "_")
            top_k = int(ctx["params"]["top_k"])
            min_confidence = ctx["params"]["min_confidence"]

            generator = JustificationGenerator()
            payload = generator.generate(
                company_id=company_id,
                dimension=dimension,
                top_k=top_k,
                min_confidence=min_confidence,
            )
            _write_json("justification", payload)
            return payload

        @task
        def build_ic_prep() -> Dict[str, Any]:
            from app.services.workflows.ic_prep import ICPrepWorkflow

            ctx = get_current_context()
            company_id = ctx["params"]["company_id"]
            top_k = int(ctx["params"]["top_k"])
            min_confidence = ctx["params"]["min_confidence"]

            workflow = ICPrepWorkflow()
            payload = workflow.build_packet(
                company_id=company_id,
                top_k=top_k,
                min_confidence=min_confidence,
            )
            _write_json("ic_prep", payload)
            return payload

        @task
        def build_analyst_note() -> Dict[str, Any]:
            from app.services.workflows.analyst_notes import AnalystNotesCollector

            ctx = get_current_context()
            company_id = ctx["params"]["company_id"]
            dimension = str(ctx["params"]["dimension"]).strip().lower().replace(" ", "_")
            top_k = int(ctx["params"]["top_k"])
            min_confidence = ctx["params"]["min_confidence"]

            collector = AnalystNotesCollector()
            payload = collector.collect_note(
                company_id=company_id,
                dimension=dimension,
                top_k=top_k,
                min_confidence=min_confidence,
            )
            _write_json("analyst_note", payload)
            return payload

        company = load_company()
        scores = load_scores()
        justification = generate_justification()
        ic_prep = build_ic_prep()
        analyst_note = build_analyst_note()

        company >> scores >> justification
        company >> ic_prep
        company >> analyst_note

    dag_instance = cs4_pipeline_dag()
else:
    dag_instance = None
