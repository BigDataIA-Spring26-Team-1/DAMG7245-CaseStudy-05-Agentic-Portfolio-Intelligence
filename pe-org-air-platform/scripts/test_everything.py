from __future__ import annotations

import argparse
import json
import os
import py_compile
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent


@dataclass
class CheckResult:
    name: str
    status: str
    detail: str
    return_code: int = 0


def _run_command(
    name: str,
    command: List[str],
    cwd: Path,
    env: Optional[Dict[str, str]] = None,
    allowed_return_codes: Optional[List[int]] = None,
) -> CheckResult:
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
    )
    allowed = set(allowed_return_codes or [0])
    combined = "\n".join(part for part in [completed.stdout.strip(), completed.stderr.strip()] if part).strip()
    detail = combined or "command completed with no output"
    status = "PASS" if completed.returncode in allowed else "FAIL"
    return CheckResult(
        name=name,
        status=status,
        detail=detail,
        return_code=completed.returncode,
    )


def _compile_targets() -> CheckResult:
    targets = [
        ROOT / "app" / "main.py",
        ROOT / "scripts" / "demo_justify.py",
        ROOT / "scripts" / "index_evidence.py",
        ROOT / "scripts" / "test_everything.py",
        ROOT / "exercises" / "agentic_due_diligence.py",
        ROOT / "exercises" / "complete_pipeline.py",
    ]

    for target in targets:
        py_compile.compile(str(target), doraise=True)

    return CheckResult(
        name="syntax",
        status="PASS",
        detail=f"compiled {len(targets)} entrypoints successfully",
    )


def _openapi_smoke() -> CheckResult:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app)
    response = client.get("/openapi.json")
    if response.status_code != 200:
        return CheckResult(
            name="openapi_smoke",
            status="FAIL",
            detail=f"/openapi.json returned status {response.status_code}",
            return_code=response.status_code,
        )

    schema = response.json()
    paths = schema.get("paths", {})
    required = [
        "/api/v1/search",
        "/api/v1/justify/",
        "/api/v1/companies",
        "/api/v1/assessments",
    ]
    missing = [path for path in required if path not in paths]
    if missing:
        return CheckResult(
            name="openapi_smoke",
            status="FAIL",
            detail=f"missing required paths: {', '.join(missing)}",
            return_code=1,
        )

    return CheckResult(
        name="openapi_smoke",
        status="PASS",
        detail=f"OpenAPI schema loaded with {len(paths)} paths",
    )


def _exercise_help_smoke() -> CheckResult:
    return _run_command(
        name="exercise_help",
        command=[sys.executable, str(ROOT / "exercises" / "complete_pipeline.py"), "--help"],
        cwd=ROOT,
    )


def _agentic_exercise_help_smoke() -> CheckResult:
    return _run_command(
        name="agentic_exercise_help",
        command=[sys.executable, str(ROOT / "exercises" / "agentic_due_diligence.py"), "--help"],
        cwd=ROOT,
    )


def _pytest_suite(pytest_args: List[str]) -> CheckResult:
    command = [sys.executable, "-m", "pytest"]
    command.extend(pytest_args)
    return _run_command(
        name="pytest",
        command=command,
        cwd=ROOT,
    )


def _live_pipeline_smoke(identifier: str, dimension: str, top_k: int) -> CheckResult:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    return _run_command(
        name="live_pipeline",
        command=[
            sys.executable,
            str(ROOT / "exercises" / "complete_pipeline.py"),
            "--identifier",
            identifier,
            "--dimension",
            dimension,
            "--top-k",
            str(top_k),
            "--json",
        ],
        cwd=ROOT,
        env=env,
    )


def _run_check(name: str, fn: Callable[[], CheckResult]) -> CheckResult:
    try:
        return fn()
    except Exception as exc:
        return CheckResult(
            name=name,
            status="FAIL",
            detail=f"{type(exc).__name__}: {exc}",
            return_code=1,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the local verification harness for the PE OrgAIR CS5 repository."
    )
    parser.add_argument(
        "--skip-pytest",
        action="store_true",
        help="Skip the pytest suite.",
    )
    parser.add_argument(
        "--pytest-args",
        nargs="*",
        default=["-q"],
        help="Additional arguments passed to pytest. Default: -q",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Run the live complete-pipeline exercise against real integrations.",
    )
    parser.add_argument(
        "--identifier",
        default="NVDA",
        help="Ticker or company identifier used for the live pipeline check.",
    )
    parser.add_argument(
        "--dimension",
        default="data_infrastructure",
        help="Dimension used for the live pipeline check.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=3,
        help="Evidence count used for the live pipeline check.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the final summary as JSON.",
    )
    args = parser.parse_args()

    results: List[CheckResult] = []
    results.append(_run_check("syntax", _compile_targets))
    results.append(_run_check("openapi_smoke", _openapi_smoke))
    results.append(_run_check("exercise_help", _exercise_help_smoke))
    results.append(_run_check("agentic_exercise_help", _agentic_exercise_help_smoke))

    if args.skip_pytest:
        results.append(
            CheckResult(
                name="pytest",
                status="SKIP",
                detail="skipped by --skip-pytest",
            )
        )
    else:
        results.append(_run_check("pytest", lambda: _pytest_suite(args.pytest_args)))

    if args.live:
        results.append(
            _run_check(
                "live_pipeline",
                lambda: _live_pipeline_smoke(
                    identifier=args.identifier,
                    dimension=args.dimension,
                    top_k=args.top_k,
                ),
            )
        )
    else:
        results.append(
            CheckResult(
                name="live_pipeline",
                status="SKIP",
                detail="live pipeline check skipped; pass --live to exercise real integrations",
            )
        )

    failures = [result for result in results if result.status == "FAIL"]
    summary = {
        "repo_root": str(REPO_ROOT),
        "app_root": str(ROOT),
        "overall_status": "PASS" if not failures else "FAIL",
        "results": [asdict(result) for result in results],
    }

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print("\nPE OrgAIR CS5 verification summary\n")
        for result in results:
            print(f"[{result.status}] {result.name}")
            print(f"  {result.detail}")
        print(f"\nOverall: {summary['overall_status']}")

    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
