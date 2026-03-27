# DAMG7245 Case Study 5: Agentic Portfolio Intelligence

This repository contains the CS5 submission for an agentic private-equity due diligence platform built on top of the earlier CS1-CS4 services. The implementation combines:

- `FastAPI` APIs for company, evidence, scoring, search, and justification workflows
- an `MCP` server exposing the platform as reusable tools, resources, and prompts
- `LangGraph` specialist agents plus a supervisor with HITL approval gates
- `Streamlit` dashboard views for portfolio monitoring and evidence-backed review
- `Snowflake`, `Redis`, and `Chroma` integration for persistence, caching, and retrieval

## Repository Layout

- `pe-org-air-platform/app`
  Backend application code, MCP server, LangGraph agents, scoring, retrieval, and dashboard components.
- `pe-org-air-platform/tests`
  API, MCP, observability, and workflow-oriented tests.
- `pe-org-air-platform/exercises`
  Runnable coursework entrypoints, including `agentic_due_diligence.py`.
- `pe-org-air-platform/docs`
  Architecture, Airflow, and deployment documentation.
- `pe-org-air-platform/scripts`
  Operational scripts for schema application, scoring, indexing, verification, and MCP launch.

## Environment

Copy `pe-org-air-platform/.env.example` to `pe-org-air-platform/.env` and populate at least:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `REDIS_URL`
- `OPENAI_API_KEY` or `GEMINI_API_KEY`

Optional:

- `CS1_PORTFOLIOS_JSON` for explicit portfolio membership
- `MCP_CLIENT_TRANSPORT`, `MCP_SERVER_URL`, `MCP_SERVER_COMMAND`, `MCP_SERVER_ARGS` for MCP transport overrides

## Install

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r pe-org-air-platform\requirements.txt
```

If you prefer Poetry:

```powershell
poetry install
```

## Apply Schema

```powershell
.\.venv\Scripts\python.exe pe-org-air-platform\scripts\apply_schema.py
```

This creates the CS1-CS3 tables plus the CS5 `assessment_history_snapshots` table used for trend tracking.

## Run The Platform

Start the API:

```powershell
cd pe-org-air-platform
..\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

Start the MCP server over HTTP:

```powershell
cd pe-org-air-platform
..\.venv\Scripts\python.exe scripts\run_mcp_http.py
```

Or start it over stdio:

```powershell
cd pe-org-air-platform
..\.venv\Scripts\python.exe scripts\run_mcp_server.py
```

Start the CS5 dashboard:

```powershell
cd pe-org-air-platform
..\.venv\Scripts\python.exe -m streamlit run app\dashboard\app.py
```

## Run The CS5 Exercise

```powershell
cd pe-org-air-platform
..\.venv\Scripts\python.exe exercises\agentic_due_diligence.py --company-id NVDA --assessment-type full
```

JSON output:

```powershell
cd pe-org-air-platform
..\.venv\Scripts\python.exe exercises\agentic_due_diligence.py --company-id NVDA --json
```

## Tests

Focused CS5 verification:

```powershell
.\.venv\Scripts\python.exe -m pytest pe-org-air-platform\tests\test_mcp_server.py pe-org-air-platform\tests\test_mcp_integration.py pe-org-air-platform\tests\test_mcp_client.py -q
```

Broader API and observability slice:

```powershell
.\.venv\Scripts\python.exe -m pytest pe-org-air-platform\tests\test_api.py pe-org-air-platform\tests\test_justifications_api.py pe-org-air-platform\tests\test_assessment_history.py pe-org-air-platform\tests\test_observability.py -q
```

Verifier script:

```powershell
.\.venv\Scripts\python.exe pe-org-air-platform\scripts\test_everything.py --skip-pytest --json
```

## Key Docs

- `pe-org-air-platform/docs/architecture.md`
- `pe-org-air-platform/docs/airflow_runbook.md`
- `pe-org-air-platform/DEPLOY_GCP_CLOUD_RUN.md`
