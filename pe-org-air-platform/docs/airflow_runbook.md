# Airflow Runbook

## What Airflow Does In This Repo

Airflow is the orchestration layer for the OrgAIR platform. It does not replace the scoring or retrieval code. It schedules and sequences the existing Python pipelines so the team can run the platform as a repeatable workflow instead of as ad hoc scripts.

The repository currently exposes three DAGs:

- `org_air_full_platform_pipeline`: end-to-end company backfill, evidence collection, external signal collection, scoring, and indexing.
- `cs4_pipeline_dag`: company profile lookup, score retrieval, justification generation, IC prep, and analyst note generation.
- `evidence_indexing_pipeline`: daily CS4 indexing pipeline for retrieval refresh.

## Prerequisites

- Docker Desktop must be running.
- The repo must contain `pe-org-air-platform/.env` with valid service credentials.
- The machine needs network access for services used by the DAGs:
  - Snowflake
  - SEC EDGAR
  - external signal providers
  - LLM providers
  - AWS/S3 if artifact upload is enabled

Do not share raw `.env` values in chat, slides, or screenshots.

## Start Airflow

Run from the repository root:

```powershell
docker compose -f pe-org-air-platform/docker/airflow/docker-compose.airflow.yml up -d --build
```

What this starts:

- `airflow-postgres`: metadata database for DAG state, task instances, and users
- `airflow-init`: one-time DB migration plus admin-user creation
- `airflow-webserver`: UI on port `8081`
- `airflow-scheduler`: DAG parsing and task scheduling

## Access The UI

- URL: `http://localhost:8081`
- Username: `airflow`
- Password: `airflow`

Quick verification commands:

```powershell
docker compose -f pe-org-air-platform/docker/airflow/docker-compose.airflow.yml ps
docker exec airflow-airflow-scheduler-1 airflow dags list
curl.exe -I http://localhost:8081/
```

## What Each DAG Does

### `org_air_full_platform_pipeline`

This is the main end-to-end pipeline. The task order is:

1. `backfill_companies`
2. `collect_evidence`
3. `collect_signals`
4. `compute_signal_scores`
5. `compute_company_signal_summaries`
6. `compute_dimension_scores`
7. `run_scoring_engine`
8. `index_evidence`

Operational meaning:

1. Seed or update company rows in Snowflake.
2. Pull SEC filings, parse them, chunk them, and store document/chunk records.
3. Collect external signals such as jobs, news, patents, and tech-stack hints.
4. Convert raw signals into normalized signal-level scores.
5. Aggregate signal-level scores into company-level signal summaries.
6. Map evidence to OrgAIR dimensions and calculate dimension scores.
7. Produce the OrgAIR company score, supporting breakdown, SEM bounds, and artifacts.
8. Push evidence into Chroma so the retrieval and justification layer can query it.

### `cs4_pipeline_dag`

This DAG is used after evidence and scores already exist.

Its tasks are:

1. `load_company`
2. `load_scores`
3. `generate_justification`
4. `build_ic_prep`
5. `build_analyst_note`

It writes JSON artifacts to `pe-org-air-platform/artifacts/airflow_runs`.

### `evidence_indexing_pipeline`

- Schedule: daily at `06:00` UTC
- Purpose: refresh the retrieval index and run a lightweight validation pass

Task order:

1. `index_evidence`
2. `compute_scoring`
3. `smoke_validate`

## Team Explanation: End-to-End Flow

Use this narrative with teammates:

1. Airflow is only the conductor. The business logic still lives in the scripts and app modules.
2. The pipeline starts by making sure the company universe exists in Snowflake.
3. It then gathers first-party evidence from SEC filings and third-party external signals.
4. That raw evidence is normalized into scores and summaries so the scoring engine has structured inputs.
5. The scoring engine computes dimension-level and company-level OrgAIR results.
6. The final step indexes evidence into Chroma so justification, search, IC prep, and analyst workflows can retrieve it quickly.
7. A second DAG sits on top of those outputs and turns them into stakeholder-facing artifacts such as justifications, IC packets, and analyst notes.

In short:

`raw data -> normalized signals -> dimension scores -> OrgAIR score -> retrieval index -> narrative outputs`

## How To Run A DAG

Recommended path:

1. Open `http://localhost:8081`
2. Unpause the DAG if needed
3. Click `Trigger DAG`
4. Override parameters in the trigger dialog only when needed

Common parameters in the main DAG:

- `companies`: ticker list or `all`
- `company_id`: target company UUID for downstream scoring/indexing steps
- `tickers`: optional ticker filter for batch scoring
- `as_of_date`: summary date
- `reindex`: whether to delete and rebuild existing vectors for the company

## Outputs To Show In A Demo

- Airflow UI for orchestration status
- Snowflake tables for stored companies, documents, chunks, signals, and scores
- `results/` artifacts for scoring outputs
- `pe-org-air-platform/artifacts/airflow_runs` for CS4 JSON outputs
- `chroma/` for the persisted local vector index

## Stop Airflow

```powershell
docker compose -f pe-org-air-platform/docker/airflow/docker-compose.airflow.yml down
```

## Common Issues

### Docker Desktop Not Running

Symptom:

- Docker named-pipe or engine connection errors

Fix:

- Start Docker Desktop first, then rerun the compose command

### Postgres Not Running After Docker Restart

Symptom:

- Airflow webserver or scheduler complains about `airflow-postgres`
- UI does not load even though some containers exist

Fix:

```powershell
docker compose -f pe-org-air-platform/docker/airflow/docker-compose.airflow.yml up -d airflow-postgres airflow-webserver airflow-scheduler
```

### DAG Runs Fail Even Though Airflow Starts

Typical causes:

- expired or missing credentials in `.env`
- no network access to Snowflake or external APIs
- missing company rows or invalid `company_id`

## Validated On This Machine

The following checks succeeded during setup:

- Docker engine became available
- Airflow scheduler registered all three DAGs
- Airflow UI responded on `http://localhost:8081` via `curl.exe`
