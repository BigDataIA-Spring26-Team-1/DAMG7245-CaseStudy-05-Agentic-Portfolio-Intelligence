# Deploy FastAPI To Google Cloud Run

## Prerequisites
- Google Cloud project with billing enabled
- `gcloud` CLI installed and authenticated
- Docker Desktop running (optional for local checks)

If PowerShell blocks `gcloud` script execution, use `gcloud.cmd` instead.

Authenticate once:

```powershell
gcloud.cmd auth login
gcloud.cmd auth application-default login
```

## One-command deploy (PowerShell)

From repo root:

```powershell
.\pe-org-air-platform\scripts\deploy_cloud_run.ps1 `
  -ProjectId "<YOUR_GCP_PROJECT_ID>" `
  -Region "us-central1" `
  -ServiceName "orgair-api" `
  -AllowUnauthenticated
```

Optional:
- `-DotEnvPath` to pass a specific `.env` file to Cloud Run
- `-ArtifactRepo` to change Artifact Registry repo name
- `-ImageTag` for custom image tags

## Manual deploy (equivalent)

```powershell
$PROJECT="<YOUR_GCP_PROJECT_ID>"
$REGION="us-central1"
$REPO="orgair"
$SERVICE="orgair-api"
$IMAGE="$REGION-docker.pkg.dev/$PROJECT/$REPO/$SERVICE:latest"
$CONFIG=".\\pe-org-air-platform\\cloudbuild.temp.yaml"

@"
steps:
- name: gcr.io/cloud-builders/docker
  args: ['build', '-f', 'docker/Dockerfile', '-t', '$IMAGE', '.']
images:
- '$IMAGE'
"@ | Set-Content $CONFIG

gcloud.cmd config set project $PROJECT
gcloud.cmd services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com
gcloud.cmd artifacts repositories describe $REPO --location $REGION 2>$null
if ($LASTEXITCODE -ne 0) {
  gcloud.cmd artifacts repositories create $REPO --repository-format=docker --location=$REGION --description="Container images for PE OrgAIR API"
}
gcloud.cmd builds submit .\pe-org-air-platform --config $CONFIG
gcloud.cmd run deploy $SERVICE --image $IMAGE --region $REGION --platform managed --port 8000 --allow-unauthenticated
Remove-Item $CONFIG -Force
```

## Verify

```powershell
gcloud.cmd run services describe orgair-api --region us-central1 --format="value(status.url)"
```

Then call:
- `<SERVICE_URL>/health`
- `<SERVICE_URL>/api/v1/companies?page=1&page_size=5`

## Notes
- The API reads runtime settings from env vars (Snowflake, Redis, AWS, etc.).
- If external dependencies are not configured, `/health` may return `degraded` by design.
