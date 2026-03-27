param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectId,

    [string]$Region = "us-central1",
    [string]$ServiceName = "orgair-api",
    [string]$ArtifactRepo = "orgair",
    [string]$ImageTag = "latest",
    [string]$DotEnvPath = "",
    [switch]$AllowUnauthenticated
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Require-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found in PATH: $Name"
    }
}

function Convert-DotEnvToYaml {
    param(
        [Parameter(Mandatory = $true)]
        [string]$InputPath,
        [Parameter(Mandatory = $true)]
        [string]$OutputPath
    )

    $pairs = @{}
    foreach ($raw in Get-Content -Path $InputPath) {
        $line = $raw.Trim()
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        if ($line.StartsWith("#")) { continue }
        $idx = $line.IndexOf("=")
        if ($idx -lt 1) { continue }
        $key = $line.Substring(0, $idx).Trim()
        $value = $line.Substring($idx + 1)
        $pairs[$key] = $value
    }

    $yamlLines = New-Object System.Collections.Generic.List[string]
    foreach ($entry in $pairs.GetEnumerator()) {
        $escaped = $entry.Value.Replace("`"", "\`"")
        $yamlLines.Add("$($entry.Key): `"$escaped`"")
    }

    Set-Content -Path $OutputPath -Value ($yamlLines -join [Environment]::NewLine) -Encoding utf8
}

if (Get-Command gcloud.cmd -ErrorAction SilentlyContinue) {
    $Gcloud = "gcloud.cmd"
} elseif (Get-Command gcloud -ErrorAction SilentlyContinue) {
    $Gcloud = "gcloud"
} else {
    throw "Required command not found in PATH: gcloud (or gcloud.cmd)"
}

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$AppRoot = Join-Path $ProjectRoot "pe-org-air-platform"
if ([string]::IsNullOrWhiteSpace($DotEnvPath)) {
    $DotEnvPath = Join-Path $AppRoot ".env"
}

Write-Host "Project: $ProjectId"
Write-Host "Region:  $Region"
Write-Host "Service: $ServiceName"
Write-Host "Project: $ProjectRoot"
Write-Host "App:     $AppRoot"

$ImageUri = "$Region-docker.pkg.dev/$ProjectId/$ArtifactRepo/$($ServiceName):$ImageTag"
Write-Host "Image:   $ImageUri"
Write-Host "CLI:     $Gcloud"

Write-Host "`n[1/6] Setting active project..."
& $Gcloud config set project $ProjectId | Out-Host

Write-Host "`n[2/6] Enabling required APIs..."
& $Gcloud services enable `
    run.googleapis.com `
    cloudbuild.googleapis.com `
    artifactregistry.googleapis.com `
    --project $ProjectId `
    | Out-Host

Write-Host "`n[3/6] Ensuring Artifact Registry repo exists..."
$repoCheckOk = $true
try {
    & $Gcloud artifacts repositories describe $ArtifactRepo --location $Region --project $ProjectId | Out-Null
} catch {
    $repoCheckOk = $false
}
if (-not $repoCheckOk) {
    & $Gcloud artifacts repositories create $ArtifactRepo `
        --repository-format=docker `
        --location=$Region `
        --description="Container images for PE OrgAIR API" `
        --project=$ProjectId `
        | Out-Host
}

$tmpBuildConfig = Join-Path ([System.IO.Path]::GetTempPath()) ("cloudbuild-" + [guid]::NewGuid().ToString("N") + ".yaml")
$cloudBuildYaml = @"
steps:
- name: gcr.io/cloud-builders/docker
  args: ['build', '-f', 'docker/Dockerfile', '-t', '$ImageUri', '.']
images:
- '$ImageUri'
"@
Set-Content -Path $tmpBuildConfig -Value $cloudBuildYaml -Encoding utf8

Write-Host "`n[4/6] Building and pushing container image..."
& $Gcloud builds submit $ProjectRoot --config $tmpBuildConfig --project $ProjectId | Out-Host

$deployArgs = @(
    "run", "deploy", $ServiceName,
    "--image", $ImageUri,
    "--region", $Region,
    "--platform", "managed",
    "--port", "8000",
    "--project", $ProjectId
)

$tmpEnvFile = ""
if (Test-Path $DotEnvPath) {
    $tmpEnvFile = Join-Path ([System.IO.Path]::GetTempPath()) ("cloudrun-env-" + [guid]::NewGuid().ToString("N") + ".yaml")
    Convert-DotEnvToYaml -InputPath $DotEnvPath -OutputPath $tmpEnvFile
    $deployArgs += @("--env-vars-file", $tmpEnvFile)
    Write-Host "`nUsing env vars from: $DotEnvPath"
} else {
    Write-Host "`nNo .env file found at $DotEnvPath. Deploying without extra env vars."
}

if ($AllowUnauthenticated) {
    $deployArgs += "--allow-unauthenticated"
} else {
    $deployArgs += "--no-allow-unauthenticated"
}

try {
    Write-Host "`n[5/6] Deploying to Cloud Run..."
    & $Gcloud @deployArgs | Out-Host
} finally {
    if ($tmpEnvFile -and (Test-Path $tmpEnvFile)) {
        Remove-Item -Path $tmpEnvFile -Force
    }
    if (Test-Path $tmpBuildConfig) {
        Remove-Item -Path $tmpBuildConfig -Force
    }
}

Write-Host "`n[6/6] Fetching service URL..."
$url = & $Gcloud run services describe $ServiceName `
    --region $Region `
    --project $ProjectId `
    --format "value(status.url)"

Write-Host "`nDeployment complete."
Write-Host "Service URL: $url"
Write-Host "Health check: $url/health"
