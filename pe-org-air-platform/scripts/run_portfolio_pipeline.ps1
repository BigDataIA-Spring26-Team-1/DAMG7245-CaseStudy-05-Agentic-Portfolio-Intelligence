param(
    [string]$Tickers = "NVDA,JPM,WMT,GE,DG",
    [string]$Dimension = "data_infrastructure",
    [string]$PythonPath = "",
    [switch]$SkipBackfill,
    [switch]$SkipValidation,
    [switch]$SkipCompletePipeline
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$AppRoot = Join-Path $RepoRoot "pe-org-air-platform"

function Resolve-Python {
    param([string]$Preferred)

    if ($Preferred) {
        return (Resolve-Path $Preferred).Path
    }

    $venvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        return (Resolve-Path $venvPython).Path
    }

    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd) {
        return $cmd.Source
    }

    throw "Python executable not found. Pass -PythonPath or create .venv\Scripts\python.exe."
}

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)][string]$Title,
        [Parameter(Mandatory = $true)][string[]]$Command
    )

    Write-Host ""
    Write-Host ("=" * 88)
    Write-Host $Title
    Write-Host ("=" * 88)
    Write-Host ($Command -join " ")
    & $Command[0] $Command[1..($Command.Length - 1)]
    if ($LASTEXITCODE -ne 0) {
        throw "Step failed: $Title"
    }
}

$Python = Resolve-Python -Preferred $PythonPath
$tickerList = ($Tickers -split ",") | ForEach-Object { $_.Trim().ToUpper() } | Where-Object { $_ }
$tickerCsv = ($tickerList | Select-Object -Unique) -join ","

Write-Host "Repo root: $RepoRoot"
Write-Host "App root:  $AppRoot"
Write-Host "Python:    $Python"
Write-Host "Tickers:   $tickerCsv"
Write-Host "Dimension: $Dimension"

if (-not $SkipBackfill) {
    Invoke-Step `
        -Title "1. Backfill Portfolio Companies" `
        -Command @($Python, (Join-Path $AppRoot "scripts\backfill_companies.py"))
}

Invoke-Step `
    -Title "2. Collect SEC Evidence" `
    -Command @($Python, (Join-Path $AppRoot "scripts\collect_evidence.py"), "--companies", $tickerCsv)

Invoke-Step `
    -Title "3. Collect External Signals" `
    -Command @($Python, (Join-Path $AppRoot "scripts\collect_signals.py"), "--companies", $tickerCsv)

Invoke-Step `
    -Title "4. Score Individual Signals" `
    -Command @($Python, (Join-Path $AppRoot "scripts\compute_signal_scores.py"))

Invoke-Step `
    -Title "5. Build Company Signal Summaries" `
    -Command @($Python, (Join-Path $AppRoot "scripts\compute_company_signal_summaries.py"))

Invoke-Step `
    -Title "6. Run CS3 Scoring" `
    -Command @($Python, (Join-Path $AppRoot "scripts\run_scoring_engine.py"), "--batch", "--tickers", $tickerCsv)

if (-not $SkipValidation) {
    Invoke-Step `
        -Title "7. Validate Portfolio Score Ranges" `
        -Command @($Python, (Join-Path $AppRoot "scripts\validate_portfolio_scores.py"))
}

if (-not $SkipCompletePipeline) {
    $exerciseScript = Join-Path $RepoRoot "exercises\complete_pipeline.py"
    foreach ($ticker in ($tickerList | Select-Object -Unique)) {
        Invoke-Step `
            -Title ("8. Generate CS4 End-to-End Artifact for " + $ticker) `
            -Command @($Python, $exerciseScript, "--identifier", $ticker, "--dimension", $Dimension)
    }
}

Write-Host ""
Write-Host "Pipeline run complete."
Write-Host "Stored outputs:"
Write-Host "- Snowflake: companies, documents, document_chunks, external_signals, company_signal_summaries, assessments, dimension_scores, scoring_runs, scoring_audit_log, org_air_scores"
Write-Host "- Local results copies: $AppRoot\results\<ticker>\..."
Write-Host "- Portfolio validation and batch summaries: $AppRoot\results\PORTFOLIO\..."
Write-Host "- Local vector index: $AppRoot\chroma (or CHROMA_PATH / Docker volume)"
Write-Host "- S3 artifacts when configured: results/<ticker>/..., results/PORTFOLIO/..., plus data/raw, data/processed, and data/signals prefixes"
