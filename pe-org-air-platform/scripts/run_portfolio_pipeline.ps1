param(
    [string]$Tickers = "NVDA,JPM,WMT,GE,DG",
    [string]$Dimension = "data_infrastructure",
    [string]$PythonPath = "",
    [string]$PoetryPath = "",
    [switch]$SkipBackfill,
    [switch]$SkipValidation,
    [switch]$SkipCompletePipeline
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$AppRoot = Join-Path $RepoRoot "pe-org-air-platform"

function Resolve-Runner {
    param(
        [string]$PreferredPython,
        [string]$PreferredPoetry
    )

    if ($PreferredPython) {
        return @((Resolve-Path $PreferredPython).Path)
    }

    if ($PreferredPoetry) {
        return @((Resolve-Path $PreferredPoetry).Path, "run", "python")
    }

    $poetryCmd = Get-Command poetry -ErrorAction SilentlyContinue
    if ($poetryCmd) {
        return @($poetryCmd.Source, "run", "python")
    }

    $venvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        return @((Resolve-Path $venvPython).Path)
    }

    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd) {
        return @($cmd.Source)
    }

    throw "Poetry or Python executable not found. Pass -PoetryPath, -PythonPath, install Poetry, or create .venv\Scripts\python.exe."
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

$Runner = Resolve-Runner -PreferredPython $PythonPath -PreferredPoetry $PoetryPath
$tickerList = ($Tickers -split ",") | ForEach-Object { $_.Trim().ToUpper() } | Where-Object { $_ }
$tickerCsv = ($tickerList | Select-Object -Unique) -join ","

Write-Host "Repo root: $RepoRoot"
Write-Host "App root:  $AppRoot"
Write-Host "Runner:    $($Runner -join ' ')"
Write-Host "Tickers:   $tickerCsv"
Write-Host "Dimension: $Dimension"

if (-not $SkipBackfill) {
    Invoke-Step `
        -Title "1. Backfill Portfolio Companies" `
        -Command ($Runner + @((Join-Path $AppRoot "scripts\backfill_companies.py")))
}

Invoke-Step `
    -Title "2. Collect SEC Evidence" `
    -Command ($Runner + @((Join-Path $AppRoot "scripts\collect_evidence.py"), "--companies", $tickerCsv))

Invoke-Step `
    -Title "3. Collect External Signals" `
    -Command ($Runner + @((Join-Path $AppRoot "scripts\collect_signals.py"), "--companies", $tickerCsv))

Invoke-Step `
    -Title "4. Score Individual Signals" `
    -Command ($Runner + @((Join-Path $AppRoot "scripts\compute_signal_scores.py")))

Invoke-Step `
    -Title "5. Build Company Signal Summaries" `
    -Command ($Runner + @((Join-Path $AppRoot "scripts\compute_company_signal_summaries.py")))

Invoke-Step `
    -Title "6. Run CS3 Scoring" `
    -Command ($Runner + @((Join-Path $AppRoot "scripts\run_scoring_engine.py"), "--batch", "--tickers", $tickerCsv))

if (-not $SkipValidation) {
    Invoke-Step `
        -Title "7. Validate Portfolio Score Ranges" `
        -Command ($Runner + @((Join-Path $AppRoot "scripts\validate_portfolio_scores.py")))
}

if (-not $SkipCompletePipeline) {
    $exerciseScript = Join-Path $AppRoot "exercises\complete_pipeline.py"
    foreach ($ticker in ($tickerList | Select-Object -Unique)) {
        Invoke-Step `
            -Title ("8. Generate CS4 End-to-End Artifact for " + $ticker) `
            -Command ($Runner + @($exerciseScript, "--identifier", $ticker, "--dimension", $Dimension))
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
