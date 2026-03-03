param(
    [Parameter(Mandatory = $false)]
    [string]$StartDate = "2022-01-01",

    [Parameter(Mandatory = $false)]
    [string]$EndDate = "2025-11-30",

    [Parameter(Mandatory = $false)]
    [string]$PipelinePath = "./pipeline/pipeline.yml",

    [Parameter(Mandatory = $false)]
    [string]$TaxiTypesJson = "",

    [Parameter(Mandatory = $false)]
    [int]$ChunkMonths = 1,

    [Parameter(Mandatory = $false)]
    [int]$MaxMonthsPerRun = 3,

    [Parameter(Mandatory = $false)]
    [switch]$SkipInitialFullRefresh,

    [Parameter(Mandatory = $false)]
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($ChunkMonths -lt 1) {
    throw "ChunkMonths must be >= 1."
}

if ($MaxMonthsPerRun -lt 1) {
    throw "MaxMonthsPerRun must be >= 1."
}

if ($ChunkMonths -gt $MaxMonthsPerRun) {
    throw "ChunkMonths ($ChunkMonths) cannot be greater than MaxMonthsPerRun ($MaxMonthsPerRun)."
}

$start = [datetime]::Parse($StartDate).Date
$endInclusive = [datetime]::Parse($EndDate).Date

if ($endInclusive -lt $start) {
    throw "EndDate must be on or after StartDate."
}

$tlcLastDate = [datetime]::Parse("2025-11-30").Date
if ($endInclusive -gt $tlcLastDate) {
    Write-Host "EndDate $($endInclusive.ToString('yyyy-MM-dd')) is beyond available TLC data. Capping to $($tlcLastDate.ToString('yyyy-MM-dd'))." -ForegroundColor Yellow
    $endInclusive = $tlcLastDate
}

$currentStart = Get-Date -Date (Get-Date -Year $start.Year -Month $start.Month -Day 1).Date
$targetEndExclusive = $endInclusive.AddDays(1)
$runNumber = 0

while ($currentStart -lt $targetEndExclusive) {
    $runNumber++

    $currentEndExclusive = $currentStart.AddMonths($ChunkMonths)
    if ($currentEndExclusive -gt $targetEndExclusive) {
        $currentEndExclusive = $targetEndExclusive
    }

    $startArg = $currentStart.ToString("yyyy-MM-dd")
    $endArg = $currentEndExclusive.ToString("yyyy-MM-dd")

    $args = @(
        "run",
        $PipelinePath,
        "--start-date", $startArg,
        "--end-date", $endArg,
        "--var", "max_months_per_run=$MaxMonthsPerRun"
    )

    if (-not [string]::IsNullOrWhiteSpace($TaxiTypesJson)) {
        $args += @("--var", "taxi_types=$TaxiTypesJson")
    }

    if (($runNumber -eq 1) -and (-not $SkipInitialFullRefresh)) {
        $args += "--full-refresh"
    }

    $displayCmd = "bruin " + ($args -join " ")
    Write-Host "`n[$runNumber] $displayCmd" -ForegroundColor Cyan

    if (-not $DryRun) {
        & bruin @args
        if ($LASTEXITCODE -ne 0) {
            throw "Chunk run failed (exit code $LASTEXITCODE) for interval $startArg to $endArg."
        }
    }

    $currentStart = $currentEndExclusive
}

Write-Host "`nBackfill completed successfully." -ForegroundColor Green
