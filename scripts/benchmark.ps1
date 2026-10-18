<#
.SYNOPSIS
    The RDD lab on the laptop and on the Docker cluster, side by side (roadmap P7.5).

        .\scripts\benchmark.ps1

.DESCRIPTION
    Keeps the laptop run of rdd-fundamentals (dev mode, local[20], from the pipeline or submit.ps1), runs the same
    job on the Docker Spark cluster, and stores both exports and job reports in data\serving\benchmarks\. The
    laptop run's files are put back afterwards, so the RDD lab keeps showing the laptop's numbers and its
    "Laptop or cluster?" act shows both. Needs the Spark containers up:

        docker compose -f infra/docker-compose.yml --profile spark up -d
#>
$ErrorActionPreference = 'Stop'
$serving = Join-Path (Split-Path -Parent $PSScriptRoot) 'data\serving'
$bench = Join-Path $serving 'benchmarks'
$export = Join-Path $serving 'rdd-fundamentals.json'
$report = Join-Path $serving 'jobs\rdd-fundamentals.json'

if (-not (Test-Path $report) -or (Get-Content $report -Raw | ConvertFrom-Json).data.mode -ne 'dev') {
    throw 'No laptop run to compare with: run .\scripts\submit.ps1 rdd-fundamentals first.'
}
New-Item -ItemType Directory -Force $bench | Out-Null
Copy-Item $export (Join-Path $bench 'rdd-fundamentals-local.json') -Force
Copy-Item $report (Join-Path $bench 'rdd-fundamentals-local-job.json') -Force

try {
    & (Join-Path $PSScriptRoot 'submit.ps1') rdd-fundamentals -Mode cluster
    Copy-Item $export (Join-Path $bench 'rdd-fundamentals-cluster.json') -Force
    Copy-Item $report (Join-Path $bench 'rdd-fundamentals-cluster-job.json') -Force
} finally {
    # The laptop run stays the lab's result everywhere else: the portal, the report and the slides quote it.
    Copy-Item (Join-Path $bench 'rdd-fundamentals-local.json') $export -Force
    Copy-Item (Join-Path $bench 'rdd-fundamentals-local-job.json') $report -Force
}

$local = (Get-Content (Join-Path $bench 'rdd-fundamentals-local-job.json') -Raw | ConvertFrom-Json).data
$cluster = (Get-Content (Join-Path $bench 'rdd-fundamentals-cluster-job.json') -Raw | ConvertFrom-Json).data
Write-Host ("rdd-fundamentals: laptop {0:N1} s ({1}), cluster {2:N1} s ({3}); both kept in {4}" -f `
    ($local.durationMs / 1000), $local.master, ($cluster.durationMs / 1000), $cluster.master, $bench)
