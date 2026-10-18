<#
.SYNOPSIS
    The whole batch pipeline in one command, stage by stage, with timings.

        .\scripts\pipeline.ps1                              # every stage, raw CSV to web exports
        .\scripts\pipeline.ps1 -Stages graph,ml,live        # a part of it
        .\scripts\pipeline.ps1 -Stages ml -WithLabs         # re-run the PySpark lab notebooks too

.DESCRIPTION
    Runs on the host (local[20], scripts\submit.ps1 dev mode), in this order:

        bronze  bronze-trips
        silver  silver-trips, silver-stations, silver-weather, silver-calendar
        gold    gold-station-hour, gold-routes
        sql     rdd-fundamentals, insights
        graph   graph-metrics
        ml      station-archetypes, feature-build, [labs 01-04 with -WithLabs], train-champion
        live    live-features
        export  hero-trips, then pnpm sync for the website

    Heavy batch work runs with the Docker containers stopped (16 GB machine, ARCHITECTURE section 3), so the script
    stops them first; scripts\demo.ps1 starts them again. Every job writes data\serving\jobs\<job>.json; this
    script also writes the stage timings to data\logs\pipeline-<time>.log and prints a summary.
#>
[CmdletBinding()]
param(
    [ValidateSet('bronze', 'silver', 'gold', 'sql', 'graph', 'ml', 'live', 'export')]
    [string[]]$Stages = @('bronze', 'silver', 'gold', 'sql', 'graph', 'ml', 'live', 'export'),
    [switch]$WithLabs,
    [switch]$KeepCluster
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'env.ps1') -Quiet
$submit = Join-Path $PSScriptRoot 'submit.ps1'

# Each step is 'job:driver memory' (what the job needs on the full Jun-Sep data); 'labs' and 'web-sync' are not
# Spark jobs. Plain strings, because PowerShell flattens a one-element array of arrays into its contents.
$plan = [ordered]@{
    bronze = @('bronze-trips:4g')
    silver = @('silver-trips:6g', 'silver-stations:4g', 'silver-weather:4g', 'silver-calendar:4g')
    gold   = @('gold-station-hour:6g', 'gold-routes:4g')
    sql    = @('rdd-fundamentals:4g', 'insights:4g')
    graph  = @('graph-metrics:6g')
    ml     = @('station-archetypes:6g', 'feature-build:7g', 'labs', 'train-champion:8g')
    live   = @('live-features:6g')
    export = @('hero-trips:4g', 'web-sync')
}

$logDir = Join-Path $env:VELOGRAPH_DATA_ROOT 'logs'
New-Item -ItemType Directory -Force $logDir | Out-Null
$log = Join-Path $logDir ("pipeline-{0:yyyyMMdd-HHmmss}.log" -f (Get-Date))

function Invoke-Native([scriptblock]$Command) {
    # Native stderr must not abort the script under 'Stop' (Windows PowerShell 5.1); the exit code decides.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { & $Command 2>&1 | ForEach-Object { "$_" } } finally { $ErrorActionPreference = $previous }
}

if (-not $KeepCluster) {
    $running = Invoke-Native { docker ps --filter 'name=velograph-' --format '{{.Names}}' }
    if ($LASTEXITCODE -eq 0 -and $running) {
        Write-Host 'Stopping the Docker containers: heavy batch work runs with them stopped (16 GB machine).'
        Invoke-Native { docker compose -f (Join-Path $root 'infra\docker-compose.yml') --profile kafka --profile spark stop } | Out-Null
    }
}

$timings = New-Object System.Collections.Generic.List[object]
$started = Get-Date
foreach ($stage in $plan.Keys) {
    if ($Stages -notcontains $stage) { continue }
    foreach ($step in $plan[$stage]) {
        $job, $memory = $step -split ':'
        if ($job -eq 'labs' -and -not $WithLabs) { continue }
        $t0 = Get-Date
        Write-Host ("[{0:HH:mm:ss}] {1,-7} {2}" -f $t0, $stage, $job)
        switch ($job) {
            'labs' {
                # One notebook at a time: each takes an 8 GB driver.
                Push-Location (Join-Path $root 'python')
                try {
                    $env:PYTHONUTF8 = '1'
                    foreach ($lab in '01_eda', '02_regression', '03_classification', '04_ablation') {
                        Invoke-Native { uv run jupytext --to ipynb --execute "lab/$lab.py" } | Out-File -Append -Encoding utf8 $log
                        if ($LASTEXITCODE -ne 0) { throw "lab $lab failed (exit code $LASTEXITCODE)" }
                    }
                } finally { Pop-Location }
            }
            'web-sync' {
                Push-Location (Join-Path $root 'web')
                try {
                    Invoke-Native { pnpm -s sync } | Out-File -Append -Encoding utf8 $log
                    if ($LASTEXITCODE -ne 0) { throw "pnpm sync failed (exit code $LASTEXITCODE)" }
                } finally { Pop-Location }
            }
            default {
                # submit.ps1 throws when the job fails, which stops the pipeline here.
                & $submit $job -DriverMemory $memory *>&1 | Out-File -Append -Encoding utf8 $log
            }
        }
        $seconds = ((Get-Date) - $t0).TotalSeconds
        $timings.Add([pscustomobject]@{ Stage = $stage; Step = $job; Minutes = [math]::Round($seconds / 60, 1) })
        ("{0} {1} {2:N1} s" -f $stage, $job, $seconds) | Out-File -Append -Encoding utf8 $log
    }
}

$total = ((Get-Date) - $started).TotalMinutes
Write-Host ($timings | Format-Table -AutoSize | Out-String).TrimEnd()
Write-Host ("Pipeline done: {0} steps in {1:N1} min. Log: {2}" -f $timings.Count, $total, $log)
