<#
.SYNOPSIS
    Run a VeloGraph Spark job on the host (dev) or on the Docker cluster (cluster).

        .\scripts\submit.ps1 smoke
        .\scripts\submit.ps1 smoke -Mode cluster
        .\scripts\submit.ps1 <job> -DriverMemory 6g -- --some-job-arg value

.DESCRIPTION
    dev     : spark-submit from C:\spark with JDK 17 (scripts\env.ps1), master local[20], run from the repo root.
    cluster : spark-submit inside velograph-spark-master (client mode), master spark://spark-master:7077.
    Both run the same fat jar: spark\target\scala-2.12\velograph-spark.jar (build it with 'sbt assembly').
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory, Position = 0)][string]$Job,
    [ValidateSet('dev', 'cluster')][string]$Mode = 'dev',
    [string]$DriverMemory = '4g',
    [Parameter(ValueFromRemainingArguments)][string[]]$JobArgs = @()
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot

# Short job name -> main class. Extended as each roadmap phase adds jobs.
$jobClasses = @{
    'smoke'           = 'velograph.smoke.SmokeJob'
    'bronze-trips'    = 'velograph.etl.BronzeIngestJob'
    'silver-trips'    = 'velograph.etl.SilverTripsJob'
    'silver-stations' = 'velograph.etl.SilverStationsJob'
    'silver-weather'  = 'velograph.etl.SilverWeatherJob'
    'silver-calendar' = 'velograph.etl.SilverCalendarJob'
    'gold-station-hour' = 'velograph.gold.GoldStationHourJob'
    'gold-routes'     = 'velograph.gold.GoldRoutesJob'
    'rdd-fundamentals' = 'velograph.rdd.RddFundamentalsJob'
    'insights'        = 'velograph.sql.InsightsJob'
    'graph-metrics'   = 'velograph.graph.GraphMetricsJob'
    'station-archetypes' = 'velograph.ml.StationArchetypesJob'
    'feature-build'   = 'velograph.ml.FeatureBuildJob'
    'train-champion'  = 'velograph.ml.TrainChampionJob'
    'live-features'   = 'velograph.ml.LiveFeaturesJob'
    'live-station-scoring' = 'velograph.streaming.LiveStationScoringJob'
    'trip-replay-analytics' = 'velograph.streaming.TripReplayAnalyticsJob'
}
if (-not $jobClasses.ContainsKey($Job)) {
    throw "Unknown job '$Job'. Known jobs: $(($jobClasses.Keys | Sort-Object) -join ', ')"
}
$class = $jobClasses[$Job]
$jar = Join-Path $root 'spark\target\scala-2.12\velograph-spark.jar'
if (-not (Test-Path $jar)) { throw "Missing $jar - run 'sbt assembly' in spark\ first." }

function Invoke-Native([scriptblock]$Command) {
    # Windows PowerShell 5.1 turns each stderr line of a native program into an error record, and with 'Stop' the
    # first Spark log line would abort the script. Stream everything as text; success is judged by the exit code.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { & $Command 2>&1 | ForEach-Object { "$_" } } finally { $ErrorActionPreference = $previous }
}

if ($Mode -eq 'dev') {
    . (Join-Path $PSScriptRoot 'env.ps1') -Quiet
    $log4j = 'file:///' + ((Join-Path $root 'conf\log4j2.properties') -replace '\\', '/')
    Push-Location $root # data\ and conf\ resolve relative to the repository root
    try {
        # UI on 4050: the Docker master publishes 4040-4042 for cluster drivers.
        Invoke-Native {
            spark-submit --master 'local[20]' --driver-memory $DriverMemory `
                --driver-java-options "-Dlog4j.configurationFile=$log4j" `
                --conf spark.ui.port=4050 --class $class $jar @JobArgs
        }
        if ($LASTEXITCODE -ne 0) { throw "$Job failed (exit code $LASTEXITCODE)" }
    } finally {
        Pop-Location
    }
} else {
    Invoke-Native {
        docker exec velograph-spark-master /opt/spark/bin/spark-submit --deploy-mode client `
            --class $class /opt/velograph/jars/velograph-spark.jar @JobArgs
    }
    if ($LASTEXITCODE -ne 0) { throw "$Job failed on the cluster (exit code $LASTEXITCODE)" }
}
