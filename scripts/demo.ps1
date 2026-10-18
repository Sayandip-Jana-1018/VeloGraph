<#
.SYNOPSIS
    The live demo in one command: cluster, topics, producer, live scoring, gateway and website.

        .\scripts\demo.ps1                                         # the live Citi Bike feed
        .\scripts\demo.ps1 -Replay -Day 2026-10-16 -Start 20:30    # a recorded evening (shown as replay)
        .\scripts\demo.ps1 -Stop                                   # stop what demo.ps1 started
        .\scripts\demo.ps1 -Stop -Down                             # ... and the Docker containers too

.DESCRIPTION
    1. Docker: Kafka (3 KRaft brokers, the topic bootstrap, Kafka UI) and Spark (master + 2 workers); waits until
       every container is healthy.
    2. Producer: velograph.ingest.gbfs_producer, the live feed or (-Replay) a recorded day.
    3. Live scoring: LiveStationScoringJob on the Spark cluster for -DurationMin minutes, from the latest offsets.
    4. Gateway: FastAPI on http://localhost:8000 (SSE for the live page).
    5. Website: the production build on http://localhost:3000 (built first if there is none).

    The background processes are windowless; their ids are kept in data\run\demo.json for -Stop, and their logs
    go to data\logs\demo-*.log. The script returns once the gateway serves a micro-batch of this run.
#>
[CmdletBinding()]
param(
    [switch]$Replay,
    [string]$Day = '2026-10-16',
    [string]$Start = '20:30',
    [double]$Speed = 10,
    [int]$DurationMin = 60,
    [switch]$Stop,
    [switch]$Down
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'env.ps1') -Quiet
$data = $env:VELOGRAPH_DATA_ROOT
$runDir = Join-Path $data 'run'
$logDir = Join-Path $data 'logs'
$state = Join-Path $runDir 'demo.json'
$compose = Join-Path $root 'infra\docker-compose.yml'
$python = Join-Path $root 'python\.venv\Scripts\python.exe'
$pythonw = Join-Path $root 'python\.venv\Scripts\pythonw.exe'
New-Item -ItemType Directory -Force $runDir, $logDir | Out-Null

function Invoke-Native([scriptblock]$Command) {
    # Native stderr must not abort the script under 'Stop' (Windows PowerShell 5.1); the exit code decides.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { & $Command 2>&1 | ForEach-Object { "$_" } } finally { $ErrorActionPreference = $previous }
}

function Get-Json([string]$Url) {
    try { return Invoke-RestMethod -Uri $Url -TimeoutSec 5 } catch { return $null }
}

function Start-Hidden([string]$Name, [string]$File, [string[]]$Arguments, [string]$Directory) {
    $out = Join-Path $logDir "demo-$Name.log"
    $err = Join-Path $logDir "demo-$Name.err.log"
    $proc = Start-Process -FilePath $File -ArgumentList $Arguments -WorkingDirectory $Directory -WindowStyle Hidden `
        -RedirectStandardOutput $out -RedirectStandardError $err -PassThru
    Write-Host ("  {0,-9} pid {1,-6} log {2}" -f $Name, $proc.Id, $out)
    return $proc.Id
}

if ($Stop) {
    # Graceful first: the producer and the scoring job watch their stop files and finish the current step.
    foreach ($name in 'gbfs_producer', 'live-station-scoring') {
        New-Item -ItemType File -Force (Join-Path $runDir "$name.stop") | Out-Null
    }
    if (Test-Path $state) {
        $ids = Get-Content $state -Raw | ConvertFrom-Json
        Start-Sleep -Seconds 20
        foreach ($name in 'gateway', 'web', 'scoring', 'producer') {
            $id = $ids.$name
            if ($id -and (Get-Process -Id $id -ErrorAction SilentlyContinue)) {
                # taskkill /T also stops the children (uvicorn's worker, next start under cmd, spark-submit).
                Invoke-Native { taskkill /PID $id /T /F } | Out-Null
            }
        }
        Remove-Item $state
    }
    if ($Down) { Invoke-Native { docker compose -f $compose --profile kafka --profile spark down } | Out-Null }
    Write-Host 'Demo stopped.'
    return
}

$t0 = Get-Date
Invoke-Native { docker info --format '{{.ServerVersion}}' } | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Docker is not running: start Docker Desktop, then run this again.' }
$jar = Join-Path $root 'spark\target\scala-2.12\velograph-spark.jar'
if (-not (Test-Path $jar)) { throw "Missing $jar - run 'sbt assembly' in spark\ first." }

Write-Host '1/5 Cluster: Kafka x3, Kafka UI, Spark master + 2 workers'
Invoke-Native { docker compose -f $compose --profile kafka --profile spark up -d } | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'docker compose up failed' }
$services = 'velograph-kafka-1', 'velograph-kafka-2', 'velograph-kafka-3', 'velograph-spark-master'
foreach ($i in 1..60) {
    $health = $services | ForEach-Object { Invoke-Native { docker inspect -f '{{.State.Health.Status}}' $_ } }
    if (@($health | Where-Object { $_ -ne 'healthy' }).Count -eq 0) { break }
    if ($i -eq 60) { throw "The cluster did not become healthy: $($health -join ', ')" }
    Start-Sleep -Seconds 2
}
# The topic bootstrap runs once per 'up'; wait for it, so the producer never writes to a missing topic.
foreach ($i in 1..60) {
    $init = Invoke-Native { docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' velograph-kafka-init }
    if ($init -eq 'exited 0') { break }
    if ($init -like 'exited *') { throw "Topic bootstrap failed ($init): docker logs velograph-kafka-init" }
    Start-Sleep -Seconds 2
}

# "Live" means a snapshot written by this run: anything newer than the file a previous run left behind.
$snapshotFile = Join-Path $data 'serving\live\snapshot.json'
$before = if (Test-Path $snapshotFile) { (Get-Content $snapshotFile -Raw | ConvertFrom-Json).meta.generatedAt } else { '' }
$ids = [ordered]@{}
Write-Host '2/5 Producer, 3/5 live scoring, 4/5 gateway, 5/5 website'
$producerArgs = @('-m', 'velograph.ingest.gbfs_producer')
if ($Replay) { $producerArgs += @('--source', 'recorded', '--day', $Day, '--start', $Start, '--speed', $Speed) }
$ids.producer = Start-Hidden 'producer' $pythonw $producerArgs (Join-Path $root 'python')

# -Command, not -File: a script started with -File receives '--' as a parameter name, not as the end of its
# options. One pre-quoted string, because Start-Process joins an argument array without quoting it.
$jobArgs = "--duration-min $DurationMin --fresh" + $(if ($Replay) { ' --source replay' } else { '' })
$submit = Join-Path $PSScriptRoot 'submit.ps1'
$scoringArgs = "-NoProfile -ExecutionPolicy Bypass -Command `"& '$submit' live-station-scoring -Mode cluster -- $jobArgs`""
$ids.scoring = Start-Hidden 'scoring' 'powershell.exe' @($scoringArgs) $root

# Under a restart loop: the Kafka client can crash natively during a broker outage, and the website reconnects
# to a gateway that comes back.
$gatewayLoop = "while (`$true) { & '$python' -m uvicorn velograph.api.main:app --port 8000; if (`$LASTEXITCODE -eq 0) { break }; Start-Sleep -Seconds 2 }"
$ids.gateway = Start-Hidden 'gateway' 'powershell.exe' @("-NoProfile -ExecutionPolicy Bypass -Command `"$gatewayLoop`"") (Join-Path $root 'python')

$web = Join-Path $root 'web'
if (-not (Test-Path (Join-Path $web '.next\BUILD_ID'))) {
    Write-Host '  building the website first (pnpm build)'
    Push-Location $web
    try { Invoke-Native { pnpm -s build } | Out-Null } finally { Pop-Location }
    if ($LASTEXITCODE -ne 0) { throw 'pnpm build failed' }
}
$ids.web = Start-Hidden 'web' 'cmd.exe' @('/c', 'pnpm start') $web
$ids | ConvertTo-Json | Set-Content -Encoding ascii $state

Write-Host 'Waiting for the first micro-batch of this run...'
foreach ($i in 1..120) {
    $snap = Get-Json 'http://localhost:8000/api/live/snapshot'
    $site = try { (Invoke-WebRequest -Uri 'http://localhost:3000/live' -UseBasicParsing -TimeoutSec 5).StatusCode } catch { 0 }
    $fresh = $snap -and $snap.meta -and ($snap.meta.generatedAt -ne $before)
    if ($fresh -and $site -eq 200) { break }
    if ($i -eq 120) { throw "No fresh micro-batch after 6 minutes: see $logDir\demo-scoring.log" }
    Start-Sleep -Seconds 3
}
$elapsed = ((Get-Date) - $t0).TotalSeconds
Write-Host ("Live in {0:N0} s: batch {1}, {2} stations, source {3}." -f $elapsed, $snap.data.batchId, $snap.data.kpis.stations, $snap.data.source)
Write-Host @'

  Website      http://localhost:3000        (the live map: http://localhost:3000/live)
  Gateway      http://localhost:8000/api/health
  Spark master http://localhost:8080        (the running app: http://localhost:4040)
  Kafka UI     http://localhost:8089

  Stop with: .\scripts\demo.ps1 -Stop   (add -Down to stop the containers)
'@
