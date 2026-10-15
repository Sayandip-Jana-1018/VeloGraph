<#
.SYNOPSIS
    Start, stop or inspect the background GBFS recorder.

        .\scripts\recorder.ps1 start      # windowless; keeps running after this terminal closes
        .\scripts\recorder.ps1 status
        .\scripts\recorder.ps1 stop       # graceful: finishes the current poll first

.DESCRIPTION
    The recorder (python -m velograph.ingest.gbfs_recorder) polls the live Citi Bike feed every 60 s and writes
    gzipped JSONL under data\raw\gbfs_recordings\<date>\. It runs only while this laptop is awake and is not
    registered to start at login.
#>
[CmdletBinding()]
param(
    [ValidateSet('start', 'stop', 'status')][string]$Action = 'status',
    [int]$Interval = 60
)

$root = Split-Path -Parent $PSScriptRoot
$dataRoot = if ($env:VELOGRAPH_DATA_ROOT) { $env:VELOGRAPH_DATA_ROOT } else { Join-Path $root 'data' }
$runDir = Join-Path $dataRoot 'run'
$pidFile = Join-Path $runDir 'gbfs_recorder.pid'
$stopFile = Join-Path $runDir 'gbfs_recorder.stop'
$logFile = Join-Path $dataRoot 'logs\gbfs_recorder.log'
$pythonw = Join-Path $root 'python\.venv\Scripts\pythonw.exe'

function Get-RecorderProcess {
    if (-not (Test-Path $pidFile)) { return $null }
    $info = Get-Content $pidFile -Raw | ConvertFrom-Json
    return Get-Process -Id $info.pid -ErrorAction SilentlyContinue
}

switch ($Action) {
    'start' {
        $running = Get-RecorderProcess
        if ($running) { Write-Host "Recorder already running (pid $($running.Id))."; return }
        if (-not (Test-Path $pythonw)) { throw "Python venv missing: run 'uv sync' in python\ first." }
        New-Item -ItemType Directory -Force $runDir | Out-Null
        Remove-Item $stopFile -ErrorAction SilentlyContinue
        $env:VELOGRAPH_DATA_ROOT = $dataRoot
        Start-Process -FilePath $pythonw -WorkingDirectory (Join-Path $root 'python') -WindowStyle Hidden `
            -ArgumentList @('-m', 'velograph.ingest.gbfs_recorder', '--interval', $Interval)
        foreach ($i in 1..30) {
            Start-Sleep -Milliseconds 500
            $proc = Get-RecorderProcess
            if ($proc) { Write-Host "Recorder started (pid $($proc.Id)); log: $logFile"; return }
        }
        throw "Recorder did not report a PID within 15 s; check $logFile"
    }
    'stop' {
        $proc = Get-RecorderProcess
        if (-not $proc) { Write-Host 'Recorder is not running.'; return }
        New-Item -ItemType File -Force $stopFile | Out-Null
        if (-not $proc.WaitForExit(90000)) {
            Write-Warning 'Recorder did not stop within 90 s; terminating it.'
            Stop-Process -Id $proc.Id -Force
        }
        Write-Host 'Recorder stopped.'
    }
    'status' {
        $proc = Get-RecorderProcess
        if ($proc) {
            $info = Get-Content $pidFile -Raw | ConvertFrom-Json
            Write-Host "Recorder running (pid $($proc.Id)) since $($info.startedAt), every $($info.interval) s."
        } else {
            Write-Host 'Recorder is not running.'
        }
        $latest = Get-ChildItem (Join-Path $dataRoot 'raw\gbfs_recordings') -Recurse -Filter 'station_status.jsonl.gz' `
            -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($latest) {
            $age = [int]((Get-Date) - $latest.LastWriteTime).TotalSeconds
            Write-Host ("Latest recording: {0} ({1:N1} MB, written {2} s ago)" -f $latest.FullName, ($latest.Length / 1MB), $age)
        }
        if (Test-Path $logFile) { Get-Content $logFile -Tail 5 }
    }
}
