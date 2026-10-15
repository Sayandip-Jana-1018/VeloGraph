<#
.SYNOPSIS
    One green/red checklist for the VeloGraph workstation, cluster and data (roadmap P0.7).

        .\scripts\doctor.ps1                # everything, including a live Kafka round trip
        .\scripts\doctor.ps1 -SkipCluster   # toolchain and data only (cluster intentionally down)
        .\scripts\doctor.ps1 -Quick         # skip the Kafka round trip

.DESCRIPTION
    Required checks print [ok] or [FAIL]; optional ones print [warn] instead of failing. Exit code 1 when any
    required check fails, so other scripts can gate on it.
#>
[CmdletBinding()]
param([switch]$SkipCluster, [switch]$Quick)

$root = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'env.ps1') -Quiet
# Format numbers the same way as the Python and Scala logs (1,234,567) regardless of the Windows locale.
[System.Threading.Thread]::CurrentThread.CurrentCulture = [System.Globalization.CultureInfo]::InvariantCulture
$dataRoot = $env:VELOGRAPH_DATA_ROOT
$results = New-Object System.Collections.Generic.List[object]

function Test-Item {
    param([string]$Group, [string]$Name, [scriptblock]$Probe, [switch]$Optional)
    try {
        $detail = & $Probe
        $ok = $true
    } catch {
        $detail = $_.Exception.Message
        $ok = $false
    }
    $results.Add([pscustomobject]@{ Group = $Group; Name = $Name; Ok = $ok; Optional = [bool]$Optional; Detail = "$detail" })
}

function Assert-That([bool]$Condition, [string]$Message) { if (-not $Condition) { throw $Message } }

function ConvertTo-MiB([string]$Text) {
    if ($Text -match '([\d.]+)\s*(KiB|MiB|GiB|B)') {
        $value = [double]$Matches[1]
        switch ($Matches[2]) { 'GiB' { return $value * 1024 } 'KiB' { return $value / 1024 } 'B' { return $value / 1MB } default { return $value } }
    }
    return 0
}

# --- Toolchain ---------------------------------------------------------------------------------------------
Test-Item 'toolchain' 'JDK 17' {
    $release = Get-Content (Join-Path $env:JAVA_HOME 'release') -Raw
    Assert-That ($release -match 'JAVA_VERSION="(17[^"]*)"') "JAVA_HOME is not a JDK 17: $env:JAVA_HOME"
    "Temurin $($Matches[1])"
}
Test-Item 'toolchain' 'sbt' {
    Assert-That (Test-Path (Join-Path $env:USERPROFILE '.local\sbt-1.12.15\bin\sbt.bat')) 'sbt 1.12.15 not found'
    (Get-Content (Join-Path $root 'spark\project\build.properties') -Raw).Trim()
}
Test-Item 'toolchain' 'Spark (host)' {
    $release = Get-Content (Join-Path $env:SPARK_HOME 'RELEASE') -TotalCount 1
    Assert-That ($release -match 'Spark 3\.5\.8') "expected Spark 3.5.8, found: $release"
    $release
}
Test-Item 'toolchain' 'winutils + hadoop.dll' {
    foreach ($f in 'winutils.exe', 'hadoop.dll') {
        Assert-That (Test-Path (Join-Path $env:HADOOP_HOME "bin\$f")) "$f missing in $env:HADOOP_HOME\bin"
    }
    $env:HADOOP_HOME
}
Test-Item 'toolchain' 'Python venv 3.11' {
    $version = & $env:PYSPARK_PYTHON -c 'import sys; print(sys.version.split()[0])'
    Assert-That ($version -like '3.11.*') "venv Python is $version"
    $version
}
Test-Item 'toolchain' 'PySpark 3.5.8' {
    $version = & $env:PYSPARK_PYTHON -c 'import pyspark; print(pyspark.__version__)'
    Assert-That ($version -eq '3.5.8') "pyspark not installed (run: cd python; uv sync --all-groups)"
    $version
}
Test-Item 'toolchain' 'Fat jar' {
    $jar = Get-Item (Join-Path $root 'spark\target\scala-2.12\velograph-spark.jar') -ErrorAction Stop
    '{0:N1} MB, built {1:yyyy-MM-dd HH:mm}' -f ($jar.Length / 1MB), $jar.LastWriteTime
}
Test-Item 'toolchain' 'Node + pnpm (web, P6)' -Optional {
    "node $(node --version), pnpm $(pnpm --version)"
}

# --- Cluster -----------------------------------------------------------------------------------------------
if (-not $SkipCluster) {
    Test-Item 'cluster' 'Docker engine' {
        $info = docker info --format '{{.ServerVersion}}|{{.MemTotal}}|{{.NCPU}}'
        Assert-That ($LASTEXITCODE -eq 0) 'Docker engine not reachable - start Docker Desktop'
        $server, $mem, $cpus = $info -split '\|'
        'Docker {0}, VM {1:N1} GB, {2} CPUs' -f $server, ([double]$mem / 1GB), $cpus
    }
    $expected = 'velograph-kafka-1', 'velograph-kafka-2', 'velograph-kafka-3', 'velograph-kafka-ui',
                'velograph-spark-master', 'velograph-spark-worker-1', 'velograph-spark-worker-2'
    Test-Item 'cluster' 'Containers running' {
        $running = docker ps --filter 'name=velograph-' --format '{{.Names}}|{{.Status}}'
        $status = @{}
        foreach ($line in $running) { $name, $state = $line -split '\|', 2; $status[$name] = $state }
        $missing = $expected | Where-Object { -not $status.ContainsKey($_) }
        Assert-That (-not $missing) "not running: $($missing -join ', ')"
        $unhealthy = $expected | Where-Object { $status[$_] -match 'unhealthy|starting' }
        Assert-That (-not $unhealthy) "not healthy yet: $($unhealthy -join ', ')"
        "$($expected.Count) up"
    }
    Test-Item 'cluster' 'Spark master + 2 workers' {
        $master = Invoke-RestMethod -Uri 'http://localhost:8080/json/' -TimeoutSec 5
        $alive = @($master.workers | Where-Object { $_.state -eq 'ALIVE' })
        Assert-That ($alive.Count -eq 2) "expected 2 ALIVE workers, found $($alive.Count)"
        '{0} ({1} cores, {2} MB for executors)' -f $master.status, $master.cores, $master.memory
    }
    Test-Item 'cluster' 'Kafka: 3 brokers online' {
        $cluster = @(Invoke-RestMethod -Uri 'http://localhost:8089/api/clusters' -TimeoutSec 5)[0]
        Assert-That ($cluster.status -eq 'ONLINE' -and $cluster.brokerCount -eq 3) "status $($cluster.status), brokers $($cluster.brokerCount)"
        "$($cluster.brokerCount) brokers, $($cluster.onlinePartitionCount) partitions online"
    }
    Test-Item 'cluster' 'Kafka topics' {
        $topics = (Invoke-RestMethod -Uri 'http://localhost:8089/api/clusters/velograph/topics?perPage=100' -TimeoutSec 5).topics.name
        $wanted = 'velograph.gbfs.station-status.v1', 'velograph.weather.current.v1', 'velograph.trips.replay.v1',
                  'velograph.trip-metrics.v1', 'velograph.predictions.v1', 'velograph.alerts.v1', 'velograph.dlq.v1'
        $missing = $wanted | Where-Object { $topics -notcontains $_ }
        Assert-That (-not $missing) "missing: $($missing -join ', ') (re-run: docker start -a velograph-kafka-init)"
        "$($wanted.Count) VeloGraph topics"
    }
    if (-not $Quick) {
        Test-Item 'cluster' 'Kafka round trip (host)' {
            Push-Location (Join-Path $root 'python')
            try {
                $out = & $env:PYSPARK_PYTHON -m velograph.ops.kafka_smoke --messages 100 2>&1 | Out-String
                Assert-That ($LASTEXITCODE -eq 0) "round trip failed: $($out.Trim() -split "`n" | Select-Object -Last 1)"
            } finally { Pop-Location }
            '100 messages, acks=all, RF 3'
        }
    }
    Test-Item 'cluster' 'Docker memory in use' {
        $usage = docker stats --no-stream --format '{{.Name}}|{{.MemUsage}}' | Where-Object { $_ -like 'velograph-*' }
        $total = 0
        foreach ($line in $usage) { $total += ConvertTo-MiB (($line -split '\|')[1] -split '/')[0] }
        Assert-That ($total -lt 5600) ('{0:N0} MiB is close to the 6 GB VM limit' -f $total)
        '{0:N2} GiB across {1} containers' -f ($total / 1024), @($usage).Count
    }
}

# --- Data --------------------------------------------------------------------------------------------------
Test-Item 'data' 'Free disk space' {
    $drive = Get-PSDrive -Name ((Resolve-Path $root).Drive.Name)
    $freeGb = $drive.Free / 1GB
    Assert-That ($freeGb -gt 20) ('only {0:N0} GB free' -f $freeGb)
    '{0:N0} GB free on {1}:' -f $freeGb, $drive.Name
}
Test-Item 'data' 'Trip months downloaded' -Optional {
    $manifest = Get-Content (Join-Path $dataRoot 'raw\citibike\manifest.json') -Raw | ConvertFrom-Json
    $months = @($manifest.months.PSObject.Properties | Sort-Object Name)
    $rows = ($months | ForEach-Object { $_.Value.rows } | Measure-Object -Sum).Sum
    Assert-That ($months.Count -ge 4) ("{0} of 4 months so far ({1:N0} rows)" -f $months.Count, $rows)
    '{0} ({1:N0} rows)' -f (($months | ForEach-Object Name) -join ', '), $rows
}
Test-Item 'data' 'GBFS recorder' -Optional {
    $pidFile = Join-Path $dataRoot 'run\gbfs_recorder.pid'
    Assert-That (Test-Path $pidFile) 'not running (start: .\scripts\recorder.ps1 start)'
    $info = Get-Content $pidFile -Raw | ConvertFrom-Json
    Assert-That ([bool](Get-Process -Id $info.pid -ErrorAction SilentlyContinue)) "stale PID file (pid $($info.pid))"
    $latest = Get-ChildItem (Join-Path $dataRoot 'raw\gbfs_recordings') -Recurse -Filter 'station_status.jsonl.gz' |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    $age = ((Get-Date) - $latest.LastWriteTime).TotalSeconds
    Assert-That ($age -lt 300) ('last write {0:N0} s ago' -f $age)
    'pid {0}, last write {1:N0} s ago' -f $info.pid, $age
}
Test-Item 'data' 'Smoke job report' -Optional {
    $report = Get-Content (Join-Path $dataRoot 'serving\jobs\smoke.json') -Raw | ConvertFrom-Json
    Assert-That ($report.data.status -eq 'succeeded') "last smoke run: $($report.data.status)"
    '{0} on {1} in {2:N0} ms' -f $report.data.status, $report.data.master, $report.data.durationMs
}

# --- Report ------------------------------------------------------------------------------------------------
$failed = 0
foreach ($group in ($results | Select-Object -ExpandProperty Group -Unique)) {
    Write-Host "`n$group" -ForegroundColor Cyan
    foreach ($r in ($results | Where-Object Group -eq $group)) {
        if ($r.Ok) { $mark, $color = '[ok]  ', 'Green' }
        elseif ($r.Optional) { $mark, $color = '[warn]', 'Yellow' }
        else { $mark, $color = '[FAIL]', 'Red'; $failed++ }
        Write-Host ("  {0} {1,-28} {2}" -f $mark, $r.Name, $r.Detail) -ForegroundColor $color
    }
}
Write-Host ''
if ($failed -gt 0) { Write-Host "$failed required check(s) failed." -ForegroundColor Red; exit 1 }
Write-Host 'All required checks passed.' -ForegroundColor Green
