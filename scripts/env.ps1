<#
.SYNOPSIS
    Session-scoped environment for VeloGraph. Dot-source it from the repository root:

        . .\scripts\env.ps1

.DESCRIPTION
    Points JAVA_HOME at JDK 17 (Spark 3.5 supports Java 8/11/17; the machine default is JDK 21), puts sbt, Spark and
    winutils on PATH, and wires PySpark to the project virtual environment. Nothing is written to the registry or the
    user profile: closing the terminal undoes everything.

    Every location can be overridden with a VELOGRAPH_* variable set before dot-sourcing.
#>
[CmdletBinding()]
param([switch]$Quiet)

$veloRoot = Split-Path -Parent $PSScriptRoot

$env:VELOGRAPH_ROOT = $veloRoot
$env:VELOGRAPH_DATA_ROOT = if ($env:VELOGRAPH_DATA_ROOT) { $env:VELOGRAPH_DATA_ROOT } else { Join-Path $veloRoot 'data' }
$env:JAVA_HOME = if ($env:VELOGRAPH_JAVA_HOME) { $env:VELOGRAPH_JAVA_HOME } else { Join-Path $env:USERPROFILE '.jdks\temurin-17' }
$env:SPARK_HOME = if ($env:VELOGRAPH_SPARK_HOME) { $env:VELOGRAPH_SPARK_HOME } else { 'C:\spark' }
$env:HADOOP_HOME = if ($env:VELOGRAPH_HADOOP_HOME) { $env:VELOGRAPH_HADOOP_HOME } else { 'C:\hadoopsetup\hadoop-3.2.4' }
$veloSbtHome = if ($env:VELOGRAPH_SBT_HOME) { $env:VELOGRAPH_SBT_HOME } else { Join-Path $env:USERPROFILE '.local\sbt-1.12.15' }

# PySpark must use the project's Python 3.11, never the system 3.14 that PySpark 3.5 does not support.
$env:PYSPARK_PYTHON = Join-Path $veloRoot 'python\.venv\Scripts\python.exe'
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON

$veloPrepend = @(
    (Join-Path $env:JAVA_HOME 'bin'),
    (Join-Path $veloSbtHome 'bin'),
    (Join-Path $env:SPARK_HOME 'bin'),
    (Join-Path $env:HADOOP_HOME 'bin')
)
$veloRest = $env:PATH -split ';' | Where-Object { $_ -and ($veloPrepend -notcontains $_) }
$env:PATH = ($veloPrepend + $veloRest) -join ';'

if (-not $Quiet) {
    $veloChecks = [ordered]@{
        'JDK 17 (JAVA_HOME)' = Join-Path $env:JAVA_HOME 'bin\java.exe'
        'sbt'                = Join-Path $veloSbtHome 'bin\sbt.bat'
        'Spark (SPARK_HOME)' = Join-Path $env:SPARK_HOME 'bin\spark-submit.cmd'
        'winutils'           = Join-Path $env:HADOOP_HOME 'bin\winutils.exe'
        'Python venv'        = $env:PYSPARK_PYTHON
    }
    Write-Host "VeloGraph environment (this session only) - root $veloRoot" -ForegroundColor Cyan
    foreach ($item in $veloChecks.GetEnumerator()) {
        $ok = Test-Path $item.Value
        $mark = if ($ok) { '[ok]     ' } else { '[missing]' }
        $color = if ($ok) { 'Green' } else { 'Yellow' }
        Write-Host ("  {0} {1,-20} {2}" -f $mark, $item.Key, $item.Value) -ForegroundColor $color
    }
}

Remove-Variable veloRoot, veloSbtHome, veloPrepend, veloRest -ErrorAction SilentlyContinue
if (Get-Variable veloChecks -ErrorAction SilentlyContinue) { Remove-Variable veloChecks }
