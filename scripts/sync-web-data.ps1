<#
.SYNOPSIS
    Copy the serving exports, screenshots and code excerpts into the website (web\public, web\src\generated).

        .\scripts\sync-web-data.ps1

.DESCRIPTION
    The same as `cd web; pnpm sync`: run it after any job that writes data\serving, then rebuild or reload the site.
#>
$ErrorActionPreference = 'Stop'
Push-Location (Join-Path (Split-Path -Parent $PSScriptRoot) 'web')
try {
    & pnpm -s sync
    if ($LASTEXITCODE -ne 0) { throw "pnpm sync failed (exit code $LASTEXITCODE)" }
} finally {
    Pop-Location
}
