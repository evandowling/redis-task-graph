<#
.SYNOPSIS
Stops the OpenObserve server started by start-openobserve.ps1. Its data is kept.
#>
$exe = Join-Path $PSScriptRoot "openobserve\openobserve.exe"
$procs = Get-Process -Name openobserve -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $exe }
if (-not $procs) {
    Write-Host "OpenObserve is not running"
    exit 0
}
$procs | Stop-Process
Write-Host "Stopped OpenObserve (pid $($procs.Id -join ', '))"
