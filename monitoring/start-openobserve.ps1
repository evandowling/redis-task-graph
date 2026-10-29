<#
.SYNOPSIS
Starts a local OpenObserve server for redis-task-graph metrics and imports the dashboard.

.DESCRIPTION
Downloads the pinned open-source OpenObserve Windows build into monitoring/openobserve on first
run, creates local credentials in monitoring/.env (gitignored), starts the server in the
background with its data in monitoring/data, imports monitoring/dashboards/*.json if they're
missing, and prints the OTEL_* environment variables that send metrics to it.

    powershell -ExecutionPolicy Bypass -File monitoring/start-openobserve.ps1
#>
param(
    [string]$Version = "v1.1.2",
    [int]$Port = 5080
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$binDir = Join-Path $root "openobserve"
$dataDir = Join-Path $root "data"
$envFile = Join-Path $root ".env"
$exe = Join-Path $binDir "openobserve.exe"
# 127.0.0.1, not localhost: OpenObserve listens on IPv4 only, and a refused IPv6 (::1) attempt
# costs about 2 s on Windows before the client falls back.
$base = "http://127.0.0.1:$Port"

# Local credentials, created once.
if (-not (Test-Path $envFile)) {
    # OpenObserve requires a lowercase letter, an uppercase letter, a digit and a symbol.
    $chars = (48..57) + (65..90) + (97..122) | Get-Random -Count 20 | ForEach-Object { [char]$_ }
    $password = (-join $chars) + "aA1-"
    "ZO_ROOT_USER_EMAIL=admin@localhost.local`nZO_ROOT_USER_PASSWORD=$password" | Set-Content -Encoding ascii $envFile
    Write-Host "Created $envFile with new local credentials."
}
$settings = @{}
Get-Content $envFile | Where-Object { $_ -match "^\s*([^#=]+)=(.*)$" } | ForEach-Object {
    $settings[$Matches[1].Trim()] = $Matches[2].Trim()
}
$email = $settings["ZO_ROOT_USER_EMAIL"]
$password = $settings["ZO_ROOT_USER_PASSWORD"]
$auth = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${email}:${password}"))
$headers = @{ Authorization = "Basic $auth" }

# Binary, downloaded once.
if (-not (Test-Path $exe)) {
    $url = "https://downloads.openobserve.ai/releases/openobserve/$Version/openobserve-$Version-windows-amd64.zip"
    $zip = Join-Path $env:TEMP "openobserve-$Version.zip"
    Write-Host "Downloading $url"
    Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile $zip
    New-Item -ItemType Directory -Force $binDir | Out-Null
    Expand-Archive -Force $zip $binDir
    Remove-Item $zip
    $found = Get-ChildItem $binDir -Recurse -Filter "openobserve*.exe" | Select-Object -First 1
    if ($found.FullName -ne $exe) { Move-Item $found.FullName $exe }
}

function Test-Healthy {
    try { (Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 "$base/healthz").StatusCode -eq 200 } catch { $false }
}

if (Test-Healthy) {
    Write-Host "OpenObserve is already running at $base"
} else {
    New-Item -ItemType Directory -Force $dataDir | Out-Null
    $env:ZO_ROOT_USER_EMAIL = $email
    $env:ZO_ROOT_USER_PASSWORD = $password
    $env:ZO_DATA_DIR = Join-Path $dataDir "openobserve"
    $env:ZO_HTTP_PORT = "$Port"
    # A detached wrapper does the log redirection, so the server never inherits this script's
    # output handles (which would keep a caller's pipe open for as long as the server runs).
    $log = Join-Path $dataDir "openobserve.log"
    $command = "& '$exe' *> '$log'"
    Start-Process -FilePath "powershell.exe" -WorkingDirectory $dataDir -WindowStyle Hidden `
        -ArgumentList "-NoProfile", "-Command", $command | Out-Null
    $deadline = (Get-Date).AddSeconds(60)
    while (-not (Test-Healthy)) {
        if ((Get-Date) -gt $deadline) {
            throw "OpenObserve did not become healthy; see $log"
        }
        Start-Sleep -Milliseconds 500
    }
    Write-Host "OpenObserve started at $base"
}

# Dashboards, imported if no dashboard with the same title exists yet.
$existing = (Invoke-RestMethod -Headers $headers "$base/api/default/dashboards").dashboards |
    ForEach-Object { if ($_.v5) { $_.v5.title } elseif ($_.title) { $_.title } }
Get-ChildItem (Join-Path $root "dashboards") -Filter *.json -ErrorAction SilentlyContinue | ForEach-Object {
    $body = Get-Content -Raw $_.FullName
    $title = ($body | ConvertFrom-Json).title
    if ($existing -contains $title) {
        Write-Host "Dashboard '$title' already exists"
    } else {
        Invoke-RestMethod -Method Post -Headers $headers -ContentType "application/json" `
            -Body ([Text.Encoding]::UTF8.GetBytes($body)) "$base/api/default/dashboards" | Out-Null
        Write-Host "Imported dashboard '$title'"
    }
}

Write-Host ""
Write-Host "UI: $base  (user $email, password in monitoring/.env)"
Write-Host "Send metrics with:"
Write-Host "  `$env:OTEL_EXPORTER_OTLP_ENDPOINT = '$base/api/default'"
Write-Host "  `$env:OTEL_EXPORTER_OTLP_HEADERS = 'Authorization=Basic%20$auth'"
