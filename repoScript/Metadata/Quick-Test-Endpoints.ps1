<#
.SYNOPSIS
    Quick endpoint availability checker
#>

param(
    [string]$BaseUrl = "http://localhost:18001"
)

$endpoints = @(
    @{Method="GET"; Path="/api/v1/metadata/health"; Name="Health Check"},
    @{Method="GET"; Path="/api/v1/metadata/metrics"; Name="Metrics"},
    @{Method="GET"; Path="/api/v1/metadata/profile?db=optimusdb.db&table=products"; Name="Profile"},
    @{Method="DELETE"; Path="/api/v1/metadata/cache"; Name="Clear Cache"},
    @{Method="GET"; Path="/swarmkb/peers"; Name="Peers (Baseline)"}
)

Write-Host "`n╔════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║  OptimusDB Endpoint Availability Test ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════╝`n" -ForegroundColor Cyan

$available = 0
$unavailable = 0

foreach ($endpoint in $endpoints) {
    $url = $BaseUrl + $endpoint.Path

    Write-Host "Testing: $($endpoint.Name)..." -NoNewline

    try {
        if ($endpoint.Method -eq "DELETE") {
            $response = Invoke-WebRequest -Uri $url -Method Delete -ErrorAction Stop
        } else {
            $response = Invoke-WebRequest -Uri $url -Method Get -ErrorAction Stop
        }

        if ($response.StatusCode -eq 200) {
            Write-Host " ✓ OK (200)" -ForegroundColor Green
            $available++
        } else {
            Write-Host " ✗ Unexpected Status: $($response.StatusCode)" -ForegroundColor Yellow
            $unavailable++
        }
    } catch {
        $statusCode = $_.Exception.Response.StatusCode.value__
        if ($statusCode -eq 404) {
            Write-Host " ✗ NOT FOUND (404)" -ForegroundColor Red
        } else {
            Write-Host " ✗ Error: $statusCode" -ForegroundColor Red
        }
        $unavailable++
    }
}

Write-Host "`n────────────────────────────────────────" -ForegroundColor Gray
Write-Host "Available:   $available" -ForegroundColor Green
Write-Host "Unavailable: $unavailable" -ForegroundColor Red
Write-Host "────────────────────────────────────────`n" -ForegroundColor Gray

if ($unavailable -gt 0) {
    Write-Host "⚠ Some endpoints are not available!" -ForegroundColor Yellow
    Write-Host "This usually means the metadata router is not properly mounted.`n" -ForegroundColor Yellow
} else {
    Write-Host "✓ All endpoints are available!`n" -ForegroundColor Green
}