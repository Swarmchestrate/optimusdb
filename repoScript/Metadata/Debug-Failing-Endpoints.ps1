<#
.SYNOPSIS
    Debug the failing metadata endpoints
#>

Write-Host "`n=== Debugging Health Check Endpoint ===" -ForegroundColor Yellow

try {
    $response = Invoke-WebRequest -Uri "http://localhost:18001/api/v1/metadata/health" -ErrorAction Stop
    Write-Host "✓ Success!" -ForegroundColor Green
    $response.Content | ConvertFrom-Json | ConvertTo-Json -Depth 10
} catch {
    Write-Host "✗ Failed with status: $($_.Exception.Response.StatusCode.value__)" -ForegroundColor Red
    Write-Host "Error details:" -ForegroundColor Red
    $_.Exception.Message

    # Get the error response body
    $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
    $reader.BaseStream.Position = 0
    $reader.DiscardBufferedData()
    $errorBody = $reader.ReadToEnd()
    Write-Host "`nServer Error Message:" -ForegroundColor Red
    Write-Host $errorBody -ForegroundColor Red
}

Write-Host "`n=== Debugging Profile Endpoint ===" -ForegroundColor Yellow

try {
    $response = Invoke-WebRequest -Uri "http://localhost:18001/api/v1/metadata/profile?db=optimusdb.db&table=products" -ErrorAction Stop
    Write-Host "✓ Success!" -ForegroundColor Green
    $response.Content | ConvertFrom-Json | ConvertTo-Json -Depth 10
} catch {
    Write-Host "✗ Failed with status: $($_.Exception.Response.StatusCode.value__)" -ForegroundColor Red
    Write-Host "Error details:" -ForegroundColor Red
    $_.Exception.Message

    # Get the error response body
    $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
    $reader.BaseStream.Position = 0
    $reader.DiscardBufferedData()
    $errorBody = $reader.ReadToEnd()
    Write-Host "`nServer Error Message:" -ForegroundColor Red
    Write-Host $errorBody -ForegroundColor Red
}

Write-Host "`n=== Checking Docker Logs for Errors ===" -ForegroundColor Yellow
docker logs optimusdb1 --tail 50 | Select-String -Pattern "metadata|error|panic" -Context 2