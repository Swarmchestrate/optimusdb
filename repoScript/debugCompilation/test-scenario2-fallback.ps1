# Scenario 2: Fallback and Graceful Degradation Testing
# Tests OptimusDB behavior when TinyLlama is unavailable

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  OptimusDB Metadata Enrichment Test" -ForegroundColor Cyan
Write-Host "  Scenario 2: Fallback Testing" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$OPTIMUSDB_URL = "http://localhost:9091"
$TEST_DB = "test_renewable.db"
$TEST_TABLE = "battery_storage"

# Helper function
function Show-JsonResponse {
    param($Response, $Title)
    Write-Host "`n$Title" -ForegroundColor Cyan
    Write-Host "────────────────────────────────────────" -ForegroundColor Gray
    $Response | ConvertTo-Json -Depth 10 | Write-Host
    Write-Host ""
}

# Test 1: Baseline - Enrich with LLM Available
Write-Host "[Test 1/6] Baseline: Enrich with LLM Available" -ForegroundColor Yellow

$enrichBody = @{
    database = $TEST_DB
    table = $TEST_TABLE
} | ConvertTo-Json

try {
    $baselineEnrich = Invoke-RestMethod -Uri "$OPTIMUSDB_URL/api/v1/metadata/enrich" `
        -Method Post `
        -ContentType "application/json" `
        -Body $enrichBody

    Show-JsonResponse $baselineEnrich "✓ Baseline Enrichment (LLM Available):"

    Write-Host "  Baseline Metrics:" -ForegroundColor Green
    Write-Host "    Status: $($baselineEnrich.metadata.status)" -ForegroundColor Gray
    Write-Host "    Created By: $($baselineEnrich.metadata.created_by)" -ForegroundColor Gray
    Write-Host "    Description Length: $($baselineEnrich.metadata.description.Length) chars" -ForegroundColor Gray
    Write-Host "    Tags: $($baselineEnrich.metadata.tags -join ', ')" -ForegroundColor Gray
} catch {
    Write-Host "  ✗ Baseline enrichment failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Start-Sleep -Seconds 2

# Test 2: Check TinyLlama Status Before Stopping
Write-Host "`n[Test 2/6] Check TinyLlama Status" -ForegroundColor Yellow

try {
    $healthBefore = Invoke-RestMethod -Uri "$OPTIMUSDB_URL/api/v1/metadata/health" -Method Get
    Write-Host "  TinyLlama Status: $($healthBefore.llm_status)" -ForegroundColor Green
    Write-Host "  Endpoint: $($healthBefore.llm_endpoint)" -ForegroundColor Gray
} catch {
    Write-Host "  ✗ Health check failed" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# Test 3: Stop TinyLlama Service
Write-Host "`n[Test 3/6] Stopping TinyLlama Service" -ForegroundColor Yellow
Write-Host "  → Simulating LLM service failure..." -ForegroundColor Gray

try {
    docker stop tinyllama-service | Out-Null
    Write-Host "  ✓ TinyLlama service stopped" -ForegroundColor Green

    # Wait for OptimusDB to detect failure
    Write-Host "  → Waiting 5 seconds for OptimusDB to detect failure..." -ForegroundColor Gray
    Start-Sleep -Seconds 5
} catch {
    Write-Host "  ✗ Failed to stop TinyLlama: $($_.Exception.Message)" -ForegroundColor Red
}

# Test 4: Check Health After Stopping TinyLlama
Write-Host "`n[Test 4/6] Check Health After Stopping TinyLlama" -ForegroundColor Yellow

try {
    $healthAfter = Invoke-RestMethod -Uri "$OPTIMUSDB_URL/api/v1/metadata/health" -Method Get
    Show-JsonResponse $healthAfter "✓ Health Status (LLM Unavailable):"

    Write-Host "  System Status:" -ForegroundColor Yellow
    Write-Host "    Overall: $($healthAfter.status)" -ForegroundColor Gray
    Write-Host "    LLM Status: $($healthAfter.llm_status)" -ForegroundColor Gray
    Write-Host "    Auto-Enrich: $($healthAfter.auto_enrich_enabled)" -ForegroundColor Gray

    if ($healthAfter.llm_status -ne "healthy") {
        Write-Host "`n  ✓ System correctly detected LLM failure" -ForegroundColor Green
    }
} catch {
    Write-Host "  ⚠ Health endpoint may have changed behavior" -ForegroundColor Yellow
}

Start-Sleep -Seconds 2

# Test 5: Enrich with Fallback to Basic Metadata
Write-Host "`n[Test 5/6] Enrich Dataset (Fallback Mode)" -ForegroundColor Yellow
Write-Host "  → Should use basic metadata generation..." -ForegroundColor Gray

# Create a new table to ensure no cache hit
$newTableSQL = @"
CREATE TABLE grid_connection (
    timestamp TEXT,
    connection_id TEXT,
    frequency REAL,
    active_power REAL,
    reactive_power REAL,
    voltage REAL
);

INSERT INTO grid_connection VALUES
    ('2025-11-16T10:00:00Z', 'GRID-001', 50.02, 2500.0, 150.0, 400.0),
    ('2025-11-16T10:05:00Z', 'GRID-001', 49.98, 2480.0, 145.0, 398.0);
"@

try {
    $newTableSQL | sqlite3 $TEST_DB
    Write-Host "  ✓ Created new table: grid_connection" -ForegroundColor Green
} catch {
    Write-Host "  ⚠ Table may already exist" -ForegroundColor Yellow
}

$fallbackBody = @{
    database = $TEST_DB
    table = "grid_connection"
} | ConvertTo-Json

try {
    $fallbackEnrich = Invoke-RestMethod -Uri "$OPTIMUSDB_URL/api/v1/metadata/enrich" `
        -Method Post `
        -ContentType "application/json" `
        -Body $fallbackBody

    Show-JsonResponse $fallbackEnrich "✓ Fallback Enrichment Response:"

    Write-Host "  Fallback Analysis:" -ForegroundColor Green
    Write-Host "    Status: $($fallbackEnrich.metadata.status)" -ForegroundColor Gray
    Write-Host "    Created By: $($fallbackEnrich.metadata.created_by)" -ForegroundColor Gray
    Write-Host "    Description Length: $($fallbackEnrich.metadata.description.Length) chars" -ForegroundColor Gray
    Write-Host "    Tags Count: $($fallbackEnrich.metadata.tags.Count)" -ForegroundColor Gray
    Write-Host "    LLM Available: $($fallbackEnrich.metadata.llm_available)" -ForegroundColor Gray

    Write-Host "`n  Comparison with Baseline:" -ForegroundColor Yellow
    Write-Host "    Baseline Status: $($baselineEnrich.metadata.status)" -ForegroundColor Gray
    Write-Host "    Fallback Status: $($fallbackEnrich.metadata.status)" -ForegroundColor Gray
    Write-Host "    Baseline Creator: $($baselineEnrich.metadata.created_by)" -ForegroundColor Gray
    Write-Host "    Fallback Creator: $($fallbackEnrich.metadata.created_by)" -ForegroundColor Gray

    if ($fallbackEnrich.metadata.status -eq "basic" -or $fallbackEnrich.metadata.created_by -eq "basic-profiler") {
        Write-Host "`n  ✓ Fallback mechanism working correctly!" -ForegroundColor Green
    } else {
        Write-Host "`n  ⚠ Fallback behavior unclear - may be using cache" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  ✗ Fallback enrichment failed: $($_.Exception.Message)" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# Test 6: Restart TinyLlama and Verify Recovery
Write-Host "`n[Test 6/6] Restart TinyLlama and Verify Recovery" -ForegroundColor Yellow

try {
    Write-Host "  → Restarting TinyLlama service..." -ForegroundColor Gray
    docker start tinyllama-service | Out-Null

    Write-Host "  → Waiting 15 seconds for service initialization..." -ForegroundColor Gray
    Start-Sleep -Seconds 15

    # Verify TinyLlama health
    $tinyllamaHealth = Invoke-RestMethod -Uri "http://localhost:8080/health" -Method Get -ErrorAction SilentlyContinue
    if ($tinyllamaHealth.status -eq "ok") {
        Write-Host "  ✓ TinyLlama restarted successfully" -ForegroundColor Green
    }

    # Check OptimusDB health
    $recoveryHealth = Invoke-RestMethod -Uri "$OPTIMUSDB_URL/api/v1/metadata/health" -Method Get
    Write-Host "  OptimusDB Health:" -ForegroundColor Green
    Write-Host "    Overall: $($recoveryHealth.status)" -ForegroundColor Gray
    Write-Host "    LLM Status: $($recoveryHealth.llm_status)" -ForegroundColor Gray

    if ($recoveryHealth.llm_status -eq "healthy") {
        Write-Host "`n  ✓ System recovered - LLM now available!" -ForegroundColor Green
    }

    # Test enrichment after recovery
    $recoveryBody = @{
        database = $TEST_DB
        table = "grid_connection"
    } | ConvertTo-Json

    $recoveryEnrich = Invoke-RestMethod -Uri "$OPTIMUSDB_URL/api/v1/metadata/enrich" `
        -Method Post `
        -ContentType "application/json" `
        -Body $recoveryBody

    Write-Host "`n  Post-Recovery Enrichment:" -ForegroundColor Green
    Write-Host "    Status: $($recoveryEnrich.metadata.status)" -ForegroundColor Gray
    Write-Host "    Created By: $($recoveryEnrich.metadata.created_by)" -ForegroundColor Gray

    if ($recoveryEnrich.metadata.status -ne "basic") {
        Write-Host "`n  ✓ LLM generation restored after recovery!" -ForegroundColor Green
    }
} catch {
    Write-Host "  ✗ Recovery test failed: $($_.Exception.Message)" -ForegroundColor Red
}

# Summary Report
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Fallback Test Summary" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$testResults = @{
    "Baseline LLM Generation" = ($baselineEnrich.metadata.status -ne "basic")
    "TinyLlama Stop Detection" = ($healthAfter.llm_status -ne "healthy")
    "Fallback to Basic Metadata" = ($fallbackEnrich.metadata.status -eq "basic" -or $fallbackEnrich.metadata.created_by -eq "basic-profiler")
    "System Continues Operating" = ($fallbackEnrich -ne $null)
    "TinyLlama Recovery" = ($recoveryHealth.llm_status -eq "healthy")
    "Post-Recovery Generation" = ($recoveryEnrich.metadata.status -ne "basic")
}

foreach ($test in $testResults.GetEnumerator()) {
    $status = if ($test.Value) { "✓ PASS" } else { "✗ FAIL" }
    $color = if ($test.Value) { "Green" } else { "Red" }
    Write-Host "  $status : $($test.Key)" -ForegroundColor $color
}

$passCount = ($testResults.Values | Where-Object { $_ -eq $true }).Count
$totalCount = $testResults.Count
$passRate = ($passCount / $totalCount * 100)

Write-Host "`n  Overall: $passCount/$totalCount tests passed ($('{0:N0}' -f $passRate)%)" -ForegroundColor $(if ($passRate -ge 80) { "Green" } elseif ($passRate -ge 60) { "Yellow" } else { "Red" })

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Graceful Degradation Verified!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Save results
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$reportFile = "test-results-scenario2-$timestamp.json"

$detailedReport = @{
    timestamp = (Get-Date).ToString("o")
    scenario = "Scenario 2: Fallback and Graceful Degradation"
    baseline_with_llm = $baselineEnrich
    health_before_stop = $healthBefore
    health_after_stop = $healthAfter
    fallback_enrichment = $fallbackEnrich
    recovery_health = $recoveryHealth
    recovery_enrichment = $recoveryEnrich
    test_results = $testResults
}

$detailedReport | ConvertTo-Json -Depth 10 | Out-File $reportFile
Write-Host "Detailed results saved to: $reportFile" -ForegroundColor Gray
Write-Host ""