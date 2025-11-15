<#
.SYNOPSIS
    OptimusDB Metadata Endpoints Test Suite for Windows PowerShell
.DESCRIPTION
    Complete test suite for testing OptimusDB metadata enrichment endpoints
.PARAMETER BaseUrl
    Base URL for OptimusDB API (default: http://localhost:18001)
.PARAMETER TinyLlamaPort
    TinyLlama port if exposed (default: 18081)
.PARAMETER NodeNumber
    Node number to test (1-8, default: 1)
.EXAMPLE
    .\Test-OptimusDB-Metadata.ps1
.EXAMPLE
    .\Test-OptimusDB-Metadata.ps1 -NodeNumber 2
.EXAMPLE
    .\Test-OptimusDB-Metadata.ps1 -BaseUrl "http://localhost:18003" -NodeNumber 3
#>

param(
    [Parameter(Mandatory=$false)]
    [string]$BaseUrl = "http://localhost:18001",

    [Parameter(Mandatory=$false)]
    [int]$TinyLlamaPort = 18081,

    [Parameter(Mandatory=$false)]
    [ValidateRange(1,8)]
    [int]$NodeNumber = 1
)

# Color functions
function Write-Success {
    param([string]$Message)
    Write-Host "✓ PASS - $Message" -ForegroundColor Green
}

function Write-Failure {
    param([string]$Message)
    Write-Host "✗ FAIL - $Message" -ForegroundColor Red
}

function Write-Warning2 {
    param([string]$Message)
    Write-Host "⚠ WARN - $Message" -ForegroundColor Yellow
}

function Write-Info {
    param([string]$Message)
    Write-Host "ℹ INFO - $Message" -ForegroundColor Cyan
}

function Write-TestHeader {
    param([string]$TestName)
    Write-Host "`n============================================" -ForegroundColor Yellow
    Write-Host "Test: $TestName" -ForegroundColor Yellow
    Write-Host "============================================" -ForegroundColor Yellow
}

function Write-Section {
    param([string]$Title)
    Write-Host "`n--- $Title ---" -ForegroundColor Magenta
}

# API endpoints
$ApiPrefix = "/api/v1/metadata"
$SwarmKbPrefix = "/swarmkb"

# Container name
$ContainerName = "optimusdb$NodeNumber"

# Test results tracking
$script:TestsPassed = 0
$script:TestsFailed = 0
$script:TestsWarning = 0

Write-Host @"

╔═══════════════════════════════════════════════════════════╗
║   OptimusDB Metadata Endpoints Test Suite                ║
║   PowerShell Edition for Windows                         ║
╚═══════════════════════════════════════════════════════════╝

"@ -ForegroundColor Cyan

Write-Info "Testing Node: $NodeNumber"
Write-Info "Base URL: $BaseUrl"
Write-Info "Container: $ContainerName"
Write-Host ""

#region Pre-flight Checks
Write-TestHeader "Pre-flight Checks"

# Check if Docker is available
Write-Section "Checking Docker"
try {
    $dockerVersion = docker --version 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Docker is available: $dockerVersion"
    } else {
        Write-Failure "Docker is not available"
        exit 1
    }
} catch {
    Write-Failure "Docker command failed: $_"
    exit 1
}

# Check if container is running
Write-Section "Checking Container Status"
try {
    $containerStatus = docker ps --filter "name=$ContainerName" --format "{{.Status}}" 2>$null
    if ($containerStatus) {
        Write-Success "Container $ContainerName is running: $containerStatus"
    } else {
        Write-Failure "Container $ContainerName is not running"
        Write-Info "Start it with: docker start $ContainerName"
        exit 1
    }
} catch {
    Write-Failure "Failed to check container status: $_"
    exit 1
}

# Check port mappings
Write-Section "Checking Port Mappings"
try {
    $portMappings = docker port $ContainerName 2>$null
    if ($portMappings) {
        Write-Success "Port mappings found:"
        $portMappings | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
    } else {
        Write-Warning2 "No port mappings found or unable to retrieve"
    }
} catch {
    Write-Warning2 "Failed to check port mappings: $_"
}

# Check TinyLlama inside container
Write-Section "Checking TinyLlama (Inside Container)"
try {
    $llamaProcess = docker exec $ContainerName ps aux 2>$null | Select-String "llama"
    if ($llamaProcess) {
        Write-Success "TinyLlama process is running"
        Write-Host "  $($llamaProcess.Line)" -ForegroundColor Gray
    } else {
        Write-Warning2 "TinyLlama process not found"
    }

    # Health check inside container
    $healthCheck = docker exec $ContainerName curl -s http://127.0.0.1:8080/health 2>$null
    if ($healthCheck) {
        Write-Success "TinyLlama health check (internal): OK"
        Write-Host "  Response: $healthCheck" -ForegroundColor Gray
    } else {
        Write-Warning2 "TinyLlama health check (internal): Failed"
    }
} catch {
    Write-Warning2 "Failed to check TinyLlama: $_"
}

# Check TinyLlama from host (if port is exposed)
Write-Section "Checking TinyLlama (From Host)"
try {
    $tinyLlamaUrl = "http://localhost:$TinyLlamaPort/health"
    $tinyLlamaResponse = Invoke-RestMethod -Uri $tinyLlamaUrl -Method Get -TimeoutSec 5 -ErrorAction Stop
    Write-Success "TinyLlama accessible from host on port $TinyLlamaPort"
    $script:TinyLlamaExternal = $true
} catch {
    Write-Warning2 "TinyLlama NOT accessible from host on port $TinyLlamaPort"
    Write-Info "This is OK - OptimusDB will use it internally"
    $script:TinyLlamaExternal = $false
}

# Check if OptimusDB API is accessible
Write-Section "Checking OptimusDB API"
try {
    $apiHealthUrl = "$BaseUrl$SwarmKbPrefix/peers"
    $apiResponse = Invoke-RestMethod -Uri $apiHealthUrl -Method Get -TimeoutSec 5 -ErrorAction Stop
    Write-Success "OptimusDB API is accessible"
} catch {
    Write-Failure "OptimusDB API is NOT accessible at $BaseUrl"
    Write-Info "Error: $_"
    exit 1
}

#endregion

#region Test 1: Health Check
Write-TestHeader "Test 1: Metadata Service Health Check"

try {
    $healthUrl = "$BaseUrl$ApiPrefix/health"
    Write-Info "URL: $healthUrl"

    $response = Invoke-RestMethod -Uri $healthUrl -Method Get -ErrorAction Stop

    Write-Success "Health check endpoint accessible (HTTP 200)"

    # Display response
    Write-Host "`nResponse:" -ForegroundColor Cyan
    $response | ConvertTo-Json -Depth 10 | Write-Host -ForegroundColor Gray

    # Check TinyLlama status in response
    if ($response.tinyllama_status) {
        if ($response.tinyllama_status -eq "healthy") {
            Write-Success "TinyLlama status: healthy"
        } else {
            Write-Warning2 "TinyLlama status: $($response.tinyllama_status)"
        }
    }

    $script:TestsPassed++
} catch {
    Write-Failure "Health check failed"
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    $script:TestsFailed++
}
#endregion

#region Test 2: Direct TinyLlama Test (If Accessible)
if ($script:TinyLlamaExternal) {
    Write-TestHeader "Test 2: Direct TinyLlama Completion"

    try {
        $tinyLlamaUrl = "http://localhost:$TinyLlamaPort/v1/completions"
        $body = @{
            prompt = "Describe this product: Gaming laptop with RTX 4090"
            max_tokens = 30
            temperature = 0.7
        } | ConvertTo-Json

        Write-Info "URL: $tinyLlamaUrl"
        Write-Info "Sending completion request..."

        $response = Invoke-RestMethod -Uri $tinyLlamaUrl -Method Post -Body $body -ContentType "application/json" -ErrorAction Stop

        Write-Success "TinyLlama completion successful"
        Write-Host "`nGenerated text:" -ForegroundColor Cyan
        Write-Host $response.choices[0].text -ForegroundColor Gray

        $script:TestsPassed++
    } catch {
        Write-Failure "TinyLlama completion failed"
        Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
        $script:TestsFailed++
    }
}
#endregion

#region Test 3: Create Test Data
Write-TestHeader "Test 3: Create Test Data (If Needed)"

try {
    # Check if products table exists
    $checkTableSql = "SELECT name FROM sqlite_master WHERE type='table' AND name='products'"

    $checkBody = @{
        method = @{
            cmd = "sqldml"
            argcnt = 1
        }
        sqldml = $checkTableSql
    } | ConvertTo-Json -Depth 10

    $commandUrl = "$BaseUrl$SwarmKbPrefix/command"

    try {
        $tableCheck = Invoke-RestMethod -Uri $commandUrl -Method Post -Body $checkBody -ContentType "application/json" -ErrorAction Stop

        if ($tableCheck.data.records -and $tableCheck.data.records.Count -gt 0) {
            Write-Success "Products table exists"

            # Check row count
            $countSql = "SELECT COUNT(*) as count FROM products"
            $countBody = @{
                method = @{
                    cmd = "sqldml"
                    argcnt = 1
                }
                sqldml = $countSql
            } | ConvertTo-Json -Depth 10

            $countResponse = Invoke-RestMethod -Uri $commandUrl -Method Post -Body $countBody -ContentType "application/json" -ErrorAction Stop
            $rowCount = $countResponse.data.records[0].count

            Write-Info "Products table has $rowCount rows"

            if ($rowCount -lt 3) {
                Write-Warning2 "Few rows in products table, inserting test data..."

                # Insert test data
                $insertSql = @"
INSERT INTO products (name, description, category, price) VALUES
('Gaming Laptop Pro', 'High-performance gaming laptop with RTX 4090, Intel i9, 32GB RAM, 2TB SSD', 'Electronics', 2999.99),
('Wireless Mouse X1', 'Ergonomic wireless mouse with 16000 DPI sensor, RGB lighting', 'Accessories', 79.99),
('Mechanical Keyboard', 'Professional mechanical keyboard with Cherry MX switches', 'Accessories', 149.99)
"@

                $insertBody = @{
                    method = @{
                        cmd = "sqldml"
                        argcnt = 1
                    }
                    sqldml = $insertSql
                } | ConvertTo-Json -Depth 10

                $insertResponse = Invoke-RestMethod -Uri $commandUrl -Method Post -Body $insertBody -ContentType "application/json" -ErrorAction Stop
                Write-Success "Test data inserted"
            }
        } else {
            Write-Warning2 "Products table does not exist, creating..."

            # Create table
            $createTableSql = @"
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    category TEXT,
    price REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"@

            $createBody = @{
                method = @{
                    cmd = "sqldml"
                    argcnt = 1
                }
                sqldml = $createTableSql
            } | ConvertTo-Json -Depth 10

            $createResponse = Invoke-RestMethod -Uri $commandUrl -Method Post -Body $createBody -ContentType "application/json" -ErrorAction Stop
            Write-Success "Products table created"

            # Insert test data
            $insertSql = @"
INSERT INTO products (name, description, category, price) VALUES
('Gaming Laptop Pro', 'High-performance gaming laptop with RTX 4090, Intel i9, 32GB RAM, 2TB SSD', 'Electronics', 2999.99),
('Wireless Mouse X1', 'Ergonomic wireless mouse with 16000 DPI sensor, RGB lighting', 'Accessories', 79.99),
('Mechanical Keyboard', 'Professional mechanical keyboard with Cherry MX switches', 'Accessories', 149.99)
"@

            $insertBody = @{
                method = @{
                    cmd = "sqldml"
                    argcnt = 1
                }
                sqldml = $insertSql
            } | ConvertTo-Json -Depth 10

            $insertResponse = Invoke-RestMethod -Uri $commandUrl -Method Post -Body $insertBody -ContentType "application/json" -ErrorAction Stop
            Write-Success "Test data inserted"
        }

        $script:TestsPassed++
    } catch {
        Write-Failure "Failed to check/create test data"
        Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
        $script:TestsFailed++
    }
} catch {
    Write-Failure "Test data setup failed"
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    $script:TestsFailed++
}
#endregion

#region Test 4: Profile Dataset
Write-TestHeader "Test 4: Profile Dataset"

try {
    $profileUrl = "$BaseUrl$ApiPrefix/profile?db=optimusdb.db&table=products"
    Write-Info "URL: $profileUrl"

    $response = Invoke-RestMethod -Uri $profileUrl -Method Get -ErrorAction Stop

    Write-Success "Profile retrieved successfully (HTTP 200)"

    # Display key information
    Write-Host "`nDataset Profile:" -ForegroundColor Cyan
    Write-Host "  Dataset: $($response.dataset)" -ForegroundColor Gray
    Write-Host "  Row Count: $($response.row_count)" -ForegroundColor Gray
    Write-Host "  Column Count: $($response.columns.Count)" -ForegroundColor Gray

    if ($response.metadata_coverage) {
        Write-Host "  Metadata Coverage: $([math]::Round($response.metadata_coverage * 100, 2))%" -ForegroundColor Gray
    }

    # Show full response
    Write-Host "`nFull Response:" -ForegroundColor Cyan
    $response | ConvertTo-Json -Depth 10 | Write-Host -ForegroundColor Gray

    $script:TestsPassed++
} catch {
    Write-Failure "Profile retrieval failed"
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ErrorDetails.Message) {
        Write-Host "Details: $($_.ErrorDetails.Message)" -ForegroundColor Red
    }
    $script:TestsFailed++
}
#endregion

#region Test 5: Get Metrics
Write-TestHeader "Test 5: Get Enrichment Metrics"

try {
    $metricsUrl = "$BaseUrl$ApiPrefix/metrics"
    Write-Info "URL: $metricsUrl"

    $response = Invoke-RestMethod -Uri $metricsUrl -Method Get -ErrorAction Stop

    Write-Success "Metrics retrieved successfully (HTTP 200)"

    # Display response
    Write-Host "`nMetrics:" -ForegroundColor Cyan
    $response | ConvertTo-Json -Depth 10 | Write-Host -ForegroundColor Gray

    $script:TestsPassed++
} catch {
    Write-Failure "Metrics retrieval failed"
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    $script:TestsFailed++
}
#endregion

#region Test 6: Enrich Single Dataset
Write-TestHeader "Test 6: Enrich Single Dataset (5 rows)"

try {
    $enrichUrl = "$BaseUrl$ApiPrefix/enrich"

    $body = @{
        db = "optimusdb.db"
        table = "products"
        maxRows = 5
        greek = $false
    } | ConvertTo-Json -Depth 10

    Write-Info "URL: $enrichUrl"
    Write-Info "Enriching up to 5 rows (this may take 20-30 seconds)..."
    Write-Host "Please wait..." -ForegroundColor Yellow

    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

    $response = Invoke-RestMethod -Uri $enrichUrl -Method Post -Body $body -ContentType "application/json" -TimeoutSec 120 -ErrorAction Stop

    $stopwatch.Stop()

    Write-Success "Enrichment completed in $($stopwatch.Elapsed.TotalSeconds) seconds"

    # Display response
    Write-Host "`nEnrichment Results:" -ForegroundColor Cyan
    $response | ConvertTo-Json -Depth 10 | Write-Host -ForegroundColor Gray

    if ($response.rows_processed) {
        Write-Info "Rows processed: $($response.rows_processed)"
    }

    if ($response.processing_time_ms) {
        Write-Info "Processing time: $($response.processing_time_ms) ms"
    }

    if ($response.tinyllama_available -ne $null) {
        if ($response.tinyllama_available) {
            Write-Success "TinyLlama was available during enrichment"
        } else {
            Write-Warning2 "TinyLlama was NOT available (fallback metadata used)"
        }
    }

    $script:TestsPassed++
} catch {
    Write-Failure "Enrichment failed"
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ErrorDetails.Message) {
        Write-Host "Details: $($_.ErrorDetails.Message)" -ForegroundColor Red
    }
    $script:TestsFailed++
}
#endregion

#region Test 7: Verify Enriched Metadata
Write-TestHeader "Test 7: Verify Enriched Metadata"

try {
    $commandUrl = "$BaseUrl$SwarmKbPrefix/command"

    $body = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "kbmetadata"
        criteria = @(
            @{
                component = "products"
            }
        )
    } | ConvertTo-Json -Depth 10

    Write-Info "Querying metadata from OrbitDB..."

    $response = Invoke-RestMethod -Uri $commandUrl -Method Post -Body $body -ContentType "application/json" -ErrorAction Stop

    if ($response.data -and $response.data.Count -gt 0) {
        Write-Success "Found $($response.data.Count) metadata entries in OrbitDB"

        # Show first entry
        Write-Host "`nSample Metadata Entry:" -ForegroundColor Cyan
        $response.data[0] | ConvertTo-Json -Depth 10 | Write-Host -ForegroundColor Gray
    } else {
        Write-Warning2 "No metadata entries found in OrbitDB"
    }

    $script:TestsPassed++
} catch {
    Write-Failure "Metadata verification failed"
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    $script:TestsFailed++
}
#endregion

#region Test 8: Batch Enrich
Write-TestHeader "Test 8: Batch Enrich Multiple Datasets"

try {
    $batchUrl = "$BaseUrl$ApiPrefix/enrich-batch"

    $body = @{
        datasets = @(
            @{
                db = "optimusdb.db"
                table = "products"
                maxRows = 3
            }
        )
        greek = $false
    } | ConvertTo-Json -Depth 10

    Write-Info "URL: $batchUrl"
    Write-Info "Batch enriching datasets (this may take time)..."

    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

    $response = Invoke-RestMethod -Uri $batchUrl -Method Post -Body $body -ContentType "application/json" -TimeoutSec 180 -ErrorAction Stop

    $stopwatch.Stop()

    Write-Success "Batch enrichment completed in $($stopwatch.Elapsed.TotalSeconds) seconds"

    # Display response
    Write-Host "`nBatch Enrichment Results:" -ForegroundColor Cyan
    $response | ConvertTo-Json -Depth 10 | Write-Host -ForegroundColor Gray

    $script:TestsPassed++
} catch {
    Write-Failure "Batch enrichment failed"
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ErrorDetails.Message) {
        Write-Host "Details: $($_.ErrorDetails.Message)" -ForegroundColor Red
    }
    $script:TestsFailed++
}
#endregion

#region Test 9: Clear Cache
Write-TestHeader "Test 9: Clear Metadata Cache"

try {
    $cacheUrl = "$BaseUrl$ApiPrefix/cache"
    Write-Info "URL: $cacheUrl"

    $response = Invoke-RestMethod -Uri $cacheUrl -Method Delete -ErrorAction Stop

    Write-Success "Cache cleared successfully"

    # Display response
    Write-Host "`nResponse:" -ForegroundColor Cyan
    $response | ConvertTo-Json -Depth 10 | Write-Host -ForegroundColor Gray

    $script:TestsPassed++
} catch {
    Write-Failure "Cache clear failed"
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    $script:TestsFailed++
}
#endregion

#region Test Summary
Write-Host "`n"
Write-Host "╔═══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                    TEST SUMMARY                           ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

Write-Host "Tests Passed:  " -NoNewline
Write-Host "$script:TestsPassed" -ForegroundColor Green

Write-Host "Tests Failed:  " -NoNewline
Write-Host "$script:TestsFailed" -ForegroundColor Red

Write-Host "Tests Warning: " -NoNewline
Write-Host "$script:TestsWarning" -ForegroundColor Yellow

$totalTests = $script:TestsPassed + $script:TestsFailed
Write-Host "Total Tests:   $totalTests"

if ($script:TestsFailed -eq 0) {
    Write-Host "`n✓ ALL TESTS PASSED!" -ForegroundColor Green
} else {
    Write-Host "`n✗ SOME TESTS FAILED" -ForegroundColor Red
}

Write-Host ""
#endregion