# OptimusDB Metadata Enrichment - Docker Deployment Test Script
# Tests metadata enrichment on Docker-deployed OptimusDB cluster

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  OptimusDB Metadata Enrichment Test" -ForegroundColor Cyan
Write-Host "  Docker Deployment Version" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Port mapping configuration
# Your deployment: -p 18001:8089 -p 18081:8080 -p 14001:4001 -p 15001:5001
# HTTP API is on port 8089 inside container, mapped to 18001-18008 outside
$AGENT_HTTP_PORTS = @(18001, 18002, 18003, 18004, 18005, 18006, 18007, 18008)

# Test configuration
$TEST_DB = "test_solar.db"
$TEST_TABLE = "solar_production"

# Helper function to display JSON
function Show-JsonResponse {
    param($Response, $Title)
    Write-Host "`n$Title" -ForegroundColor Cyan
    Write-Host "────────────────────────────────────────" -ForegroundColor Gray
    $Response | ConvertTo-Json -Depth 10 | Write-Host
    Write-Host ""
}

# Step 1: Discover running agents
Write-Host "[Step 1/9] Discovering running OptimusDB agents..." -ForegroundColor Yellow
Write-Host ""

$runningAgents = @()
foreach ($port in $AGENT_HTTP_PORTS) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:$port/api/v1/metadata/health" `
            -TimeoutSec 2 `
            -UseBasicParsing `
            -ErrorAction Stop
        $runningAgents += $port
        Write-Host "  ✓ Agent found on port $port" -ForegroundColor Green
    } catch {
        Write-Host "  ✗ No agent on port $port" -ForegroundColor Gray
    }
}

Write-Host "`n  Running Agents: $($runningAgents.Count)" -ForegroundColor $(if ($runningAgents.Count -gt 0) { "Green" } else { "Red" })

if ($runningAgents.Count -eq 0) {
    Write-Host "`n  ✗ No OptimusDB agents found!" -ForegroundColor Red
    Write-Host "  → Please start your Docker containers first:" -ForegroundColor Yellow
    Write-Host "    docker run -d --network=swarmnet --name=optimusdb1 -p 18001:8089 -p 18081:8080 -p 14001:4001 -p 15001:5001 optimusdb" -ForegroundColor Gray
    Write-Host "    docker run -d --network=swarmnet --name=optimusdb2 -p 18002:8089 -p 18082:8080 -p 14002:4001 -p 15002:5001 optimusdb" -ForegroundColor Gray
    Write-Host "    ... (and so on)" -ForegroundColor Gray
    exit 1
}

# Use first available agent for detailed testing
$PRIMARY_AGENT_PORT = $runningAgents[0]
Write-Host "`n  → Using agent on port $PRIMARY_AGENT_PORT for detailed tests" -ForegroundColor Cyan

Start-Sleep -Seconds 2

# Test 1: Health Check
Write-Host "`n[Test 1/9] Health Check (Port $PRIMARY_AGENT_PORT)" -ForegroundColor Yellow

try {
    $health = Invoke-RestMethod -Uri "http://localhost:${PRIMARY_AGENT_PORT}/api/v1/metadata/health" -Method Get
    Show-JsonResponse $health "✓ Health Check Response:"

    if ($health.status -eq "healthy" -and $health.llm_status -eq "healthy") {
        Write-Host "  ✓ All systems healthy!" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ System degraded" -ForegroundColor Yellow
        Write-Host "    Status: $($health.status)" -ForegroundColor Gray
        Write-Host "    LLM Status: $($health.llm_status)" -ForegroundColor Gray
    }
} catch {
    Write-Host "  ✗ Health check failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Start-Sleep -Seconds 2

# Test 2: Profile Dataset
Write-Host "`n[Test 2/9] Profile Dataset" -ForegroundColor Yellow

try {
    $profile = Invoke-RestMethod -Uri "http://localhost:${PRIMARY_AGENT_PORT}/api/v1/metadata/profile?db=$TEST_DB&table=$TEST_TABLE" -Method Get
    Show-JsonResponse $profile "✓ Profile Response:"

    Write-Host "  Dataset Information:" -ForegroundColor Green
    Write-Host "    Database: $($profile.profile.database)" -ForegroundColor Gray
    Write-Host "    Table: $($profile.profile.table)" -ForegroundColor Gray
    Write-Host "    Rows: $($profile.profile.row_count)" -ForegroundColor Gray
    Write-Host "    Columns: $($profile.profile.column_count)" -ForegroundColor Gray
    Write-Host "    Domain: $($profile.profile.domain)" -ForegroundColor Gray
    Write-Host "    Profile Time: $($profile.profile.profile_time_ms)ms" -ForegroundColor Gray
} catch {
    Write-Host "  ✗ Profile failed: $($_.Exception.Message)" -ForegroundColor Red

    # Check if database exists
    Write-Host "`n  Troubleshooting:" -ForegroundColor Yellow
    Write-Host "    1. Make sure test databases exist in the container" -ForegroundColor Gray
    Write-Host "    2. Check container logs: docker logs optimusdb1" -ForegroundColor Gray
    Write-Host "    3. Database should be mounted or accessible inside container" -ForegroundColor Gray
}

Start-Sleep -Seconds 2

# Test 3: Enrich Dataset (First Time - LLM Generation)
Write-Host "`n[Test 3/9] Enrich Dataset (First Time - LLM Generation)" -ForegroundColor Yellow
Write-Host "  → This should take 2-3 seconds..." -ForegroundColor Gray

$enrichBody = @{
    database = $TEST_DB
    table = $TEST_TABLE
} | ConvertTo-Json

try {
    $startTime = Get-Date
    $enrich1 = Invoke-RestMethod -Uri "http://localhost:${PRIMARY_AGENT_PORT}/api/v1/metadata/enrich" `
        -Method Post `
        -ContentType "application/json" `
        -Body $enrichBody
    $duration1 = ((Get-Date) - $startTime).TotalMilliseconds

    Show-JsonResponse $enrich1 "✓ Enrichment Response (First Time):"

    Write-Host "  Performance Metrics:" -ForegroundColor Green
    Write-Host "    Status: $($enrich1.metadata.status)" -ForegroundColor Gray
    Write-Host "    Created By: $($enrich1.metadata.created_by)" -ForegroundColor Gray
    Write-Host "    Cache Hit: $($enrich1.metadata.cache_hit)" -ForegroundColor Gray
    Write-Host "    Total Time: ${duration1}ms" -ForegroundColor Gray

    Write-Host "`n  Metadata Quality:" -ForegroundColor Green
    Write-Host "    Description Length: $($enrich1.metadata.description.Length) chars" -ForegroundColor Gray
    Write-Host "    Tags Count: $($enrich1.metadata.tags.Count)" -ForegroundColor Gray
    Write-Host "    Domain: $($enrich1.metadata.domain)" -ForegroundColor Gray
} catch {
    Write-Host "  ✗ Enrichment failed: $($_.Exception.Message)" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# Test 4: Enrich Dataset (Second Time - Cached)
Write-Host "`n[Test 4/9] Enrich Dataset (Second Time - Should be Cached)" -ForegroundColor Yellow

try {
    $startTime = Get-Date
    $enrich2 = Invoke-RestMethod -Uri "http://localhost:${PRIMARY_AGENT_PORT}/api/v1/metadata/enrich" `
        -Method Post `
        -ContentType "application/json" `
        -Body $enrichBody
    $duration2 = ((Get-Date) - $startTime).TotalMilliseconds

    Write-Host "  Performance Comparison:" -ForegroundColor Green
    Write-Host "    First Time: ${duration1}ms" -ForegroundColor Gray
    Write-Host "    Second Time: ${duration2}ms" -ForegroundColor Gray
    if ($duration1 -gt 0 -and $duration2 -gt 0) {
        Write-Host "    Speedup: $('{0:N1}' -f ($duration1 / $duration2))x faster" -ForegroundColor Gray
    }
    Write-Host "    Status: $($enrich2.metadata.status)" -ForegroundColor Gray
    Write-Host "    Cache Hit: $($enrich2.metadata.cache_hit)" -ForegroundColor Gray

    if ($enrich2.metadata.cache_hit -and $duration2 -lt 100) {
        Write-Host "`n  ✓ Cache working perfectly!" -ForegroundColor Green
    }
} catch {
    Write-Host "  ✗ Cached enrichment failed: $($_.Exception.Message)" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# Test 5: Get Metrics
Write-Host "`n[Test 5/9] Get Metrics (Port $PRIMARY_AGENT_PORT)" -ForegroundColor Yellow

try {
    $metrics = Invoke-RestMethod -Uri "http://localhost:${PRIMARY_AGENT_PORT}/api/v1/metadata/metrics" -Method Get
    Show-JsonResponse $metrics "✓ Metrics Response:"

    Write-Host "  Performance Metrics:" -ForegroundColor Green
    Write-Host "    Total Enrichments: $($metrics.total_enrichments)" -ForegroundColor Gray
    Write-Host "    Successful LLM: $($metrics.successful_llm)" -ForegroundColor Gray
    Write-Host "    Cached Hits: $($metrics.cached_hits)" -ForegroundColor Gray
    Write-Host "    Failed LLM: $($metrics.failed_llm)" -ForegroundColor Gray
} catch {
    Write-Host "  ✗ Metrics retrieval failed: $($_.Exception.Message)" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# Test 6: Test Multiple Agents (if available)
if ($runningAgents.Count -gt 1) {
    Write-Host "`n[Test 6/9] Test Multiple Agents ($($runningAgents.Count) agents found)" -ForegroundColor Yellow
    Write-Host ""

    $agentResults = @{}

    foreach ($port in $runningAgents) {
        try {
            $startTime = Get-Date
            $agentEnrich = Invoke-RestMethod -Uri "http://localhost:${port}/api/v1/metadata/enrich" `
                -Method Post `
                -ContentType "application/json" `
                -Body $enrichBody `
                -TimeoutSec 10
            $duration = ((Get-Date) - $startTime).TotalMilliseconds

            $agentResults[$port] = @{
                success = $true
                duration = $duration
                status = $agentEnrich.metadata.status
                created_by = $agentEnrich.metadata.created_by
                description = $agentEnrich.metadata.description
                domain = $agentEnrich.metadata.domain
            }

            $icon = if ($agentEnrich.metadata.cache_hit) { "⚡" } else { "✨" }
            Write-Host "  $icon Agent :$port - $($agentEnrich.metadata.status) (${duration}ms)" -ForegroundColor Green
        } catch {
            $agentResults[$port] = @{
                success = $false
                error = $_.Exception.Message
            }
            Write-Host "  ✗ Agent :$port - Failed" -ForegroundColor Red
        }
    }

    # Check consistency
    $domains = $agentResults.Values | Where-Object { $_.success } | ForEach-Object { $_.domain } | Select-Object -Unique
    $statuses = $agentResults.Values | Where-Object { $_.success } | ForEach-Object { $_.status }

    Write-Host "`n  Consistency Analysis:" -ForegroundColor Yellow
    Write-Host "    Unique Domains: $($domains.Count)" -ForegroundColor Gray
    Write-Host "    Cached Responses: $(($statuses | Where-Object { $_ -eq 'cached' }).Count)" -ForegroundColor Gray
    Write-Host "    Generated Responses: $(($statuses | Where-Object { $_ -eq 'generated' }).Count)" -ForegroundColor Gray

    if ($domains.Count -eq 1) {
        Write-Host "    ✓ All agents agree on domain: $($domains[0])" -ForegroundColor Green
    }
} else {
    Write-Host "`n[Test 6/9] Test Multiple Agents - Skipped (only 1 agent found)" -ForegroundColor Gray
}

Start-Sleep -Seconds 2

# Test 7: Check All Agent Health
Write-Host "`n[Test 7/9] Check All Running Agents Health" -ForegroundColor Yellow
Write-Host ""

$healthyAgents = 0
foreach ($port in $runningAgents) {
    try {
        $agentHealth = Invoke-RestMethod -Uri "http://localhost:${port}/api/v1/metadata/health" -Method Get -TimeoutSec 5

        $llmIcon = if ($agentHealth.llm_status -eq "healthy") { "✓" } else { "⚠" }
        $statusColor = if ($agentHealth.status -eq "healthy" -and $agentHealth.llm_status -eq "healthy") { "Green" } else { "Yellow" }

        Write-Host "  $llmIcon Agent :$port - Status: $($agentHealth.status), LLM: $($agentHealth.llm_status)" -ForegroundColor $statusColor

        if ($agentHealth.status -eq "healthy" -and $agentHealth.llm_status -eq "healthy") {
            $healthyAgents++
        }
    } catch {
        Write-Host "  ✗ Agent :$port - Not responding" -ForegroundColor Red
    }
}

Write-Host "`n  Healthy Agents: $healthyAgents/$($runningAgents.Count)" -ForegroundColor $(if ($healthyAgents -eq $runningAgents.Count) { "Green" } else { "Yellow" })

Start-Sleep -Seconds 2

# Test 8: Docker Container Check
Write-Host "`n[Test 8/9] Docker Container Status" -ForegroundColor Yellow
Write-Host ""

try {
    $containers = docker ps --filter "name=optimusdb" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>$null
    if ($containers) {
        Write-Host $containers
        Write-Host ""

        $containerCount = (docker ps --filter "name=optimusdb" --format "{{.Names}}" 2>$null | Measure-Object).Count
        Write-Host "  Running Containers: $containerCount" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ No OptimusDB containers found" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  ⚠ Could not check Docker containers: $($_.Exception.Message)" -ForegroundColor Yellow
}

Start-Sleep -Seconds 2

# Test 9: Summary Report
Write-Host "`n[Test 9/9] Test Summary Report" -ForegroundColor Yellow

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Test Summary" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$testResults = @{
    "Agents Discovered" = ($runningAgents.Count -gt 0)
    "Health Check" = ($health -ne $null -and $health.status -eq "healthy")
    "LLM Available" = ($health.llm_status -eq "healthy")
    "Dataset Profiling" = ($profile -ne $null)
    "LLM Generation" = ($enrich1 -ne $null -and $duration1 -gt 0)
    "Cache Performance" = ($enrich2.metadata.cache_hit -and $duration2 -lt 100)
    "Metrics Tracking" = ($metrics -ne $null)
    "All Agents Healthy" = ($healthyAgents -eq $runningAgents.Count)
}

foreach ($test in $testResults.GetEnumerator()) {
    $status = if ($test.Value) { "✓ PASS" } else { "✗ FAIL" }
    $color = if ($test.Value) { "Green" } else { "Red" }
    Write-Host "  $status : $($test.Key)" -ForegroundColor $color
}

$passCount = ($testResults.Values | Where-Object { $_ -eq $true }).Count
$totalCount = $testResults.Count
$passRate = ($passCount / $totalCount * 100)

Write-Host "`n  Overall: $passCount/$totalCount tests passed ($('{0:N0}' -f $passRate)%)" -ForegroundColor $(if ($passRate -eq 100) { "Green" } elseif ($passRate -ge 75) { "Yellow" } else { "Red" })

Write-Host "`n  Deployment Statistics:" -ForegroundColor Yellow
Write-Host "    Running Agents: $($runningAgents.Count)" -ForegroundColor Gray
Write-Host "    Healthy Agents: $healthyAgents" -ForegroundColor Gray
Write-Host "    Primary Port: $PRIMARY_AGENT_PORT" -ForegroundColor Gray
if ($duration1 -gt 0) {
    Write-Host "    LLM Generation Time: $('{0:N0}' -f $duration1)ms" -ForegroundColor Gray
}
if ($duration2 -gt 0) {
    Write-Host "    Cached Response Time: $('{0:N0}' -f $duration2)ms" -ForegroundColor Gray
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Testing Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Save detailed results
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$reportFile = "test-results-docker-$timestamp.json"

$detailedReport = @{
    timestamp = (Get-Date).ToString("o")
    deployment_type = "Docker with swarmnet"
    agent_ports = $runningAgents
    primary_agent_port = $PRIMARY_AGENT_PORT
    health = $health
    profile = $profile
    enrichment_first = $enrich1
    enrichment_cached = $enrich2
    metrics = $metrics
    agent_results = $agentResults
    test_results = $testResults
    performance = @{
        first_enrichment_ms = $duration1
        cached_enrichment_ms = $duration2
        speedup_factor = if ($duration1 -gt 0 -and $duration2 -gt 0) { ($duration1 / $duration2) } else { 0 }
    }
}

$detailedReport | ConvertTo-Json -Depth 10 | Out-File $reportFile

Write-Host "Detailed results saved to: $reportFile" -ForegroundColor Gray
Write-Host ""