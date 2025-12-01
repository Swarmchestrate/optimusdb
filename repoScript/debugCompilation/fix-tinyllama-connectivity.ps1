# OptimusDB Docker - TinyLlama Connectivity Diagnostic and Fix

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  TinyLlama Connectivity Diagnostic" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Check TinyLlama container status
Write-Host "[Step 1/7] Checking TinyLlama container..." -ForegroundColor Yellow

$tinyllamaContainer = docker ps --filter "name=tinyllama" --format "{{.Names}}" 2>$null

if ($tinyllamaContainer) {
    Write-Host "  ✓ TinyLlama container found: $tinyllamaContainer" -ForegroundColor Green

    # Check which network it's on
    $networks = docker inspect $tinyllamaContainer --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' 2>$null
    Write-Host "    Networks: $networks" -ForegroundColor Gray

    # Check if it's on swarmnet
    if ($networks -like "*swarmnet*") {
        Write-Host "    ✓ TinyLlama is on swarmnet" -ForegroundColor Green
    } else {
        Write-Host "    ✗ TinyLlama is NOT on swarmnet" -ForegroundColor Red
        Write-Host "    → This is the problem!" -ForegroundColor Yellow
    }
} else {
    Write-Host "  ✗ TinyLlama container not found" -ForegroundColor Red
    Write-Host "    Checking for container with 'llama' in name..." -ForegroundColor Gray

    $llamaContainers = docker ps --filter "name=llama" --format "{{.Names}}" 2>$null
    if ($llamaContainers) {
        Write-Host "    Found: $llamaContainers" -ForegroundColor Yellow
        $tinyllamaContainer = $llamaContainers[0]
    } else {
        Write-Host "    No LLM containers found" -ForegroundColor Red
    }
}

# Step 2: Check TinyLlama accessibility from host
Write-Host "`n[Step 2/7] Checking TinyLlama from host..." -ForegroundColor Yellow

try {
    $hostHealth = Invoke-RestMethod -Uri "http://localhost:8080/health" -Method Get -TimeoutSec 5 -ErrorAction Stop
    Write-Host "  ✓ TinyLlama accessible from host on port 8080" -ForegroundColor Green
} catch {
    Write-Host "  ✗ TinyLlama NOT accessible from host on port 8080" -ForegroundColor Red
    Write-Host "    Error: $($_.Exception.Message)" -ForegroundColor Gray
}

# Step 3: Check TinyLlama accessibility from OptimusDB container
Write-Host "`n[Step 3/7] Checking TinyLlama from OptimusDB container..." -ForegroundColor Yellow

try {
    # Try to access TinyLlama from inside optimusdb1 container
    $curlTest = docker exec optimusdb1 curl -s http://localhost:8080/health 2>$null

    if ($curlTest) {
        Write-Host "  ⚠ Accessible via localhost:8080 (not ideal for Docker)" -ForegroundColor Yellow
    } else {
        Write-Host "  ✗ Not accessible via localhost:8080 from container" -ForegroundColor Red
    }

    # Try container name
    if ($tinyllamaContainer) {
        $curlTestName = docker exec optimusdb1 curl -s "http://${tinyllamaContainer}:8080/health" 2>$null

        if ($curlTestName) {
            Write-Host "  ✓ Accessible via container name: $tinyllamaContainer" -ForegroundColor Green
        } else {
            Write-Host "  ✗ Not accessible via container name: $tinyllamaContainer" -ForegroundColor Red
        }
    }

    # Try host.docker.internal (Windows/Mac)
    $curlTestHost = docker exec optimusdb1 curl -s http://host.docker.internal:8080/health 2>$null

    if ($curlTestHost) {
        Write-Host "  ✓ Accessible via host.docker.internal:8080" -ForegroundColor Green
    } else {
        Write-Host "  ✗ Not accessible via host.docker.internal:8080" -ForegroundColor Red
    }

} catch {
    Write-Host "  ✗ Cannot test from container: $($_.Exception.Message)" -ForegroundColor Red
}

# Step 4: Check OptimusDB logs for metadata service
Write-Host "`n[Step 4/7] Checking OptimusDB logs..." -ForegroundColor Yellow

$logs = docker logs optimusdb1 --tail 50 2>&1
$metadataLogs = $logs | Select-String "METADATA|metadata|tinyllama|TinyLlama" -CaseSensitive:$false

if ($metadataLogs) {
    Write-Host "  Metadata-related logs:" -ForegroundColor Gray
    $metadataLogs | ForEach-Object { Write-Host "    $_" -ForegroundColor Gray }
} else {
    Write-Host "  ⚠ No metadata-related logs found" -ForegroundColor Yellow
    Write-Host "  → Metadata service may not be enabled in OptimusDB" -ForegroundColor Yellow
}

# Step 5: Identify the problem
Write-Host "`n[Step 5/7] Problem Analysis..." -ForegroundColor Yellow
Write-Host ""

$problems = @()
$solutions = @()

if (-not $tinyllamaContainer) {
    $problems += "TinyLlama container not running"
    $solutions += @"
Solution 1: Start TinyLlama on swarmnet
docker run -d --name tinyllama-service \
  --network swarmnet \
  -p 8080:8080 \
  -v ${PWD}\models:/models:ro \
  ghcr.io/ggerganov/llama.cpp:server \
  --model /models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf \
  --host 0.0.0.0 --port 8080
"@
} elseif ($networks -notlike "*swarmnet*") {
    $problems += "TinyLlama not on swarmnet network"
    $solutions += @"
Solution 2: Connect TinyLlama to swarmnet
docker network connect swarmnet $tinyllamaContainer

Then update OptimusDB environment:
docker stop optimusdb1
docker start optimusdb1
(repeat for all containers)
"@
}

if ($problems.Count -eq 0) {
    $problems += "TinyLlama on swarmnet but OptimusDB can't connect"
    $solutions += @"
Solution 3: Check OptimusDB environment variables
The containers need to know where TinyLlama is.

Option A: Use container name (recommended)
Set TINYLLAMA_ENDPOINT=http://${tinyllamaContainer}:8080/v1/completions

Option B: Use host.docker.internal (Windows/Mac)
Set TINYLLAMA_ENDPOINT=http://host.docker.internal:8080/v1/completions

Rebuild containers with correct environment variable.
"@
}

Write-Host "Problems Found:" -ForegroundColor Red
$problems | ForEach-Object { Write-Host "  • $_" -ForegroundColor Red }

Write-Host "`nRecommended Solutions:" -ForegroundColor Yellow
$solutions | ForEach-Object {
    Write-Host $_ -ForegroundColor Gray
    Write-Host ""
}

# Step 6: Offer automated fix
Write-Host "`n[Step 6/7] Automated Fix Options" -ForegroundColor Yellow
Write-Host ""
Write-Host "  1. Start TinyLlama on swarmnet (if not running)" -ForegroundColor White
Write-Host "  2. Connect existing TinyLlama to swarmnet" -ForegroundColor White
Write-Host "  3. Restart OptimusDB containers with correct TinyLlama endpoint" -ForegroundColor White
Write-Host "  4. Show detailed instructions (manual fix)" -ForegroundColor White
Write-Host "  5. Exit" -ForegroundColor White
Write-Host ""

$fixChoice = Read-Host "Select option (1-5)"

switch ($fixChoice) {
    "1" {
        Write-Host "`nStarting TinyLlama on swarmnet..." -ForegroundColor Yellow

        # Check for model file
        $modelPath = "C:\Users\georg\GolandProjects\optimusdb-lsa\models\tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf"
        if (-not (Test-Path $modelPath)) {
            Write-Host "  ✗ Model file not found at: $modelPath" -ForegroundColor Red
            Write-Host "  → Please download the model first" -ForegroundColor Yellow
            exit 1
        }

        # Stop existing TinyLlama if any
        if ($tinyllamaContainer) {
            Write-Host "  → Stopping existing TinyLlama..." -ForegroundColor Gray
            docker stop $tinyllamaContainer 2>&1 | Out-Null
            docker rm $tinyllamaContainer 2>&1 | Out-Null
        }

        # Start TinyLlama on swarmnet
        $modelsDir = "C:\Users\georg\GolandProjects\optimusdb-lsa\models"

        docker run -d --name tinyllama-service `
            --network swarmnet `
            -p 8080:8080 `
            -v "${modelsDir}:/models:ro" `
            ghcr.io/ggerganov/llama.cpp:server `
            --model /models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf `
            --host 0.0.0.0 --port 8080 `
            --ctx-size 2048 `
            --threads 4 `
            --parallel 2

        Write-Host "  ✓ TinyLlama started on swarmnet" -ForegroundColor Green
        Write-Host "  → Waiting 15 seconds..." -ForegroundColor Gray
        Start-Sleep -Seconds 15

        Write-Host "`n  → Now restart OptimusDB containers..." -ForegroundColor Yellow
        1..8 | ForEach-Object {
            docker restart "optimusdb$_" 2>&1 | Out-Null
            Write-Host "    ✓ Restarted optimusdb$_" -ForegroundColor Green
        }

        Write-Host "`n  → Waiting for restart..." -ForegroundColor Gray
        Start-Sleep -Seconds 10
    }

    "2" {
        if (-not $tinyllamaContainer) {
            Write-Host "  ✗ No TinyLlama container found to connect" -ForegroundColor Red
            exit 1
        }

        Write-Host "`nConnecting $tinyllamaContainer to swarmnet..." -ForegroundColor Yellow
        docker network connect swarmnet $tinyllamaContainer 2>&1 | Out-Null
        Write-Host "  ✓ Connected to swarmnet" -ForegroundColor Green

        Write-Host "`n  → Restarting OptimusDB containers..." -ForegroundColor Yellow
        1..8 | ForEach-Object {
            docker restart "optimusdb$_" 2>&1 | Out-Null
            Write-Host "    ✓ Restarted optimusdb$_" -ForegroundColor Green
        }

        Write-Host "`n  → Waiting for restart..." -ForegroundColor Gray
        Start-Sleep -Seconds 10
    }

    "3" {
        Write-Host "`nRestarting OptimusDB containers..." -ForegroundColor Yellow
        1..8 | ForEach-Object {
            docker restart "optimusdb$_" 2>&1 | Out-Null
            Write-Host "  ✓ Restarted optimusdb$_" -ForegroundColor Green
        }

        Write-Host "`n  → Waiting for restart..." -ForegroundColor Gray
        Start-Sleep -Seconds 10
    }

    "4" {
        Write-Host "`n========================================" -ForegroundColor Cyan
        Write-Host "  Manual Fix Instructions" -ForegroundColor Cyan
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host ""

        Write-Host "Step 1: Ensure TinyLlama is on swarmnet" -ForegroundColor Yellow
        Write-Host "docker network connect swarmnet tinyllama-service" -ForegroundColor Gray
        Write-Host ""

        Write-Host "Step 2: Check OptimusDB can reach TinyLlama" -ForegroundColor Yellow
        Write-Host "docker exec optimusdb1 curl http://tinyllama-service:8080/health" -ForegroundColor Gray
        Write-Host ""

        Write-Host "Step 3: Update OptimusDB environment" -ForegroundColor Yellow
        Write-Host "Edit your Dockerfile or docker-compose to set:" -ForegroundColor Gray
        Write-Host "ENV TINYLLAMA_ENDPOINT=http://tinyllama-service:8080/v1/completions" -ForegroundColor Gray
        Write-Host ""

        Write-Host "Step 4: Rebuild and restart" -ForegroundColor Yellow
        Write-Host "docker stop optimusdb1 ... optimusdb8" -ForegroundColor Gray
        Write-Host "docker rm optimusdb1 ... optimusdb8" -ForegroundColor Gray
        Write-Host "# Then run deploy-docker-cluster.ps1 again" -ForegroundColor Gray
        Write-Host ""
        exit 0
    }

    "5" {
        Write-Host "Exiting..." -ForegroundColor Gray
        exit 0
    }

    default {
        Write-Host "Invalid option" -ForegroundColor Red
        exit 1
    }
}

# Step 7: Verify fix
Write-Host "`n[Step 7/7] Verifying fix..." -ForegroundColor Yellow
Write-Host ""

$fixedAgents = 0
1..8 | ForEach-Object {
    $port = 18000 + $_
    try {
        $health = Invoke-RestMethod -Uri "http://localhost:${port}/api/v1/metadata/health" -Method Get -TimeoutSec 5

        if ($health.status -eq "healthy" -and $health.llm_status -eq "healthy") {
            Write-Host "  ✓ Agent :$port - Healthy" -ForegroundColor Green
            $fixedAgents++
        } else {
            Write-Host "  ⚠ Agent :$port - Status: $($health.status), LLM: $($health.llm_status)" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "  ✗ Agent :$port - Error: $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host "`n  Healthy Agents: $fixedAgents/8" -ForegroundColor $(if ($fixedAgents -eq 8) { "Green" } elseif ($fixedAgents -gt 0) { "Yellow" } else { "Red" })

if ($fixedAgents -eq 8) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "  ✓ All agents healthy!" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "You can now run: .\test-docker-deployment.ps1" -ForegroundColor White
    Write-Host ""
} elseif ($fixedAgents -gt 0) {
    Write-Host "`n⚠ Some agents are still unhealthy" -ForegroundColor Yellow
    Write-Host "Check logs: docker logs optimusdb1" -ForegroundColor Gray
} else {
    Write-Host "`n✗ Fix did not resolve the issue" -ForegroundColor Red
    Write-Host "Additional troubleshooting:" -ForegroundColor Yellow
    Write-Host "  1. Check OptimusDB Dockerfile for TINYLLAMA_ENDPOINT" -ForegroundColor Gray
    Write-Host "  2. Verify metadata service is compiled in OptimusDB" -ForegroundColor Gray
    Write-Host "  3. Check logs: docker logs optimusdb1" -ForegroundColor Gray
}

Write-Host ""