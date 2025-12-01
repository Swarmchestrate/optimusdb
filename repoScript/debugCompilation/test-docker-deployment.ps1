# OptimusDB Docker Deployment - Startup and Test Script
# Starts OptimusDB containers and prepares for metadata enrichment testing

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  OptimusDB Docker Deployment" -ForegroundColor Cyan
Write-Host "  Startup & Test Preparation" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Configuration
$NETWORK_NAME = "swarmnet"
$IMAGE_NAME = "optimusdb"
$AGENT_COUNT = 8

# Port mappings (adjust if needed)
# Format: -p <external>:<internal>
# 18001-18008: HTTP API (8089 inside container)
# 18081-18088: P2P HTTP (8080 inside container)
# 14001-14008: LibP2P (4001 inside container)
# 15001-15008: IPFS (5001 inside container)

Write-Host "[Step 1/6] Checking Docker environment..." -ForegroundColor Yellow

# Check if Docker is running
try {
    docker info 2>&1 | Out-Null
    Write-Host "  ✓ Docker is running" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Docker is not running" -ForegroundColor Red
    Write-Host "  → Please start Docker Desktop first" -ForegroundColor Yellow
    exit 1
}

# Check if swarmnet exists
$networkExists = docker network ls --filter "name=$NETWORK_NAME" --format "{{.Name}}" 2>$null
if ($networkExists -eq $NETWORK_NAME) {
    Write-Host "  ✓ Network '$NETWORK_NAME' exists" -ForegroundColor Green
} else {
    Write-Host "  ⚠ Network '$NETWORK_NAME' does not exist" -ForegroundColor Yellow
    Write-Host "  → Creating network..." -ForegroundColor Gray

    try {
        docker network create $NETWORK_NAME 2>&1 | Out-Null
        Write-Host "  ✓ Network created" -ForegroundColor Green
    } catch {
        Write-Host "  ✗ Failed to create network: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }
}

# Check if image exists
$imageExists = docker images --filter "reference=$IMAGE_NAME" --format "{{.Repository}}" 2>$null
if ($imageExists -eq $IMAGE_NAME) {
    Write-Host "  ✓ Image '$IMAGE_NAME' exists" -ForegroundColor Green
} else {
    Write-Host "  ✗ Image '$IMAGE_NAME' not found" -ForegroundColor Red
    Write-Host "  → Please build the OptimusDB Docker image first:" -ForegroundColor Yellow
    Write-Host "    docker build -t optimusdb ." -ForegroundColor Gray
    exit 1
}

Start-Sleep -Seconds 2

# Step 2: Check existing containers
Write-Host "`n[Step 2/6] Checking existing containers..." -ForegroundColor Yellow

$existingContainers = docker ps -a --filter "name=optimusdb" --format "{{.Names}}" 2>$null
if ($existingContainers) {
    Write-Host "  Found existing containers:" -ForegroundColor Yellow
    $existingContainers | ForEach-Object { Write-Host "    • $_" -ForegroundColor Gray }

    Write-Host "`n  Options:" -ForegroundColor Yellow
    Write-Host "    1. Stop and remove existing containers (recommended)" -ForegroundColor White
    Write-Host "    2. Keep existing containers and skip deployment" -ForegroundColor White
    Write-Host "    3. Exit" -ForegroundColor White

    $choice = Read-Host "`n  Select option (1-3)"

    switch ($choice) {
        "1" {
            Write-Host "`n  → Stopping and removing existing containers..." -ForegroundColor Yellow
            docker ps -a --filter "name=optimusdb" --format "{{.Names}}" | ForEach-Object {
                docker stop $_ 2>&1 | Out-Null
                docker rm $_ 2>&1 | Out-Null
                Write-Host "    ✓ Removed: $_" -ForegroundColor Green
            }
        }
        "2" {
            Write-Host "`n  → Keeping existing containers" -ForegroundColor Yellow
            Write-Host "  → Skipping to tests..." -ForegroundColor Gray
            Start-Sleep -Seconds 2

            Write-Host "`n========================================" -ForegroundColor Cyan
            Write-Host "  Running Tests on Existing Deployment" -ForegroundColor Cyan
            Write-Host "========================================" -ForegroundColor Cyan

            & ".\test-docker-deployment.ps1"
            exit 0
        }
        "3" {
            Write-Host "  → Exiting" -ForegroundColor Gray
            exit 0
        }
        default {
            Write-Host "  Invalid choice. Exiting." -ForegroundColor Red
            exit 1
        }
    }
} else {
    Write-Host "  ✓ No existing containers found" -ForegroundColor Green
}

Start-Sleep -Seconds 2

# Step 3: Deploy containers
Write-Host "`n[Step 3/6] Deploying OptimusDB containers..." -ForegroundColor Yellow

$deployedAgents = @()

for ($i = 1; $i -le $AGENT_COUNT; $i++) {
    $containerName = "optimusdb$i"
    $httpPort = 18000 + $i
    $p2pHttpPort = 18080 + $i
    $libp2pPort = 14000 + $i
    $ipfsPort = 15000 + $i

    Write-Host "`n  Agent $i ($containerName):" -ForegroundColor Cyan
    Write-Host "    HTTP API: $httpPort" -ForegroundColor Gray
    Write-Host "    P2P HTTP: $p2pHttpPort" -ForegroundColor Gray
    Write-Host "    LibP2P: $libp2pPort" -ForegroundColor Gray
    Write-Host "    IPFS: $ipfsPort" -ForegroundColor Gray

    try {
        docker run -d `
            --network=$NETWORK_NAME `
            --name=$containerName `
            -p ${httpPort}:8089 `
            -p ${p2pHttpPort}:8080 `
            -p ${libp2pPort}:4001 `
            -p ${ipfsPort}:5001 `
            $IMAGE_NAME 2>&1 | Out-Null

        Write-Host "    ✓ Container started" -ForegroundColor Green
        $deployedAgents += $httpPort
    } catch {
        Write-Host "    ✗ Failed to start: $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host "`n  Deployed: $($deployedAgents.Count)/$AGENT_COUNT agents" -ForegroundColor $(if ($deployedAgents.Count -eq $AGENT_COUNT) { "Green" } else { "Yellow" })

if ($deployedAgents.Count -eq 0) {
    Write-Host "  ✗ No agents deployed successfully" -ForegroundColor Red
    exit 1
}

Start-Sleep -Seconds 2

# Step 4: Wait for agents to initialize
Write-Host "`n[Step 4/6] Waiting for agents to initialize..." -ForegroundColor Yellow
Write-Host "  → Waiting 30 seconds for cluster formation..." -ForegroundColor Gray

for ($i = 30; $i -gt 0; $i--) {
    Write-Host "`r  → $i seconds remaining..." -NoNewline -ForegroundColor Gray
    Start-Sleep -Seconds 1
}
Write-Host "`r  ✓ Wait complete" -ForegroundColor Green

Start-Sleep -Seconds 2

# Step 5: Verify agent health
Write-Host "`n[Step 5/6] Verifying agent health..." -ForegroundColor Yellow
Write-Host ""

$healthyAgents = 0
foreach ($port in $deployedAgents) {
    try {
        $health = Invoke-RestMethod -Uri "http://localhost:${port}/api/v1/metadata/health" -Method Get -TimeoutSec 5

        if ($health.status -eq "healthy" -and $health.llm_status -eq "healthy") {
            Write-Host "  ✓ Agent :$port - Healthy (LLM: OK)" -ForegroundColor Green
            $healthyAgents++
        } else {
            Write-Host "  ⚠ Agent :$port - Status: $($health.status), LLM: $($health.llm_status)" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "  ✗ Agent :$port - Not responding" -ForegroundColor Red
    }
}

Write-Host "`n  Healthy Agents: $healthyAgents/$($deployedAgents.Count)" -ForegroundColor $(if ($healthyAgents -gt 0) { "Green" } else { "Red" })

if ($healthyAgents -eq 0) {
    Write-Host "`n  ✗ No healthy agents found" -ForegroundColor Red
    Write-Host "  → Check container logs:" -ForegroundColor Yellow
    Write-Host "    docker logs optimusdb1" -ForegroundColor Gray
    Write-Host "  → Check if TinyLlama is accessible from containers" -ForegroundColor Yellow
    exit 1
}

Start-Sleep -Seconds 2

# Step 6: Database setup note
Write-Host "`n[Step 6/6] Test Database Setup" -ForegroundColor Yellow
Write-Host ""

Write-Host "  For metadata enrichment tests, you need test databases accessible to containers." -ForegroundColor White
Write-Host ""
Write-Host "  Option 1: Use existing databases in containers" -ForegroundColor Cyan
Write-Host "    If your OptimusDB image already includes test databases, you're ready!" -ForegroundColor Gray
Write-Host ""
Write-Host "  Option 2: Copy databases to containers" -ForegroundColor Cyan
Write-Host "    docker cp test_solar.db optimusdb1:/app/test_solar.db" -ForegroundColor Gray
Write-Host "    docker cp test_solar.db optimusdb2:/app/test_solar.db" -ForegroundColor Gray
Write-Host "    ... (repeat for all containers)" -ForegroundColor Gray
Write-Host ""
Write-Host "  Option 3: Mount volume with databases" -ForegroundColor Cyan
Write-Host "    Add to docker run command:" -ForegroundColor Gray
Write-Host "    -v \${PWD}/databases:/app/databases" -ForegroundColor Gray
Write-Host ""

# Summary
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Deployment Summary" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Network: $NETWORK_NAME" -ForegroundColor Gray
Write-Host "  Deployed Agents: $($deployedAgents.Count)" -ForegroundColor Gray
Write-Host "  Healthy Agents: $healthyAgents" -ForegroundColor Gray
Write-Host "  HTTP Ports: $($deployedAgents -join ', ')" -ForegroundColor Gray
Write-Host ""

if ($healthyAgents -gt 0) {
    Write-Host "  ✓ Deployment successful!" -ForegroundColor Green
    Write-Host ""
    Write-Host "  Next Steps:" -ForegroundColor Yellow
    Write-Host "    1. Ensure test databases are accessible in containers" -ForegroundColor White
    Write-Host "    2. Run tests: .\test-docker-deployment.ps1" -ForegroundColor White
    Write-Host "    3. View container logs: docker logs optimusdb1" -ForegroundColor White
    Write-Host ""

    $runTests = Read-Host "  Run tests now? (y/n)"
    if ($runTests -eq "y" -or $runTests -eq "Y") {
        Write-Host ""
        & ".\test-docker-deployment.ps1"
    }
} else {
    Write-Host "  ⚠ Deployment completed with issues" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Troubleshooting:" -ForegroundColor Yellow
    Write-Host "    • Check container logs: docker logs optimusdb1" -ForegroundColor Gray
    Write-Host "    • Verify TinyLlama accessibility from swarmnet" -ForegroundColor Gray
    Write-Host "    • Check if metadata service is enabled in container" -ForegroundColor Gray
}

Write-Host ""