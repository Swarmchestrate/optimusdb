# ==============================================================================
# OptimusDB Combined Container Monitoring Script
# ==============================================================================
# Real-time monitoring of 8-agent cluster with embedded TinyLlama
# ==============================================================================

param(
    [int]$RefreshInterval = 5
)

function Get-ColoredStatus {
    param([string]$Status)

    switch ($Status) {
        "running" { Write-Host $Status -ForegroundColor Green -NoNewline }
        "healthy" { Write-Host $Status -ForegroundColor Green -NoNewline }
        "unhealthy" { Write-Host $Status -ForegroundColor Red -NoNewline }
        "starting" { Write-Host $Status -ForegroundColor Yellow -NoNewline }
        default { Write-Host $Status -ForegroundColor Gray -NoNewline }
    }
}

function Get-ContainerHealth {
    param([string]$ContainerName)

    try {
        $health = docker inspect --format='{{.State.Health.Status}}' $ContainerName 2>$null
        if ($LASTEXITCODE -eq 0) {
            return $health
        }
        return "unknown"
    } catch {
        return "error"
    }
}

function Get-AgentRole {
    param([string]$ContainerName, [int]$AgentPort)

    try {
        $response = Invoke-RestMethod -Uri "http://localhost:$AgentPort/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
        return $response.role
    } catch {
        return "unknown"
    }
}

function Get-TinyLlamaStatus {
    param([string]$ContainerName, [int]$TinyLlamaPort)

    try {
        $response = Invoke-WebRequest -Uri "http://localhost:$TinyLlamaPort/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
            return "ready"
        }
        return "not ready"
    } catch {
        return "not ready"
    }
}

function Get-ContainerStats {
    param([string]$ContainerName)

    $stats = docker stats --no-stream --format "{{.CPUPerc}},{{.MemUsage}}" $ContainerName 2>$null
    if ($LASTEXITCODE -eq 0 -and $stats) {
        $parts = $stats -split ','
        return @{
            CPU = $parts[0]
            Memory = $parts[1]
        }
    }
    return @{
        CPU = "N/A"
        Memory = "N/A"
    }
}

# Main monitoring loop
Clear-Host
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "OptimusDB Combined Container Monitoring" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

while ($true) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

    # Clear screen and show header
    Clear-Host
    Write-Host "=========================================" -ForegroundColor Cyan
    Write-Host "OptimusDB Combined Container Monitoring" -ForegroundColor Cyan
    Write-Host "Time: $timestamp" -ForegroundColor Cyan
    Write-Host "=========================================" -ForegroundColor Cyan
    Write-Host ""

    # Container status table
    Write-Host "CONTAINER STATUS" -ForegroundColor Yellow
    Write-Host "─────────────────────────────────────────────────────────────────────────────────" -ForegroundColor DarkGray
    Write-Host ("{0,-20} {1,-12} {2,-12} {3,-12} {4,-15} {5,-15}" -f "Container", "Status", "Health", "Role", "CPU", "Memory") -ForegroundColor White
    Write-Host "─────────────────────────────────────────────────────────────────────────────────" -ForegroundColor DarkGray

    for ($i = 1; $i -le 8; $i++) {
        $containerName = "optimusdb-agent-$i"
        $agentPort = 9090 + $i
        $tinyLlamaPort = 8080 + $i

        # Get container status
        $status = docker inspect --format='{{.State.Status}}' $containerName 2>$null
        if ($LASTEXITCODE -ne 0) { $status = "not found" }

        # Get health status
        $health = Get-ContainerHealth -ContainerName $containerName

        # Get agent role
        $role = Get-AgentRole -ContainerName $containerName -AgentPort $agentPort

        # Get resource stats
        $stats = Get-ContainerStats -ContainerName $containerName

        # Display row
        Write-Host ("{0,-20} " -f $containerName) -NoNewline
        Get-ColoredStatus -Status $status
        Write-Host (" {0,-12} " -f "") -NoNewline
        Get-ColoredStatus -Status $health
        Write-Host (" {0,-12}" -f "") -NoNewline

        if ($role -eq "coordinator") {
            Write-Host ("{0,-12} " -f $role) -ForegroundColor Cyan -NoNewline
        } elseif ($role -eq "follower") {
            Write-Host ("{0,-12} " -f $role) -ForegroundColor Green -NoNewline
        } else {
            Write-Host ("{0,-12} " -f $role) -ForegroundColor Gray -NoNewline
        }

        Write-Host ("{0,-15} {1,-15}" -f $stats.CPU, $stats.Memory)
    }

    Write-Host ""

    # TinyLlama status
    Write-Host "TINYLLAMA STATUS (per container)" -ForegroundColor Yellow
    Write-Host "─────────────────────────────────────────────────────────────────────────────────" -ForegroundColor DarkGray
    Write-Host ("{0,-20} {1,-15} {2,-40}" -f "Agent", "Port", "Status") -ForegroundColor White
    Write-Host "─────────────────────────────────────────────────────────────────────────────────" -ForegroundColor DarkGray

    for ($i = 1; $i -le 8; $i++) {
        $containerName = "optimusdb-agent-$i"
        $tinyLlamaPort = 8080 + $i

        $llamaStatus = Get-TinyLlamaStatus -ContainerName $containerName -TinyLlamaPort $tinyLlamaPort

        Write-Host ("{0,-20} {1,-15} " -f "agent-$i", "localhost:$tinyLlamaPort") -NoNewline

        if ($llamaStatus -eq "ready") {
            Write-Host $llamaStatus -ForegroundColor Green
        } else {
            Write-Host $llamaStatus -ForegroundColor Red
        }
    }

    Write-Host ""

    # Cluster summary
    Write-Host "CLUSTER SUMMARY" -ForegroundColor Yellow
    Write-Host "─────────────────────────────────────────────────────────────────────────────────" -ForegroundColor DarkGray

    $runningCount = 0
    $healthyCount = 0
    $coordinatorCount = 0

    for ($i = 1; $i -le 8; $i++) {
        $containerName = "optimusdb-agent-$i"
        $agentPort = 9090 + $i

        $status = docker inspect --format='{{.State.Status}}' $containerName 2>$null
        if ($status -eq "running") { $runningCount++ }

        $health = Get-ContainerHealth -ContainerName $containerName
        if ($health -eq "healthy") { $healthyCount++ }

        $role = Get-AgentRole -ContainerName $containerName -AgentPort $agentPort
        if ($role -eq "coordinator") { $coordinatorCount++ }
    }

    Write-Host "Total Containers: 8" -ForegroundColor White
    Write-Host ("Running: {0}/8" -f $runningCount) -ForegroundColor $(if ($runningCount -eq 8) { "Green" } else { "Yellow" })
    Write-Host ("Healthy: {0}/8" -f $healthyCount) -ForegroundColor $(if ($healthyCount -eq 8) { "Green" } else { "Yellow" })
    Write-Host ("Coordinators: {0}" -f $coordinatorCount) -ForegroundColor $(if ($coordinatorCount -eq 1) { "Green" } elseif ($coordinatorCount -eq 0) { "Red" } else { "Yellow" })

    Write-Host ""
    Write-Host "Press Ctrl+C to exit | Refreshing every $RefreshInterval seconds..." -ForegroundColor DarkGray

    Start-Sleep -Seconds $RefreshInterval
}