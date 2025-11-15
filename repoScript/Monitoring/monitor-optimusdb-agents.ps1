# monitor-optimusdb-agents.ps1
# Monitoring script for OptimusDB distributed cluster

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("status", "logs", "live", "tinyllama", "optimusdb", "errors", "all")]
    [string]$Mode = "status",

    [Parameter(Mandatory=$false)]
    [int]$NodeNumber = 0,  # 0 = all nodes, 1-8 = specific node

    [Parameter(Mandatory=$false)]
    [int]$Lines = 20
)

$nodes = 1..8
$containerPrefix = "optimusdb"

function Show-Banner {
    Write-Host @"
╔════════════════════════════════════════════════════════════╗
║        OptimusDB Distributed Cluster Monitor               ║
║              8-Node Monitoring Tool                        ║
╚════════════════════════════════════════════════════════════╝
"@ -ForegroundColor Cyan
}

function Get-NodeStatus {
    param([int]$node)

    $containerName = "$containerPrefix$node"
    Write-Host "`n🔍 Node $node ($containerName)" -ForegroundColor Yellow
    Write-Host ("="*60) -ForegroundColor Gray

    # Check if container is running
    $running = docker ps --filter "name=$containerName" --format "{{.Names}}" 2>$null
    if ($running) {
        Write-Host "  ✅ Container: RUNNING" -ForegroundColor Green

        # Get supervisor status
        $supervisorStatus = docker exec $containerName supervisorctl status 2>$null
        if ($supervisorStatus) {
            Write-Host "`n  📊 Services:" -ForegroundColor Cyan
            $supervisorStatus | ForEach-Object {
                if ($_ -match "RUNNING") {
                    Write-Host "    ✅ $_" -ForegroundColor Green
                } elseif ($_ -match "FATAL") {
                    Write-Host "    ❌ $_" -ForegroundColor Red
                } else {
                    Write-Host "    ⚠️  $_" -ForegroundColor Yellow
                }
            }
        }

        # Get ports
        $ports = docker port $containerName 2>$null
        if ($ports) {
            Write-Host "`n  🔌 Ports:" -ForegroundColor Cyan
            $ports | ForEach-Object { Write-Host "    $_" -ForegroundColor White }
        }

        # Get resource usage
        $stats = docker stats $containerName --no-stream --format "{{.CPUPerc}} CPU, {{.MemUsage}}" 2>$null
        if ($stats) {
            Write-Host "`n  💻 Resources: $stats" -ForegroundColor Cyan
        }

    } else {
        Write-Host "  ❌ Container: NOT RUNNING" -ForegroundColor Red
    }
}

function Show-Logs {
    param(
        [int]$node,
        [string]$service,
        [int]$lineCount
    )

    $containerName = "$containerPrefix$node"

    # Check if container exists
    $exists = docker ps -a --filter "name=$containerName" --format "{{.Names}}" 2>$null
    if (-not $exists) {
        Write-Host "❌ Container $containerName not found" -ForegroundColor Red
        return
    }

    Write-Host "`n📋 Node $node - $service Logs (last $lineCount lines)" -ForegroundColor Yellow
    Write-Host ("="*60) -ForegroundColor Gray

    $logPath = switch ($service) {
        "optimusdb" { "/var/log/supervisor/optimusdb.log" }
        "tinyllama" { "/var/log/supervisor/tinyllama.log" }
        "optimusdb_err" { "/var/log/supervisor/optimusdb_err.log" }
        "tinyllama_err" { "/var/log/supervisor/tinyllama_err.log" }
    }

    $logs = docker exec $containerName tail -$lineCount $logPath 2>$null
    if ($logs) {
        $logs | ForEach-Object { Write-Host "  $_" -ForegroundColor White }
    } else {
        Write-Host "  ⚠️  No logs available or container not running" -ForegroundColor Yellow
    }
}

function Show-LiveLogs {
    param(
        [int]$node,
        [string]$service
    )

    $containerName = "$containerPrefix$node"

    Write-Host "📡 Live monitoring: Node $node - $service" -ForegroundColor Cyan
    Write-Host "Press Ctrl+C to stop`n" -ForegroundColor Yellow

    $logPath = switch ($service) {
        "optimusdb" { "/var/log/supervisor/optimusdb.log" }
        "tinyllama" { "/var/log/supervisor/tinyllama.log" }
        "both" { "/var/log/supervisor/optimusdb.log /var/log/supervisor/tinyllama.log" }
    }

    docker exec $containerName tail -f $logPath
}

function Show-AllStatus {
    Show-Banner
    Write-Host "`n🌐 Cluster Overview" -ForegroundColor Cyan
    Write-Host ("="*60) -ForegroundColor Gray

    # Check network
    $network = docker network ls --filter "name=swarmnet" --format "{{.Name}}" 2>$null
    if ($network) {
        Write-Host "✅ Network 'swarmnet': EXISTS" -ForegroundColor Green
    } else {
        Write-Host "❌ Network 'swarmnet': NOT FOUND" -ForegroundColor Red
    }

    # Count running containers
    $runningCount = 0
    foreach ($n in $nodes) {
        $running = docker ps --filter "name=$containerPrefix$n" --format "{{.Names}}" 2>$null
        if ($running) { $runningCount++ }
    }

    Write-Host "📊 Running Nodes: $runningCount / 8" -ForegroundColor $(if($runningCount -eq 8){"Green"}else{"Yellow"})

    # Show each node
    foreach ($n in $nodes) {
        Get-NodeStatus -node $n
    }

    # Summary
    Write-Host "`n" + ("="*60) -ForegroundColor Gray
    Write-Host "📝 Summary:" -ForegroundColor Cyan
    Write-Host "  Total Nodes: 8" -ForegroundColor White
    Write-Host "  Running: $runningCount" -ForegroundColor $(if($runningCount -eq 8){"Green"}else{"Yellow"})
    Write-Host "  Stopped: $(8 - $runningCount)" -ForegroundColor $(if($runningCount -eq 8){"Green"}else{"Red"})
}

function Show-ErrorLogs {
    Show-Banner
    Write-Host "`n🔴 Error Logs from All Nodes" -ForegroundColor Red
    Write-Host ("="*60) -ForegroundColor Gray

    foreach ($n in $nodes) {
        $containerName = "$containerPrefix$n"
        $exists = docker ps --filter "name=$containerName" --format "{{.Names}}" 2>$null

        if ($exists) {
            Write-Host "`n🔍 Node $n - OptimusDB Errors:" -ForegroundColor Yellow
            $errLogs = docker exec $containerName tail -20 /var/log/supervisor/optimusdb_err.log 2>$null
            if ($errLogs -and $errLogs.Trim()) {
                $errLogs | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
            } else {
                Write-Host "  ✅ No errors" -ForegroundColor Green
            }

            Write-Host "`n🔍 Node $n - TinyLlama Errors:" -ForegroundColor Yellow
            $llamaErrLogs = docker exec $containerName tail -20 /var/log/supervisor/tinyllama_err.log 2>$null
            if ($llamaErrLogs -and $llamaErrLogs.Trim()) {
                $llamaErrLogs | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
            } else {
                Write-Host "  ✅ No errors" -ForegroundColor Green
            }
        }
    }
}

# Main execution
switch ($Mode) {
    "status" {
        if ($NodeNumber -eq 0) {
            Show-AllStatus
        } else {
            Show-Banner
            Get-NodeStatus -node $NodeNumber
        }
    }

    "logs" {
        Show-Banner
        if ($NodeNumber -eq 0) {
            foreach ($n in $nodes) {
                Show-Logs -node $n -service "optimusdb" -lineCount $Lines
            }
        } else {
            Show-Logs -node $NodeNumber -service "optimusdb" -lineCount $Lines
        }
    }

    "tinyllama" {
        Show-Banner
        if ($NodeNumber -eq 0) {
            foreach ($n in $nodes) {
                Show-Logs -node $n -service "tinyllama" -lineCount $Lines
            }
        } else {
            Show-Logs -node $NodeNumber -service "tinyllama" -lineCount $Lines
        }
    }

    "optimusdb" {
        Show-Banner
        if ($NodeNumber -eq 0) {
            foreach ($n in $nodes) {
                Show-Logs -node $n -service "optimusdb" -lineCount $Lines
            }
        } else {
            Show-Logs -node $NodeNumber -service "optimusdb" -lineCount $Lines
        }
    }

    "live" {
        if ($NodeNumber -eq 0) {
            Write-Host "⚠️  Please specify a node number for live monitoring" -ForegroundColor Yellow
            Write-Host "Example: .\monitor-optimusdb-agents.ps1 -Mode live -NodeNumber 1" -ForegroundColor White
        } else {
            Show-LiveLogs -node $NodeNumber -service "both"
        }
    }

    "errors" {
        Show-ErrorLogs
    }

    "all" {
        Show-AllStatus
        Start-Sleep -Seconds 2
        Show-ErrorLogs
    }
}

# Show usage help at the end
Write-Host "`n💡 Usage Examples:" -ForegroundColor Cyan
Write-Host "  Status of all nodes:      .\monitor-optimusdb-agents.ps1 -Mode status" -ForegroundColor White
Write-Host "  Status of node 3:         .\monitor-optimusdb-agents.ps1 -Mode status -NodeNumber 3" -ForegroundColor White
Write-Host "  OptimusDB logs (all):     .\monitor-optimusdb-agents.ps1 -Mode optimusdb" -ForegroundColor White
Write-Host "  TinyLlama logs (node 2):  .\monitor-optimusdb-agents.ps1 -Mode tinyllama -NodeNumber 2" -ForegroundColor White
Write-Host "  Live logs (node 1):       .\monitor-optimusdb-agents.ps1 -Mode live -NodeNumber 1" -ForegroundColor White
Write-Host "  All error logs:           .\monitor-optimusdb-agents.ps1 -Mode errors"