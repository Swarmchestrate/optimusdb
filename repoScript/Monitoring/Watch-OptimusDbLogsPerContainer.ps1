param(
    [string]$ContainerName = "optimusdb1"
)

# Path inside the container
$LogPath  = "/var/log/supervisor"

# Known log files
$LogFiles = @(
    "supervisord.log",
    "optimusdb.log",
    "optimusdb_info.log",
    "tinyllama.log",
    "tinyllama_info.log"
)

# Host log directory = where you *execute* the script (current location)
$HostLogDir = (Get-Location).Path

function Show-Menu {
    Clear-Host
    Write-Host "====================================="
    Write-Host "   OptimusDB / TinyLlama Log Viewer  "
    Write-Host "   Container: $ContainerName"
    Write-Host "   Container log path : $LogPath"
    Write-Host "   Host save path     : $HostLogDir"
    Write-Host "====================================="
    Write-Host ""
    Write-Host "Select a log to follow (Ctrl+C to stop):"
    Write-Host ""

    for ($i = 0; $i -lt $LogFiles.Count; $i++) {
        $index = $i + 1
        Write-Host "  $index) $($LogFiles[$i])"
    }

    Write-Host ""
    Write-Host "  A) All logs (tail *.log, mixed)"
    Write-Host "  Q) Quit"
    Write-Host ""
}

function Watch-Log([string]$LogFile) {
    $timestamp  = Get-Date -Format "yyyyMMdd_HHmmss"
    $safeName   = $LogFile -replace '[\\/:\*\?"<>\|]', '_'
    $hostLog    = Join-Path $HostLogDir ("{0}-{1}-{2}.log" -f $ContainerName, $safeName, $timestamp)

    Write-Host "Following $LogFile from container '$ContainerName'..." -ForegroundColor Yellow
    Write-Host "Output will also be saved to (on HOST):" -ForegroundColor Yellow
    Write-Host "  $hostLog" -ForegroundColor Cyan
    Write-Host "Press Ctrl+C to stop and return to the menu." -ForegroundColor Yellow
    Write-Host ""

    # Read from inside the container, save to host file + show on screen
    docker exec $ContainerName bash -lc "cd $LogPath && tail -n 50 -F $LogFile" `
        | Tee-Object -FilePath $hostLog -Append
}

function Watch-AllLogs {
    $timestamp  = Get-Date -Format "yyyyMMdd_HHmmss"
    $hostLog    = Join-Path $HostLogDir ("{0}-ALL-logs-{1}.log" -f $ContainerName, $timestamp)

    Write-Host "Following ALL logs (*.log) from container '$ContainerName'..." -ForegroundColor Yellow
    Write-Host "Output will also be saved to (on HOST):" -ForegroundColor Yellow
    Write-Host "  $hostLog" -ForegroundColor Cyan
    Write-Host "Press Ctrl+C to stop and return to the menu." -ForegroundColor Yellow
    Write-Host ""

    docker exec $ContainerName bash -lc "cd $LogPath && tail -n 50 -F *.log" `
        | Tee-Object -FilePath $hostLog -Append
}

# Main loop
$running = $true
while ($running) {
    Show-Menu
    $choice = (Read-Host "Enter your choice").Trim()

    if ([string]::IsNullOrWhiteSpace($choice)) {
        continue
    }

    switch ($choice.ToUpper()) {
        "Q" {
            Write-Host "Exiting log viewer..." -ForegroundColor Cyan
            $running = $false
        }

        "A" {
            Watch-AllLogs
        }

        default {
            if ($choice -as [int]) {
                $idx = [int]$choice - 1
                if ($idx -ge 0 -and $idx -lt $LogFiles.Count) {
                    Watch-Log -LogFile $LogFiles[$idx]
                } else {
                    Write-Host "Invalid selection, press Enter to continue..." -ForegroundColor Red
                    Read-Host | Out-Null
                }
            } else {
                Write-Host "Invalid selection, press Enter to continue..." -ForegroundColor Red
                Read-Host | Out-Null
            }
        }
    }
}
