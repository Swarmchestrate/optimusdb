# Deploy-OptimusDB-3Agents.ps1
# PowerShell script to build the OptimusDB image and deploy 3 containers
# on the 'swarmnet' network.

# ===========================================
# Configuration
# ===========================================
$networkName    = "swarmnet"
$imageName      = "optimusdb"
$containerCount = 3

# Base ports (host → container)
$baseHttpPort = 18000
$baseP2PPort  = 14000
$baseRPCPort  = 15000
$baseP2PQUIC  = 13000
$baseLlama    = 18080

# Container internal ports
$internalHttp   = 8089
$internalP2P    = 4001
$internalP2PQUIC = 4002  # libp2p QUIC UDP
$internalRPC    = 5001
$internalLlama  = 8080

# ===========================================
# Script Start
# ===========================================
Write-Host "===========================================" -ForegroundColor Cyan
Write-Host "  OptimusDB Build & Deployment (3 agents)  " -ForegroundColor Cyan
Write-Host "===========================================" -ForegroundColor Cyan
Write-Host ""

# ===========================================
# Build Docker Image
# ===========================================
Write-Host "Building Docker image '$imageName' from current directory..." -ForegroundColor Yellow
#docker build -t $imageName .
docker build --no-cache -t $imageName .


if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Docker build failed (exit code $LASTEXITCODE). Aborting deployment." -ForegroundColor Red
    exit 1
}
Write-Host "✅ Docker image '$imageName' built successfully." -ForegroundColor Green
Write-Host ""

# ===========================================
# Ensure Docker network exists
# ===========================================
$networkExists = docker network ls --format '{{.Name}}' | Where-Object { $_ -eq $networkName }
if (-not $networkExists) {
    Write-Host "Creating Docker network '$networkName'..." -ForegroundColor Yellow
    docker network create $networkName | Out-Null
} else {
    Write-Host "Docker network '$networkName' already exists." -ForegroundColor Green
}

Write-Host ""
Write-Host "Starting $containerCount OptimusDB containers..." -ForegroundColor Cyan
Write-Host ""

# ===========================================
# Container Deployment Loop
# ===========================================
for ($i = 1; $i -le $containerCount; $i++) {

    $containerName = "optimusdb$i"

    $httpPort = $baseHttpPort + $i
    $p2pPort  = $baseP2PPort  + $i
    $rpcPort  = $baseRPCPort  + $i
    $p2pQUIC  = $baseP2PQUIC  + $i
    $llamaPort = $baseLlama   + $i

    # Remove container if it already exists
    $exists = docker ps -a --format '{{.Names}}' | Where-Object { $_ -eq $containerName }
    if ($exists) {
        Write-Host "⚠️  Container '$containerName' already exists. Removing old instance..." -ForegroundColor Yellow
        docker rm -f $containerName | Out-Null
    }

    Write-Host "▶️  Starting $containerName ..." -ForegroundColor Cyan
    Write-Host "   Host Ports: HTTP=$httpPort, P2P=$p2pPort, RPC=$rpcPort, P2PQUIC=$p2pQUIC, Llama=$llamaPort"

    try {
        docker run -d `
            --network $networkName `
            --name $containerName `
            -p "$($httpPort):$($internalHttp)" `
            -p "$($p2pPort):$($internalP2P)" `
            -p "$($rpcPort):$($internalRPC)" `
            -p "$($p2pQUIC):$($internalP2PQUIC)" `
            -p "$($llamaPort):$($internalLlama)" `
            $imageName | Out-Null

        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✅ $containerName started successfully." -ForegroundColor Green
        } else {
            Write-Host "   ❌ Failed to start $containerName (exit code $LASTEXITCODE)." -ForegroundColor Red
        }
    }
    catch {
        Write-Host "   ⚠️ Error launching $containerName : $_" -ForegroundColor Red
    }

    Write-Host ""
}

# ===========================================
# Final Status Summary
# ===========================================
Write-Host "===========================================" -ForegroundColor Cyan
Write-Host "All containers processed. Current status:" -ForegroundColor Cyan
Write-Host "===========================================" -ForegroundColor Cyan

docker ps --filter "network=$networkName"

Write-Host "`n✅ Deployment complete!" -ForegroundColor Green
