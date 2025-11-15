#!/bin/bash
# ==============================================================================
# OptimusDB Combined Container Deployment Script
# ==============================================================================

set -e

echo "=========================================="
echo "OptimusDB + TinyLlama Combined Deployment"
echo "=========================================="

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed"
    exit 1
fi

echo "✅ Docker and Docker Compose are installed"

# Download TinyLlama model if not present
MODEL_PATH="./models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf"
if [ ! -f "$MODEL_PATH" ]; then
    echo ""
    echo "📥 Downloading TinyLlama Q4_K_M model (0.67 GB)..."
    echo "This may take 5-10 minutes depending on your connection..."
    mkdir -p ./models

    wget -O "$MODEL_PATH" \
        https://huggingface.co/TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF/resolve/main/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf

    if [ $? -eq 0 ]; then
        echo "✅ Model downloaded successfully"
    else
        echo "❌ Failed to download model"
        exit 1
    fi
else
    echo "✅ TinyLlama model already exists"
fi

# Verify model size
MODEL_SIZE=$(stat -f%z "$MODEL_PATH" 2>/dev/null || stat -c%s "$MODEL_PATH" 2>/dev/null)
EXPECTED_SIZE=669000000  # ~669 MB

if [ "$MODEL_SIZE" -lt "$EXPECTED_SIZE" ]; then
    echo "⚠️  Warning: Model file seems incomplete (${MODEL_SIZE} bytes)"
    echo "Expected at least ${EXPECTED_SIZE} bytes"
    echo "You may want to re-download the model"
fi

# Create necessary directories
echo ""
echo "📁 Creating directories..."
mkdir -p data/{agent-1,agent-2,agent-3,agent-4,agent-5,agent-6,agent-7,agent-8}
mkdir -p logs/{agent-1,agent-2,agent-3,agent-4,agent-5,agent-6,agent-7,agent-8}

echo "✅ Directories created"

# Stop existing containers
echo ""
echo "🛑 Stopping existing containers..."
docker-compose -f docker-compose.dev.yml down 2>/dev/null || true

# Build images
echo ""
echo "🔨 Building Docker images..."
echo "This may take 5-10 minutes on first run..."
docker-compose -f docker-compose.dev.yml build --parallel

if [ $? -eq 0 ]; then
    echo "✅ Images built successfully"
else
    echo "❌ Failed to build images"
    exit 1
fi

# Start containers
echo ""
echo "🚀 Starting 8-agent cluster..."
docker-compose -f docker-compose.dev.yml up -d

if [ $? -eq 0 ]; then
    echo "✅ Containers started"
else
    echo "❌ Failed to start containers"
    exit 1
fi

# Wait for services to be ready
echo ""
echo "⏳ Waiting for services to initialize (60 seconds)..."
sleep 60

# Check health
echo ""
echo "🔍 Checking cluster health..."
HEALTHY=0
for i in {1..8}; do
    PORT=$((9090 + i))
    if curl -sf http://localhost:$PORT/health > /dev/null 2>&1; then
        echo "  ✅ Agent $i is healthy (port $PORT)"
        HEALTHY=$((HEALTHY + 1))
    else
        echo "  ❌ Agent $i is not responding (port $PORT)"
    fi
done

echo ""
echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo "Healthy Agents: $HEALTHY/8"
echo ""

if [ $HEALTHY -eq 8 ]; then
    echo "🎉 All agents are healthy!"
else
    echo "⚠️  Some agents are not healthy. Check logs:"
    echo "  docker-compose -f docker-compose.dev.yml logs"
fi

echo ""
echo "📊 Monitoring Commands:"
echo "  Monitor cluster:  pwsh scripts/monitor-cluster.ps1"
echo "  View logs:        docker-compose -f docker-compose.dev.yml logs -f"
echo "  View agent-1:     docker-compose -f docker-compose.dev.yml logs -f optimusdb-agent-1"
echo "  Stop cluster:     docker-compose -f docker-compose.dev.yml down"
echo ""
echo "🔗 Agent Endpoints:"
for i in {1..8}; do
    PORT=$((9090 + i))
    LLAMA_PORT=$((8080 + i))
    echo "  Agent $i: http://localhost:$PORT (TinyLlama: http://localhost:$LLAMA_PORT)"
done
echo ""
echo "🧪 Test Commands:"
echo "  # Check agent health"
echo "  curl http://localhost:9091/health"
echo ""
echo "  # Generate metadata"
echo "  curl -X POST http://localhost:9091/api/metadata/generate \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"data_source\":\"solar_panels\",\"context\":\"renewable_energy\"}'"
echo ""