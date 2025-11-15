#!/bin/bash
# ==============================================================================
# Combined Container Startup Script
# ==============================================================================
# This script handles the startup of both TinyLlama and OptimusDB in a single
# container using supervisord
# ==============================================================================

set -e

echo "=========================================="
echo "OptimusDB + TinyLlama Combined Container"
echo "=========================================="
echo "Node ID: ${NODE_ID:-unknown}"
echo "Agent Port: ${AGENT_PORT:-9091}"
echo "LibP2P Port: ${LIBP2P_PORT:-4001}"
echo "TinyLlama Endpoint: ${TINYLLAMA_ENDPOINT:-http://localhost:8080}"
echo "=========================================="

# Check if model exists
MODEL_PATH="/models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf"
if [ ! -f "$MODEL_PATH" ]; then
    echo "❌ ERROR: TinyLlama model not found at $MODEL_PATH"
    echo "Please download the model:"
    echo "  mkdir -p models"
    echo "  wget -O models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf https://huggingface.co/TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF/resolve/main/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf"
    exit 1
fi

echo "✅ Model found: $MODEL_PATH"

# Set default TinyLlama parameters if not provided
export TINYLLAMA_THREADS=${TINYLLAMA_THREADS:-2}
export TINYLLAMA_PARALLEL=${TINYLLAMA_PARALLEL:-1}

echo "TinyLlama Config:"
echo "  Threads: $TINYLLAMA_THREADS"
echo "  Parallel: $TINYLLAMA_PARALLEL"
echo "=========================================="

# Create required directories
mkdir -p /data /logs

# Wait for TinyLlama to be ready (when started by supervisord)
echo "Waiting for TinyLlama to start..."
MAX_WAIT=60
COUNTER=0
while ! curl -sf http://localhost:8080/health > /dev/null 2>&1; do
    sleep 1
    COUNTER=$((COUNTER + 1))
    if [ $COUNTER -ge $MAX_WAIT ]; then
        echo "❌ TinyLlama failed to start within ${MAX_WAIT}s"
        exit 1
    fi
done

echo "✅ TinyLlama is ready"
echo "=========================================="

# If this script is called as event listener, exit
if [ "$1" = "READY" ]; then
    exit 0
fi