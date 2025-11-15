#!/bin/bash
# Check if a private registry is running
# Usage: ./check-registry.sh <registry-host> <registry-port>
# Example: ./check-registry.sh localhost 5000

REGISTRY_HOST=${1:-localhost}
REGISTRY_PORT=${2:-5000}
REGISTRY_URL="http://${REGISTRY_HOST}:${REGISTRY_PORT}/v2/_catalog"

echo "🔍 Checking private registry at $REGISTRY_HOST:$REGISTRY_PORT"

echo "-----------------------------------------------------"
echo "1️⃣  API Reachability Test"
if curl -s --max-time 5 "$REGISTRY_URL" | grep -q '{'; then
    echo "✅ Registry API is responding at $REGISTRY_URL"
else
    echo "❌ Registry API did not respond (might require auth or is down)"
fi

echo "-----------------------------------------------------"
echo "2️⃣  Docker Container Check"
if command -v docker >/dev/null 2>&1; then
    if docker ps --format '{{.Names}}' | grep -q "registry"; then
        echo "✅ Docker container named 'registry' is running"
    else
        echo "ℹ️ No 'registry' container running (might be using another method)"
    fi
else
    echo "⚠️ Docker not installed, skipping container check"
fi

echo "-----------------------------------------------------"
echo "3️⃣  Kubernetes (K3s) Check"
if command -v kubectl >/dev/null 2>&1; then
    if kubectl get pods -A 2>/dev/null | grep -qi "registry"; then
        echo "✅ Found registry pod in Kubernetes"
        kubectl get svc -A | grep -i registry || echo "ℹ️ No registry service found"
    else
        echo "ℹ️ No registry pod found in Kubernetes"
    fi
else
    echo "⚠️ kubectl not installed, skipping Kubernetes check"
fi

echo "-----------------------------------------------------"
echo "✅ Done."
