#!/bin/bash
# Host-level private Docker registry setup script
# Runs registry:2 container on port 5000

REGISTRY_NAME="registry"
REGISTRY_PORT=5000

echo "🔍 Checking for Docker..."
if ! command -v docker >/dev/null 2>&1; then
    echo "⚙️ Installing Docker..."
    sudo apt-get update -y
    sudo apt-get install -y docker.io
    sudo systemctl enable --now docker
else
    echo "✅ Docker already installed."
fi

echo "---------------------------------------------------"
echo "🔍 Checking if registry container is already running..."
if sudo docker ps --format '{{.Names}}' | grep -q "^${REGISTRY_NAME}$"; then
    echo "✅ Registry container '${REGISTRY_NAME}' is already running."
else
    echo "⚙️ Starting registry container on port ${REGISTRY_PORT}..."
    sudo docker run -d \
        --restart=always \
        --name ${REGISTRY_NAME} \
        -p ${REGISTRY_PORT}:5000 \
        registry:2
    echo "✅ Registry started."
fi

echo "---------------------------------------------------"
echo "🔍 Testing registry API..."
sleep 3
if curl -s http://localhost:${REGISTRY_PORT}/v2/_catalog | grep -q '{'; then
    echo "✅ Private registry is UP and reachable at http://localhost:${REGISTRY_PORT}"
else
    echo "❌ Registry API not responding. Check container logs:"
    echo "    sudo docker logs ${REGISTRY_NAME}"
fi

echo "---------------------------------------------------"
echo "🎉 Done. You can now push/pull images using:"
echo "    docker tag myimage localhost:${REGISTRY_PORT}/myimage"
echo "    docker push localhost:${REGISTRY_PORT}/myimage"
