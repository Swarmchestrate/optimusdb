#!/bin/bash

###############################################################################
# cleanUplocal_ImageRegistry.sh
#
# 📦 Deletes all images from the local Docker registry and from the local host.
# 🧹 Runs garbage collection to clean registry layers.
# 🧼 Optionally removes local Docker images after cleanup.
#
# Author: ICCS
# Updated: 2025-04-21
###############################################################################

set -euo pipefail

REGISTRY_NAME="registry"
REGISTRY_PORT=5000
REGISTRY_DATA_DIR="/var/lib/registry"
REGISTRY_CONFIG_DIR="/etc/docker/registry"
REGISTRY_CONFIG_FILE="$REGISTRY_CONFIG_DIR/config.yml"
K3S_REG_FILE="/etc/rancher/k3s/registries.yaml"
REGISTRY_URL="http://localhost:$REGISTRY_PORT"

# Get host IP
IP=$(ip route get 1 | awk '{print $7; exit}')
echo "📡 Host IP detected: $IP"

# -----------------------------------------------------------------------------
# Step 1: Configure Docker Registry (with delete enabled)
# -----------------------------------------------------------------------------
echo "🧾 Configuring Docker Registry..."
sudo mkdir -p "$REGISTRY_CONFIG_DIR"
sudo tee "$REGISTRY_CONFIG_FILE" > /dev/null <<EOF
version: 0.1
storage:
  filesystem:
    rootdirectory: /var/lib/registry
  delete:
    enabled: true
http:
  addr: :5000
EOF

# -----------------------------------------------------------------------------
# Step 2: Start or create registry
# -----------------------------------------------------------------------------
if docker ps -a --format '{{.Names}}' | grep -q "^$REGISTRY_NAME$"; then
  echo "✅ Registry container exists. Starting..."
  docker start "$REGISTRY_NAME" > /dev/null || true
else
  echo "🚀 Creating Docker registry container..."
  docker run -d \
    -p "$IP:$REGISTRY_PORT:5000" \
    --restart=always \
    --name "$REGISTRY_NAME" \
    -v "$REGISTRY_DATA_DIR:/var/lib/registry" \
    -v "$REGISTRY_CONFIG_FILE:/etc/docker/registry/config.yml" \
    registry:2
fi

sleep 2

# -----------------------------------------------------------------------------
# Step 3: Configure K3s registry access (if needed)
# -----------------------------------------------------------------------------
if [ ! -f "$K3S_REG_FILE" ] || ! grep -q "$IP:$REGISTRY_PORT" "$K3S_REG_FILE"; then
  echo "🔧 Configuring K3s registry access..."
  sudo mkdir -p "$(dirname "$K3S_REG_FILE")"
  sudo tee "$K3S_REG_FILE" > /dev/null <<EOF
mirrors:
  "$IP:$REGISTRY_PORT":
    endpoint:
      - "http://$IP:$REGISTRY_PORT"
EOF
  sudo systemctl restart k3s
else
  echo "✅ K3s already configured for local registry access."
fi

# -----------------------------------------------------------------------------
# Step 4: Delete all registry images/tags via API
# -----------------------------------------------------------------------------
echo "🧨 Deleting all image tags from the registry..."
REPOS=$(curl -s "$REGISTRY_URL/v2/_catalog" | sed -e 's/[{}"]//g' | cut -d: -f2 | tr ',' '\n')

for REPO in $REPOS; do
  TAGS=$(curl -s "$REGISTRY_URL/v2/$REPO/tags/list" | sed -e 's/[{}"]//g' | grep tags | cut -d: -f2 | tr ',' '\n')
  for TAG in $TAGS; do
    echo "🔎 Deleting $REPO:$TAG..."
    DIGEST=$(curl -sI -H "Accept: application/vnd.docker.distribution.manifest.v2+json" \
      "$REGISTRY_URL/v2/$REPO/manifests/$TAG" \
      | grep Docker-Content-Digest | awk '{print $2}' | tr -d $'\r')
    if [ -n "$DIGEST" ]; then
      curl -s -X DELETE "$REGISTRY_URL/v2/$REPO/manifests/$DIGEST"
      echo "✅ Deleted $REPO@$DIGEST"
    else
      echo "⚠️  Could not resolve digest for $REPO:$TAG"
    fi
  done
done

# -----------------------------------------------------------------------------
# Step 6: Remove local Docker images matching registry
# -----------------------------------------------------------------------------
sudo docker images --format '{{.Repository}}:{{.Tag}}' \
  | grep "^${IPA}" \
  | while read -r image; do
      echo "🗑️  Removing $image"
      sudo docker rmi -f "$image" || echo "⚠️  Failed to remove $image"
  done

# -----------------------------------------------------------------------------
# Step 5: Garbage collection
# -----------------------------------------------------------------------------
echo "🛑 Stopping registry container for garbage collection..."
docker stop "$REGISTRY_NAME"

echo "🧹 Running registry garbage collection..."
docker run --rm \
  -v "$REGISTRY_DATA_DIR:/var/lib/registry" \
  -v "$REGISTRY_CONFIG_FILE:/etc/docker/registry/config.yml" \
  registry:2 \
  bin/registry garbage-collect /etc/docker/registry/config.yml

echo "🚀 Restarting Docker registry..."
docker start "$REGISTRY_NAME"
