#!/bin/bash

set -e

# Get local IP address
IP=$(ip route get 1 | awk '{print $7; exit}')
echo "📡 Detected IP: $IP"

# Registry config
REGISTRY_NAME=registry
REGISTRY_PORT=5000

# Check if registry container exists
if docker ps -a --format '{{.Names}}' | grep -q "^$REGISTRY_NAME$"; then
  echo "✅ Registry container already exists."
  docker start $REGISTRY_NAME > /dev/null || true
else
  echo "🚀 Creating and starting Docker registry at $IP:$REGISTRY_PORT..."
  docker run -d \
    -p ${IP}:${REGISTRY_PORT}:5000 \
    --restart=always \
    --name $REGISTRY_NAME \
    registry:2
fi

# Create registries.yaml only if it doesn't exist or IP has changed
REG_FILE="/etc/rancher/k3s/registries.yaml"
if [ ! -f "$REG_FILE" ] || ! grep -q "$IP:$REGISTRY_PORT" "$REG_FILE"; then
  echo "🛠️  Configuring K3s to use local registry ($IP:$REGISTRY_PORT)..."
  sudo mkdir -p /etc/rancher/k3s
  cat <<EOF | sudo tee $REG_FILE
mirrors:
  "$IP:$REGISTRY_PORT":
    endpoint:
      - "http://$IP:$REGISTRY_PORT"
EOF

  echo "🔄 Restarting K3s to apply registry configuration..."
  sudo systemctl restart k3s
else
  echo "✅ K3s already configured for local registry."
fi

echo "🎉 Local Docker registry is ready at http://$IP:$REGISTRY_PORT"
