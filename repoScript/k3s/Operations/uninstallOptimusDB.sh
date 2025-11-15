#!/bin/bash
set -euo pipefail

NAMESPACE="default"
RELEASE_BASE="optimusdb"
# Make Helm use the K3s kubeconfig
export KUBECONFIG="/etc/rancher/k3s/k3s.yaml"

echo "🧹 Uninstalling OptimusDB Helm releases from namespace: $NAMESPACE"

for i in 1 2 3; do
  RELEASE_NAME="${RELEASE_BASE}-${i}"
  echo "🔸 Uninstalling $RELEASE_NAME..."
  helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" || echo "⚠️  $RELEASE_NAME not found, skipping."
done

echo "✅ All OptimusDB releases removed."

echo "🔎 Remaining resources in $NAMESPACE:"
kubectl -n "$NAMESPACE" get all | grep optimusdb || echo "✅ No OptimusDB resources found."

