#!/bin/bash

################################################################################
# uninstall_EMS.sh
#
# Description:
# Uninstalls the EMS server deployment from Kubernetes (Helm release + namespace
# resources) and optionally removes local Docker images.
#
# Author: George Georgakakos, ICCS
# Updated: 2025-09-13
################################################################################

set -euo pipefail

RELEASE_NAME="emsserver"
NAMESPACE="default"
REGISTRY="127.0.0.1:5000"
IMAGE_NAME="emsserver"

echo "🚀 Starting EMS uninstallation..."

# Step 1: Helm uninstall
if helm status "$RELEASE_NAME" -n "$NAMESPACE" > /dev/null 2>&1; then
  echo "🗑️  Uninstalling Helm release: $RELEASE_NAME (namespace: $NAMESPACE)..."
  helm uninstall "$RELEASE_NAME" -n "$NAMESPACE"
else
  echo "ℹ️  Helm release $RELEASE_NAME not found in namespace $NAMESPACE."
fi

# Step 2: Delete orphaned Kubernetes resources (if any)
echo "🧹 Cleaning up leftover resources..."
kubectl delete all -n "$NAMESPACE" -l app.kubernetes.io/name=$RELEASE_NAME --ignore-not-found=true
kubectl delete pvc -n "$NAMESPACE" -l app.kubernetes.io/name=$RELEASE_NAME --ignore-not-found=true

# Step 3: Remove local Docker image (optional)
if docker images "$REGISTRY/$IMAGE_NAME" --format '{{.Repository}}:{{.Tag}}' | grep -q "$REGISTRY/$IMAGE_NAME"; then
  echo "🗑️  Removing local Docker images for $REGISTRY/$IMAGE_NAME..."
  docker rmi -f $(docker images "$REGISTRY/$IMAGE_NAME" -q)
else
  echo "ℹ️  No local Docker images found for $REGISTRY/$IMAGE_NAME."
fi

echo "✅ EMS uninstallation complete!"
