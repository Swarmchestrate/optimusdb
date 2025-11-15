#!/bin/bash

NAMESPACE="cattle-system"
SERVICE_ACCOUNT="admin-user"
TOKEN_FILE="./rancher-token.txt"

echo "🔐 Fetching token for ServiceAccount: $SERVICE_ACCOUNT in namespace: $NAMESPACE"

# Check if the service account exists
if ! kubectl get sa "$SERVICE_ACCOUNT" -n "$NAMESPACE" > /dev/null 2>&1; then
  echo "❌ ServiceAccount '$SERVICE_ACCOUNT' not found in namespace '$NAMESPACE'"
  exit 1
fi

# Check if Kubernetes supports `create token` (Kubernetes >=1.24)
if kubectl -n "$NAMESPACE" create token "$SERVICE_ACCOUNT" > /dev/null 2>&1; then
  TOKEN=$(kubectl -n "$NAMESPACE" create token "$SERVICE_ACCOUNT")
else
  # Fallback for Kubernetes <1.24
  SECRET_NAME=$(kubectl -n "$NAMESPACE" get sa "$SERVICE_ACCOUNT" -o jsonpath="{.secrets[0].name}")
  TOKEN=$(kubectl -n "$NAMESPACE" get secret "$SECRET_NAME" -o jsonpath="{.data.token}" | base64 -d)
fi

# Save to file
echo "$TOKEN" > "$TOKEN_FILE"
chmod 600 "$TOKEN_FILE"

echo "✅ Token saved to $TOKEN_FILE"
echo "📋 Use this token to log in to Rancher or authenticate with kubectl or curl."
