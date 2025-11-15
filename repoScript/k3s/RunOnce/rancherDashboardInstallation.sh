#!/bin/bash

set -e

echo "📦 Adding Rancher Helm repository..."
helm repo add rancher-latest https://releases.rancher.com/server-charts/latest
helm repo update

echo "📁 Creating namespace cattle-system..."
kubectl create namespace cattle-system || true

echo "🔐 Installing cert-manager (required for Rancher)..."
helm repo add jetstack https://charts.jetstack.io
helm repo update

kubectl apply --validate=false -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.3/cert-manager.crds.yaml

helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version v1.14.3

echo "⏳ Waiting for cert-manager to be ready..."
kubectl rollout status deployment cert-manager -n cert-manager --timeout=120s

# Replace this with your desired hostname or external IP
RANCHER_HOSTNAME="rancher.localhost"

echo "🚀 Installing Rancher on hostname: $RANCHER_HOSTNAME"
helm install rancher rancher-latest/rancher \
  --namespace cattle-system \
  --set hostname=$RANCHER_HOSTNAME \
  --set replicas=1

echo "✅ Rancher installation started!"
echo "🔍 You can monitor progress with: kubectl get pods -n cattle-system -w"
echo "🌐 When ready, access Rancher at: https://$RANCHER_HOSTNAME"

echo "⚠️ NOTE: You must map $RANCHER_HOSTNAME to your cluster IP in /etc/hosts if not using DNS."
