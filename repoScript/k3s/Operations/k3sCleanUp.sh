

#!/bin/bash

###############################################################################
# cleanup-k3s-preserve-metallb.sh
#
# 🧽 Purpose:
#   Clean up a K3s single-node cluster by removing all workloads, system
#   components, and Helm releases — except for:
#     ✅ the 'default' namespace (your applications)
#     ✅ the 'metallb-system' namespace (MetalLB LoadBalancer support)
#
# 🔧 What this script does:
#   - Uninstalls all Helm releases (except those in 'default' and 'metallb-system')
#   - Deletes all other namespaces (except 'metallb-system', system-reserved ones)
#   - Force-deletes failed/stuck pods (including known bad ones)
#   - Removes K3s default auto-deployed manifests: Traefik, Metrics Server, etc.
#   - Restarts the K3s service to complete cleanup
#
# 📌 Use case:
#   Perfect for reducing cluster noise or resetting a dev/test setup
#   while preserving essential MetalLB support for `LoadBalancer` services.
#
# 🔒 Warning:
#   Only run this on a non-production cluster. It will remove most system services.
###############################################################################

echo "🔧 Starting K3s Cleanup – Keep only 'default' and 'metallb-system'"

# Step 1: Uninstall Helm releases (excluding 'default' and 'metallb-system')
echo "🚮 Uninstalling Helm releases (excluding 'default' and 'metallb-system')..."
helm list -A | tail -n +2 | awk '$2 != "default" && $2 != "metallb-system" {print $1, $2}' | while read release namespace; do
  echo "➡️ Uninstalling $release from namespace $namespace"
  helm uninstall "$release" -n "$namespace" || echo "⚠️ Failed to uninstall $release in $namespace (may already be removed)"
done

# Step 2: Delete non-default namespaces (except 'metallb-system')
echo "🧼 Deleting non-default namespaces (except 'metallb-system')..."
kubectl get ns --no-headers | awk '{print $1}' | grep -vE '^default$|^metallb-system$|^kube-public$|^kube-node-lease$' | while read ns; do
  echo "➡️ Deleting namespace: $ns"
  kubectl delete ns "$ns"
done

# Step 3: Force delete stuck pods
echo "🧹 Force-deleting stuck pods..."
kubectl get pods -A --field-selector=status.phase=Failed -o custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name --no-headers | while read ns pod; do
  echo "➡️ Forcing delete of failed pod: $pod in $ns"
  kubectl delete pod "$pod" -n "$ns" --grace-period=0 --force
done

# Also delete known stuck pods (if present)
known_pods=(
  "default optimusdb2-f9f7fc578-xr8x5"
  "kube-system svclb-optimusdb-8ec58b40-84dbl"
  "kube-system svclb-rancher-972faf75-8vssf"
)
for item in "${known_pods[@]}"; do
  ns=$(echo $item | awk '{print $1}')
  pod=$(echo $item | awk '{print $2}')
  echo "➡️ Forcing delete of known problematic pod: $pod in $ns"
  kubectl delete pod "$pod" -n "$ns" --grace-period=0 --force 2>/dev/null
done

# Step 4: Remove default K3s manifests (Traefik, Metrics, etc.)
echo "🗑 Removing default K3s manifests (Traefik, Metrics, Local Storage, Cert Manager)..."
rm -v /var/lib/rancher/k3s/server/manifests/{traefik.yaml,metrics-server.yaml,local-storage.yaml,cert-manager.yaml} 2>/dev/null

# Step 5: Restart K3s
echo "🔄 Restarting K3s service..."
sudo systemctl restart k3s
sleep 10

# Final check
echo "✅ Final namespaces:"
kubectl get ns
echo
echo "✅ Final pods:"
kubectl get pods -A -o wide

echo
echo "🎉 Cleanup complete! Your cluster now uses only the 'default' namespace and preserves MetalLB."

