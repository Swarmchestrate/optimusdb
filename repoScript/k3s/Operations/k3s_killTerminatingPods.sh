#!/bin/bash
# Force delete all pods stuck in "Terminating" across all namespaces
# Works for K3s / Kubernetes

echo "🔍 Searching for Terminating pods..."
terminating_pods=$(kubectl get pods --all-namespaces --field-selector=status.phase=Pending,status.phase=Unknown 2>/dev/null \
    && kubectl get pods --all-namespaces | grep Terminating | awk '{print $1" "$2}')

if [ -z "$terminating_pods" ]; then
    echo "✅ No Terminating pods found."
    exit 0
fi

echo "⚠️ Found the following Terminating pods:"
echo "$terminating_pods"
echo "-----------------------------------------------------"

while read -r ns pod; do
    if [ -n "$ns" ] && [ -n "$pod" ]; then
        echo "🗑️  Deleting pod $pod in namespace $ns ..."
        kubectl delete pod "$pod" -n "$ns" --grace-period=0 --force
    fi
done <<< "$terminating_pods"

echo "-----------------------------------------------------"
echo "✅ Cleanup finished. Current pod status:"
kubectl get pods -A | grep -E "Running|Pending|CrashLoopBackOff|Completed"

