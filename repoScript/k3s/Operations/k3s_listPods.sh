#!/bin/bash

echo "📦 Listing all pods with IPs and status across all namespaces..."
echo "------------------------------------------------------------------------------------"
printf "%-20s %-35s %-15s %-15s %-10s\n" "NAMESPACE" "POD NAME" "POD IP" "NODE IP" "STATUS"
echo "------------------------------------------------------------------------------------"

# Build a map of node name -> external IP
declare -A NODE_EXTERNAL_IPS

while read -r name addresses; do
  NODE_EXTERNAL_IPS["$name"]="$addresses"
done < <(kubectl get nodes -o custom-columns="NAME:.metadata.name,EXTERNAL-IP:.status.addresses[?(@.type==\"ExternalIP\")].address" --no-headers)

# Get pod info
kubectl get pods --all-namespaces -o wide --no-headers | while read -r namespace pod ready status rest; do
  pod_ip=$(echo "$rest" | awk '{print $5}')
  node=$(echo "$rest" | awk '{print $6}')
  node_ip="${NODE_EXTERNAL_IPS[$node]:-N/A}"

  printf "%-20s %-35s %-15s %-15s %-10s\n" "$namespace" "$pod" "$pod_ip" "$node_ip" "$status"
done
