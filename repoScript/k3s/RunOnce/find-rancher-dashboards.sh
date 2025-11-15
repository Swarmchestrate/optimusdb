
#!/bin/bash

echo "🔍 Searching for Rancher Dashboard services across all namespaces..."
echo "--------------------------------------------------------------------------------------"
printf "%-20s %-30s %-15s %-25s %-15s\n" "NAMESPACE" "SERVICE NAME" "TYPE" "EXTERNAL-IP" "PORT(S)"
echo "--------------------------------------------------------------------------------------"

kubectl get svc --all-namespaces | grep -i rancher | while read -r namespace name type cluster_ip external_ip ports age; do
  printf "%-20s %-30s %-15s %-25s %-15s\n" "$namespace" "$name" "$type" "$external_ip" "$ports"
done

