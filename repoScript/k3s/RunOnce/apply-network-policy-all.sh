#!/bin/bash

# Your allow-all NetworkPolicy definition
read -r -d '' POLICY <<EOF
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-all
spec:
  podSelector: {}
  ingress:
  - {}
  egress:
  - {}
  policyTypes:
  - Ingress
  - Egress
EOF

# Apply to all existing namespaces
for ns in $(kubectl get ns -o jsonpath='{.items[*].metadata.name}'); do
  echo "🔄 Applying to namespace: $ns"
  echo "$POLICY" | kubectl apply -n "$ns" -f -
done
