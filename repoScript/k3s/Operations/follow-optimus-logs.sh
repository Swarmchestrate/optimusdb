#!/bin/bash

NAMESPACE="default"

echo "📡 Starting log follower for pods starting with 'optimus' in namespace: $NAMESPACE"

# Track already followed pods
declare -A FOLLOWED

while true; do
  # Get all pod names starting with 'optimus'
  PODS=$(kubectl get pods -n "$NAMESPACE" --no-headers | awk '/^optimus/ {print $1}')

  for POD in $PODS; do
    # If we haven't started following this pod, do it now
    if [[ -z "${FOLLOWED[$POD]}" ]]; then
      echo "➡️  Following logs for new pod: $POD"
      kubectl logs -n "$NAMESPACE" -f "$POD" &
      FOLLOWED[$POD]=1
    fi
  done

  sleep 10
done
