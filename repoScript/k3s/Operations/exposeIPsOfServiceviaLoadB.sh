#!/usr/bin/env bash
set -euo pipefail

# Namespace to work in
NS="${NS:-default}"

# Which services to expose: emsserver + any optimusdb* in NS
EXPLICIT_SERVICES=(emsserver)
OPTIMUS_SERVICES=()
while IFS= read -r s; do
  [[ -n "$s" ]] && OPTIMUS_SERVICES+=("$s")
done < <(kubectl -n "$NS" get svc -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -E '^optimusdb' || true)

SERVICES=("${EXPLICIT_SERVICES[@]}" "${OPTIMUS_SERVICES[@]}")

if [[ ${#SERVICES[@]} -eq 0 ]]; then
  echo "No target services found in namespace '$NS'."
  exit 0
fi

echo "🔎 Target services in namespace '$NS': ${SERVICES[*]}"

# Node IP (fallback external IP if K3s doesn't assign one)
NODE_IP="$(hostname -I | awk '{print $1}')"
if [[ -z "${NODE_IP}" ]]; then
  echo "❌ Could not determine node IP."
  exit 1
fi
echo "🖥  Using node IP fallback: ${NODE_IP}"

make_lb() {
  local svc="$1"

  if ! kubectl -n "$NS" get svc "$svc" >/dev/null 2>&1; then
    echo "⚠️  Service '$svc' not found in namespace '$NS' (skipping)."
    return
  fi

  echo "🛠  Patching $svc to type=LoadBalancer (externalTrafficPolicy=Local)..."
  kubectl -n "$NS" patch svc "$svc" --type merge -p '{
    "spec": {
      "type": "LoadBalancer",
      "externalTrafficPolicy": "Local"
    }
  }' >/dev/null

  # Wait up to ~30s for EXTERNAL-IP
  echo "⏳ Waiting up to 30s for EXTERNAL-IP on $svc..."
  for i in {1..30}; do
    ext=$(kubectl -n "$NS" get svc "$svc" -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
    host=$(kubectl -n "$NS" get svc "$svc" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')
    if [[ -n "$ext" || -n "$host" ]]; then
      break
    fi
    sleep 1
  done

  if [[ -z "${ext:-}" && -z "${host:-}" ]]; then
    echo "⚠️  EXTERNAL-IP still pending. Applying externalIPs fallback to ${NODE_IP}..."
    kubectl -n "$NS" patch svc "$svc" --type merge -p "{
      \"spec\": { \"externalIPs\": [\"${NODE_IP}\"] }
    }" >/dev/null
  fi

  # Show final
  echo "📡 Final Service:"
  kubectl -n "$NS" get svc "$svc" -o wide
  echo

  # Show handy curl examples
  echo "🔗 Example endpoints for $svc:"
  # List TCP ports
  mapfile -t ports < <(kubectl -n "$NS" get svc "$svc" -o jsonpath='{range .spec.ports[*]}{.port}{"\n"}{end}')
  extip=$(kubectl -n "$NS" get svc "$svc" -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
  [[ -z "$extip" ]] && extip=$(kubectl -n "$NS" get svc "$svc" -o jsonpath='{.spec.externalIPs[0]}')
  [[ -z "$extip" ]] && extip=$(kubectl -n "$NS" get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' )

  for p in "${ports[@]}"; do
    [[ -n "$p" ]] && echo "  curl -sS http://${extip}:${p}/ -I || true"
  done
  echo
}

for s in "${SERVICES[@]}"; do
  make_lb "$s"
done

echo "✅ Done."
