#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# Update Docker in-place (keep images/containers) and enable BuildKit + buildx
# Supports Ubuntu 22.04/24.04 (and similar)
# ------------------------------------------------------------------------------

need_sudo() { if [[ $EUID -ne 0 ]]; then echo "sudo -E $0 $*"; exit 1; fi; }

main() {
  # Require root
  if [[ $EUID -ne 0 ]]; then
    echo "Please run as root (or with sudo):"
    echo "  sudo -E $0"
    exit 1
  fi

  # Detect codename (e.g., jammy, noble)
  . /etc/os-release
  CODENAME="${VERSION_CODENAME:-$(lsb_release -sc 2>/dev/null || echo noble)}"
  ARCH="$(dpkg --print-architecture)"

  echo "==> OS: $NAME ($CODENAME), Arch: $ARCH"

  echo "==> Installing prerequisites..."
  apt-get update -y
  apt-get install -y ca-certificates curl gnupg

  echo "==> Adding Docker official GPG key..."
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg

  echo "==> Adding Docker APT repo (stable) for $CODENAME..."
  echo "deb [arch=${ARCH} signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${CODENAME} stable" \
    | tee /etc/apt/sources.list.d/docker.list >/dev/null

  echo "==> Updating APT and installing/upgrading Docker Engine + plugins..."
  apt-get update -y
  apt-get install -y \
    docker-ce docker-ce-cli containerd.io \
    docker-buildx-plugin docker-compose-plugin

  echo "==> Enabling and starting Docker service..."
  systemctl enable --now docker

  # Enable BuildKit by default (daemon-side)
  echo "==> Enabling BuildKit in /etc/docker/daemon.json ..."
  mkdir -p /etc/docker
  if [[ -f /etc/docker/daemon.json ]]; then
    # If jq exists, merge; otherwise add features if missing
    if command -v jq >/dev/null 2>&1; then
      tmpfile="$(mktemp)"
      jq '.features.buildkit=true' /etc/docker/daemon.json > "$tmpfile" || {
        echo "WARN: Could not update daemon.json with jq; leaving file as-is."
        rm -f "$tmpfile"
      }
      if [[ -s "$tmpfile" ]]; then
        cp /etc/docker/daemon.json "/etc/docker/daemon.json.bak.$(date -u +%Y%m%dT%H%M%S)"
        mv "$tmpfile" /etc/docker/daemon.json
      fi
    else
      # Lightweight fallback: only write minimal config if feature not present
      if ! grep -q '"buildkit"' /etc/docker/daemon.json 2>/dev/null; then
        cp /etc/docker/daemon.json "/etc/docker/daemon.json.bak.$(date -u +%Y%m%dT%H%M%S)"
        printf '{\n  "features": { "buildkit": true }\n}\n' > /etc/docker/daemon.json
        echo "NOTE: Replaced daemon.json to enable BuildKit (backup created)."
      else
        echo "BuildKit already referenced in daemon.json; leaving file as-is."
      fi
    fi
  else
    printf '{\n  "features": { "buildkit": true }\n}\n' > /etc/docker/daemon.json
  fi

  echo "==> Restarting Docker..."
  systemctl restart docker

  echo "==> Verifying versions..."
  docker --version || true
  docker buildx version || true
  docker compose version || true

  echo "==> Verifying BuildKit availability..."
  docker info 2>/dev/null | grep -i buildkit || echo "BuildKit enabled via daemon.json."

  echo "==> (Optional) Create and use a buildx builder (recommended)"
  if ! docker buildx ls | grep -q '\*'; then
    docker buildx create --name builder --use || true
    docker buildx inspect --bootstrap || true
  fi

  echo "✅ Done. Docker is updated with BuildKit + buildx."
  echo
  echo "Tips:"
  echo "  • Build with BuildKit features now works:"
  echo "      docker build -t myimage:tag -f Dockerfile ."
  echo "  • Or use buildx explicitly:"
  echo "      docker buildx build -t myimage:tag -f Dockerfile ."
  echo "  • Your images/containers were preserved."
}

main "$@"
