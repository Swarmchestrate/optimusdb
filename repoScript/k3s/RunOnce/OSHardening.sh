#!/bin/bash

################################################################################
# Idempotent K3s OS-Level Preparation Script for minOS or Lightweight Linux
#
# This script ensures the system is ready for K3s by:
# - Ensuring kernel modules are loaded
# - Applying sysctl parameters only if not set
# - Disabling swap safely
# - Installing packages conditionally
# - Verifying overlay FS and cgroup support
# - Configuring time sync and system limits only if needed
################################################################################

set -e

echo "🔧 Starting idempotent OS configuration for K3s..."

# 1. Ensure required kernel modules
echo "📦 Checking kernel modules..."
for mod in overlay br_netfilter; do
  if ! lsmod | grep -q "$mod"; then
    echo "➕ Loading module: $mod"
    modprobe "$mod"
  else
    echo "✅ Module $mod already loaded"
  fi
done

modules_file="/etc/modules-load.d/k3s.conf"
for mod in overlay br_netfilter; do
  grep -qxF "$mod" "$modules_file" 2>/dev/null || echo "$mod" >> "$modules_file"
done

# 2. Set sysctl parameters only if missing or incorrect
echo "⚙️ Verifying sysctl settings..."
declare -A sysctl_settings=(
  ["net.bridge.bridge-nf-call-iptables"]=1
  ["net.ipv4.ip_forward"]=1
  ["net.bridge.bridge-nf-call-ip6tables"]=1
)

for key in "${!sysctl_settings[@]}"; do
  current=$(sysctl -n "$key" 2>/dev/null || echo "")
  if [[ "$current" != "${sysctl_settings[$key]}" ]]; then
    echo "🔧 Setting $key = ${sysctl_settings[$key]}"
    sysctl -w "$key=${sysctl_settings[$key]}"
  else
    echo "✅ $key already set correctly"
  fi
done

# Save to persistent file
sysctl_conf="/etc/sysctl.d/99-k3s.conf"
for key in "${!sysctl_settings[@]}"; do
  grep -q "$key" "$sysctl_conf" 2>/dev/null || echo "$key = ${sysctl_settings[$key]}" >> "$sysctl_conf"
done

# Apply sysctl settings
sysctl --system

# 3. Disable swap safely
echo "❌ Disabling swap if active..."
if swapon --summary | grep -q '^'; then
  swapoff -a
  sed -i.bak '/ swap / s/^/#/' /etc/fstab
  echo "✅ Swap disabled"
else
  echo "✅ Swap already disabled"
fi

# 4. Install dependencies if not installed
echo "📦 Installing required packages if missing..."
PKGS="curl iptables ipset conntrack net-tools e2fsprogs chrony"
if command -v apt >/dev/null 2>&1; then
    apt update
    for pkg in $PKGS; do
      dpkg -s "$pkg" >/dev/null 2>&1 || apt install -y "$pkg"
    done
elif command -v yum >/dev/null 2>&1; then
    for pkg in $PKGS; do
      rpm -q "$pkg" >/dev/null 2>&1 || yum install -y "$pkg"
    done
elif command -v apk >/dev/null 2>&1; then
    for pkg in $PKGS; do
      apk info -e "$pkg" >/dev/null 2>&1 || apk add "$pkg"
    done
else
    echo "⚠️ Unsupported package manager. Install $PKGS manually."
fi

# 5. Enable time sync
echo "⏰ Ensuring chrony is enabled..."
systemctl enable chronyd --now 2>/dev/null || systemctl enable chrony --now 2>/dev/null || echo "⚠️ Chrony not installed or no systemd"

# 6. Check for cgroup and overlay support
echo "🧠 Verifying cgroup and overlay support..."
grep -q cgroup /proc/filesystems && echo "✅ Cgroup supported" || echo "⚠️ Cgroup NOT supported"
grep -q overlay /proc/filesystems && echo "✅ OverlayFS supported" || echo "⚠️ OverlayFS NOT supported"

# 7. Optional: Increase open file limits
limits_file="/etc/security/limits.d/99-k3s.conf"
if [ ! -f "$limits_file" ]; then
  echo "📈 Setting open file limits..."
  cat <<EOF > "$limits_file"
* soft nofile 1048576
* hard nofile 1048576
EOF
else
  echo "✅ File descriptor limits already configured"
fi

echo "✅ OS-level preparation for K3s is complete."
