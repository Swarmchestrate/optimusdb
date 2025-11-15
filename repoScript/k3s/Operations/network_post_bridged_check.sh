#!/bin/bash

echo "🚀 Starting Network Post-Bridged Check..."

echo "🧪 Checking Host Networking (VM)..."
echo "------------------------------------"
echo "🔎 IP Address:"
ip a

echo
echo "🌎 Pinging Google DNS (8.8.8.8)..."
ping -c 4 8.8.8.8 || { echo "❌ Ping to 8.8.8.8 failed!"; exit 1; }

echo
echo "🌍 Pinging Google.com..."
ping -c 4 www.google.com || { echo "❌ Ping to www.google.com failed!"; exit 1; }

echo
echo "🌐 Curl to npmjs.org..."
curl -I https://registry.npmjs.org/ || { echo "❌ Curl to npmjs.org failed!"; exit 1; }

echo
echo "🛠 Testing DNS Resolution with dig (if available)..."
if command -v dig &> /dev/null; then
    dig registry.npmjs.org || { echo "❌ Dig failed!"; exit 1; }
else
    echo "⚠️  'dig' not installed. Skipping dig test."
fi

echo
echo "🐳 Testing Docker Internet Access (node + npm)..."
docker run --rm node:18-alpine sh -c "npm view ace-builds" || { echo "❌ Docker npm test failed!"; exit 1; }

echo
echo "✅ All tests passed. Your Bridged Network setup is working perfectly! 🎉"
exit 0

