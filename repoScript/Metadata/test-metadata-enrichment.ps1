# test-metadata-enrichment.ps1 (FIXED VERSION)
# Comprehensive test for OptimusDB metadata enrichment

param(
    [int]$NodeNumber = 1,
    [int]$Port = 18001
)

$baseUrl = "http://localhost:$Port"

Write-Host "🧪 Testing OptimusDB Metadata Enrichment on Node $NodeNumber" -ForegroundColor Cyan
Write-Host "="*70 -ForegroundColor Gray

# Test 1: Check if services are running using ps
Write-Host "`n1️⃣ Checking services status..." -ForegroundColor Yellow
$processes = docker exec optimusdb$NodeNumber ps aux

$supervisorRunning = $processes | Select-String -Pattern "supervisord" -Quiet
$llamaRunning = $processes | Select-String -Pattern "llama-server" -Quiet
$optimusRunning = $processes | Select-String -Pattern "/usr/local/bin/optimusdb" -Quiet

if ($supervisorRunning) {
    Write-Host "  ✅ Supervisor: RUNNING" -ForegroundColor Green
} else {
    Write-Host "  ❌ Supervisor: NOT RUNNING" -ForegroundColor Red
}

if ($llamaRunning) {
    Write-Host "  ✅ TinyLlama: RUNNING" -ForegroundColor Green
} else {
    Write-Host "  ❌ TinyLlama: NOT RUNNING" -ForegroundColor Red
}

if ($optimusRunning) {
    Write-Host "  ✅ OptimusDB: RUNNING" -ForegroundColor Green
} else {
    Write-Host "  ❌ OptimusDB: NOT RUNNING" -ForegroundColor Red
    exit 1
}

if (-not ($llamaRunning -and $optimusRunning)) {
    Write-Host "`n❌ Critical services not running. Exiting." -ForegroundColor Red
    exit 1
}

# Test 2: Test TinyLlama endpoint directly using proper JSON escaping
Write-Host "`n2️⃣ Testing TinyLlama endpoint..." -ForegroundColor Yellow

# Create a temp JSON file inside the container to avoid escaping issues
$testScript = @'
cat > /tmp/test_prompt.json << 'EOF'
{
  "prompt": "Describe: gaming laptop",
  "max_tokens": 30,
  "temperature": 0.7
}
EOF

curl -s -X POST http://127.0.0.1:8080/v1/completions \
  -H "Content-Type: application/json" \
  -d @/tmp/test_prompt.json
'@

$llamaTest = docker exec optimusdb$NodeNumber bash -c $testScript

if ($llamaTest) {
    Write-Host "✅ TinyLlama responding" -ForegroundColor Green
    try {
        $response = $llamaTest | ConvertFrom-Json -ErrorAction Stop
        if ($response.choices) {
            Write-Host "Generated text: $($response.choices[0].text)" -ForegroundColor Cyan
        } elseif ($response.error) {
            Write-Host "⚠️  Error from TinyLlama: $($response.error.message)" -ForegroundColor Yellow
        } else {
            Write-Host "Raw response: $llamaTest" -ForegroundColor Gray
        }
    } catch {
        Write-Host "Raw response: $llamaTest" -ForegroundColor Gray
    }
} else {
    Write-Host "❌ TinyLlama not responding" -ForegroundColor Red
}

# Test 3: Check listening ports
Write-Host "`n3️⃣ Checking listening ports..." -ForegroundColor Yellow
$ports = docker exec optimusdb$NodeNumber netstat -tlnp 2>$null | Select-String -Pattern "LISTEN"
if ($ports) {
    Write-Host "Active ports:" -ForegroundColor Cyan
    $ports | ForEach-Object {
        if ($_ -match "8080|8089") {
            Write-Host "  ✅ $_" -ForegroundColor Green
        } else {
            Write-Host "  $_" -ForegroundColor White
        }
    }
}

# Test 4: Check OptimusDB connectivity
Write-Host "`n4️⃣ Checking OptimusDB connectivity..." -ForegroundColor Yellow
Write-Host "OptimusDB is listening on:" -ForegroundColor Cyan
Write-Host "  - Internal port 4001 (IPFS)" -ForegroundColor White
Write-Host "  - External port 8089 (mapped to $Port on host)" -ForegroundColor White

# Test 5: Check OptimusDB logs
Write-Host "`n5️⃣ Checking OptimusDB logs..." -ForegroundColor Yellow
$startupLogs = docker exec optimusdb$NodeNumber tail -50 /var/log/supervisor/optimusdb.log
if ($startupLogs) {
    Write-Host "Recent OptimusDB logs:" -ForegroundColor Cyan
    $startupLogs | Select-Object -Last 20 | ForEach-Object {
        if ($_ -match "error|fail|fatal" -and $_ -notmatch "(?i)no error|Could not find fonts") {
            Write-Host "  ⚠️  $_" -ForegroundColor Red
        } elseif ($_ -match "started|listening|ready|initialized|Connected") {
            Write-Host "  ✅ $_" -ForegroundColor Green
        } else {
            Write-Host "  $_" -ForegroundColor Gray
        }
    }
}

# Test 6: Check TinyLlama model loading
Write-Host "`n6️⃣ Checking TinyLlama model loading..." -ForegroundColor Yellow
$llamaLogs = docker exec optimusdb$NodeNumber grep -i "model loaded\|listening\|server" /var/log/supervisor/tinyllama.log 2>$null
if ($llamaLogs) {
    Write-Host "✅ TinyLlama status:" -ForegroundColor Green
    $llamaLogs | Select-Object -Last 5 | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
} else {
    Write-Host "ℹ️  Checking full logs..." -ForegroundColor Cyan
    docker exec optimusdb$NodeNumber tail -20 /var/log/supervisor/tinyllama.log
}

# Test 7: Multiple TinyLlama generation tests with proper JSON
Write-Host "`n7️⃣ Testing metadata generation with multiple prompts..." -ForegroundColor Yellow

$testPrompts = @(
    "Summarize: gaming laptop with RTX 4090",
    "Keywords: wireless mouse",
    "Describe: mechanical keyboard"
)

foreach ($i in 0..($testPrompts.Count - 1)) {
    $prompt = $testPrompts[$i]
    Write-Host "`n  Test $($i+1): $prompt" -ForegroundColor Cyan

    # Create JSON file inside container
    $cmdScript = @"
cat > /tmp/prompt$i.json << 'JSONEOF'
{
  "prompt": "$prompt",
  "max_tokens": 40,
  "temperature": 0.7,
  "stop": ["\n"]
}
JSONEOF

curl -s -X POST http://127.0.0.1:8080/v1/completions \
  -H "Content-Type: application/json" \
  -d @/tmp/prompt$i.json
"@

    $result = docker exec optimusdb$NodeNumber bash -c $cmdScript

    if ($result) {
        try {
            $parsed = $result | ConvertFrom-Json
            if ($parsed.choices -and $parsed.choices[0].text) {
                $text = $parsed.choices[0].text.Trim()
                Write-Host "  ✅ Generated: $text" -ForegroundColor Green
            } elseif ($parsed.error) {
                Write-Host "  ❌ Error: $($parsed.error.message)" -ForegroundColor Red
            } else {
                Write-Host "  ⚠️  Unexpected response format" -ForegroundColor Yellow
            }
        } catch {
            Write-Host "  ⚠️  Could not parse response: $result" -ForegroundColor Yellow
        }
    }

    Start-Sleep -Seconds 1
}

# Test 8: Search for metadata activity
Write-Host "`n8️⃣ Searching for metadata/enrichment activity..." -ForegroundColor Yellow
$metadataActivity = docker exec optimusdb$NodeNumber grep -i "metadata\|enrichment\|tinyllama\|llama.*http" /var/log/supervisor/optimusdb.log 2>$null

if ($metadataActivity) {
    Write-Host "✅ Found metadata-related log entries:" -ForegroundColor Green
    $metadataActivity | Select-Object -Last 15 | ForEach-Object {
        if ($_ -match "error|fail") {
            Write-Host "  ⚠️  $_" -ForegroundColor Red
        } else {
            Write-Host "  $_" -ForegroundColor Gray
        }
    }
} else {
    Write-Host "ℹ️  No metadata enrichment activity detected yet." -ForegroundColor Cyan
    Write-Host "   This means OptimusDB may:" -ForegroundColor White
    Write-Host "   - Not have received data insertion requests yet" -ForegroundColor White
    Write-Host "   - Trigger metadata generation only on specific operations" -ForegroundColor White
    Write-Host "   - Use a different endpoint or logging pattern" -ForegroundColor White
}

# Test 9: Check for any HTTP calls from OptimusDB to TinyLlama
Write-Host "`n9️⃣ Checking TinyLlama access logs..." -ForegroundColor Yellow
$llamaAccess = docker exec optimusdb$NodeNumber grep -i "POST\|completion\|/v1" /var/log/supervisor/tinyllama.log 2>$null | Select-Object -Last 10

if ($llamaAccess) {
    Write-Host "✅ TinyLlama received requests:" -ForegroundColor Green
    $llamaAccess | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
} else {
    Write-Host "ℹ️  No incoming requests to TinyLlama yet from OptimusDB" -ForegroundColor Cyan
}

# Test 10: Resource usage
Write-Host "`n🔟 Resource usage..." -ForegroundColor Yellow
$stats = docker stats optimusdb$NodeNumber --no-stream --format "{{.Container}}: {{.CPUPerc}} CPU, {{.MemUsage}}"
Write-Host "  $stats" -ForegroundColor White

# Summary
Write-Host "`n" + ("="*70) -ForegroundColor Gray
Write-Host "✅ Test Suite Complete!" -ForegroundColor Green
Write-Host "`n📊 Summary:" -ForegroundColor Cyan
Write-Host "  Services:" -ForegroundColor White
Write-Host "    - Supervisor: $(if($supervisorRunning){'✅ Running'}else{'❌ Stopped'})" -ForegroundColor White
Write-Host "    - TinyLlama: $(if($llamaRunning){'✅ Running (Port 8080)'}else{'❌ Stopped'})" -ForegroundColor White
Write-Host "    - OptimusDB: $(if($optimusRunning){'✅ Running (Port 8089)'}else{'❌ Stopped'})" -ForegroundColor White

Write-Host "`n🎯 To Trigger Metadata Enrichment:" -ForegroundColor Cyan
Write-Host "  You need to insert data into OptimusDB via its API." -ForegroundColor White
Write-Host "  Check your OptimusDB API documentation for:" -ForegroundColor White
Write-Host "    - Data insertion endpoints" -ForegroundColor White
Write-Host "    - SQL execution endpoints" -ForegroundColor White
Write-Host "    - Document/record creation methods" -ForegroundColor White

Write-Host "`n📝 Monitoring Commands:" -ForegroundColor Cyan
Write-Host "  # Watch OptimusDB logs" -ForegroundColor Gray
Write-Host "  docker exec optimusdb$NodeNumber tail -f /var/log/supervisor/optimusdb.log" -ForegroundColor White
Write-Host "`n  # Watch TinyLlama logs" -ForegroundColor Gray
Write-Host "  docker exec optimusdb$NodeNumber tail -f /var/log/supervisor/tinyllama.log" -ForegroundColor White
Write-Host ""