# Simple direct test - save as test-tinyllama-direct.ps1

param([int]$NodeNumber = 1)

Write-Host "🧪 Direct TinyLlama Test - Node $NodeNumber" -ForegroundColor Cyan
Write-Host "="*60 -ForegroundColor Gray

# Test 1: Simple prompt test
Write-Host "`n1️⃣ Testing TinyLlama with simple prompt..." -ForegroundColor Yellow

$result = docker exec optimusdb$NodeNumber bash -c @'
echo '{"prompt":"Hello, describe a laptop","max_tokens":30}' | curl -s -X POST http://127.0.0.1:8080/v1/completions -H "Content-Type: application/json" -d @-
'@

Write-Host "Response:" -ForegroundColor Cyan
Write-Host $result -ForegroundColor Gray

# Test 2: Parse and display nicely
Write-Host "`n2️⃣ Parsing response..." -ForegroundColor Yellow
try {
    $json = $result | ConvertFrom-Json
    if ($json.choices) {
        Write-Host "✅ Success! Generated text:" -ForegroundColor Green
        Write-Host "   $($json.choices[0].text)" -ForegroundColor White
    } elseif ($json.error) {
        Write-Host "❌ Error: $($json.error.message)" -ForegroundColor Red
    }
} catch {
    Write-Host "⚠️  Could not parse JSON" -ForegroundColor Yellow
}

# Test 3: Test with product description
Write-Host "`n3️⃣ Testing with product description..." -ForegroundColor Yellow

$result2 = docker exec optimusdb$NodeNumber bash -c @'
echo '{"prompt":"Summarize this product: High-performance gaming laptop with NVIDIA RTX 4090 GPU, 32GB RAM, and 2TB SSD storage. Perfect for gaming and content creation.","max_tokens":50}' | curl -s -X POST http://127.0.0.1:8080/v1/completions -H "Content-Type: application/json" -d @-
'@

try {
    $json2 = $result2 | ConvertFrom-Json
    if ($json2.choices) {
        Write-Host "✅ Generated summary:" -ForegroundColor Green
        Write-Host "   $($json2.choices[0].text)" -ForegroundColor White
    }
} catch {
    Write-Host "Raw: $result2" -ForegroundColor Gray
}

# Test 4: Monitor TinyLlama logs
Write-Host "`n4️⃣ Checking TinyLlama logs for activity..." -ForegroundColor Yellow
docker exec optimusdb$NodeNumber tail -20 /var/log/supervisor/tinyllama.log

Write-Host "`n✅ Test complete!" -ForegroundColor Green