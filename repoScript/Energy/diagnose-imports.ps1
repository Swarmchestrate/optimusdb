# Diagnose Import Cycle in OptimusDB
# Run: .\scripts\diagnose-imports.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "OptimusDB Import Cycle Diagnostics" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
cd C:\Users\georg\GolandProjects\optimusdb-lsa
# Check if in correct directory
if (-not (Test-Path "go.mod")) {
    Write-Host "ERROR: Not in OptimusDB root directory!" -ForegroundColor Red
    Write-Host "Please run from C:\Users\georg\GolandProjects\optimusdb-lsa" -ForegroundColor Yellow
    exit 1
}

Write-Host "Scanning for import statements..." -ForegroundColor Yellow
Write-Host ""

# Find all Go files
$goFiles = Get-ChildItem -Path . -Recurse -Filter "*.go" -Exclude "*_test.go"

# Track imports
$imports = @{}

foreach ($file in $goFiles) {
    $relativePath = $file.FullName.Replace($PWD.Path, "").TrimStart("\")
    $package = Split-Path (Split-Path $relativePath -Parent) -Leaf

    $content = Get-Content $file.FullName

    foreach ($line in $content) {
        if ($line -match '^\s*"optimusdb/(api|app|contextualmetadata)') {
            Write-Host "  $relativePath" -ForegroundColor White
            Write-Host "    → $($matches[0].Trim('"'))" -ForegroundColor Gray
        }
    }
}

Write-Host ""
Write-Host "Checking for cycles with Go tools..." -ForegroundColor Yellow
go list -f '{{.ImportPath}}' ./... 2>&1 | Select-String "cycle"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan