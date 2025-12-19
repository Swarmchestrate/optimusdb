<#
.SYNOPSIS
    Upload TOSCA files to OptimusDB with full structure support
.DESCRIPTION
    Uploads multiple TOSCA files to OptimusDB, storing them with queryable nested structure
.PARAMETER BaseURL
    Base URL of OptimusDB API (default: http://localhost:18001)
.PARAMETER FilesPath
    Directory containing TOSCA files (default: current directory)
#>

param(
    [string]$BaseURL = "http://localhost:18001",
    [string]$FilesPath = "."
)

$ErrorActionPreference = "Stop"

# Color functions
function Write-Success { param($Message) Write-Host "✅ $Message" -ForegroundColor Green }
function Write-Failure { param($Message) Write-Host "❌ $Message" -ForegroundColor Red }
function Write-Info { param($Message) Write-Host "ℹ️  $Message" -ForegroundColor Cyan }

# TOSCA files to upload
$toscaFiles = @(
    @{
        Filename = "webapp_adt.yaml"
        Description = "WebApp Microservices Application"
    },
    @{
        Filename = "capacity_profile.yaml"
        Description = "Edge Cluster Capacity Profile"
    },
    @{
        Filename = "opentofu_hybrid.yaml"
        Description = "Hybrid Infrastructure with OpenTofu"
    },
    @{
        Filename = "deployment_plan.yaml"
        Description = "Deployment Plan with Workflows"
    },
    @{
        Filename = "app_requirements.yaml"
        Description = "ML Training Application Requirements"
    }
)

Write-Info "Starting TOSCA file upload to $BaseURL"
Write-Host ""

$uploadedFiles = @()
$failedFiles = @()

foreach ($file in $toscaFiles) {
    $filepath = Join-Path $FilesPath $file.Filename

    if (-not (Test-Path $filepath)) {
        Write-Failure "File not found: $filepath"
        $failedFiles += $file.Filename
        continue
    }

    Write-Info "Uploading: $($file.Description) ($($file.Filename))"

    try {
        # Read file and convert to base64
        $fileContent = Get-Content $filepath -Raw
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($fileContent)
        $base64 = [Convert]::ToBase64String($bytes)

        # Prepare request body
        $body = @{
            file = $base64
            filename = $file.Filename
            store_full_structure = $true
        } | ConvertTo-Json -Depth 10

        # Upload to OptimusDB
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/upload" `
            -Method Post `
            -Body $body `
            -ContentType "application/json" `
            -TimeoutSec 30

        if ($response.status -eq 200) {
            Write-Success "Uploaded successfully"
            Write-Host "  Template ID: $($response.data.template_id)" -ForegroundColor Gray
            Write-Host "  Queryable: $($response.data.queryable)" -ForegroundColor Gray
            Write-Host "  Storage: $($response.data.storage_location)" -ForegroundColor Gray

            $uploadedFiles += @{
                Filename = $file.Filename
                TemplateId = $response.data.template_id
                Description = $file.Description
            }
        } else {
            Write-Failure "Upload failed: $($response.message)"
            $failedFiles += $file.Filename
        }

    } catch {
        Write-Failure "Error uploading file: $_"
        $failedFiles += $file.Filename
    }

    Write-Host ""
    Start-Sleep -Seconds 1
}

# Summary
Write-Host "================================" -ForegroundColor Cyan
Write-Host "Upload Summary" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host "Total files: $($toscaFiles.Count)"
Write-Host "Uploaded: $($uploadedFiles.Count)" -ForegroundColor Green
Write-Host "Failed: $($failedFiles.Count)" -ForegroundColor Red

if ($uploadedFiles.Count -gt 0) {
    Write-Host ""
    Write-Host "Uploaded Files:" -ForegroundColor Green
    foreach ($file in $uploadedFiles) {
        Write-Host "  - $($file.Description)" -ForegroundColor Gray
        Write-Host "    ID: $($file.TemplateId)" -ForegroundColor Gray
    }
}

if ($failedFiles.Count -gt 0) {
    Write-Host ""
    Write-Host "Failed Files:" -ForegroundColor Red
    foreach ($file in $failedFiles) {
        Write-Host "  - $file" -ForegroundColor Gray
    }
}

# Save template IDs to file for later queries
$uploadedFiles | ConvertTo-Json | Out-File "uploaded_tosca_ids.json"
Write-Host ""
Write-Info "Template IDs saved to: uploaded_tosca_ids.json"