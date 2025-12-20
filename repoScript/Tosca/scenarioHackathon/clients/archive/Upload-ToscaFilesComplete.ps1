<#
.SYNOPSIS
    Upload TOSCA YAML files to OptimusDB with base64 encoding and persist template IDs
.DESCRIPTION
    Comprehensive PowerShell script to upload multiple TOSCA files to OptimusDB,
    convert to base64, and save template IDs to JSON for later use.

    Project: OptimusDB - EU Horizon Europe Grant 101135012
.PARAMETER BaseURL
    Base URL of OptimusDB API (default: http://localhost:18001)
.PARAMETER FilesPath
    Directory containing TOSCA YAML files (default: current directory)
.PARAMETER OutputFile
    JSON file to save template IDs (default: uploaded_tosca_templates.json)
.EXAMPLE
    .\Upload-ToscaFilesComplete.ps1
.EXAMPLE
    .\Upload-ToscaFilesComplete.ps1 -BaseURL "http://localhost:18001" -FilesPath "C:\tosca_samples"
#>

[CmdletBinding()]
param(
    [string]$BaseURL = "http://localhost:18001",
    [string]$FilesPath = ".",
    [string]$OutputFile = "uploaded_tosca_templates.json"
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"  # Faster Invoke-RestMethod

################################################################################
# Configuration
################################################################################

$LogFile = "upload_log_$(Get-Date -Format 'yyyyMMdd_HHmmss').txt"

# TOSCA files configuration
$ToscaFiles = @{
    "webapp_adt.yaml" = "WebApp Microservices Application"
    "capacity_profile.yaml" = "Edge Cluster Capacity Profile"
    "opentofu_hybrid.yaml" = "Hybrid Infrastructure with OpenTofu"
    "deployment_plan.yaml" = "Deployment Plan with Workflows"
    "app_requirements.yaml" = "ML Training Application Requirements"
}

# Statistics
$TotalFiles = $ToscaFiles.Count
$UploadedCount = 0
$FailedCount = 0
$UploadResults = @()

################################################################################
# Helper Functions
################################################################################

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "[$timestamp] $Message" | Out-File -FilePath $LogFile -Append -Encoding UTF8
}

function Write-Header {
    param([string]$Text)
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host $Text -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Success {
    param([string]$Message)
    Write-Host "✅ $Message" -ForegroundColor Green
    Write-Log "SUCCESS: $Message"
}

function Write-Failure {
    param([string]$Message)
    Write-Host "❌ $Message" -ForegroundColor Red
    Write-Log "ERROR: $Message"
}

function Write-Warning2 {
    param([string]$Message)
    Write-Host "⚠️  $Message" -ForegroundColor Yellow
    Write-Log "WARNING: $Message"
}

function Write-Info {
    param([string]$Message)
    Write-Host "ℹ️  $Message" -ForegroundColor Cyan
}

function Write-Detail {
    param([string]$Message)
    Write-Host "   $Message" -ForegroundColor Gray
}

function Test-Dependencies {
    Write-Info "Checking PowerShell version..."

    $psVersion = $PSVersionTable.PSVersion
    if ($psVersion.Major -lt 5) {
        Write-Failure "PowerShell 5.0 or higher required (current: $psVersion)"
        exit 1
    }

    Write-Success "PowerShell version $psVersion detected"
}

function Test-Connectivity {
    Write-Info "Testing connection to $BaseURL..."

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/health" -Method Get -TimeoutSec 5 -ErrorAction SilentlyContinue
        Write-Success "API is reachable"
        return $true
    }
    catch {
        Write-Warning2 "Health endpoint not responding (this may be normal)"
        Write-Info "Attempting to continue anyway..."
        return $true
    }
}

function Convert-FileToBase64 {
    param([string]$FilePath)

    try {
        $content = Get-Content -Path $FilePath -Raw -Encoding UTF8
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($content)
        $base64 = [Convert]::ToBase64String($bytes)
        return $base64
    }
    catch {
        Write-Failure "Failed to convert file to base64: $_"
        return $null
    }
}

function Upload-ToscaFile {
    param(
        [string]$Filename,
        [string]$Description
    )

    Write-Host ""
    Write-Info "Processing: $Description"
    Write-Detail "File: $Filename"

    $filepath = Join-Path $FilesPath $Filename

    # Check file exists
    if (-not (Test-Path $filepath)) {
        Write-Failure "File not found: $filepath"
        return $null
    }

    # Get file size
    $fileInfo = Get-Item $filepath
    $sizeKB = [math]::Round($fileInfo.Length / 1KB, 2)
    Write-Detail "Size: $sizeKB KB"

    # Convert to base64
    Write-Detail "Converting to base64..."
    $base64Content = Convert-FileToBase64 -FilePath $filepath

    if (-not $base64Content) {
        return $null
    }

    # Prepare request body
    $body = @{
        file = $base64Content
        filename = $Filename
        store_full_structure = $true
    } | ConvertTo-Json -Depth 10

    # Upload to OptimusDB
    Write-Detail "Uploading to $BaseURL/swarmkb/upload..."

    try {
        $response = Invoke-RestMethod `
            -Uri "$BaseURL/swarmkb/upload" `
            -Method Post `
            -Body $body `
            -ContentType "application/json" `
            -TimeoutSec 60

        # Check response status
        if ($response.status -ne 200) {
            Write-Failure "Upload failed: $($response.message)"
            return $null
        }

        # Extract data
        $templateId = $response.data.template_id
        $queryable = $response.data.queryable
        $storageLocation = $response.data.storage_location

        if (-not $templateId) {
            Write-Failure "No template ID returned in response"
            return $null
        }

        # Success!
        Write-Success "Upload successful"
        Write-Detail "Template ID: $templateId"
        Write-Detail "Queryable: $queryable"
        Write-Detail "Storage: $storageLocation"

        # Return result object
        return [PSCustomObject]@{
            filename = $Filename
            description = $Description
            template_id = $templateId
            queryable = $queryable
            storage_location = $storageLocation
            uploaded_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        }
    }
    catch {
        Write-Failure "Upload failed: $_"
        Write-Detail "Error details: $($_.Exception.Message)"
        return $null
    }
}

function Save-Results {
    param([array]$Results)

    $outputData = [PSCustomObject]@{
        upload_session = [PSCustomObject]@{
            timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
            base_url = $BaseURL
            total_files = $TotalFiles
            uploaded = $UploadedCount
            failed = $FailedCount
        }
        templates = $Results
    }

    $outputData | ConvertTo-Json -Depth 10 | Out-File -FilePath $OutputFile -Encoding UTF8

    Write-Success "Results saved to: $OutputFile"
}

function Test-Uploads {
    Write-Info "Verifying uploads..."

    $queryBody = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @()
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod `
            -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -Body $queryBody `
            -ContentType "application/json" `
            -TimeoutSec 30

        $totalCount = $response.data.Count

        if ($totalCount -gt 0) {
            Write-Success "Verified: $totalCount total templates in database"

            # Count TOSCA templates
            $toscaCount = ($response.data | Where-Object {
                $null -ne $_.tosca_definitions_version
            }).Count

            Write-Detail "TOSCA templates: $toscaCount"

            return $true
        }
        else {
            Write-Warning2 "Could not verify uploads (query returned no results)"
            return $false
        }
    }
    catch {
        Write-Warning2 "Could not verify uploads: $_"
        return $false
    }
}

################################################################################
# Main Script
################################################################################

function Main {
    Write-Header "OptimusDB TOSCA Upload Script"

    Write-Host "Configuration:"
    Write-Host "  Base URL: $BaseURL"
    Write-Host "  Files Directory: $FilesPath"
    Write-Host "  Output File: $OutputFile"
    Write-Host "  Log File: $LogFile"
    Write-Host ""

    # Check dependencies
    Write-Info "Checking dependencies..."
    Test-Dependencies

    # Test connectivity
    Test-Connectivity

    # Process each file
    Write-Header "Uploading TOSCA Files"

    foreach ($entry in $ToscaFiles.GetEnumerator()) {
        $filename = $entry.Key
        $description = $entry.Value

        $result = Upload-ToscaFile -Filename $filename -Description $description

        if ($result) {
            $script:UploadResults += $result
            $script:UploadedCount++
        }
        else {
            $script:FailedCount++
        }

        # Brief pause between uploads
        Start-Sleep -Seconds 1
    }

    # Summary
    Write-Host ""
    Write-Header "Upload Summary"

    Write-Host "Total Files:     $TotalFiles"
    Write-Host "Uploaded:        " -NoNewline
    Write-Host $UploadedCount -ForegroundColor Green
    Write-Host "Failed:          " -NoNewline
    Write-Host $FailedCount -ForegroundColor Red
    Write-Host ""

    # Save results if any succeeded
    if ($UploadedCount -gt 0) {
        Save-Results -Results $UploadResults

        Write-Host ""
        Write-Info "Template IDs saved to $OutputFile"
        Write-Host ""

        # Show uploaded templates
        Write-Host "Uploaded Templates:"
        foreach ($result in $UploadResults) {
            Write-Host "  ✓ $($result.description)" -ForegroundColor Green
            Write-Host "    ID: $($result.template_id)" -ForegroundColor Gray
        }

        # Verify uploads
        Write-Host ""
        Test-Uploads
    }

    # Final status
    Write-Host ""
    if ($FailedCount -eq 0) {
        Write-Header "✅ All Uploads Successful!"
        exit 0
    }
    elseif ($UploadedCount -gt 0) {
        Write-Header "⚠️  Partial Success - Some Uploads Failed"
        exit 1
    }
    else {
        Write-Header "❌ All Uploads Failed"
        exit 1
    }
}

# Execute main function
try {
    Main
}
catch {
    Write-Failure "Unexpected error: $_"
    Write-Host $_.ScriptStackTrace -ForegroundColor Red
    exit 1
}