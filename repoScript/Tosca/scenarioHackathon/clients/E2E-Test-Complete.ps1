<#
.SYNOPSIS
    OptimusDB End-to-End Testing Script with Complete Debug

.DESCRIPTION
    Comprehensive E2E testing for OptimusDB with built-in response display,
    all fixes applied for TOSCA 1.3 standard compliance.

    Project: OptimusDB - EU Horizon Europe Grant 101135012

.PARAMETER BaseURL
    OptimusDB API base URL (default: http://localhost:18001)

.PARAMETER FilesPath
    Path to TOSCA template files (default: ../)

.EXAMPLE
    .\E2E-Test-Complete-Debug.ps1
    .\E2E-Test-Complete-Debug.ps1 -BaseURL "http://localhost:18002" -FilesPath "C:\tosca"

.NOTES
    Fixes Applied:
    - Docker filter changed to Container (TOSCA 1.3 standard)
    - Workflows checked at top level (TOSCA 1.3 structure)
    - Complete response display for all tests
#>

param(
    [string]$BaseURL = "http://localhost:18001",
    [string]$FilesPath = "../"
)

################################################################################
# Global Variables
################################################################################

$script:TestResults = @()
$script:TemplateIDs = @()
$script:TestStartTime = Get-Date
$script:ReportTimestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$script:ReportFile = "test_report_${script:ReportTimestamp}.txt"
$script:JsonReportFile = "test_report_${script:ReportTimestamp}.json"

# TOSCA template files
$script:ToscaFiles = @{
    "webapp_adt.yaml" = "WebApp Microservices Application"
    "capacity_profile.yaml" = "Edge Cluster Capacity Profile"
    "opentofu_hybrid.yaml" = "Hybrid Infrastructure with OpenTofu"
    "deployment_plan.yaml" = "Deployment Plan with Workflows"
    "app_requirements.yaml" = "ML Training Application Requirements"
}

################################################################################
# Helper Functions
################################################################################

function Write-Banner {
    param([string]$Text)
    Write-Host ""
    Write-Host ("=" * 80) -ForegroundColor Cyan
    #Write-Host ("{0,$((40 + $Text.Length / 2))" -f $Text) -ForegroundColor Cyan -BackgroundColor Black
    Write-Host ("{0,$([int](40 + ($Text.Length / 2)))}" -f $Text) -ForegroundColor Cyan -BackgroundColor Black
    Write-Host ("=" * 80) -ForegroundColor Cyan
    Write-Host ""
}

function Write-Section {
    param([string]$Text)
    Write-Host ""
    Write-Host ("-" * 80) -ForegroundColor Blue
    Write-Host $Text -ForegroundColor Blue -BackgroundColor Black
    Write-Host ("-" * 80) -ForegroundColor Blue
    Write-Host ""
}

function Write-TestHeader {
    param(
        [string]$Scenario,
        [string]$Description
    )
    Write-Host ""
    Write-Host "TEST SCENARIO: $Scenario" -ForegroundColor Magenta -BackgroundColor Black
    Write-Host "Description: $Description" -ForegroundColor Cyan
}

function Write-Expected {
    param([string]$Expected)
    Write-Host "Expected: $Expected" -ForegroundColor Yellow
}

function Write-Command {
    param([string]$Command)
    Write-Host "Command: $Command" -ForegroundColor Gray
}

function Write-Payload {
    param([string]$Payload)
    Write-Host "Payload:" -ForegroundColor Magenta
    try {
        $jsonObj = $Payload | ConvertFrom-Json
        $prettyJson = $jsonObj | ConvertTo-Json -Depth 10
        $prettyJson -split "`n" | ForEach-Object {
            Write-Host "  $_" -ForegroundColor DarkGray
        }
    } catch {
        Write-Host "  $Payload" -ForegroundColor DarkGray
    }
}

function Write-Response {
    param(
        [Parameter(Mandatory=$true)]
        $Response,
        [int]$MaxLines = 50
    )
    Write-Host "Response:" -ForegroundColor Cyan
    try {
        # Convert response to JSON for display
        $responseJson = $Response | ConvertTo-Json -Depth 10
        $lines = $responseJson -split "`n"

        # Limit output if too long
        if ($lines.Count -gt $MaxLines) {
            $lines[0..($MaxLines-1)] | ForEach-Object {
                Write-Host "  $_" -ForegroundColor DarkCyan
            }
            Write-Host "  ... (truncated, $($lines.Count - $MaxLines) more lines)" -ForegroundColor Gray
        } else {
            $lines | ForEach-Object {
                Write-Host "  $_" -ForegroundColor DarkCyan
            }
        }

        # Show summary info
        if ($Response.status) {
            $statusColor = if ($Response.status -eq 200) { "Green" } else { "Yellow" }
            Write-Host "  Status Code: $($Response.status)" -ForegroundColor $statusColor
        }
        if ($Response.data -is [Array]) {
            Write-Host "  Data Count: $($Response.data.Count) items" -ForegroundColor Gray
        } elseif ($Response.data) {
            Write-Host "  Data Type: $($Response.data.GetType().Name)" -ForegroundColor Gray
        }
    } catch {
        Write-Host "  $Response" -ForegroundColor DarkCyan
    }
}

function Write-TestResult {
    param(
        [bool]$Passed,
        [string]$Message
    )
    if ($Passed) {
        Write-Host "✅ PASS: $Message" -ForegroundColor Green
    } else {
        Write-Host "❌ FAIL: $Message" -ForegroundColor Red
    }
}

function Record-TestResult {
    param(
        [string]$Scenario,
        [string]$Description,
        [string]$Expected,
        [string]$Command,
        [bool]$Passed,
        [string]$ActualResult,
        [double]$ExecutionTime
    )

    $result = [PSCustomObject]@{
        Scenario = $Scenario
        Description = $Description
        Expected = $Expected
        Command = $Command
        Passed = $Passed
        ActualResult = $ActualResult
        ExecutionTime = $ExecutionTime
        Timestamp = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")
    }

    $script:TestResults += $result
}

################################################################################
# Upload Functions
################################################################################

function Upload-ToscaFile {
    param(
        [string]$FileName,
        [string]$Description
    )

    $filePath = Join-Path $FilesPath $FileName

    if (-not (Test-Path $filePath)) {
        return $null
    }

    # Read and convert to base64
    $fileBytes = [System.IO.File]::ReadAllBytes($filePath)
    $base64Content = [Convert]::ToBase64String($fileBytes)

    # Prepare request body
    $body = @{
        file = $base64Content
        filename = $FileName
        store_full_structure = $true
    } | ConvertTo-Json

    try {
        # Upload to OptimusDB
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/upload" `
            -Method Post `
            -ContentType "application/json" `
            -Body $body `
            -TimeoutSec 60

        if ($response.status -eq 200) {
            return $response.data.template_id
        }
    } catch {
        Write-Host "Error uploading $FileName : $_" -ForegroundColor Red
    }

    return $null
}

function Upload-AllToscaFiles {
    Write-Section "PHASE 1: Upload TOSCA Files"

    $successCount = 0
    $totalCount = $script:ToscaFiles.Count

    foreach ($file in $script:ToscaFiles.GetEnumerator()) {
        $fileName = $file.Key
        $description = $file.Value

        Write-Host "Uploading: $description..." -NoNewline

        $templateId = Upload-ToscaFile -FileName $fileName -Description $description

        if ($templateId) {
            $script:TemplateIDs += $templateId
            $idPreview = $templateId.Substring(0, [Math]::Min(20, $templateId.Length))
            Write-Host " ✅ Success" -ForegroundColor Green -NoNewline
            Write-Host " (ID: $idPreview...)" -ForegroundColor Gray
            $successCount++
        } else {
            Write-Host " ❌ Failed" -ForegroundColor Red
        }
    }

    Write-Host ""
    Write-Host "Upload Summary: $successCount/$totalCount successful" -ForegroundColor $(if ($successCount -eq $totalCount) { "Green" } else { "Yellow" })

    return ($successCount -eq $totalCount)
}

################################################################################
# Test Functions
################################################################################

function Test-GetAllTemplates {
    $scenario = "Get All TOSCA Templates"
    $description = "Retrieve all templates from dsswres"
    $expected = "Returns array with $($script:ToscaFiles.Count)+ templates"

    $payload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @()
    } | ConvertTo-Json -Depth 10

    $command = "POST $BaseURL/swarmkb/command"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command
    Write-Payload -Payload $payload

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        $count = $response.data.Count

        if ($count -ge $script:ToscaFiles.Count) {
            $actual = "Returned $count templates"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
        } else {
            $actual = "Returned $count templates"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
    }
}

function Test-FindByTemplateId {
    if ($script:TemplateIDs.Count -eq 0) {
        Write-Host "Skipping: No template IDs available" -ForegroundColor Yellow
        return
    }

    $scenario = "Find Template by ID"
    $description = "Retrieve specific template using its ID"
    $expected = "Returns exactly 1 template with matching ID"

    $testId = $script:TemplateIDs[0]

    $payload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @(
            @{ _id = $testId }
        )
    } | ConvertTo-Json -Depth 10

    $command = "POST $BaseURL/swarmkb/command"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command
    Write-Payload -Payload $payload

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        $count = $response.data.Count
        $returnedId = $response.data[0]._id

        if ($count -eq 1 -and $returnedId -eq $testId) {
            $actual = "Returned $count template(s), ID match: True"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
        } else {
            $actual = "Returned $count template(s), ID match: False"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
    }
}

function Test-FindByToscaVersion {
    $scenario = "Find by TOSCA Version"
    $description = "Find all templates using tosca_simple_yaml_1_3"
    $expected = "Returns $($script:ToscaFiles.Count) templates"

    $payload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @(
            @{ tosca_definitions_version = "tosca_simple_yaml_1_3" }
        )
    } | ConvertTo-Json -Depth 10

    $command = "POST $BaseURL/swarmkb/command"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command
    Write-Payload -Payload $payload

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        $count = $response.data.Count

        if ($count -ge $script:ToscaFiles.Count) {
            $actual = "Returned $count templates"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
        } else {
            $actual = "Returned $count templates"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
    }
}

function Test-FindContainerNodes {
    $scenario = "Find Templates with Container Nodes"
    $description = "Find all templates containing Container node types"
    $expected = "Returns 2+ templates (webapp_adt, deployment_plan)"

    $payload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @()
    } | ConvertTo-Json -Depth 10

    $command = "POST $BaseURL/swarmkb/command (with client-side filtering for Container nodes)"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command
    Write-Payload -Payload $payload
    Write-Host "Note: Filtering for node types containing 'Container' (client-side)" -ForegroundColor Yellow

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        # Client-side filtering for Container nodes (FIXED: was *Docker*, now *Container*)
        $containerTemplates = $response.data | Where-Object {
            $template = $_
            $hasContainer = $false

            if ($template.topology_template.node_templates) {
                foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
                    if ($node.Value.type -like "*Container*") {
                        $hasContainer = $true
                        break
                    }
                }
            }
            $hasContainer
        }

        $count = $containerTemplates.Count
        $templateNames = ($containerTemplates | ForEach-Object { $_.metadata.template_name }) -join ", "

        if ($count -ge 2) {
            $actual = "Found $count templates with Container nodes ($templateNames)"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
        } else {
            $actual = "Found $count templates with Container nodes"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
    }
}

function Test-FindGPUResources {
    $scenario = "Find Templates with GPU Resources"
    $description = "Find all templates containing GPU nodes or requirements"
    $expected = "Returns 2+ templates (capacity_profile, app_requirements)"

    $payload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @()
    } | ConvertTo-Json -Depth 10

    $command = "POST $BaseURL/swarmkb/command (with client-side GPU filtering)"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command
    Write-Payload -Payload $payload
    Write-Host "Note: Filtering for GPU nodes/properties (client-side)" -ForegroundColor Yellow

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        # Client-side filtering for GPU resources
        $gpuTemplates = $response.data | Where-Object {
            $template = $_
            $hasGPU = $false

            if ($template.topology_template.node_templates) {
                foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
                    if ($node.Value.type -like "*GPU*" -or
                            $node.Value.properties.gpu_model -or
                            $node.Value.properties.gpu_count_preferred -or
                            $node.Value.properties.gpu_memory) {
                        $hasGPU = $true
                        break
                    }
                }
            }
            $hasGPU
        }

        $count = $gpuTemplates.Count
        $templateNames = ($gpuTemplates | ForEach-Object { $_.metadata.template_name }) -join ", "

        if ($count -ge 2) {
            $actual = "Found $count templates with GPU resources ($templateNames)"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
        } else {
            $actual = "Found $count templates with GPU resources"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
    }
}

function Test-FindByPort {
    $scenario = "Find Templates with Specific Ports"
    $description = "Find templates exposing port 443 (HTTPS)"
    $expected = "Returns 1+ templates with HTTPS endpoints"

    $payload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @()
    } | ConvertTo-Json -Depth 10

    $command = "POST $BaseURL/swarmkb/command (filtering for port 443)"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command
    Write-Payload -Payload $payload
    Write-Host "Note: Filtering for port 443 in node properties (client-side)" -ForegroundColor Yellow

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        # Client-side filtering for port 443
        $portTemplates = $response.data | Where-Object {
            $template = $_
            $hasPort443 = $false

            if ($template.topology_template.node_templates) {
                foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
                    if ($node.Value.properties.ports) {
                        foreach ($port in $node.Value.properties.ports) {
                            if ($port -like "*443*") {
                                $hasPort443 = $true
                                break
                            }
                        }
                    }
                    if ($hasPort443) { break }
                }
            }
            $hasPort443
        }

        $count = $portTemplates.Count

        if ($count -ge 1) {
            $actual = "Found $count templates with port 443"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
        } else {
            $actual = "Found $count templates with port 443"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
    }
}

function Test-FindWorkflows {
    $scenario = "Find Templates with Workflows"
    $description = "Find templates containing deployment or operational workflows"
    $expected = "Returns 1+ templates (deployment_plan with 2 workflows)"

    $payload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @()
    } | ConvertTo-Json -Depth 10

    $command = "POST $BaseURL/swarmkb/command (filtering for workflows)"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command
    Write-Payload -Payload $payload
    Write-Host "Note: Filtering for workflows at top level (client-side)" -ForegroundColor Yellow

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        # Client-side filtering for workflows (FIXED: at top level, not in topology_template)
        $workflowTemplates = $response.data | Where-Object {
            $null -ne $_.workflows
        }

        # Get workflow details
        $workflowDetails = $workflowTemplates | ForEach-Object {
            $templateName = $_.metadata.template_name
            $wfCount = ($_.workflows.PSObject.Properties | Measure-Object).Count
            "$templateName`: $wfCount workflows"
        }

        $count = $workflowTemplates.Count
        $details = $workflowDetails -join "; "

        if ($count -ge 1) {
            $actual = "Found $count templates with workflows ($details)"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
        } else {
            $actual = "Found $count templates with workflows ($details)"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
    }
}

function Test-FindPolicies {
    $scenario = "Find Templates with Policies"
    $description = "Find templates containing scaling, monitoring, or cost policies"
    $expected = "Returns 2+ templates with policy definitions"

    $payload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @()
    } | ConvertTo-Json -Depth 10

    $command = "POST $BaseURL/swarmkb/command (filtering for policies)"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command
    Write-Payload -Payload $payload
    Write-Host "Note: Filtering for topology_template.policies (client-side)" -ForegroundColor Yellow

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        # Client-side filtering for policies
        $policyTemplates = $response.data | Where-Object {
            $null -ne $_.topology_template.policies
        }

        # Get policy details
        $policyDetails = $policyTemplates | ForEach-Object {
            $templateName = $_.metadata.template_name
            $policyCount = $_.topology_template.policies.Count
            "$templateName`: $policyCount policies"
        }

        $count = $policyTemplates.Count
        $details = $policyDetails -join "; "

        if ($count -ge 2) {
            $actual = "Found $count templates with policies ($details)"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
        } else {
            $actual = "Found $count templates with policies ($details)"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
    }
}

function Test-FindHighMemoryNodes {
    $scenario = "Find Templates with High Memory Requirements"
    $description = "Find templates requiring nodes with >64 GB memory"
    $expected = "Returns 2+ templates (capacity_profile: 128GB, app_requirements: 64-128GB)"

    $payload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @()
    } | ConvertTo-Json -Depth 10

    $command = "POST $BaseURL/swarmkb/command (filtering for memory >64GB)"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command
    Write-Payload -Payload $payload
    Write-Host "Note: Filtering for memory specifications >64GB (client-side)" -ForegroundColor Yellow

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        # Client-side filtering for high memory (>64GB)
        $highMemTemplates = $response.data | Where-Object {
            $template = $_
            $hasHighMem = $false

            if ($template.topology_template.node_templates) {
                foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
                    $memSize = $node.Value.properties.mem_size
                    $totalMemory = $node.Value.properties.total_memory
                    $memoryPreferred = $node.Value.properties.memory_preferred

                    # Extract numeric value from strings like "128 GB"
                    if ($memSize -match '(\d+)') {
                        if ([int]$matches[1] -gt 64) { $hasHighMem = $true }
                    }
                    if ($totalMemory -match '(\d+)') {
                        if ([int]$matches[1] -gt 64) { $hasHighMem = $true }
                    }
                    if ($memoryPreferred -and $memoryPreferred -gt 64) {
                        $hasHighMem = $true
                    }

                    if ($hasHighMem) { break }
                }
            }
            $hasHighMem
        }

        $count = $highMemTemplates.Count

        if ($count -ge 2) {
            $actual = "Found $count templates with >64GB memory requirements"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
        } else {
            $actual = "Found $count templates with >64GB memory requirements"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
    }
}

function Test-CrudInsert {
    $scenario = "CRUD - INSERT"
    $description = "Insert a test renewable energy resource document"
    $expected = "Document inserted successfully with confirmation message"

    $testId = "test_solar_farm_$(Get-Date -Format 'yyyyMMddHHmmss')"

    $payload = @{
        method = @{
            cmd = "crudput"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @(
            @{
                _id = $testId
                name = "Athens Solar Farm Test"
                type = "solar"
                capacity_mw = 500
                location = @{
                    country = "Greece"
                    region = "Attica"
                    coordinates = @{
                        lat = 37.9838
                        lon = 23.7275
                    }
                }
                status = "operational"
                commissioned_date = "2024-06-15"
            }
        )
    } | ConvertTo-Json -Depth 10

    $command = "POST $BaseURL/swarmkb/command"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command
    Write-Payload -Payload $payload

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        $message = $response.data

        if ($message -like "*inserted*" -or $message -like "*success*") {
            $actual = "Response: $message"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
            return $testId
        } else {
            $actual = "Response: $message"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
            return $null
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        return $null
    }
}

function Test-CrudQuery {
    param([string]$TestId)

    $scenario = "CRUD - QUERY"
    $description = "Query the test document we just inserted"
    $expected = "Returns exactly 1 document with matching _id"

    $payload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @(
            @{ _id = $TestId }
        )
    } | ConvertTo-Json -Depth 10

    $command = "POST /swarmkb/command with crudget, _id: $TestId"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        $count = $response.data.Count
        $returnedId = $response.data[0]._id

        if ($count -eq 1 -and $returnedId -eq $TestId) {
            $actual = "Returned $count document(s), _id match: True"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
            return $true
        } else {
            $actual = "Returned $count document(s), _id match: False"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
            return $false
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        return $false
    }
}

function Test-CrudUpdate {
    param([string]$TestId)

    $scenario = "CRUD - UPDATE"
    $description = "Update test document with new values"
    $expected = "Document updated successfully, _id preserved"

    $payload = @{
        method = @{
            cmd = "crudupdate"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @(
            @{ _id = $TestId }
        )
        UpdateData = @(
            @{
                status = "maintenance"
                maintenance_reason = "Scheduled panel cleaning"
                capacity_mw = 550
            }
        )
    } | ConvertTo-Json -Depth 10

    $command = "POST /swarmkb/command with crudupdate, _id: $TestId"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        $message = $response.data

        if ($message -like "*updated*" -or $message -like "*success*") {
            $actual = "Response: $message"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
            return $true
        } else {
            $actual = "Response: $message"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
            return $false
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        return $false
    }
}

function Test-CrudVerifyUpdate {
    param([string]$TestId)

    $scenario = "CRUD - VERIFY UPDATE (CRITICAL)"
    $description = "Verify update applied correctly and _id was preserved"
    $expected = "_id preserved, status='maintenance', capacity_mw=550, has _updated_at"

    $payload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @(
            @{ _id = $TestId }
        )
    } | ConvertTo-Json -Depth 10

    $command = "POST /swarmkb/command with crudget, verify _id preserved"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        $count = $response.data.Count

        if ($count -eq 1) {
            $doc = $response.data[0]
            $docId = $doc._id
            $status = $doc.status
            $capacity = $doc.capacity_mw
            $updatedAt = $doc._updated_at

            $idPreserved = ($docId -eq $TestId)
            $hasTimestamp = ($null -ne $updatedAt)

            if ($idPreserved -and $status -eq "maintenance" -and $capacity -eq 550 -and $hasTimestamp) {
                $actual = "_id preserved: $idPreserved, status: $status, capacity: $capacity, has _updated_at: $hasTimestamp"
                Write-TestResult -Passed $true -Message $actual
                Write-Host "   🎉 CRITICAL TEST PASSED - UPDATE fix working correctly!" -ForegroundColor Green
                Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
                return $true
            } else {
                $actual = "_id preserved: $idPreserved, status: $status, capacity: $capacity, has _updated_at: $hasTimestamp"
                Write-TestResult -Passed $false -Message $actual
                Write-Host "   ⚠️  CRITICAL TEST FAILED - UPDATE may have issues!" -ForegroundColor Red
                Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
                return $false
            }
        } else {
            $actual = "Expected 1 document, got $count"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
            return $false
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        return $false
    }
}

function Test-CrudDelete {
    param([string]$TestId)

    $scenario = "CRUD - DELETE"
    $description = "Delete the test document"
    $expected = "Document deleted successfully"

    $payload = @{
        method = @{
            cmd = "cruddelete"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @(
            @{ _id = $TestId }
        )
    } | ConvertTo-Json -Depth 10

    $command = "POST /swarmkb/command with cruddelete, _id: $TestId"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        $message = $response.data

        if ($message -like "*deleted*" -or $message -like "*success*") {
            $actual = "Response: $message"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
            return $true
        } else {
            $actual = "Response: $message"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
            return $false
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        return $false
    }
}

function Test-CrudVerifyDelete {
    param([string]$TestId)

    $scenario = "CRUD - VERIFY DELETE"
    $description = "Verify document was deleted"
    $expected = "Query returns empty array (0 results)"

    $payload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "dsswres"
        criteria = @(
            @{ _id = $TestId }
        )
    } | ConvertTo-Json -Depth 10

    $command = "POST /swarmkb/command with crudget, should return empty"

    Write-TestHeader -Scenario $scenario -Description $description
    Write-Expected -Expected $expected
    Write-Command -Command $command

    $startTime = Get-Date

    try {
        $response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $payload `
            -TimeoutSec 30

        $execTime = ((Get-Date) - $startTime).TotalSeconds
        Write-Response -Response $response

        $count = $response.data.Count

        if ($count -eq 0) {
            $actual = "Returned $count document(s)"
            Write-TestResult -Passed $true -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $true -ActualResult $actual -ExecutionTime $execTime
            return $true
        } else {
            $actual = "Returned $count document(s)"
            Write-TestResult -Passed $false -Message $actual
            Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
            return $false
        }
    } catch {
        $execTime = ((Get-Date) - $startTime).TotalSeconds
        $actual = "Error: $_"
        Write-TestResult -Passed $false -Message $actual
        Record-TestResult -Scenario $scenario -Description $description -Expected $expected -Command $command -Passed $false -ActualResult $actual -ExecutionTime $execTime
        return $false
    }
}

function Run-CrudTests {
    Write-Section "PHASE 3: CRUD Operations Testing"

    # INSERT
    $testId = Test-CrudInsert

    if (-not $testId) {
        Write-Host "CRUD tests aborted - INSERT failed" -ForegroundColor Red
        return
    }

    Start-Sleep -Seconds 1

    # QUERY
    Test-CrudQuery -TestId $testId
    Start-Sleep -Seconds 1

    # UPDATE
    Test-CrudUpdate -TestId $testId
    Start-Sleep -Seconds 1

    # VERIFY UPDATE (CRITICAL)
    Test-CrudVerifyUpdate -TestId $testId
    Start-Sleep -Seconds 1

    # DELETE
    Test-CrudDelete -TestId $testId
    Start-Sleep -Seconds 1

    # VERIFY DELETE
    Test-CrudVerifyDelete -TestId $testId
}

################################################################################
# Report Generation
################################################################################

function Generate-Report {
    Write-Section "PHASE 4: Test Report Generation"

    $endTime = Get-Date
    $totalDuration = ($endTime - $script:TestStartTime).TotalSeconds

    $passed = ($script:TestResults | Where-Object { $_.Passed -eq $true }).Count
    $failed = ($script:TestResults | Where-Object { $_.Passed -eq $false }).Count
    $total = $script:TestResults.Count

    # Console report
    Write-Host ""
    Write-Host ("=" * 80) -ForegroundColor Cyan
    Write-Host "TEST EXECUTION SUMMARY" -ForegroundColor Cyan -BackgroundColor Black
    Write-Host ("=" * 80) -ForegroundColor Cyan
    Write-Host ""

    Write-Host "Total Tests:     $total"
    Write-Host "Passed:          " -NoNewline
    Write-Host $passed -ForegroundColor Green
    Write-Host "Failed:          " -NoNewline
    Write-Host $failed -ForegroundColor Red
    Write-Host "Duration:        $([math]::Round($totalDuration, 1))s"

    $successRate = if ($total -gt 0) { [math]::Round(($passed / $total) * 100, 1) } else { 0 }
    Write-Host "Success Rate:    $successRate%"
    Write-Host ""

    # Detailed results
    Write-Host "DETAILED RESULTS:" -ForegroundColor Cyan
    Write-Host ""

    $i = 1
    foreach ($result in $script:TestResults) {
        if ($result.Passed) {
            Write-Host "$i. ✅ PASS - $($result.Scenario)" -ForegroundColor Green
        } else {
            Write-Host "$i. ❌ FAIL - $($result.Scenario)" -ForegroundColor Red
        }

        Write-Host "   Expected: $($result.Expected)" -ForegroundColor Gray
        Write-Host "   Actual:   $($result.ActualResult)" -ForegroundColor Gray
        Write-Host "   Time:     $([math]::Round($result.ExecutionTime, 3))s" -ForegroundColor Gray
        Write-Host ""

        $i++
    }

    # Save text report
    $textReport = @"
OptimusDB End-to-End Test Report
================================================================================

Test Session: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
Base URL: $BaseURL
Total Tests: $total
Passed: $passed
Failed: $failed
Duration: $([math]::Round($totalDuration, 1))s
Success Rate: $successRate%

Detailed Results:
--------------------------------------------------------------------------------
"@

    $i = 1
    foreach ($result in $script:TestResults) {
        $status = if ($result.Passed) { "PASS" } else { "FAIL" }
        $textReport += @"

        $i. $status - $($result.Scenario)
   Description: $($result.Description)
   Expected: $($result.Expected)
   Actual: $($result.ActualResult)
   Command: $($result.Command)
   Execution Time: $([math]::Round($result.ExecutionTime, 3))s
   Timestamp: $($result.Timestamp)
"@
        $i++
    }

    $textReport | Out-File -FilePath $script:ReportFile -Encoding UTF8
    Write-Host "✅ Text report saved to: $($script:ReportFile)" -ForegroundColor Green

    # Save JSON report
    $jsonReport = @{
        session = @{
            timestamp = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")
            base_url = $BaseURL
            total_tests = $total
            passed = $passed
            failed = $failed
            duration = $totalDuration
            success_rate = $successRate
        }
        test_results = $script:TestResults
    } | ConvertTo-Json -Depth 10

    $jsonReport | Out-File -FilePath $script:JsonReportFile -Encoding UTF8
    Write-Host "✅ JSON report saved to: $($script:JsonReportFile)" -ForegroundColor Green

    # Final status
    Write-Host ""
    if ($failed -eq 0) {
        Write-Host "🎉 ALL TESTS PASSED! 🎉" -ForegroundColor Green -BackgroundColor Black
        return $true
    } else {
        Write-Host "⚠️  $failed TEST(S) FAILED" -ForegroundColor Yellow -BackgroundColor Black
        return $false
    }
}

################################################################################
# Main Execution
################################################################################

function Main {
    Write-Banner "OptimusDB End-to-End Test Suite with Complete Debug"

    Write-Host "Configuration:" -ForegroundColor Cyan
    Write-Host "  Base URL: $BaseURL"
    Write-Host "  Files Directory: $FilesPath"
    Write-Host "  Test Report: $($script:ReportFile)"
    Write-Host ""
    Write-Host "Fixes Applied:" -ForegroundColor Yellow
    Write-Host "  ✅ Container filter (was Docker, now Container per TOSCA 1.3)"
    Write-Host "  ✅ Workflows at top level (was topology_template.workflows)"
    Write-Host "  ✅ Complete response display for all tests"
    Write-Host ""

    # Phase 1: Upload TOSCA files
    if (-not (Upload-AllToscaFiles)) {
        Write-Host "Upload phase failed - aborting tests" -ForegroundColor Red
        return $false
    }

    Start-Sleep -Seconds 2  # Wait for replication

    # Phase 2: Query Tests
    Write-Section "PHASE 2: Query Operation Tests"

    Test-GetAllTemplates
    Start-Sleep -Seconds 1

    Test-FindByTemplateId
    Start-Sleep -Seconds 1

    Test-FindByToscaVersion
    Start-Sleep -Seconds 1

    Test-FindContainerNodes
    Start-Sleep -Seconds 1

    Test-FindGPUResources
    Start-Sleep -Seconds 1

    Test-FindByPort
    Start-Sleep -Seconds 1

    Test-FindWorkflows
    Start-Sleep -Seconds 1

    Test-FindPolicies
    Start-Sleep -Seconds 1

    Test-FindHighMemoryNodes
    Start-Sleep -Seconds 1

    # Phase 3: CRUD Tests
    Run-CrudTests

    # Phase 4: Generate Report
    $success = Generate-Report

    return $success
}

# Execute
$result = Main
exit $(if ($result) { 0 } else { 1 })