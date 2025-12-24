# OptimusDB TOSCA Upload and Query Guide
## Complete Guide for development purposes

---

## Prerequisites

1. Docker Desktop
2. Linux OS (for running the shell script) or Windows (for running the commands of powershell)

---
## Aim

Conduct a  number of scenarios to persist and retrieve tosca information representing repsective use cases for the swarmchestrate ecosystem, anticipated to be handled by OptimusDB as part of a Knowledge Base lifecycle.
Respective information are in a form of file yaml format representing different scenarios for application deployment, capacity descriptions, openTofu templates etc.

---

## 🗂️ TOSCA Files Overview representing the scenarios

### Files to Upload

| File | Type | Datastore | Description |
|------|------|-----------|-------------|
| webapp_adt.yaml | Application Description | ADT | Web application with microservices |
| capacity_profile.yaml | Capacity Description | Capacity_Descriptions | Edge cluster resources |
| opentofu_hybrid.yaml | OpenTofu/TOSCA | OpenTofu_TOSCA_Templates | Hybrid infrastructure with K8s |
| deployment_plan.yaml | Deployment Plan | Deployment_Release_Plans | Executable deployment plan |
| app_requirements.yaml | Requirements | (none specified) | ML training workload specs |

### Each yaml file represents a custom scenario, anticipated to be performed during the KB lifecycle.
### The aim is to persist the file representing the use case and search it for certain reasons.

**webapp_adt.yaml:**
- 4 Docker containers (frontend, backend, PostgreSQL, Redis)
- 4 container runtimes
- Scaling policies (2-10 instances)
- Monitoring policy
- Groups and outputs

**capacity_profile.yaml:**
- Physical compute node (32 cores, 128 GB RAM)
- NVMe SSD storage (2 TB)
- 10 Gbps network interface
- NVIDIA A100 GPU
- Kubernetes runtime

**opentofu_hybrid.yaml:**
- Kubernetes namespace with OpenTofu mapping
- Nginx ingress controller
- Istio service mesh
- Prometheus monitoring
- ConfigMaps and Secrets

**deployment_plan.yaml:**
- Complete deployment workflow (9 steps)
- Rollback workflow (6 steps)
- Resource allocation details
- Health checks and monitoring
- Ingress configuration

**app_requirements.yaml:**
- ML training requirements
- 2-4 GPU requirement (A100/H100/V100)
- 64-128 GB memory
- 1.75 TB storage
- Performance and cost policies

---

## Expected Results

**Upload:** 5 templates successfully stored

**Query all:** Returns 5 TOSCA documents

**Find Docker:** 2 templates (webapp_adt, deployment_plan)

**Find GPU:** 2 templates (capacity_profile, app_requirements)

**Find workflows:** 1 template (deployment_plan with 2 workflows)

---

## How to run the scenarios

### 1. Save Files
Save all 5 `.yaml` files to a directory on your system.

### 2. Upload
Use the PowerShell or Linux upload script from the guide.

**PowerShell:**
```powershell
.\Upload-ToscaFiles.ps1 -FilesPath "C:\tosca_samples"
```

**Linux:**
```bash
./upload_tosca_files.sh http://localhost:18001 ~/tosca_samples
```

### 3. Query
Try simple queries first, then move to advanced scenarios.

```bash
# Find all templates
curl -X POST http://localhost:18001/swarmkb/command \
-d '{"method":{"cmd":"crudget","argcnt":1},"dstype":"dsswres","criteria":[]}'
```

---

## 📖 Scenario Guide

The main guide includes **15 query scenarios** , as depicted in the given scripts. The scenarios are segregated into
complexity on how the results are searched within optimusDB providing respective results:

### Simple (1-4)
- Find all templates
- Find by name
- Find by datastore
- Find by TOSCA version

### Intermediate (5-7)
- Find by node type (Docker, PostgreSQL, Kubernetes)
- Find by GPU requirements
- Find by port (443, 5432, 8080)

### Advanced (8-10)
- Find by environment variables
- Find by resource requirements (>16 GB)
- Multi-criteria queries

### Complex (11-13)
- Deep nested property search
- Query workflows and policies
- Query groups

### Analytical (14-15)
- Resource statistics (full analysis script)
- Template comparison

---

## Testing Checklist outcomes according to the above scenarios

- [ ] All 5 files uploaded successfully
- [ ] Query returns 5 templates
- [ ] Find Docker containers works
- [ ] Find GPU resources works
- [ ] Nested fields accessible
- [ ] Workflows queryable
- [ ] Analytical queries work
- [ ] No errors in logs

---

## 📤 Upload Scripts

### PowerShell Upload Script

Save as `Upload-ToscaFiles.ps1`:

```powershell
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
```

### Linux/Bash Upload Script

Save as `upload_tosca_files.sh`:

```bash
#!/bin/bash

# OptimusDB TOSCA File Upload Script
# Usage: ./upload_tosca_files.sh [base_url] [files_path]

BASE_URL="${1:-http://localhost:18001}"
FILES_PATH="${2:-.}"

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
GRAY='\033[0;90m'
NC='\033[0m' # No Color

# TOSCA files to upload
declare -a TOSCA_FILES=(
"webapp_adt.yaml:WebApp Microservices Application"
"capacity_profile.yaml:Edge Cluster Capacity Profile"
"opentofu_hybrid.yaml:Hybrid Infrastructure with OpenTofu"
"deployment_plan.yaml:Deployment Plan with Workflows"
"app_requirements.yaml:ML Training Application Requirements"
)

echo -e "${CYAN}Starting TOSCA file upload to ${BASE_URL}${NC}"
echo ""

UPLOADED_COUNT=0
FAILED_COUNT=0
declare -a UPLOADED_IDS

# Create temporary file for results
TEMP_RESULTS=$(mktemp)

for file_entry in "${TOSCA_FILES[@]}"; do
IFS=':' read -r filename description <<< "$file_entry"
filepath="${FILES_PATH}/${filename}"

if [ ! -f "$filepath" ]; then
echo -e "${RED}❌ File not found: ${filepath}${NC}"
((FAILED_COUNT++))
continue
fi

echo -e "${CYAN}ℹ️  Uploading: ${description} (${filename})${NC}"

# Read file and convert to base64
file_base64=$(base64 -w 0 "$filepath" 2>/dev/null || base64 "$filepath")

# Prepare JSON body
json_body=$(cat <<EOF
        {
"file": "$file_base64",
"filename": "$filename",
"store_full_structure": true
}
EOF
)

# Upload to OptimusDB
response=$(curl -s -X POST "${BASE_URL}/swarmkb/upload" \
-H "Content-Type: application/json" \
-d "$json_body")

# Check response
status=$(echo "$response" | jq -r '.status // empty')

if [ "$status" = "200" ]; then
template_id=$(echo "$response" | jq -r '.data.template_id // "unknown"')
queryable=$(echo "$response" | jq -r '.data.queryable // false')
storage=$(echo "$response" | jq -r '.data.storage_location // "unknown"')

echo -e "${GREEN}✅ Uploaded successfully${NC}"
echo -e "${GRAY}  Template ID: ${template_id}${NC}"
echo -e "${GRAY}  Queryable: ${queryable}${NC}"
echo -e "${GRAY}  Storage: ${storage}${NC}"

((UPLOADED_COUNT++))

# Save to temp file
echo "${filename}|${template_id}|${description}" >> "$TEMP_RESULTS"
else
message=$(echo "$response" | jq -r '.message // "Unknown error"')
echo -e "${RED}❌ Upload failed: ${message}${NC}"
((FAILED_COUNT++))
fi

echo ""
sleep 1
done

# Summary
echo -e "${CYAN}================================${NC}"
echo -e "${CYAN}Upload Summary${NC}"
echo -e "${CYAN}================================${NC}"
echo "Total files: ${#TOSCA_FILES[@]}"
echo -e "${GREEN}Uploaded: ${UPLOADED_COUNT}${NC}"
echo -e "${RED}Failed: ${FAILED_COUNT}${NC}"

if [ $UPLOADED_COUNT -gt 0 ]; then
echo ""
echo -e "${GREEN}Uploaded Files:${NC}"
while IFS='|' read -r filename template_id description; do
echo -e "${GRAY}  - ${description}${NC}"
echo -e "${GRAY}    ID: ${template_id}${NC}"
done < "$TEMP_RESULTS"
fi

# Save template IDs to JSON file
if [ $UPLOADED_COUNT -gt 0 ]; then
echo "["
first=true
while IFS='|' read -r filename template_id description; do
if [ "$first" = true ]; then
first=false
else
echo ","
fi
cat <<EOF
        {
"Filename": "$filename",
"TemplateId": "$template_id",
"Description": "$description"
}
EOF
done < "$TEMP_RESULTS"
echo ""
echo "]"
} > uploaded_tosca_ids.json

rm -f "$TEMP_RESULTS"

if [ $UPLOADED_COUNT -gt 0 ]; then
echo ""
echo -e "${CYAN}ℹ️  Template IDs saved to: uploaded_tosca_ids.json${NC}"
fi
```

**Make executable:**
```bash
chmod +x upload_tosca_files.sh
```

---

## 🔍 Simple Query Scenarios

### Scenario 1: Find All TOSCA Templates

**PowerShell:**
```powershell
# Query all TOSCA templates
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

Write-Host "Total TOSCA templates: $($response.data.Count)"
$response.data | ForEach-Object {
Write-Host "  - $($_._id): $($_.metadata.template_name)"
}
```

**Linux:**
```bash
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": []
}' | jq '.data[] | {id: ._id, name: .metadata.template_name}'
```

**Expected Output:**
```
Total TOSCA templates: 5
- abc123: WebApp-MicroservicesApplication
- def456: EdgeCluster-CapacityProfile
- ghi789: HybridInfrastructure-SwarmDeployment
- jkl012: DeploymentPlan-WebApp-Release
- mno345: ApplicationRequirements-MLTrainingWorkload
```

---

### Scenario 2: Find by Template Name

**PowerShell:**
```powershell
# Find WebApp template
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @(
@{
"metadata.template_name" = "WebApp-MicroservicesApplication"
}
)
} | ConvertTo-Json -Depth 5

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

$response.data | ConvertTo-Json -Depth 10
```

**Linux:**
```bash
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": [{
"metadata": {
"template_name": "WebApp-MicroservicesApplication"
}
}]
}' | jq '.data[0]'
```

**Note:** OrbitDB Query with nested fields requires custom filter. The criteria approach works for top-level fields. For nested queries, see Advanced Scenarios.

---

### Scenario 3: Find by Datastore Type

**PowerShell:**
```powershell
# Find all templates in ADT datastore
function Find-ByDatastore {
param([string]$Datastore)

# Get all templates
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

# Filter by datastore (client-side for nested field)
$filtered = $response.data | Where-Object {
$_.metadata.kb_datastore -eq $Datastore
}

Write-Host "Templates in $Datastore datastore: $($filtered.Count)"
$filtered | ForEach-Object {
Write-Host "  - $($_.metadata.template_name)"
}
}

Find-ByDatastore -Datastore "ADT"
```

**Linux:**
```bash
# Find templates in Capacity_Descriptions datastore
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": []
}' | jq '.data[] | select(.metadata.kb_datastore == "Capacity_Descriptions") |
{name: .metadata.template_name, datastore: .metadata.kb_datastore}'
```

---

### Scenario 4: Find by TOSCA Version

**PowerShell:**
```powershell
# Find all TOSCA 1.3 templates
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @(
@{
tosca_definitions_version = "tosca_simple_yaml_1_3"
}
)
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

Write-Host "TOSCA 1.3 templates: $($response.data.Count)"
```

**Linux:**
```bash
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": [{
"tosca_definitions_version": "tosca_simple_yaml_1_3"
}]
}' | jq '.data | length'
```

---

## 🎯 Intermediate Query Scenarios

### Scenario 5: Find Templates with Specific Node Types

**PowerShell - Find Docker Containers:**
```powershell
function Find-WithNodeType {
param([string]$NodeType)

$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

$matches = $response.data | Where-Object {
$template = $_
$hasNodeType = $false

if ($template.topology_template.node_templates) {
foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
if ($node.Value.type -like "*$NodeType*") {
$hasNodeType = $true
break
}
}
}

$hasNodeType
}

Write-Host "Templates with $NodeType nodes: $($matches.Count)"
$matches | ForEach-Object {
Write-Host "  - $($_.metadata.template_name)"

# List matching nodes
$_.topology_template.node_templates.PSObject.Properties | Where-Object {
$_.Value.type -like "*$NodeType*"
} | ForEach-Object {
Write-Host "    ∟ $($_.Name): $($_.Value.type)" -ForegroundColor Gray
}
}
}

# Find templates with Docker containers
Find-WithNodeType -NodeType "Docker"

# Find templates with PostgreSQL
Find-WithNodeType -NodeType "PostgreSQL"

# Find templates with Kubernetes
Find-WithNodeType -NodeType "Kubernetes"
```

**Linux - Find Docker Containers:**
```bash
#!/bin/bash

# Function to find templates with specific node type
find_with_node_type() {
local node_type=$1

curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": []
}' | jq --arg type "$node_type" '
.data[] |
select(.topology_template.node_templates != null) |
select(
.topology_template.node_templates |
to_entries[] |
.value.type |
contains($type)
) |
{
template: .metadata.template_name,
nodes: [
.topology_template.node_templates |
to_entries[] |
select(.value.type | contains($type)) |
{name: .key, type: .value.type}
]
}'
}

# Find templates with Docker
echo "Templates with Docker containers:"
find_with_node_type "Docker"

# Find templates with PostgreSQL
echo ""
echo "Templates with PostgreSQL:"
find_with_node_type "PostgreSQL"
```

---

### Scenario 6: Find Templates with GPU Requirements

**PowerShell:**
```powershell
# Find templates that require GPUs
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

$gpuTemplates = $response.data | Where-Object {
$template = $_
$hasGPU = $false

# Check node_templates for GPU type
if ($template.topology_template.node_templates) {
foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
if ($node.Value.type -like "*GPU*") {
$hasGPU = $true
break
}
}
}

$hasGPU
}

Write-Host "Templates with GPU requirements: $($gpuTemplates.Count)"
$gpuTemplates | ForEach-Object {
Write-Host "`n📊 $($_.metadata.template_name)" -ForegroundColor Cyan

# Extract GPU details
$_.topology_template.node_templates.PSObject.Properties | Where-Object {
$_.Value.type -like "*GPU*"
} | ForEach-Object {
Write-Host "  GPU Node: $($_.Name)" -ForegroundColor Yellow
Write-Host "    Type: $($_.Value.type)" -ForegroundColor Gray

if ($_.Value.properties) {
Write-Host "    Properties:" -ForegroundColor Gray
$_.Value.properties.PSObject.Properties | ForEach-Object {
Write-Host "      - $($_.Name): $($_.Value)" -ForegroundColor DarkGray
}
}
}
}
```

**Linux:**
```bash
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": []
}' | jq '
.data[] |
select(.topology_template.node_templates != null) |
select(
.topology_template.node_templates |
to_entries[] |
.value.type |
contains("GPU")
) |
{
template: .metadata.template_name,
gpu_nodes: [
.topology_template.node_templates |
to_entries[] |
select(.value.type | contains("GPU")) |
{
name: .key,
type: .value.type,
model: .value.properties.gpu_model,
memory: .value.properties.gpu_memory
}
]
}'
```

**Expected Output:**
```json
{
"template": "EdgeCluster-CapacityProfile",
"gpu_nodes": [
{
"name": "gpu_accelerator_01",
"type": "tosca.nodes.Compute.GPU",
"model": "NVIDIA A100",
"memory": "40 GB"
}
]
}
```

---

### Scenario 7: Find Templates with Specific Ports

**PowerShell:**
```powershell
function Find-ByPort {
param([string]$Port)

$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

$matches = $response.data | Where-Object {
$template = $_
$hasPort = $false

if ($template.topology_template.node_templates) {
foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
if ($node.Value.properties.ports) {
foreach ($portMapping in $node.Value.properties.ports) {
if ($portMapping -like "*$Port*") {
$hasPort = $true
break
}
}
}
if ($hasPort) { break }
}
}

$hasPort
}

Write-Host "Templates exposing port $Port : $($matches.Count)"
$matches | ForEach-Object {
Write-Host "`n📦 $($_.metadata.template_name)" -ForegroundColor Cyan

$_.topology_template.node_templates.PSObject.Properties | ForEach-Object {
$nodeName = $_.Name
$node = $_.Value

if ($node.properties.ports) {
$matchingPorts = $node.properties.ports | Where-Object { $_ -like "*$Port*" }
if ($matchingPorts) {
Write-Host "  Node: $nodeName" -ForegroundColor Yellow
$matchingPorts | ForEach-Object {
Write-Host "    Port: $_" -ForegroundColor Gray
}
}
}
}
}
}

# Find templates exposing port 443 (HTTPS)
Find-ByPort -Port "443"

# Find templates exposing port 5432 (PostgreSQL)
Find-ByPort -Port "5432"

# Find templates exposing port 8080
Find-ByPort -Port "8080"
```

**Linux:**
```bash
#!/bin/bash

find_by_port() {
local port=$1

curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": []
}' | jq --arg port "$port" '
.data[] |
select(.topology_template.node_templates != null) |
{
template: .metadata.template_name,
nodes_with_port: [
.topology_template.node_templates |
to_entries[] |
select(.value.properties.ports != null) |
select(.value.properties.ports[] | contains($port)) |
{
name: .key,
ports: .value.properties.ports
}
]
} |
select(.nodes_with_port | length > 0)
'
}

echo "=== Templates exposing port 443 (HTTPS) ==="
find_by_port "443"

echo ""
echo "=== Templates exposing port 5432 (PostgreSQL) ==="
find_by_port "5432"
```

---

## 🚀 Advanced Query Scenarios

### Scenario 8: Find Templates with Environment Variables

**PowerShell:**
```powershell
function Find-WithEnvVar {
param([string]$VarName)

$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

$matches = $response.data | Where-Object {
$template = $_
$hasEnvVar = $false

if ($template.topology_template.node_templates) {
foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
if ($node.Value.properties.environment) {
$env = $node.Value.properties.environment
if ($env.PSObject.Properties.Name -contains $VarName) {
$hasEnvVar = $true
break
}
}
}
}

$hasEnvVar
}

Write-Host "Templates with $VarName environment variable: $($matches.Count)"
$matches | ForEach-Object {
Write-Host "`n🌍 $($_.metadata.template_name)" -ForegroundColor Cyan

$_.topology_template.node_templates.PSObject.Properties | ForEach-Object {
$nodeName = $_.Name
$node = $_.Value

if ($node.properties.environment) {
$env = $node.properties.environment
if ($env.PSObject.Properties.Name -contains $VarName) {
Write-Host "  Node: $nodeName" -ForegroundColor Yellow
Write-Host "    $VarName = $($env.$VarName)" -ForegroundColor Gray
}
}
}
}
}

# Find templates with DATABASE_URL
Find-WithEnvVar -VarName "DATABASE_URL"

# Find templates with NODE_ENV
Find-WithEnvVar -VarName "NODE_ENV"
```

**Linux:**
```bash
find_with_env_var() {
local var_name=$1

curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": []
}' | jq --arg var "$var_name" '
.data[] |
select(.topology_template.node_templates != null) |
{
template: .metadata.template_name,
nodes: [
.topology_template.node_templates |
to_entries[] |
select(.value.properties.environment != null) |
select(.value.properties.environment | has($var)) |
{
name: .key,
env_var: $var,
value: .value.properties.environment[$var]
}
]
} |
select(.nodes | length > 0)
'
}

echo "Templates with DATABASE_URL:"
find_with_env_var "DATABASE_URL"
```

---

### Scenario 9: Find by Resource Requirements

**PowerShell - Find Templates Requiring >16 GB Memory:**
```powershell
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

function Parse-MemorySize {
param([string]$Size)

if ($Size -match '(\d+)\s*GB') {
return [int]$matches[1]
}
return 0
}

$highMemoryTemplates = $response.data | Where-Object {
$template = $_
$maxMemory = 0

if ($template.topology_template.node_templates) {
foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
if ($node.Value.properties.mem_size) {
$mem = Parse-MemorySize -Size $node.Value.properties.mem_size
if ($mem -gt $maxMemory) {
$maxMemory = $mem
}
}
if ($node.Value.properties.total_memory) {
$mem = Parse-MemorySize -Size $node.Value.properties.total_memory
if ($mem -gt $maxMemory) {
$maxMemory = $mem
}
}
}
}

$maxMemory -gt 16
}

Write-Host "Templates requiring >16 GB memory: $($highMemoryTemplates.Count)"
$highMemoryTemplates | ForEach-Object {
Write-Host "`n💾 $($_.metadata.template_name)" -ForegroundColor Cyan

$_.topology_template.node_templates.PSObject.Properties | ForEach-Object {
$node = $_.Value
$mem = 0

if ($node.properties.mem_size) {
$mem = Parse-MemorySize -Size $node.properties.mem_size
} elseif ($node.properties.total_memory) {
$mem = Parse-MemorySize -Size $node.properties.total_memory
}

if ($mem -gt 16) {
Write-Host "  $($_.Name): $mem GB" -ForegroundColor Yellow
}
}
}
```

---

### Scenario 10: Multi-Criteria Complex Query

**PowerShell - Find Production Docker Templates with PostgreSQL:**
```powershell
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

$filtered = $response.data | Where-Object {
$template = $_

# Criteria 1: Has Docker nodes
$hasDocker = $false
# Criteria 2: Has PostgreSQL nodes
$hasPostgres = $false
# Criteria 3: Environment is production
$isProduction = $false

if ($template.topology_template.node_templates) {
foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
# Check for Docker
if ($node.Value.type -like "*Docker*") {
$hasDocker = $true
}

# Check for PostgreSQL
if ($node.Value.type -like "*PostgreSQL*") {
$hasPostgres = $true
}

# Check for production environment
if ($node.Value.properties.environment) {
if ($node.Value.properties.environment.NODE_ENV -eq "production") {
$isProduction = $true
}
}
}
}

# Check metadata for environment
if ($template.metadata.environment -eq "production") {
$isProduction = $true
}

# All criteria must match
$hasDocker -and $hasPostgres -and $isProduction
}

Write-Host "Production templates with Docker + PostgreSQL: $($filtered.Count)"
$filtered | ForEach-Object {
Write-Host "`n🏭 $($_.metadata.template_name)" -ForegroundColor Green
Write-Host "  ID: $($_._id)" -ForegroundColor Gray
Write-Host "  Datastore: $($_.metadata.kb_datastore)" -ForegroundColor Gray
}
```

**Linux:**
```bash
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": []
}' | jq '
.data[] |
select(.topology_template.node_templates != null) |
select(
# Has Docker
(.topology_template.node_templates | to_entries[] | .value.type | contains("Docker")) and
# Has PostgreSQL
(.topology_template.node_templates | to_entries[] | .value.type | contains("PostgreSQL")) and
# Is production
(
(.topology_template.node_templates | to_entries[] |
.value.properties.environment.NODE_ENV? == "production") or
(.metadata.environment? == "production")
)
) |
{
template: .metadata.template_name,
id: ._id,
datastore: .metadata.kb_datastore
}
'
```

---

## 🧠 Complex Nested Queries

### Scenario 11: Deep Nested Property Search

**PowerShell - Find Nodes with Specific Network Configuration:**
```powershell
# Find templates with nodes that have network_speed = "10 Gbps"
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

$matches = $response.data | Where-Object {
$template = $_
$found = $false

if ($template.topology_template.node_templates) {
foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
if ($node.Value.properties.network_speed -eq "10 Gbps") {
$found = $true
break
}
}
}

$found
}

Write-Host "Templates with 10 Gbps network: $($matches.Count)"
$matches | ForEach-Object {
Write-Host "`n🌐 $($_.metadata.template_name)" -ForegroundColor Cyan

$_.topology_template.node_templates.PSObject.Properties | Where-Object {
$_.Value.properties.network_speed -eq "10 Gbps"
} | ForEach-Object {
Write-Host "  Node: $($_.Name)" -ForegroundColor Yellow
Write-Host "    Type: $($_.Value.type)" -ForegroundColor Gray
Write-Host "    Speed: $($_.Value.properties.network_speed)" -ForegroundColor Gray

if ($_.Value.properties.mac_address) {
Write-Host "    MAC: $($_.Value.properties.mac_address)" -ForegroundColor Gray
}
if ($_.Value.properties.ip_address) {
Write-Host "    IP: $($_.Value.properties.ip_address)" -ForegroundColor Gray
}
}
}
```

---

### Scenario 12: Query Workflows and Policies

**PowerShell - Find Templates with Deployment Workflows:**
```powershell
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

$withWorkflows = $response.data | Where-Object {
$null -ne $_.topology_template.workflows
}

Write-Host "Templates with workflows: $($withWorkflows.Count)"
$withWorkflows | ForEach-Object {
Write-Host "`n⚙️  $($_.metadata.template_name)" -ForegroundColor Cyan

if ($_.topology_template.workflows) {
$_.topology_template.workflows.PSObject.Properties | ForEach-Object {
Write-Host "  Workflow: $($_.Name)" -ForegroundColor Yellow
Write-Host "    Description: $($_.Value.description)" -ForegroundColor Gray

if ($_.Value.steps) {
Write-Host "    Steps: $($_.Value.steps.Count)" -ForegroundColor Gray
$_.Value.steps | ForEach-Object {
$stepName = $_.PSObject.Properties.Name[0]
Write-Host "      - $stepName" -ForegroundColor DarkGray
}
}
}
}
}
```

**Linux:**
```bash
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": []
}' | jq '
.data[] |
select(.topology_template.workflows != null) |
{
template: .metadata.template_name,
workflows: [
.topology_template.workflows |
to_entries[] |
{
name: .key,
description: .value.description,
step_count: (.value.steps | length)
}
]
}
'
```

---

### Scenario 13: Query Groups and Policies

**PowerShell:**
```powershell
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

Write-Host "=== Templates with Groups and Policies ===" -ForegroundColor Cyan

$response.data | Where-Object {
$null -ne $_.topology_template.groups -or $null -ne $_.topology_template.policies
} | ForEach-Object {
$template = $_

Write-Host "`n📋 $($template.metadata.template_name)" -ForegroundColor Yellow

# Groups
if ($template.topology_template.groups) {
Write-Host "  Groups:" -ForegroundColor Green
$template.topology_template.groups.PSObject.Properties | ForEach-Object {
Write-Host "    - $($_.Name): $($_.Value.type)" -ForegroundColor Gray
Write-Host "      Members: $($_.Value.members -join ', ')" -ForegroundColor DarkGray
}
}

# Policies
if ($template.topology_template.policies) {
Write-Host "  Policies:" -ForegroundColor Green
$template.topology_template.policies | ForEach-Object {
$policyName = $_.PSObject.Properties.Name[0]
$policy = $_.PSObject.Properties.Value[0]
Write-Host "    - $policyName" -ForegroundColor Gray
Write-Host "      Type: $($policy.type)" -ForegroundColor DarkGray

if ($policy.properties) {
Write-Host "      Properties:" -ForegroundColor DarkGray
$policy.properties.PSObject.Properties | Select-Object -First 3 | ForEach-Object {
Write-Host "        • $($_.Name): $($_.Value)" -ForegroundColor DarkGray
}
}
}
}
}
```

---

## 📊 Analytical Queries

### Scenario 14: Resource Statistics Across Templates

**PowerShell - Complete Analysis Script:**

Save as `Analyze-ToscaResources.ps1`:

```powershell
<#
.SYNOPSIS
Comprehensive resource analysis across all TOSCA templates
#>

param(
[string]$BaseURL = "http://localhost:18001"
)

# Get all templates
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

$templates = $response.data

Write-Host "=== OptimusDB TOSCA Resource Analysis ===" -ForegroundColor Cyan
Write-Host "Total Templates: $($templates.Count)" -ForegroundColor Yellow
Write-Host ""

# Analysis 1: Templates by Datastore
Write-Host "📂 Templates by Datastore:" -ForegroundColor Green
$templates | Group-Object -Property {$_.metadata.kb_datastore} | ForEach-Object {
Write-Host "  $($_.Name): $($_.Count) templates" -ForegroundColor Gray
}
Write-Host ""

# Analysis 2: Node Type Distribution
Write-Host "🔷 Node Type Distribution:" -ForegroundColor Green
$nodeTypes = @{}
$templates | ForEach-Object {
if ($_.topology_template.node_templates) {
$_.topology_template.node_templates.PSObject.Properties | ForEach-Object {
$type = $_.Value.type
if ($nodeTypes.ContainsKey($type)) {
$nodeTypes[$type]++
} else {
$nodeTypes[$type] = 1
}
}
}
}
$nodeTypes.GetEnumerator() | Sort-Object -Property Value -Descending | ForEach-Object {
Write-Host "  $($_.Key): $($_.Value) instances" -ForegroundColor Gray
}
Write-Host ""

# Analysis 3: Total Resource Requirements
Write-Host "💻 Resource Requirements Summary:" -ForegroundColor Green

$totalCPU = 0
$totalMemoryGB = 0
$totalGPUs = 0

$templates | ForEach-Object {
if ($_.topology_template.node_templates) {
$_.topology_template.node_templates.PSObject.Properties | ForEach-Object {
$node = $_.Value

# CPU
if ($node.properties.num_cpus) {
$totalCPU += [int]$node.properties.num_cpus
}
if ($node.properties.cpu_cores_preferred) {
$totalCPU += [int]$node.properties.cpu_cores_preferred
}
if ($node.properties.total_cpu_cores) {
$totalCPU += [int]$node.properties.total_cpu_cores
}

# Memory
if ($node.properties.mem_size) {
if ($node.properties.mem_size -match '(\d+)\s*GB') {
$totalMemoryGB += [int]$matches[1]
}
}
if ($node.properties.total_memory) {
if ($node.properties.total_memory -match '(\d+)\s*GB') {
$totalMemoryGB += [int]$matches[1]
}
}
if ($node.properties.memory_preferred) {
if ($node.properties.memory_preferred -match '(\d+)\s*GB') {
$totalMemoryGB += [int]$matches[1]
}
}

# GPU
if ($node.type -like "*GPU*") {
$totalGPUs++
}
if ($node.properties.gpu_count_preferred) {
$totalGPUs += [int]$node.properties.gpu_count_preferred
}
}
}
}

Write-Host "  Total CPU Cores: $totalCPU" -ForegroundColor Yellow
Write-Host "  Total Memory: $totalMemoryGB GB" -ForegroundColor Yellow
Write-Host "  Total GPUs: $totalGPUs" -ForegroundColor Yellow
Write-Host ""

# Analysis 4: Network Ports Summary
Write-Host "🌐 Network Ports Summary:" -ForegroundColor Green
$ports = @{}
$templates | ForEach-Object {
if ($_.topology_template.node_templates) {
$_.topology_template.node_templates.PSObject.Properties | ForEach-Object {
if ($_.Value.properties.ports) {
$_.Value.properties.ports | ForEach-Object {
$portStr = $_
if ($portStr -match '(\d+)') {
$port = $matches[1]
if ($ports.ContainsKey($port)) {
$ports[$port]++
} else {
$ports[$port] = 1
}
}
}
}
}
}
}
$ports.GetEnumerator() | Sort-Object -Property {[int]$_.Key} | ForEach-Object {
$portName = switch ($_.Key) {
"80" { "HTTP" }
"443" { "HTTPS" }
"5432" { "PostgreSQL" }
"6379" { "Redis" }
"8080" { "HTTP Alt" }
default { "" }
}
$display = if ($portName) { "$($_.Key) ($portName)" } else { $_.Key }
Write-Host "  Port $display : $($_.Value) instances" -ForegroundColor Gray
}
Write-Host ""

# Analysis 5: Templates with Workflows
Write-Host "⚙️  Workflow Statistics:" -ForegroundColor Green
$withWorkflows = $templates | Where-Object { $null -ne $_.topology_template.workflows }
Write-Host "  Templates with workflows: $($withWorkflows.Count)" -ForegroundColor Gray
$withWorkflows | ForEach-Object {
Write-Host "    - $($_.metadata.template_name)" -ForegroundColor DarkGray
if ($_.topology_template.workflows) {
$workflowCount = ($_.topology_template.workflows.PSObject.Properties | Measure-Object).Count
Write-Host "      Workflows: $workflowCount" -ForegroundColor DarkGray
}
}
Write-Host ""

# Analysis 6: Policy Distribution
Write-Host "📜 Policy Types:" -ForegroundColor Green
$policyTypes = @{}
$templates | ForEach-Object {
if ($_.topology_template.policies) {
$_.topology_template.policies | ForEach-Object {
$policy = $_.PSObject.Properties.Value[0]
$type = $policy.type
if ($policyTypes.ContainsKey($type)) {
$policyTypes[$type]++
} else {
$policyTypes[$type] = 1
}
}
}
}
$policyTypes.GetEnumerator() | Sort-Object -Property Value -Descending | ForEach-Object {
Write-Host "  $($_.Key): $($_.Value) policies" -ForegroundColor Gray
}
Write-Host ""

# Analysis 7: Templates by Author
Write-Host "👤 Templates by Author:" -ForegroundColor Green
$templates | Group-Object -Property {$_.metadata.template_author} | ForEach-Object {
Write-Host "  $($_.Name): $($_.Count) templates" -ForegroundColor Gray
}

Write-Host ""
Write-Host "=== Analysis Complete ===" -ForegroundColor Cyan
```

**Run:**
```powershell
.\Analyze-ToscaResources.ps1
```

---

### Scenario 15: Compare Two Templates

**PowerShell:**

Save as `Compare-ToscaTemplates.ps1`:

```powershell
param(
[Parameter(Mandatory=$true)]
[string]$TemplateId1,

[Parameter(Mandatory=$true)]
[string]$TemplateId2,

[string]$BaseURL = "http://localhost:18001"
)

function Get-Template {
param([string]$Id)

$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @(@{_id = $Id})
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

return $response.data[0]
}

$template1 = Get-Template -Id $TemplateId1
$template2 = Get-Template -Id $TemplateId2

Write-Host "=== TOSCA Template Comparison ===" -ForegroundColor Cyan
Write-Host ""

# Basic Info
Write-Host "Template 1: $($template1.metadata.template_name)" -ForegroundColor Yellow
Write-Host "Template 2: $($template2.metadata.template_name)" -ForegroundColor Yellow
Write-Host ""

# Node Count
$nodes1 = ($template1.topology_template.node_templates.PSObject.Properties | Measure-Object).Count
$nodes2 = ($template2.topology_template.node_templates.PSObject.Properties | Measure-Object).Count

Write-Host "📊 Node Count:" -ForegroundColor Green
Write-Host "  Template 1: $nodes1 nodes" -ForegroundColor Gray
Write-Host "  Template 2: $nodes2 nodes" -ForegroundColor Gray
Write-Host "  Difference: $($ nodes1 - $nodes2)" -ForegroundColor Gray
Write-Host ""

# Node Types
Write-Host "🔷 Node Types:" -ForegroundColor Green
Write-Host "  Template 1:" -ForegroundColor Gray
$template1.topology_template.node_templates.PSObject.Properties | ForEach-Object {
Write-Host "    - $($_.Name): $($_.Value.type)" -ForegroundColor DarkGray
}
Write-Host "  Template 2:" -ForegroundColor Gray
$template2.topology_template.node_templates.PSObject.Properties | ForEach-Object {
Write-Host "    - $($_.Name): $($_.Value.type)" -ForegroundColor DarkGray
}
Write-Host ""

# Policies
Write-Host "📜 Policies:" -ForegroundColor Green
$policies1 = if ($template1.topology_template.policies) { $template1.topology_template.policies.Count } else { 0 }
$policies2 = if ($template2.topology_template.policies) { $template2.topology_template.policies.Count } else { 0 }
Write-Host "  Template 1: $policies1 policies" -ForegroundColor Gray
Write-Host "  Template 2: $policies2 policies" -ForegroundColor Gray
Write-Host ""

# Workflows
Write-Host "⚙️  Workflows:" -ForegroundColor Green
$workflows1 = if ($template1.topology_template.workflows) {
($template1.topology_template.workflows.PSObject.Properties | Measure-Object).Count
} else { 0 }
$workflows2 = if ($template2.topology_template.workflows) {
($template2.topology_template.workflows.PSObject.Properties | Measure-Object).Count
} else { 0 }
Write-Host "  Template 1: $workflows1 workflows" -ForegroundColor Gray
Write-Host "  Template 2: $workflows2 workflows" -ForegroundColor Gray
```

**Usage:**
```powershell
.\Compare-ToscaTemplates.ps1 -TemplateId1 "abc123" -TemplateId2 "def456"
```

---

## 🐛 Troubleshooting

### Problem 1: No Results Returned

**Cause:** Templates not uploaded or wrong criteria

**Solution:**
```powershell
# Verify templates exist
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post -Body $body -ContentType "application/json"

Write-Host "Total documents in dsswres: $($response.data.Count)"

# Check if they are TOSCA templates
$toscaCount = ($response.data | Where-Object {
$null -ne $_.topology_template
}).Count

Write-Host "TOSCA templates: $toscaCount"
```

---

### Problem 2: Nested Field Queries Don't Work

**Issue:** Criteria like `{"metadata.template_name": "value"}` don't work

**Reason:** OrbitDB requires custom filter functions for nested fields

**Solution:** Use client-side filtering:

```powershell
# Get all documents
$all = Get-AllDocuments

# Filter client-side
$filtered = $all | Where-Object {
$_.metadata.template_name -eq "WebApp-MicroservicesApplication"
}
```

---

### Problem 3: Upload Shows "Not Queryable"

**Cause:** Uploaded without `store_full_structure: true`

**Solution:**
```powershell
# Re-upload with correct flag
$body = @{
file = $base64Content
filename = "myfile.yaml"
store_full_structure = $true  # CRITICAL!
} | ConvertTo-Json

Invoke-RestMethod -Uri "$BaseURL/swarmkb/upload" `
-Method Post -Body $body -ContentType "application/json"
```

---

### Problem 4: PowerShell JSON Depth Issues

**Cause:** Default JSON depth is 2, TOSCA files are deeply nested

**Solution:**
```powershell
# Always use -Depth parameter
$body = @{...} | ConvertTo-Json -Depth 10  # Increase depth

# When displaying
$response.data | ConvertTo-Json -Depth 10
```

---

## Quick Reference Card

```
═══════════════════════════════════════════════════════════
OptimusDB TOSCA Query Quick Reference
═══════════════════════════════════════════════════════════

UPLOAD:
PowerShell: .\Upload-ToscaFiles.ps1
Linux:      ./upload_tosca_files.sh

GET ALL:
PowerShell: crudget -criteria @()
Linux:      criteria: []

FIND BY ID:
Criteria: [{"_id": "template_id"}]

FIND BY TOP-LEVEL FIELD:
Criteria: [{"tosca_definitions_version": "tosca_simple_yaml_1_3"}]

NESTED FIELDS (Client-side):
PowerShell: $data | Where-Object {$_.metadata.kb_datastore -eq "ADT"}
Linux:      jq '.data[] | select(.metadata.kb_datastore == "ADT")'

COMMON PATTERNS:
Find Docker:     type -like "*Docker*"
Find PostgreSQL: type -like "*PostgreSQL*"
Find GPUs:       type -like "*GPU*"
Find Port:       ports | Where { $_ -like "*443*" }
Find EnvVar:     environment.PSObject.Properties.Name -contains "VAR"

═══════════════════════════════════════════════════════════
```

---

**Project:** OptimusDB - EU Horizon Europe Grant 101135012
**Version:** 1.0
**Date:** December 19, 2025
