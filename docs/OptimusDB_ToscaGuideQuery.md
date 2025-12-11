# OptimusDB TOSCA Testing Scenarios
**Complete Guide for Uploading & Querying Your TOSCA Files**

---

## Overview

This guide covers uploading and querying the 5 TOSCA templates from:
`https://github.com/georgeGeorgakakos/optimusdb/tree/main/repoScript/Tosca/FIles`

**Files to Test:**
1. **sample_1_application_description.yaml** - Web application with 8 nodes (frontend, backend, DB, cache)
2. **sample_2_capacity_description.yaml** - Edge cluster capacity profile (Frankfurt)
3. **sample_3_opentofu_tosca_template.yaml** - Hybrid infrastructure with Kubernetes/Istio
4. **sample_4_deployment_release_plan.yaml** - Deployment plan with capacity matching
5. **sample_5_application_requirements.yaml** - Application requirements specification

---

## Part 1: Upload All TOSCA Files

### PowerShell Script: Upload All Files

```powershell
# Upload-All-TOSCA-Files.ps1
# Uploads all 5 TOSCA templates with full structure

$BaseUrl = "http://localhost:18001/optimusdb"
$ToscaDir = "C:\path\to\your\tosca\files"  # UPDATE THIS PATH

Write-Host "=== OptimusDB TOSCA Upload Test Suite ===" -ForegroundColor Cyan
Write-Host "Base URL: $BaseUrl" -ForegroundColor Gray
Write-Host ""

$Files = @(
"sample_1_application_description.yaml",
"sample_2_capacity_description.yaml",
"sample_3_opentofu_tosca_template.yaml",
"sample_4_deployment_release_plan.yaml",
"sample_5_application_requirements.yaml"
)

$UploadResults = @()

foreach ($File in $Files) {
Write-Host "[UPLOAD] Processing: $File" -ForegroundColor Yellow

$FilePath = Join-Path $ToscaDir $File

if (-not (Test-Path $FilePath)) {
Write-Host "  ✗ File not found: $FilePath" -ForegroundColor Red
continue
}

# Read and encode file
$Content = Get-Content $FilePath -Raw
$Base64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Content))

# Upload with full structure
$Payload = @{
file = $Base64
filename = $File
store_full_structure = $true
} | ConvertTo-Json

try {
$Response = Invoke-RestMethod -Uri "$BaseUrl/upload" `
-Method Post -ContentType "application/json" -Body $Payload

Write-Host "  ✓ Upload successful!" -ForegroundColor Green
Write-Host "    Template ID: $($Response.data.template_id)" -ForegroundColor Gray
Write-Host "    Storage: $($Response.data.storage_location)" -ForegroundColor Gray
Write-Host "    Queryable: $($Response.data.queryable)" -ForegroundColor Gray

$UploadResults += @{
File = $File
TemplateID = $Response.data.template_id
Status = "Success"
}
}
catch {
Write-Host "  ✗ Upload failed: $_" -ForegroundColor Red
$UploadResults += @{
File = $File
Status = "Failed"
}
}

Write-Host ""
}

Write-Host "=== Upload Summary ===" -ForegroundColor Cyan
$UploadResults | ForEach-Object {
$status = if ($_.Status -eq "Success") { "✓" } else { "✗" }
Write-Host "$status $($_.File)" -ForegroundColor $(if ($_.Status -eq "Success") { "Green" } else { "Red" })
}

Write-Host "`n✓ All uploads complete!" -ForegroundColor Green
Write-Host "Ready for querying!" -ForegroundColor Cyan
```

### Bash Script: Upload All Files

```bash
#!/bin/bash
# upload-all-tosca.sh

BASE_URL="http://localhost:18001/optimusdb"
TOSCA_DIR="/path/to/your/tosca/files"  # UPDATE THIS

echo "=== OptimusDB TOSCA Upload Test Suite ==="
echo ""

FILES=(
"sample_1_application_description.yaml"
"sample_2_capacity_description.yaml"
"sample_3_opentofu_tosca_template.yaml"
"sample_4_deployment_release_plan.yaml"
"sample_5_application_requirements.yaml"
)

for FILE in "${FILES[@]}"; do
echo "[UPLOAD] Processing: $FILE"

FILEPATH="$TOSCA_DIR/$FILE"

if [ ! -f "$FILEPATH" ]; then
echo "  ✗ File not found: $FILEPATH"
continue
fi

# Read and encode
BASE64_CONTENT=$(base64 -w 0 "$FILEPATH")

# Upload
RESPONSE=$(curl -s -X POST "$BASE_URL/upload" \
-H "Content-Type: application/json" \
-d "{
\"file\": \"$BASE64_CONTENT\",
\"filename\": \"$FILE\",
\"store_full_structure\": true
}")

echo "$RESPONSE" | jq '.'
echo ""
done

echo "✓ All uploads complete!"
```

---

## Part 2: Advanced Query Scenarios by File

### File 1: Application Description Queries

**sample_1_application_description.yaml** - Web application with 8 nodes

#### Query 1.1: Find All Application Descriptions
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "metadata.kb_datastore",
"operator": "==",
"value": "Application_Descriptions"
}]
}' | jq '.'
```

#### Query 1.2: Find Applications with PostgreSQL Database
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "topology_template.node_templates.postgres_db.type",
"operator": "==",
"value": "tosca.nodes.Database.PostgreSQL"
}]
}' | jq '.'
```

#### Query 1.3: Find Applications with Redis Cache
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "topology_template.node_templates.redis_cache.properties.version",
"operator": "==",
"value": "7.0"
}]
}' | jq '.'
```

#### Query 1.4: Complex - 3-Tier Applications with Load Balancer
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [
{
"field": "topology_template.node_templates.web_frontend",
"operator": "!=",
"value": null
},
{
"field": "topology_template.node_templates.api_gateway",
"operator": "!=",
"value": null
},
{
"field": "topology_template.node_templates.nginx_lb",
"operator": "!=",
"value": null
}
]
}' | jq '.'
```

---

### File 2: Capacity Description Queries

**sample_2_capacity_description.yaml** - Edge cluster capacity profile

#### Query 2.1: Find Available Capacity in Frankfurt
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [
{
"field": "metadata.location",
"operator": "==",
"value": "Frankfurt, Germany"
},
{
"field": "metadata.status",
"operator": "==",
"value": "available"
}
]
}' | jq '.'
```

#### Query 2.2: Find Nodes with NVIDIA A100 GPUs
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [
{
"field": "topology_template.node_templates.gpu_accelerator_01.properties.gpu_model",
"operator": "==",
"value": "NVIDIA A100"
},
{
"field": "topology_template.node_templates.gpu_accelerator_01.properties.available",
"operator": "==",
"value": true
}
]
}' | jq '.'
```

#### Query 2.3: Find High-Performance Storage (>400K IOPS)
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "topology_template.node_templates.local_storage_01.properties.iops_capability",
"operator": ">=",
"value": 400000
}]
}' | jq '.'
```

#### Query 2.4: Find Nodes with Low CPU Utilization (<30%)
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "topology_template.node_templates.edge_compute_node_01.attributes.cpu_utilization_current",
"operator": "<=",
"value": 30
}]
}' | jq '.'
```

#### Query 2.5: Find Gold SLA Tier Capacity
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "topology_template.policies.0.availability_policy.properties.sla_tier",
"operator": "==",
"value": "gold"
}]
}' | jq '.'
```

#### Query 2.6: Complex - Optimal Edge Nodes
```bash
# Find: EU region + Available GPU + Low CPU + High capacity score
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [
{
"field": "metadata.region",
"operator": "contains",
"value": "eu-central"
},
{
"field": "topology_template.node_templates.gpu_accelerator_01.properties.available",
"operator": "==",
"value": true
},
{
"field": "topology_template.node_templates.edge_compute_node_01.attributes.cpu_utilization_current",
"operator": "<=",
"value": 30
},
{
"field": "topology_template.outputs.capacity_score.value",
"operator": ">=",
"value": 0.8
}
],
"query_options": {
"strategy": "PARALLEL_MERGE",
"consistency": "QUORUM",
"quorum_n": 3
}
}' | jq '.'
```

---

### File 3: OpenTofu/TOSCA Template Queries

**sample_3_opentofu_tosca_template.yaml** - Hybrid infrastructure

#### Query 3.1: Find Healthy Swarm Clusters
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [
{
"field": "swarm_status.cluster_status",
"operator": "==",
"value": "healthy"
},
{
"field": "swarm_status.active_nodes",
"operator": ">=",
"value": 10
}
]
}' | jq '.'
```

#### Query 3.2: Find Templates with Istio Service Mesh
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [
{
"field": "topology_template.node_templates.istio_mesh.type",
"operator": "==",
"value": "tosca.nodes.ServiceMesh.Istio"
},
{
"field": "topology_template.node_templates.istio_mesh.properties.mtls_mode",
"operator": "==",
"value": "STRICT"
}
]
}' | jq '.'
```

#### Query 3.3: Find Templates with Prometheus Monitoring
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "topology_template.node_templates.prometheus_stack.properties.enabled",
"operator": "==",
"value": true
}]
}' | jq '.'
```

#### Query 3.4: Find Swarms by Coordinator Agent
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "swarm_status.coordinator_agent",
"operator": "==",
"value": "KB-coordinator-node-01"
}]
}' | jq '.'
```

#### Query 3.5: Find by Node Distribution
```bash
# Find swarms with more edge nodes than cloud nodes
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [
{
"field": "swarm_status.node_distribution.edge",
"operator": ">=",
"value": 7
},
{
"field": "swarm_status.node_distribution.cloud",
"operator": "<=",
"value": 3
}
]
}' | jq '.'
```

---

### File 4: Deployment Plan Queries

**sample_4_deployment_release_plan.yaml** - Deployment with capacity matching

#### Query 4.1: Find Ready-to-Deploy Plans
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [
{
"field": "metadata.execution_status",
"operator": "==",
"value": "ready_for_deployment"
},
{
"field": "capacity_matching.status",
"operator": "==",
"value": "successful"
}
]
}' | jq '.'
```

#### Query 4.2: Find High Match Score Deployments (>0.9)
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "capacity_matching.match_score",
"operator": ">=",
"value": 0.9
}]
}' | jq '.'
```

#### Query 4.3: Find Deployments with Blue-Green Strategy
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "topology_template.node_templates.deployment_plan.properties.deployment_strategy",
"operator": "==",
"value": "blue_green"
}]
}' | jq '.'
```

#### Query 4.4: Find Cost-Efficient Deployments (<$3/hour)
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "resource_allocation.estimated_cost_per_hour",
"operator": "<=",
"value": 3.0
}]
}' | jq '.'
```

#### Query 4.5: Find Deployments Targeting Specific Node
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "topology_template.node_templates.frontend_deployment.properties.target_node",
"operator": "==",
"value": "edge-node-01.eu-central.swarm"
}]
}' | jq '.'
```

#### Query 4.6: Complex - Production-Ready with Rollback
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [
{
"field": "capacity_matching.status",
"operator": "==",
"value": "successful"
},
{
"field": "capacity_matching.match_score",
"operator": ">=",
"value": 0.85
},
{
"field": "topology_template.node_templates.deployment_plan.properties.rollback_enabled",
"operator": "==",
"value": true
},
{
"field": "topology_template.node_templates.deployment_plan.properties.health_check_enabled",
"operator": "==",
"value": true
}
]
}' | jq '.'
```

---

### File 5: Application Requirements Queries

**sample_5_application_requirements.yaml** - Requirements specification

#### Query 5.1: Find Requirements by Template Name
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "metadata.template_name",
"operator": "contains",
"value": "Requirements"
}]
}' | jq '.'
```

#### Query 5.2: Find by Datastore Type
```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "metadata.kb_datastore",
"operator": "==",
"value": "Application_Requirements"
}]
}' | jq '.'
```

---

## Part 3: Cross-File Advanced Queries

### Scenario 1: Capacity Planning - Match Requirements to Availability

**Goal**: Find capacity that can satisfy application requirements

```bash
# Step 1: Find application requirements
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "metadata.kb_datastore",
"operator": "==",
"value": "Application_Requirements"
}]
}' > app_requirements.json

# Step 2: Extract CPU requirements (example: 12 cores)
# Then find matching capacity

curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [
{
"field": "metadata.kb_datastore",
"operator": "==",
"value": "Capacity_Descriptions"
},
{
"field": "topology_template.node_templates.edge_compute_node_01.properties.available_cpu_cores",
"operator": ">=",
"value": 12
},
{
"field": "metadata.status",
"operator": "==",
"value": "available"
}
]
}' | jq '.'
```

### Scenario 2: Deployment Matching - Find Compatible Infrastructure

**Goal**: Find OpenTofu templates that match deployment requirements

```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [
{
"field": "metadata.kb_datastore",
"operator": "==",
"value": "OpenTofu_TOSCA_Templates"
},
{
"field": "swarm_status.active_nodes",
"operator": ">=",
"value": 10
},
{
"field": "topology_template.node_templates.prometheus_stack.properties.enabled",
"operator": "==",
"value": true
}
]
}' | jq '.'
```

### Scenario 3: Cost Optimization - Find Cheapest Available Capacity

```bash
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [
{
"field": "metadata.kb_datastore",
"operator": "==",
"value": "Capacity_Descriptions"
},
{
"field": "topology_template.policies.0.cost_policy.properties.cpu_cost_per_core_hour",
"operator": "<=",
"value": 0.05
},
{
"field": "metadata.status",
"operator": "==",
"value": "available"
}
]
}' | jq '.'
```

### Scenario 4: Health Check - Find All Resources by Status

```bash
# Find all templates and group by datastore
curl -X POST http://localhost:18001/optimusdb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "dsswres",
"criteria": [{
"field": "_storage_type",
"operator": "==",
"value": "full_structure"
}]
}' | jq '[.data.results[] | {
filename: ._filename,
datastore: .metadata.kb_datastore,
status: .metadata.status // "N/A",
imported_at: ._imported_at
}]'
```

---

## Part 4: PowerShell Complete Test Suite

```powershell
# Complete-TOSCA-Test-Suite.ps1
# Comprehensive testing of all uploaded TOSCA files

$BaseUrl = "http://localhost:18001/optimusdb"

Write-Host "=== OptimusDB TOSCA Query Test Suite ===" -ForegroundColor Cyan
Write-Host ""

# Test 1: Verify All Uploads
Write-Host "[Test 1] Verify all TOSCA files uploaded" -ForegroundColor Yellow
$AllQuery = @{
method = @{ cmd = "query"; argcnt = 0 }
dstype = "dsswres"
criteria = @(
@{
field = "_storage_type"
operator = "=="
value = "full_structure"
}
)
} | ConvertTo-Json -Depth 10

$AllResults = Invoke-RestMethod -Uri "$BaseUrl/command" `
-Method Post -ContentType "application/json" -Body $AllQuery

$TotalFiles = $AllResults.data.results.Count
Write-Host "✓ Found $TotalFiles TOSCA templates" -ForegroundColor Green
$AllResults.data.results | ForEach-Object {
Write-Host "  - $($_._filename)" -ForegroundColor Gray
}
Write-Host ""

# Test 2: Query Capacity Descriptions
Write-Host "[Test 2] Query available capacity in Frankfurt" -ForegroundColor Yellow
$CapacityQuery = @{
method = @{ cmd = "query"; argcnt = 0 }
dstype = "dsswres"
criteria = @(
@{
field = "metadata.location"
operator = "=="
value = "Frankfurt, Germany"
}
)
} | ConvertTo-Json -Depth 10

$CapacityResults = Invoke-RestMethod -Uri "$BaseUrl/command" `
-Method Post -ContentType "application/json" -Body $CapacityQuery

if ($CapacityResults.data.results) {
Write-Host "✓ Found capacity:" -ForegroundColor Green
$CapacityResults.data.results | ForEach-Object {
$cpuCores = $_.topology_template.node_templates.edge_compute_node_01.properties.available_cpu_cores
$memory = $_.topology_template.node_templates.edge_compute_node_01.properties.available_memory
Write-Host "  CPU: $cpuCores cores, Memory: $memory" -ForegroundColor Gray
}
} else {
Write-Host "✗ No capacity found" -ForegroundColor Red
}
Write-Host ""

# Test 3: Query Healthy Swarms
Write-Host "[Test 3] Query healthy swarm clusters" -ForegroundColor Yellow
$SwarmQuery = @{
method = @{ cmd = "query"; argcnt = 0 }
dstype = "dsswres"
criteria = @(
@{
field = "swarm_status.cluster_status"
operator = "=="
value = "healthy"
}
)
} | ConvertTo-Json -Depth 10

$SwarmResults = Invoke-RestMethod -Uri "$BaseUrl/command" `
-Method Post -ContentType "application/json" -Body $SwarmQuery

if ($SwarmResults.data.results) {
Write-Host "✓ Found swarm clusters:" -ForegroundColor Green
$SwarmResults.data.results | ForEach-Object {
Write-Host "  Cluster: $($_.swarm_status.cluster_id)" -ForegroundColor Gray
Write-Host "  Active Nodes: $($_.swarm_status.active_nodes)" -ForegroundColor Gray
}
} else {
Write-Host "✗ No swarm clusters found" -ForegroundColor Red
}
Write-Host ""

# Test 4: Query Deployment Plans
Write-Host "[Test 4] Query ready deployment plans" -ForegroundColor Yellow
$DeployQuery = @{
method = @{ cmd = "query"; argcnt = 0 }
dstype = "dsswres"
criteria = @(
@{
field = "metadata.execution_status"
operator = "=="
value = "ready_for_deployment"
}
)
} | ConvertTo-Json -Depth 10

$DeployResults = Invoke-RestMethod -Uri "$BaseUrl/command" `
-Method Post -ContentType "application/json" -Body $DeployQuery

if ($DeployResults.data.results) {
Write-Host "✓ Found deployment plans:" -ForegroundColor Green
$DeployResults.data.results | ForEach-Object {
Write-Host "  Plan: $($_.metadata.deployment_id)" -ForegroundColor Gray
Write-Host "  Match Score: $($_.capacity_matching.match_score)" -ForegroundColor Gray
}
} else {
Write-Host "✗ No deployment plans found" -ForegroundColor Red
}
Write-Host ""

# Test 5: Complex Query - Optimal Resources
Write-Host "[Test 5] Complex query: Optimal edge resources" -ForegroundColor Yellow
$OptimalQuery = @{
method = @{ cmd = "query"; argcnt = 0 }
dstype = "dsswres"
criteria = @(
@{
field = "metadata.region"
operator = "contains"
value = "eu-central"
},
@{
field = "topology_template.node_templates.gpu_accelerator_01.properties.available"
operator = "=="
value = $true
},
@{
field = "topology_template.node_templates.edge_compute_node_01.attributes.cpu_utilization_current"
operator = "<="
value = 30
}
)
} | ConvertTo-Json -Depth 10

$OptimalResults = Invoke-RestMethod -Uri "$BaseUrl/command" `
-Method Post -ContentType "application/json" -Body $OptimalQuery

if ($OptimalResults.data.results) {
Write-Host "✓ Found optimal resources:" -ForegroundColor Green
$OptimalResults.data.results | ForEach-Object {
Write-Host "  Node: $($_.metadata.provider_id)" -ForegroundColor Gray
Write-Host "  GPU: $($_.topology_template.node_templates.gpu_accelerator_01.properties.gpu_model)" -ForegroundColor Gray
Write-Host "  CPU Utilization: $($_.topology_template.node_templates.edge_compute_node_01.attributes.cpu_utilization_current)%" -ForegroundColor Gray
}
} else {
Write-Host "✗ No optimal resources found" -ForegroundColor Red
}
Write-Host ""

Write-Host "=== All Tests Complete ===" -ForegroundColor Cyan
```

---

## Part 5: Query Cheat Sheet

### By Datastore Type
```bash
# Capacity Descriptions
"metadata.kb_datastore" == "Capacity_Descriptions"

# Application Descriptions
"metadata.kb_datastore" == "Application_Descriptions"

# OpenTofu Templates
"metadata.kb_datastore" == "OpenTofu_TOSCA_Templates"

# Deployment Plans
"metadata.kb_datastore" == "Deployment_Release_Plans"

# Requirements
"metadata.kb_datastore" == "Application_Requirements"
```

### By Resource Properties
```bash
# Available CPU >= 20 cores
"topology_template.node_templates.edge_compute_node_01.properties.available_cpu_cores" >= 20

# GPU Available
"topology_template.node_templates.gpu_accelerator_01.properties.available" == true

# Storage IOPS >= 400K
"topology_template.node_templates.local_storage_01.properties.iops_capability" >= 400000

# CPU Utilization <= 30%
"topology_template.node_templates.edge_compute_node_01.attributes.cpu_utilization_current" <= 30
```

### By Deployment Status
```bash
# Ready for deployment
"metadata.execution_status" == "ready_for_deployment"

# Successful capacity match
"capacity_matching.status" == "successful"

# High match score
"capacity_matching.match_score" >= 0.9
```

### By Infrastructure
```bash
# Healthy swarm
"swarm_status.cluster_status" == "healthy"

# Active nodes >= 10
"swarm_status.active_nodes" >= 10

# Istio enabled
"topology_template.node_templates.istio_mesh.type" == "tosca.nodes.ServiceMesh.Istio"
```

---
