# OptimusDB TOSCA Knowledge Base - Complete User Guide

## 📖 Introduction

### What is OptimusDB?

OptimusDB is a **decentralized knowledge base** designed specifically for managing infrastructure and application metadata as **queryable knowledge objects**. Unlike traditional file storage systems, OptimusDB:

- ✅ **Treats YAML as structured data**, not text files
- ✅ **Preserves semantic meaning** of infrastructure definitions
- ✅ **Enables deep querying** of nested structures
- ✅ **Distributes data** across peer-to-peer network
- ✅ **Supports multiple logical datastores** in single cluster

### Why TOSCA Templates?

**TOSCA (Topology and Orchestration Specification for Cloud Applications)** is the industry standard for describing:
- Application topologies
- Infrastructure requirements
- Deployment workflows
- Operational policies
- Service orchestration

OptimusDB makes TOSCA templates **searchable and analyzable** rather than static configuration files.

### What This Guide Covers

This guide demonstrates a **complete end-to-end workflow**:

1. **Upload**: Store 5 representative TOSCA templates
2. **Query**: Search from simple to complex patterns
3. **Analyze**: Extract insights across templates
4. **Compare**: Infrastructure-aware comparisons

**Platform Support:**
- 🪟 **Windows**: PowerShell examples
- 🐧 **Linux/macOS**: Bash examples
- ☁️ **Both**: Same functionality, different syntax

---

## 🗂️ The TOSCA Template Collection

### Overview: 5 Templates, 5 Datastores, 5 Use Cases

| Template | Datastore | Use Case | Key Features |
|----------|-----------|----------|--------------|
| **webapp_adt.yaml** | `dsswres` | Microservices App | 4 Docker containers, Nginx, Python, Postgres, Redis |
| **capacity_profile.yaml** | `dssrres` | Edge Computing Capacity | GPU NVIDIA A100, 128GB RAM, power management |
| **opentofu_hybrid.yaml** | `dssires` | Hybrid Cloud Infrastructure | AWS + Azure + Kubernetes, Istio, Prometheus |
| **deployment_plan.yaml** | `dsswres` | CI/CD Deployment | 9-step deploy workflow, 6-step rollback |
| **app_requirements.yaml** | `dssares` | ML Training Requirements | 8 GPUs, 128GB RAM, checkpoint policies |

### Template 1: webapp_adt.yaml - Microservices Application

**Datastore:** `dsswres` (Distributed Swarm System Workflow Resources)

**Purpose:** Demonstrates a complete web application stack with frontend, backend, database, and cache.

**Structure:**
```yaml
tosca_definitions_version: tosca_simple_yaml_1_3

metadata:
template_name: "WebApp-MicroservicesApplication"
template_author: "OptimusDB Team"
template_version: "1.0.0"

topology_template:
node_templates:
# Frontend - Nginx web server
frontend_container:
type: tosca.nodes.Container.Application
properties:
image: nginx:latest
ports: ["80:80", "443:443"]
environment:
- BACKEND_URL=http://backend:5000

# Backend - Python API
backend_container:
type: tosca.nodes.Container.Runtime
properties:
image: python:3.9
ports: ["5000:5000"]
command: python app.py
environment:
- DATABASE_URL=postgresql://postgres:5432/webapp
- REDIS_URL=redis://cache:6379

# Database - PostgreSQL
database_container:
type: tosca.nodes.Container.Runtime
properties:
image: postgres:14
ports: ["5432:5432"]
environment:
- POSTGRES_DB=webapp
- POSTGRES_USER=admin
- POSTGRES_PASSWORD=secret123
volumes:
- /data/postgres:/var/lib/postgresql/data

# Cache - Redis
cache_container:
type: tosca.nodes.Container.Runtime
properties:
image: redis:alpine
ports: ["6379:6379"]

policies:
- scaling_policy:
type: tosca.policies.Scaling
targets: [backend_container]
properties:
min_instances: 2
max_instances: 10
cpu_threshold: 80

- monitoring_policy:
type: tosca.policies.Monitoring
targets: [frontend_container, backend_container]
properties:
metrics: ["cpu", "memory", "requests_per_second"]

- cost_optimization:
type: tosca.policies.Performance
properties:
budget: "$500/month"
auto_shutdown_idle: true
```

**What You Can Query:**
- Container types (Docker/Podman)
- Database technologies (PostgreSQL, Redis)
- Exposed ports (80, 443, 5000, 5432, 6379)
- Environment variables
- Scaling policies
- Cost budgets

---

### Template 2: capacity_profile.yaml - Edge Computing Capacity

**Datastore:** `dssrres` (Distributed Swarm System Resource Requirements)

**Purpose:** Defines hardware capacity for GPU-accelerated edge computing clusters.

**Structure:**
```yaml
tosca_definitions_version: tosca_simple_yaml_1_3

metadata:
template_name: "EdgeCluster-CapacityProfile"
template_author: "OptimusDB Team"
template_version: "2.0.0"

topology_template:
node_templates:
edge_compute_node:
type: tosca.nodes.Compute
properties:
num_cpus: 32
cpu_frequency: "3.5 GHz"
mem_size: "128 GB"
disk_size: "2 TB"
architecture: "x86_64"
capabilities:
host:
properties:
num_cpus: 32
mem_size: "128 GB"

gpu_accelerator:
type: tosca.nodes.GPU
properties:
gpu_model: "NVIDIA A100"
gpu_memory: "40 GB"
gpu_count: 4
cuda_version: "11.8"
tensor_cores: 432

groups:
edge_cluster:
type: tosca.groups.Root
members: [edge_compute_node, gpu_accelerator]
metadata:
location: "Edge datacenter"
tier: "Premium"

policies:
- power_efficiency:
type: tosca.policies.Performance
targets: [edge_compute_node]
properties:
max_power: "500W"
idle_power: "50W"

- thermal_management:
type: tosca.policies.Placement
targets: [gpu_accelerator]
properties:
max_temperature: "85C"
cooling_type: "liquid"

- availability_policy:
type: tosca.policies.Scaling
properties:
availability: "99.9%"
redundancy: "N+1"
```

**What You Can Query:**
- GPU specifications (model, memory, count)
- CPU specifications (cores, frequency)
- Memory requirements (>64GB nodes)
- Groups and cluster configurations
- Power and thermal policies
- Availability requirements

---

### Template 3: opentofu_hybrid.yaml - Hybrid Cloud Infrastructure

**Datastore:** `dssires` (Distributed Swarm System Infrastructure Resources)

**Purpose:** Multi-cloud deployment with Kubernetes, service mesh, and monitoring.

**Structure:**
```yaml
tosca_definitions_version: tosca_simple_yaml_1_3

metadata:
template_name: "HybridInfrastructure-SwarmDeployment"
template_author: "OptimusDB Team"
template_version: "1.5.0"

topology_template:
node_templates:
aws_kubernetes_cluster:
type: tosca.nodes.Container.Runtime.Kubernetes
properties:
provider: "AWS EKS"
region: "eu-west-1"
version: "1.28"
node_count: 5
node_instance_type: "t3.xlarge"
num_cpus: 4
mem_size: "16 GB"

azure_kubernetes_cluster:
type: tosca.nodes.Container.Runtime.Kubernetes
properties:
provider: "Azure AKS"
region: "westeurope"
version: "1.28"
node_count: 3
node_instance_type: "Standard_D4s_v3"
num_cpus: 4
mem_size: "16 GB"

on_premise_kubernetes:
type: tosca.nodes.Container.Runtime.Kubernetes
properties:
provider: "On-Premise"
version: "1.28"
node_count: 8
num_cpus: 16
mem_size: "64 GB"
network_cni: "Calico"

istio_service_mesh:
type: tosca.nodes.ServiceMesh
properties:
mesh_type: "Istio"
version: "1.20"
mtls_enabled: true
tracing_enabled: true
requirements:
- host: aws_kubernetes_cluster
- host: azure_kubernetes_cluster

prometheus_monitoring:
type: tosca.nodes.Monitoring
properties:
monitoring_tool: "Prometheus"
version: "2.48"
retention_days: 30
scrape_interval: "15s"
requirements:
- monitored_cluster: aws_kubernetes_cluster
- monitored_cluster: azure_kubernetes_cluster
- monitored_cluster: on_premise_kubernetes

groups:
production_cluster:
type: tosca.groups.Root
members: [aws_kubernetes_cluster, azure_kubernetes_cluster]
metadata:
environment: "production"

development_cluster:
type: tosca.groups.Root
members: [on_premise_kubernetes]
metadata:
environment: "development"

policies:
- load_balancing:
type: tosca.policies.Placement
targets: [aws_kubernetes_cluster, azure_kubernetes_cluster]
properties:
strategy: "round-robin"
health_check_interval: "30s"

- cost_optimization:
type: tosca.policies.Performance
properties:
max_monthly_cost: "$1000"
spot_instances_allowed: true
auto_scaling_enabled: true

- disaster_recovery:
type: tosca.policies.Scaling
properties:
backup_frequency: "daily"
rpo: "1 hour"
rto: "4 hours"
geo_redundancy: true
```

**What You Can Query:**
- Kubernetes clusters (AWS, Azure, On-Premise)
- Service mesh configurations (Istio)
- Monitoring tools (Prometheus)
- Multi-cloud strategies
- Groups by environment
- Cost optimization policies

---

### Template 4: deployment_plan.yaml - CI/CD Deployment Workflows

**Datastore:** `dsswres` (Distributed Swarm System Workflow Resources)

**Purpose:** Complete DevOps deployment automation with multi-step workflows.

**Structure:**
```yaml
tosca_definitions_version: tosca_simple_yaml_1_3

metadata:
template_name: "DeploymentPlan-WebApp-Release-v1.2.3"
template_author: "OptimusDB Team"
template_version: "1.2.3"
release_date: "2024-12-15"

# WORKFLOWS AT TOP LEVEL (TOSCA 1.3 standard)
workflows:
deploy_workflow:
description: "Multi-stage deployment workflow"
steps:
- build_application:
activities:
- call_operation: build.compile
on_success: run_tests

- run_tests:
activities:
- call_operation: test.unit_tests
- call_operation: test.integration_tests
on_success: create_container

- create_container:
activities:
- call_operation: container.build
- call_operation: container.push
on_success: deploy_staging

- deploy_staging:
activities:
- call_operation: deploy.to_staging
on_success: smoke_tests

- smoke_tests:
activities:
- call_operation: test.smoke
on_success: deploy_production
on_failure: rollback_workflow

- deploy_production:
activities:
- call_operation: deploy.to_production
on_success: health_check

- health_check:
activities:
- call_operation: monitor.health
on_success: notify_team

- notify_team:
activities:
- call_operation: notify.slack
- call_operation: notify.email
on_success: update_documentation

- update_documentation:
activities:
- call_operation: docs.update

rollback_workflow:
description: "Automated rollback procedure"
steps:
- stop_new_deployment:
activities:
- call_operation: deploy.stop
on_success: restore_previous_version

- restore_previous_version:
activities:
- call_operation: deploy.rollback
on_success: verify_rollback

- verify_rollback:
activities:
- call_operation: test.smoke
on_success: clear_cache
on_failure: manual_intervention

- clear_cache:
activities:
- call_operation: cache.clear
on_success: notify_incident

- notify_incident:
activities:
- call_operation: notify.pagerduty
- call_operation: notify.slack_emergency
on_success: create_postmortem

- create_postmortem:
activities:
- call_operation: docs.postmortem

- manual_intervention:
activities:
- call_operation: escalate.ops_team

topology_template:
node_templates:
web_application:
type: tosca.nodes.Container.Application
properties:
image: "webapp:v1.2.3"
ports: ["443:443"]
replicas: 3
environment:
- ENVIRONMENT=production
- LOG_LEVEL=info

load_balancer:
type: tosca.nodes.LoadBalancer
properties:
algorithm: "least_connections"
health_check_interval: "30s"
health_check_path: "/health"
ssl_certificate: "arn:aws:acm:cert-123"

policies:
- auto_scaling:
type: tosca.policies.Scaling
targets: [web_application]
properties:
min_instances: 2
max_instances: 20
scale_up_threshold: 70
scale_down_threshold: 30

- monitoring:
type: tosca.policies.Monitoring
targets: [web_application, load_balancer]
properties:
metrics: ["cpu", "memory", "requests_per_second", "error_rate"]
alert_threshold_error_rate: 5

- security:
type: tosca.policies.Security
properties:
ssl_enabled: true
tls_version: "1.3"
waf_enabled: true
```

**What You Can Query:**
- Workflows (deploy, rollback)
- Workflow steps and dependencies
- Success/failure transitions
- Container applications
- Load balancer configurations
- Auto-scaling policies

---

### Template 5: app_requirements.yaml - ML Training Requirements

**Datastore:** `dssares` (Distributed Swarm System Application Requirements)

**Purpose:** Infrastructure requirements for GPU-intensive machine learning workloads.

**Structure:**
```yaml
tosca_definitions_version: tosca_simple_yaml_1_3

metadata:
template_name: "ApplicationRequirements-MLTrainingWorkload"
template_author: "OptimusDB Team"
template_version: "3.0.0"
use_case: "Deep Learning Training"

topology_template:
node_templates:
training_compute:
type: tosca.nodes.Compute
properties:
num_cpus: 64
cpu_frequency: "3.0 GHz"
memory_preferred: 128
disk_size: "10 TB"
network_bandwidth: "100 Gbps"
capabilities:
gpu_acceleration:
properties:
gpu_model: "NVIDIA A100"
gpu_count_preferred: 8
gpu_memory: "80 GB per GPU"
total_gpu_memory: "640 GB"
nvlink_enabled: true

data_storage:
type: tosca.nodes.Storage.ObjectStorage
properties:
size: "100 TB"
storage_class: "hot"
replication: 3
iops: 50000
requirements:
- local_attachment: training_compute

model_registry:
type: tosca.nodes.Storage.ObjectStorage
properties:
size: "10 TB"
storage_class: "standard"
versioning_enabled: true

groups:
training_infrastructure:
type: tosca.groups.Root
members: [training_compute, data_storage, model_registry]
metadata:
project: "MLOps-Platform"
cost_center: "R&D"

policies:
- cost_optimization:
type: tosca.policies.Performance
properties:
max_monthly_cost: "$10000"
spot_instances_allowed: true
preemptible_allowed: true

- performance_requirements:
type: tosca.policies.Performance
targets: [training_compute]
properties:
min_training_speed: "1000 samples/second"
max_training_time: "72 hours"
target_accuracy: 0.95

- data_locality:
type: tosca.policies.Placement
targets: [training_compute, data_storage]
properties:
prefer_same_datacenter: true
max_latency_ms: 10

- checkpoint_policy:
type: tosca.policies.Scaling
targets: [training_compute]
properties:
checkpoint_frequency: "1 hour"
max_checkpoints: 10
checkpoint_storage: model_registry

- monitoring_policy:
type: tosca.policies.Monitoring
targets: [training_compute]
properties:
metrics: ["gpu_utilization", "gpu_memory", "training_loss", "throughput"]
alert_threshold_gpu_util: 80
alert_threshold_gpu_memory: 90

- security_policy:
type: tosca.policies.Security
properties:
data_encryption: "AES-256"
access_control: "RBAC"
audit_logging: true
```

**What You Can Query:**
- GPU requirements (model, count, memory)
- High memory nodes (>64GB)
- Storage requirements (100TB datasets)
- Performance policies (speed, accuracy)
- Checkpoint strategies
- Cost budgets ($10K/month)

---

## 📤 Upload Guide: Getting Data Into OptimusDB

### Upload Process Overview

**OptimusDB Upload Flow:**
```
TOSCA YAML File → Base64 Encode → HTTP POST → OptimusDB API →
OrbitDB Store → IPFS Storage → Cluster Replication → Template ID Returned
```

### Critical Parameter: `store_full_structure`

**Why This Matters:**

```json
{
"store_full_structure": false  // ❌ Stores only metadata
"store_full_structure": true   // ✅ Stores entire YAML as queryable JSON
}
```

**Without `store_full_structure=true`:**
- ❌ Only template name, version, author stored
- ❌ Cannot query node_templates, policies, workflows
- ❌ Essentially read-only metadata

**With `store_full_structure=true`:**
- ✅ Every nested property becomes queryable
- ✅ Can search environment variables, ports, GPU specs
- ✅ Full infrastructure-aware knowledge base

---

### Upload Script: PowerShell

**File:** `upload-tosca.ps1`

```powershell
# OptimusDB TOSCA Upload Script
# Platform: Windows (PowerShell)

param(
[string]$BaseURL = "http://localhost:18001",
[string]$FilesPath = "./tosca-templates"
)

# TOSCA files to upload
$ToscaFiles = @{
"webapp_adt.yaml" = "dsswres"
"capacity_profile.yaml" = "dssrres"
"opentofu_hybrid.yaml" = "dssires"
"deployment_plan.yaml" = "dsswres"
"app_requirements.yaml" = "dssares"
}

Write-Host "OptimusDB TOSCA Upload Utility" -ForegroundColor Cyan
Write-Host "=================================" -ForegroundColor Cyan
Write-Host ""

foreach ($file in $ToscaFiles.GetEnumerator()) {
$filename = $file.Key
$datastore = $file.Value
$filepath = Join-Path $FilesPath $filename

if (-not (Test-Path $filepath)) {
Write-Host "❌ File not found: $filename" -ForegroundColor Red
continue
}

Write-Host "Uploading: $filename → Datastore: $datastore..." -NoNewline

# Read and encode file
$fileBytes = [System.IO.File]::ReadAllBytes($filepath)
$base64Content = [Convert]::ToBase64String($fileBytes)

# Prepare request
$body = @{
file = $base64Content
filename = $filename
datastore = $datastore
store_full_structure = $true  # CRITICAL!
} | ConvertTo-Json

try {
# Upload to OptimusDB
$response = Invoke-RestMethod -Uri "$BaseURL/swarmkb/upload" `
-Method Post `
-ContentType "application/json" `
-Body $body `
-TimeoutSec 60

if ($response.status -eq 200) {
$templateId = $response.data.template_id
$idPreview = $templateId.Substring(0, [Math]::Min(12, $templateId.Length))
Write-Host " ✅ Success" -ForegroundColor Green
Write-Host "  Template ID: $idPreview..." -ForegroundColor Gray
} else {
Write-Host " ❌ Failed" -ForegroundColor Red
Write-Host "  Error: $($response.message)" -ForegroundColor Red
}
} catch {
Write-Host " ❌ Error" -ForegroundColor Red
Write-Host "  Exception: $_" -ForegroundColor Red
}
}

Write-Host ""
Write-Host "Upload complete!" -ForegroundColor Green
```

**Usage:**
```powershell
# Default (localhost:18001, ./tosca-templates)
.\upload-tosca.ps1

# Custom OptimusDB URL
.\upload-tosca.ps1 -BaseURL "http://192.168.1.100:18001"

# Custom file path
.\upload-tosca.ps1 -FilesPath "C:\my-tosca-files"
```

---

### Upload Script: Bash

**File:** `upload-tosca.sh`

```bash
#!/bin/bash
# OptimusDB TOSCA Upload Script
# Platform: Linux/macOS (Bash)

BASE_URL="${1:-http://localhost:18001}"
FILES_PATH="${2:-./tosca-templates}"

# TOSCA files to upload
declare -A TOSCA_FILES=(
["webapp_adt.yaml"]="dsswres"
["capacity_profile.yaml"]="dssrres"
["opentofu_hybrid.yaml"]="dssires"
["deployment_plan.yaml"]="dsswres"
["app_requirements.yaml"]="dssares"
)

echo "OptimusDB TOSCA Upload Utility"
echo "================================="
echo ""

for filename in "${!TOSCA_FILES[@]}"; do
datastore="${TOSCA_FILES[$filename]}"
filepath="${FILES_PATH}/${filename}"

if [ ! -f "$filepath" ]; then
echo "❌ File not found: $filename"
continue
fi

echo -n "Uploading: $filename → Datastore: $datastore..."

# Read and encode file
base64_content=$(base64 -w 0 "$filepath" 2>/dev/null || base64 "$filepath")

# Prepare request body
body=$(cat <<EOF
        {
"file": "$base64_content",
"filename": "$filename",
"datastore": "$datastore",
"store_full_structure": true
}
EOF
)

# Upload to OptimusDB
response=$(curl -s -X POST "${BASE_URL}/swarmkb/upload" \
-H "Content-Type: application/json" \
-d "$body" \
--max-time 60)

status=$(echo "$response" | jq -r '.status // empty')

if [ "$status" = "200" ]; then
template_id=$(echo "$response" | jq -r '.data.template_id // empty')
id_preview="${template_id:0:12}"
echo " ✅ Success"
echo "  Template ID: ${id_preview}..."
else
echo " ❌ Failed"
message=$(echo "$response" | jq -r '.message // "Unknown error"')
echo "  Error: $message"
fi
done

echo ""
echo "Upload complete!"
```

**Usage:**
```bash
# Make executable
chmod +x upload-tosca.sh

# Default (localhost:18001, ./tosca-templates)
./upload-tosca.sh

# Custom OptimusDB URL
./upload-tosca.sh http://192.168.1.100:18001

# Custom file path
./upload-tosca.sh http://localhost:18001 /path/to/tosca-files
```

---

### Upload Verification

**Check what was uploaded:**

```powershell
# PowerShell
$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body '{"method":{"cmd":"crudget","argcnt":1},"dstype":"dsswres","criteria":[]}'

$response.data | Select-Object -Property @{Name='Name';Expression={$_.metadata.template_name}}, @{Name='ID';Expression={$_._id.Substring(0,12)}}
```

```bash
# Bash
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{"method":{"cmd":"crudget","argcnt":1},"dstype":"dsswres","criteria":[]}' | \
jq -r '.data[] | "\(.metadata.template_name) (ID: \(._id[0:12])...)"'
```

**Expected Output:**
```
WebApp-MicroservicesApplication (ID: 5581ad1dd856...)
DeploymentPlan-WebApp-Release-v1.2.3 (ID: efb4322cf1cd...)
```

---

## 🔍 Query Guide: From Simple to Complex

### Query Structure

**All OptimusDB queries use this format:**

```json
{
"method": {
"cmd": "crudget",        // Command: crudget (read)
"argcnt": 1              // Argument count
},
"dstype": "dsswres",       // Datastore type
"criteria": [              // Query filters
{"field": "value"}
]
}
```

**Response Format:**
```json
{
"status": 200,
"data": [
{
"_id": "template_id_here",
"metadata": {...},
"tosca_definitions_version": "tosca_simple_yaml_1_3",
"topology_template": {...},
"workflows": {...}
}
]
}
```

---

## 🟢 Level 1: Simple Queries

### Query 1.1: List All Templates

**Use Case:** Get inventory of all stored TOSCA templates.

**PowerShell:**
```powershell
$body = @{
method = @{
cmd = "crudget"
argcnt = 1
}
dstype = "dsswres"
criteria = @()  # Empty = get all
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

# Display results
$response.data | ForEach-Object {
[PSCustomObject]@{
Name = $_.metadata.template_name
Version = $_.metadata.template_version
Author = $_.metadata.template_author
ID = $_._id.Substring(0, 12)
}
} | Format-Table
```

**Bash:**
```bash
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": []
}' | jq -r '.data[] |
"Name: \(.metadata.template_name)\n" +
"Version: \(.metadata.template_version)\n" +
"Author: \(.metadata.template_author)\n" +
"ID: \(._id[0:12])...\n"'
```

**Sample Output:**
```
Name: WebApp-MicroservicesApplication
Version: 1.0.0
Author: OptimusDB Team
ID: 5581ad1dd856...

Name: DeploymentPlan-WebApp-Release-v1.2.3
Version: 1.2.3
Author: OptimusDB Team
ID: efb4322cf1cd...
```

---

### Query 1.2: Find by Template Name

**Use Case:** Find specific template by exact name.

**PowerShell:**
```powershell
$body = @{
method = @{
cmd = "crudget"
argcnt = 1
}
dstype = "dsswres"
criteria = @(
@{
"metadata.template_name" = "WebApp-MicroservicesApplication"
}
)
} | ConvertTo-Json -Depth 10

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

$response.data[0].metadata
```

**Bash:**
```bash
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": [
{"metadata.template_name": "WebApp-MicroservicesApplication"}
]
}' | jq '.data[0].metadata'
```

---

### Query 1.3: Find by TOSCA Version

**Use Case:** List all templates using specific TOSCA specification.

**PowerShell:**
```powershell
$body = @{
method = @{
cmd = "crudget"
argcnt = 1
}
dstype = "dsswres"
criteria = @(
@{tosca_definitions_version = "tosca_simple_yaml_1_3"}
)
} | ConvertTo-Json -Depth 10

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

Write-Host "Found $($response.data.Count) templates using TOSCA 1.3"
$response.data | Select-Object -ExpandProperty metadata | Select-Object template_name, template_version
```

**Bash:**
```bash
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswres",
"criteria": [
{"tosca_definitions_version": "tosca_simple_yaml_1_3"}
]
}' | jq '.data | length as $count |
"Found \($count) templates using TOSCA 1.3\n" +
(.[] | "\(.metadata.template_name) v\(.metadata.template_version)")'
```

---

### Query 1.4: List All Datastores

**Use Case:** Discover which datastores contain templates.

**PowerShell:**
```powershell
$datastores = @("dsswres", "dssrres", "dssires", "dssares")

foreach ($ds in $datastores) {
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = $ds
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

Write-Host "$ds : $($response.data.Count) templates"
}
```

**Bash:**
```bash
for ds in dsswres dssrres dssires dssares; do
count=$(curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d "{\"method\":{\"cmd\":\"crudget\",\"argcnt\":1},\"dstype\":\"$ds\",\"criteria\":[]}" | \
jq '.data | length')
echo "$ds: $count templates"
done
```

**Sample Output:**
```
dsswres: 2 templates (Workflows/Applications)
dssrres: 1 template (Resource Requirements)
dssires: 1 template (Infrastructure)
dssares: 1 template (Application Requirements)
```

---

## 🟡 Level 2: Intermediate Queries

### Query 2.1: Find Docker/Container Nodes

**Use Case:** Discover all containerized applications.

**Note:** Client-side filtering required (OrbitDB doesn't support wildcard matching).

**PowerShell:**
```powershell
# Step 1: Get all templates
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

# Step 2: Client-side filter for Container types
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

# Display results
Write-Host "Found $($containerTemplates.Count) templates with containers:" -ForegroundColor Green
$containerTemplates | ForEach-Object {
$containerCount = ($_.topology_template.node_templates.PSObject.Properties |
Where-Object { $_.Value.type -like "*Container*" }).Count

Write-Host "  • $($_.metadata.template_name): $containerCount containers"
}
```

**Bash:**
```bash
# Get all and filter with jq
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{"method":{"cmd":"crudget","argcnt":1},"dstype":"dsswres","criteria":[]}' | \
jq -r '
[.data[] |
select(.topology_template.node_templates != null) |
select(
.topology_template.node_templates |
to_entries[] |
.value.type |
contains("Container")
)
] as $containers |
"Found \($containers | length) templates with containers:\n" +
($containers[] |
"  • \(.metadata.template_name): \(
[.topology_template.node_templates |
to_entries[] |
select(.value.type | contains("Container"))
] | length
) containers"
)'
```

**Sample Output:**
```
Found 2 templates with containers:
• WebApp-MicroservicesApplication: 4 containers
• DeploymentPlan-WebApp-Release-v1.2.3: 1 container
```

---

### Query 2.2: Find Kubernetes Clusters

**Use Case:** Identify Kubernetes-based infrastructure.

**PowerShell:**
```powershell
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dssires"  # Infrastructure datastore
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

# Filter for Kubernetes
$k8sTemplates = $response.data | Where-Object {
$template = $_
$hasK8s = $false

if ($template.topology_template.node_templates) {
foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
if ($node.Value.type -like "*Kubernetes*") {
$hasK8s = $true
break
}
}
}
$hasK8s
}

# Display Kubernetes clusters
$k8sTemplates | ForEach-Object {
Write-Host "`nTemplate: $($_.metadata.template_name)" -ForegroundColor Cyan

$_.topology_template.node_templates.PSObject.Properties |
Where-Object { $_.Value.type -like "*Kubernetes*" } |
ForEach-Object {
Write-Host "  Cluster: $($_.Name)"
Write-Host "    Provider: $($_.Value.properties.provider)"
Write-Host "    Version: $($_.Value.properties.version)"
Write-Host "    Nodes: $($_.Value.properties.node_count)"
Write-Host "    Instance: $($_.Value.properties.node_instance_type)"
}
}
```

**Bash:**
```bash
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{"method":{"cmd":"crudget","argcnt":1},"dstype":"dssires","criteria":[]}' | \
jq -r '.data[] |
select(.topology_template.node_templates != null) |
"Template: \(.metadata.template_name)\n" +
(
[.topology_template.node_templates | to_entries[] |
select(.value.type | contains("Kubernetes"))] |
.[] |
"  Cluster: \(.key)\n" +
"    Provider: \(.value.properties.provider)\n" +
"    Version: \(.value.properties.version)\n" +
"    Nodes: \(.value.properties.node_count)\n" +
"    Instance: \(.value.properties.node_instance_type)\n"
)'
```

**Sample Output:**
```
Template: HybridInfrastructure-SwarmDeployment

Cluster: aws_kubernetes_cluster
Provider: AWS EKS
Version: 1.28
Nodes: 5
Instance: t3.xlarge

Cluster: azure_kubernetes_cluster
Provider: Azure AKS
Version: 1.28
Nodes: 3
Instance: Standard_D4s_v3

Cluster: on_premise_kubernetes
Provider: On-Premise
Version: 1.28
Nodes: 8
Instance: (bare metal)
```

---

### Query 2.3: Find PostgreSQL Databases

**Use Case:** Locate all PostgreSQL database instances.

**PowerShell:**
```powershell
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

# Find PostgreSQL containers
$postgresNodes = $response.data | ForEach-Object {
$template = $_

if ($template.topology_template.node_templates) {
$template.topology_template.node_templates.PSObject.Properties |
Where-Object { $_.Value.properties.image -like "*postgres*" } |
ForEach-Object {
[PSCustomObject]@{
Template = $template.metadata.template_name
NodeName = $_.Name
Image = $_.Value.properties.image
Database = $_.Value.properties.environment |
Where-Object { $_ -like "POSTGRES_DB=*" } |
ForEach-Object { $_.Split('=')[1] }
}
}
}
}

$postgresNodes | Format-Table
```

**Bash:**
```bash
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{"method":{"cmd":"crudget","argcnt":1},"dstype":"dsswres","criteria":[]}' | \
jq -r '.data[] |
select(.topology_template.node_templates != null) |
.topology_template.node_templates |
to_entries[] |
select(.value.properties.image // "" | contains("postgres")) |
"Template: \(.value.properties.image)\n" +
"Node: \(.key)\n" +
"Image: \(.value.properties.image)\n" +
"Database: \([.value.properties.environment[]? |
select(contains("POSTGRES_DB="))] | .[0] |
split("=")[1] // "N/A")\n"'
```

---

### Query 2.4: Detect GPU Requirements

**Use Case:** Find templates requiring GPU hardware.

**PowerShell:**
```powershell
$datastores = @("dssrres", "dssares")  # Resource-related datastores

foreach ($ds in $datastores) {
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = $ds
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

# Filter for GPU
$gpuTemplates = $response.data | Where-Object {
$template = $_
$hasGPU = $false

if ($template.topology_template.node_templates) {
foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
if ($node.Value.type -like "*GPU*" -or
$node.Value.properties.gpu_model -or
$node.Value.properties.gpu_count_preferred -or
$node.Value.capabilities.gpu_acceleration) {
$hasGPU = $true
break
}
}
}
$hasGPU
}

# Display GPU details
$gpuTemplates | ForEach-Object {
Write-Host "`nTemplate: $($_.metadata.template_name)" -ForegroundColor Magenta
Write-Host "Datastore: $ds" -ForegroundColor Gray

$_.topology_template.node_templates.PSObject.Properties |
Where-Object {
$_.Value.type -like "*GPU*" -or
$_.Value.properties.gpu_model -or
$_.Value.capabilities.gpu_acceleration
} |
ForEach-Object {
$gpu = $_.Value

# Check different property locations
if ($gpu.properties.gpu_model) {
Write-Host "  GPU Model: $($gpu.properties.gpu_model)"
Write-Host "  GPU Count: $($gpu.properties.gpu_count)"
Write-Host "  GPU Memory: $($gpu.properties.gpu_memory)"
}

if ($gpu.capabilities.gpu_acceleration) {
Write-Host "  GPU Model: $($gpu.capabilities.gpu_acceleration.properties.gpu_model)"
Write-Host "  GPU Count: $($gpu.capabilities.gpu_acceleration.properties.gpu_count_preferred)"
Write-Host "  Total GPU Memory: $($gpu.capabilities.gpu_acceleration.properties.total_gpu_memory)"
}
}
}
}
```

**Bash:**
```bash
for ds in dssrres dssares; do
echo "=== Datastore: $ds ==="

curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d "{\"method\":{\"cmd\":\"crudget\",\"argcnt\":1},\"dstype\":\"$ds\",\"criteria\":[]}" | \
jq -r '.data[] |
select(.topology_template.node_templates != null) |
select(
.topology_template.node_templates |
to_entries[] |
(.value.type | contains("GPU")) or
(.value.properties.gpu_model != null) or
(.value.capabilities.gpu_acceleration != null)
) |
"\nTemplate: \(.metadata.template_name)\n" +
(
[.topology_template.node_templates | to_entries[] |
select(
(.value.type | contains("GPU")) or
(.value.properties.gpu_model != null) or
(.value.capabilities.gpu_acceleration != null)
)] |
.[] |
if .value.properties.gpu_model then
"  GPU Model: \(.value.properties.gpu_model)\n" +
"  GPU Count: \(.value.properties.gpu_count)\n" +
"  GPU Memory: \(.value.properties.gpu_memory)"
elif .value.capabilities.gpu_acceleration then
"  GPU Model: \(.value.capabilities.gpu_acceleration.properties.gpu_model)\n" +
"  GPU Count: \(.value.capabilities.gpu_acceleration.properties.gpu_count_preferred)\n" +
"  Total GPU Memory: \(.value.capabilities.gpu_acceleration.properties.total_gpu_memory)"
else
"  GPU: Found but details not standard"
end
)'
done
```

**Sample Output:**
```
=== Datastore: dssrres ===

Template: EdgeCluster-CapacityProfile
GPU Model: NVIDIA A100
GPU Count: 4
GPU Memory: 40 GB

=== Datastore: dssares ===

Template: ApplicationRequirements-MLTrainingWorkload
GPU Model: NVIDIA A100
GPU Count: 8
Total GPU Memory: 640 GB
```

---

### Query 2.5: Search Exposed Ports

**Use Case:** Security audit - find all exposed network ports.

**PowerShell:**
```powershell
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

# Extract all ports
$portFindings = $response.data | ForEach-Object {
$template = $_

if ($template.topology_template.node_templates) {
$template.topology_template.node_templates.PSObject.Properties |
Where-Object { $_.Value.properties.ports } |
ForEach-Object {
$node = $_

$node.Value.properties.ports | ForEach-Object {
[PSCustomObject]@{
Template = $template.metadata.template_name
NodeName = $node.Name
Port = $_
Protocol = if ($_ -like "*443*") { "HTTPS" }
elseif ($_ -like "*80*") { "HTTP" }
elseif ($_ -like "*5432*") { "PostgreSQL" }
elseif ($_ -like "*6379*") { "Redis" }
else { "Other" }
}
}
}
}
}

Write-Host "`nPort Exposure Summary:" -ForegroundColor Cyan
$portFindings | Group-Object Protocol | ForEach-Object {
Write-Host "  $($_.Name): $($_.Count) ports"
}

Write-Host "`nDetailed Port List:" -ForegroundColor Cyan
$portFindings | Format-Table -AutoSize
```

**Bash:**
```bash
echo "=== Port Exposure Analysis ==="

curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{"method":{"cmd":"crudget","argcnt":1},"dstype":"dsswres","criteria":[]}' | \
jq -r '
# Extract all ports
[.data[] |
select(.topology_template.node_templates != null) |
.topology_template.node_templates |
to_entries[] |
select(.value.properties.ports != null) |
{
template: (.value.properties.image // .key),
node: .key,
ports: .value.properties.ports
} |
.ports[] as $port |
{
template: .template,
node: .node,
port: $port,
protocol: (
if ($port | contains("443")) then "HTTPS"
elif ($port | contains("80")) then "HTTP"
elif ($port | contains("5432")) then "PostgreSQL"
elif ($port | contains("6379")) then "Redis"
elif ($port | contains("5000")) then "App-HTTP"
else "Other"
end
)
}
] |

# Group by protocol
group_by(.protocol) |
"Port Exposure Summary:\n" +
(map("  \(.[0].protocol): \(length) ports") | join("\n")) +
"\n\nDetailed Port List:\n" +
(flatten | .[] | "  \(.port) - \(.protocol) - Node: \(.node)")'
```

**Sample Output:**
```
Port Exposure Summary:
HTTPS: 2 ports
HTTP: 1 port
PostgreSQL: 1 port
Redis: 1 port
App-HTTP: 1 port

Detailed Port List:
80:80 - HTTP - Node: frontend_container
443:443 - HTTPS - Node: frontend_container
5000:5000 - App-HTTP - Node: backend_container
5432:5432 - PostgreSQL - Node: database_container
6379:6379 - Redis - Node: cache_container
443:443 - HTTPS - Node: web_application
```

---

## 🟠 Level 3: Advanced Queries

### Query 3.1: Find Environment Variables

**Use Case:** Configuration management - extract all environment variables.

**PowerShell:**
```powershell
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

# Extract environment variables
$envVars = $response.data | ForEach-Object {
$template = $_

if ($template.topology_template.node_templates) {
$template.topology_template.node_templates.PSObject.Properties |
Where-Object { $_.Value.properties.environment } |
ForEach-Object {
$node = $_

$node.Value.properties.environment | ForEach-Object {
if ($_ -match '(.+?)=(.+)') {
[PSCustomObject]@{
Template = $template.metadata.template_name
Node = $node.Name
Variable = $matches[1]
Value = $matches[2]
}
}
}
}
}
}

Write-Host "`nEnvironment Variables Found: $($envVars.Count)" -ForegroundColor Green
Write-Host "`nBy Category:" -ForegroundColor Cyan

# Group by type
$envVars | Group-Object Variable | ForEach-Object {
$varName = $_.Name
$count = $_.Count

$category = if ($varName -like "*URL*" -or $varName -like "*HOST*") { "Connectivity" }
elseif ($varName -like "*PASSWORD*" -or $varName -like "*USER*" -or $varName -like "*DB*") { "Database" }
elseif ($varName -like "*LOG*" -or $varName -like "*ENVIRONMENT*") { "Configuration" }
else { "Other" }

[PSCustomObject]@{
Variable = $varName
Count = $count
Category = $category
}
} | Group-Object Category | ForEach-Object {
Write-Host "`n  $($_.Name):"
$_.Group | ForEach-Object {
Write-Host "    - $($_.Variable) (used $($_.Count) times)"
}
}
```

**Bash:**
```bash
echo "=== Environment Variables Analysis ==="

curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{"method":{"cmd":"crudget","argcnt":1},"dstype":"dsswres","criteria":[]}' | \
jq -r '
[.data[] |
select(.topology_template.node_templates != null) |
.metadata.template_name as $template |
.topology_template.node_templates |
to_entries[] |
select(.value.properties.environment != null) |
.value.properties.environment[] |
{
template: $template,
node: .key,
var: (. | split("=")[0]),
value: (. | split("=")[1])
}
] |

"Environment Variables Found: \(length)\n\n" +
"By Variable:\n" +
(group_by(.var) |
map("  \(.[0].var): used \(length) times") |
join("\n")) +
"\n\nFull List:\n" +
(map("  \(.var)=\(.value) (Node: \(.node), Template: \(.template))") | join("\n"))'
```

**Sample Output:**
```
Environment Variables Found: 7

By Variable:
BACKEND_URL: used 1 times
DATABASE_URL: used 1 times
ENVIRONMENT: used 1 times
LOG_LEVEL: used 1 times
POSTGRES_DB: used 1 times
POSTGRES_PASSWORD: used 1 times
POSTGRES_USER: used 1 times
REDIS_URL: used 1 times

Full List:
BACKEND_URL=http://backend:5000 (Node: frontend_container)
DATABASE_URL=postgresql://postgres:5432/webapp (Node: backend_container)
REDIS_URL=redis://cache:6379 (Node: backend_container)
POSTGRES_DB=webapp (Node: database_container)
POSTGRES_USER=admin (Node: database_container)
POSTGRES_PASSWORD=secret123 (Node: database_container)
ENVIRONMENT=production (Node: web_application)
LOG_LEVEL=info (Node: web_application)
```

---

### Query 3.2: Resource Threshold Analysis

**Use Case:** Capacity planning - find all resource requirements and thresholds.

**PowerShell:**
```powershell
$datastores = @("dssrres", "dssares", "dssires")
$resources = @()

foreach ($ds in $datastores) {
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = $ds
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

# Extract resource specs
$response.data | ForEach-Object {
$template = $_

if ($template.topology_template.node_templates) {
$template.topology_template.node_templates.PSObject.Properties | ForEach-Object {
$node = $_
$props = $node.Value.properties

if ($props.num_cpus -or $props.mem_size -or $props.disk_size) {
$resources += [PSCustomObject]@{
Template = $template.metadata.template_name
Node = $node.Name
CPUs = $props.num_cpus
Memory = $props.mem_size -replace ' GB', '' -replace ' ', ''
Disk = $props.disk_size -replace ' TB', '' -replace ' GB', '' -replace ' ', ''
Datastore = $ds
}
}
}
}
}
}

# Analysis
Write-Host "`nResource Analysis:" -ForegroundColor Cyan

# Memory distribution
Write-Host "`nMemory Requirements:" -ForegroundColor Yellow
$resources | Where-Object { $_.Memory } |
ForEach-Object {
[int]($_.Memory -replace '[^0-9]','')
} |
Measure-Object -Min -Max -Average |
ForEach-Object {
Write-Host "  Min: $($_.Minimum) GB"
Write-Host "  Max: $($_.Maximum) GB"
Write-Host "  Avg: $([math]::Round($_.Average, 2)) GB"
}

# High-resource nodes
Write-Host "`nHigh-Resource Nodes (>64GB RAM or >16 CPUs):" -ForegroundColor Yellow
$resources | Where-Object {
([int]($_.Memory -replace '[^0-9]','') -gt 64) -or
($_.CPUs -gt 16)
} | Format-Table Template, Node, CPUs, Memory -AutoSize
```

**Bash:**
```bash
echo "=== Resource Threshold Analysis ==="

for ds in dssrres dssares dssires; do
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d "{\"method\":{\"cmd\":\"crudget\",\"argcnt\":1},\"dstype\":\"$ds\",\"criteria\":[]}" | \
jq -r --arg ds "$ds" '
.data[] |
select(.topology_template.node_templates != null) |
.metadata.template_name as $template |
.topology_template.node_templates |
to_entries[] |
select(
(.value.properties.num_cpus != null) or
(.value.properties.mem_size != null)
) |
{
template: $template,
node: .key,
cpus: (.value.properties.num_cpus // 0),
memory: (.value.properties.mem_size // .value.properties.memory_preferred // "0"),
datastore: $ds
} |
select(.cpus > 0 or .memory != "0") |
"\(.template) - \(.node):\n" +
"  CPUs: \(.cpus)\n" +
"  Memory: \(.memory)\n" +
"  Datastore: \(.datastore)\n"'
done
```

---

### Query 3.3: Multi-Criteria Filtering

**Use Case:** Complex search - find templates matching multiple conditions.

**Example:** Find templates with:
- Containers (Docker/Podman)
- Port 443 exposed
- Auto-scaling policy

**PowerShell:**
```powershell
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

# Multi-criteria filter
$matches = $response.data | Where-Object {
$template = $_

# Criterion 1: Has containers
$hasContainers = $false
if ($template.topology_template.node_templates) {
foreach ($node in $template.topology_template.node_templates.PSObject.Properties) {
if ($node.Value.type -like "*Container*") {
$hasContainers = $true
break
}
}
}

# Criterion 2: Exposes port 443
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

# Criterion 3: Has auto-scaling policy
$hasAutoScaling = $false
if ($template.topology_template.policies) {
foreach ($policy in $template.topology_template.policies) {
if ($policy.PSObject.Properties.Name -like "*scaling*" -or
$policy.PSObject.Properties.Value.type -like "*Scaling*") {
$hasAutoScaling = $true
break
}
}
}

# All three criteria must match
$hasContainers -and $hasPort443 -and $hasAutoScaling
}

Write-Host "Multi-Criteria Search Results:" -ForegroundColor Cyan
Write-Host "Criteria: Containers + Port 443 + Auto-scaling" -ForegroundColor Gray
Write-Host "Found: $($matches.Count) templates`n" -ForegroundColor Green

$matches | ForEach-Object {
Write-Host "Template: $($_.metadata.template_name)" -ForegroundColor Yellow
Write-Host "  Version: $($_.metadata.template_version)"
Write-Host "  Containers: $(($_.topology_template.node_templates.PSObject.Properties | Where-Object { $_.Value.type -like '*Container*' }).Count)"
Write-Host "  Policies: $(($_.topology_template.policies | Measure-Object).Count)"
}
```

**Bash:**
```bash
echo "=== Multi-Criteria Search ==="
echo "Criteria: Containers + Port 443 + Auto-scaling"
echo ""

curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{"method":{"cmd":"crudget","argcnt":1},"dstype":"dsswres","criteria":[]}' | \
jq -r '[.data[] |
select(
# Has containers
(.topology_template.node_templates |
to_entries[]? |
.value.type |
contains("Container")) and

# Has port 443
(.topology_template.node_templates |
to_entries[]? |
.value.properties.ports[]? |
contains("443")) and

# Has scaling policy
(.topology_template.policies[]? |
to_entries[]? |
(.key | contains("scaling")) or
(.value.type | contains("Scaling")))
)] |

"Found: \(length) templates\n\n" +
(.[] |
"Template: \(.metadata.template_name)\n" +
"  Version: \(.metadata.template_version)\n" +
"  Containers: \([.topology_template.node_templates | to_entries[] | select(.value.type | contains("Container"))] | length)\n" +
"  Policies: \(.topology_template.policies | length)\n"
)'
```

**Sample Output:**
```
Multi-Criteria Search Results:
Criteria: Containers + Port 443 + Auto-scaling
Found: 2 templates

Template: WebApp-MicroservicesApplication
Version: 1.0.0
Containers: 4
Policies: 3

Template: DeploymentPlan-WebApp-Release-v1.2.3
Version: 1.2.3
Containers: 1
Policies: 3
```

---

## 🔴 Level 4: Complex Nested Queries

### Query 4.1: Network Configuration Analysis

**Use Case:** Network topology discovery - map all network connections.

**PowerShell:**
```powershell
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

# Build network map
Write-Host "Network Topology Map:" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Cyan

$response.data | ForEach-Object {
$template = $_

Write-Host "`nTemplate: $($template.metadata.template_name)" -ForegroundColor Yellow

if ($template.topology_template.node_templates) {
# Find network connections in environment variables
$connections = @{}

$template.topology_template.node_templates.PSObject.Properties | ForEach-Object {
$node = $_
$nodeName = $node.Name

# Extract URLs from environment
if ($node.Value.properties.environment) {
$node.Value.properties.environment | ForEach-Object {
if ($_ -match '(.+?)_URL=(.+)') {
$service = $matches[1]
$url = $matches[2]

# Parse URL to find target service
if ($url -match '://([^:/@]+)') {
$target = $matches[1]

if (-not $connections[$nodeName]) {
$connections[$nodeName] = @()
}
$connections[$nodeName] += @{
Service = $service
Target = $target
URL = $url
}
}
}
}
}
}

# Display network graph
foreach ($source in $connections.Keys) {
Write-Host "  [$source]" -ForegroundColor Green
foreach ($conn in $connections[$source]) {
Write-Host "    → $($conn.Service): $($conn.Target)" -ForegroundColor Gray
Write-Host "      URL: $($conn.URL)" -ForegroundColor DarkGray
}
}
}
}
```

**Bash:**
```bash
echo "=== Network Topology Map ==="
echo "============================================================"

curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{"method":{"cmd":"crudget","argcnt":1},"dstype":"dsswres","criteria":[]}' | \
jq -r '.data[] |
"\nTemplate: \(.metadata.template_name)\n" +
(
[.topology_template.node_templates | to_entries[] |
select(.value.properties.environment != null) |
{
node: .key,
connections: [.value.properties.environment[] |
select(contains("_URL=")) |
{
service: (. | split("=")[0]),
url: (. | split("=")[1]),
target: (. | split("=")[1] | split("://")[1] | split(":")[0])
}]
} |
select(.connections | length > 0)
] |
if length > 0 then
.[] |
"  [\(.node)]\n" +
(.connections[] |
"    → \(.service): \(.target)\n" +
"      URL: \(.url)")
else
"  No network connections defined"
end
)'
```

**Sample Output:**
```
Template: WebApp-MicroservicesApplication

[frontend_container]
→ BACKEND: backend
URL: http://backend:5000

[backend_container]
→ DATABASE: postgres
URL: postgresql://postgres:5432/webapp
→ REDIS: cache
URL: redis://cache:6379
```

---

### Query 4.2: Groups and Cluster Configuration

**Use Case:** Discover logical groupings and cluster configurations.

**PowerShell:**
```powershell
$datastores = @("dssrres", "dssires")

foreach ($ds in $datastores) {
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = $ds
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

$response.data | ForEach-Object {
$template = $_

if ($template.topology_template.groups) {
Write-Host "`nTemplate: $($template.metadata.template_name)" -ForegroundColor Cyan
Write-Host "Datastore: $ds" -ForegroundColor Gray
Write-Host "`nGroups:" -ForegroundColor Yellow

$template.topology_template.groups.PSObject.Properties | ForEach-Object {
$group = $_
Write-Host "  Group: $($group.Name)" -ForegroundColor Green
Write-Host "    Type: $($group.Value.type)"
Write-Host "    Members: $($group.Value.members -join ', ')"

if ($group.Value.metadata) {
Write-Host "    Metadata:" -ForegroundColor Magenta
$group.Value.metadata.PSObject.Properties | ForEach-Object {
Write-Host "      $($_.Name): $($_.Value)"
}
}
}
}
}
}
```

**Bash:**
```bash
echo "=== Groups and Cluster Configuration ==="

for ds in dssrres dssires; do
echo ""
echo "Datastore: $ds"
echo "------------------------"

curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d "{\"method\":{\"cmd\":\"crudget\",\"argcnt\":1},\"dstype\":\"$ds\",\"criteria\":[]}" | \
jq -r '.data[] |
select(.topology_template.groups != null) |
"\nTemplate: \(.metadata.template_name)\n" +
"Groups:\n" +
(
.topology_template.groups | to_entries[] |
"  Group: \(.key)\n" +
"    Type: \(.value.type)\n" +
"    Members: \(.value.members | join(", "))\n" +
(if .value.metadata then
"    Metadata:\n" +
(.value.metadata | to_entries[] | "      \(.key): \(.value)\n")
else
""
end)
)'
done
```

**Sample Output:**
```
Datastore: dssrres

Template: EdgeCluster-CapacityProfile

Groups:
Group: edge_cluster
Type: tosca.groups.Root
Members: edge_compute_node, gpu_accelerator
Metadata:
location: Edge datacenter
tier: Premium

Datastore: dssires

Template: HybridInfrastructure-SwarmDeployment

Groups:
Group: production_cluster
Type: tosca.groups.Root
Members: aws_kubernetes_cluster, azure_kubernetes_cluster
Metadata:
environment: production

Group: development_cluster
Type: tosca.groups.Root
Members: on_premise_kubernetes
Metadata:
environment: development
```

---

### Query 4.3: Deployment Workflows and Steps

**Use Case:** CI/CD pipeline discovery - map deployment automation.

**PowerShell:**
```powershell
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

# Find templates with workflows
$workflowTemplates = $response.data | Where-Object {
$null -ne $_.workflows
}

Write-Host "Workflow Analysis:" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Cyan

$workflowTemplates | ForEach-Object {
$template = $_

Write-Host "`nTemplate: $($template.metadata.template_name)" -ForegroundColor Yellow
Write-Host "Version: $($template.metadata.template_version)" -ForegroundColor Gray

if ($template.workflows) {
Write-Host "`nWorkflows: $($template.workflows.PSObject.Properties.Count)" -ForegroundColor Green

$template.workflows.PSObject.Properties | ForEach-Object {
$workflow = $_
Write-Host "`n  Workflow: $($workflow.Name)" -ForegroundColor Magenta
Write-Host "  Description: $($workflow.Value.description)" -ForegroundColor Gray

if ($workflow.Value.steps) {
$stepCount = ($workflow.Value.steps | Measure-Object).Count
Write-Host "  Steps: $stepCount" -ForegroundColor Cyan

$workflow.Value.steps | ForEach-Object {
$step = $_
$stepName = $step.PSObject.Properties.Name
$stepValue = $step.PSObject.Properties.Value

Write-Host "    → Step: $stepName"

if ($stepValue.activities) {
Write-Host "      Activities: $($stepValue.activities -join ', ')" -ForegroundColor DarkGray
}

if ($stepValue.on_success) {
Write-Host "      On Success → $($stepValue.on_success)" -ForegroundColor Green
}

if ($stepValue.on_failure) {
Write-Host "      On Failure → $($stepValue.on_failure)" -ForegroundColor Red
}
}
}
}
}
}
```

**Bash:**
```bash
echo "=== Workflow Analysis ==="
echo "============================================================"

curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{"method":{"cmd":"crudget","argcnt":1},"dstype":"dsswres","criteria":[]}' | \
jq -r '.data[] |
select(.workflows != null) |
"\nTemplate: \(.metadata.template_name)\n" +
"Version: \(.metadata.template_version)\n" +
"\nWorkflows: \(.workflows | length)\n" +
(
.workflows | to_entries[] |
"\n  Workflow: \(.key)\n" +
"  Description: \(.value.description)\n" +
"  Steps: \(.value.steps | length)\n" +
(
.value.steps[] | to_entries[] |
"    → Step: \(.key)\n" +
"      Activities: \(.value.activities | join(", "))\n" +
(if .value.on_success then
"      On Success → \(.value.on_success)\n"
else
""
end) +
(if .value.on_failure then
"      On Failure → \(.value.on_failure)\n"
else
""
end)
)
)'
```

**Sample Output:**
```
Template: DeploymentPlan-WebApp-Release-v1.2.3
Version: 1.2.3

Workflows: 2

Workflow: deploy_workflow
Description: Multi-stage deployment workflow
Steps: 9
→ Step: build_application
Activities: call_operation: build.compile
On Success → run_tests
→ Step: run_tests
Activities: call_operation: test.unit_tests, call_operation: test.integration_tests
On Success → create_container
→ Step: create_container
Activities: call_operation: container.build, call_operation: container.push
On Success → deploy_staging
...

Workflow: rollback_workflow
Description: Automated rollback procedure
Steps: 7
→ Step: stop_new_deployment
Activities: call_operation: deploy.stop
On Success → restore_previous_version
...
```

---

## 📊 Level 5: Analytical Queries

### Query 5.1: Resource Statistics Across All Templates

**Use Case:** Infrastructure capacity overview across entire knowledge base.

**PowerShell:**
```powershell
$datastores = @("dsswres", "dssrres", "dssires", "dssares")
$allResources = @()

foreach ($ds in $datastores) {
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = $ds
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

$response.data | ForEach-Object {
$template = $_

if ($template.topology_template.node_templates) {
$template.topology_template.node_templates.PSObject.Properties | ForEach-Object {
$node = $_
$props = $node.Value.properties

$allResources += [PSCustomObject]@{
Template = $template.metadata.template_name
Datastore = $ds
Node = $node.Name
Type = $node.Value.type
CPUs = $props.num_cpus
Memory = if ($props.mem_size) { [int]($props.mem_size -replace '[^0-9]', '') }
elseif ($props.memory_preferred) { $props.memory_preferred }
else { 0 }
Disk = if ($props.disk_size) { $props.disk_size } else { "N/A" }
GPU = if ($props.gpu_model -or $props.gpu_count) { $true } else { $false }
}
}
}
}
}

# Aggregate statistics
Write-Host "`n=== Resource Statistics Across All Templates ===" -ForegroundColor Cyan
Write-Host ""

Write-Host "Total Nodes: $($allResources.Count)" -ForegroundColor Green

Write-Host "`nCPU Statistics:" -ForegroundColor Yellow
$cpuStats = $allResources | Where-Object { $_.CPUs -gt 0 } | Select-Object -ExpandProperty CPUs | Measure-Object -Sum -Average -Maximum -Minimum
Write-Host "  Total CPUs: $($cpuStats.Sum)"
Write-Host "  Average: $([math]::Round($cpuStats.Average, 2))"
Write-Host "  Min: $($cpuStats.Minimum)"
Write-Host "  Max: $($cpuStats.Maximum)"

Write-Host "`nMemory Statistics:" -ForegroundColor Yellow
$memStats = $allResources | Where-Object { $_.Memory -gt 0 } | Select-Object -ExpandProperty Memory | Measure-Object -Sum -Average -Maximum -Minimum
Write-Host "  Total Memory: $($memStats.Sum) GB"
Write-Host "  Average: $([math]::Round($memStats.Average, 2)) GB"
Write-Host "  Min: $($memStats.Minimum) GB"
Write-Host "  Max: $($memStats.Maximum) GB"

Write-Host "`nGPU-Enabled Nodes:" -ForegroundColor Yellow
$gpuCount = ($allResources | Where-Object { $_.GPU }).Count
Write-Host "  Count: $gpuCount"
Write-Host "  Percentage: $([math]::Round(($gpuCount / $allResources.Count) * 100, 2))%"

Write-Host "`nBy Datastore:" -ForegroundColor Yellow
$allResources | Group-Object Datastore | ForEach-Object {
Write-Host "  $($_.Name): $($_.Count) nodes"
}

Write-Host "`nTop Resource Consumers:" -ForegroundColor Yellow
$allResources |
Where-Object { $_.Memory -gt 0 } |
Sort-Object Memory -Descending |
Select-Object -First 5 |
ForEach-Object {
Write-Host "  $($_.Template) / $($_.Node): $($_.CPUs) CPUs, $($_.Memory) GB RAM"
}
```

**Bash:**
```bash
echo "=== Resource Statistics Across All Templates ==="

# Collect from all datastores
for ds in dsswres dssrres dssires dssares; do
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d "{\"method\":{\"cmd\":\"crudget\",\"argcnt\":1},\"dstype\":\"$ds\",\"criteria\":[]}" | \
jq -r --arg ds "$ds" '.data[] |
select(.topology_template.node_templates != null) |
.topology_template.node_templates | to_entries[] |
{
template: .value.properties.image // .key,
datastore: $ds,
node: .key,
cpus: (.value.properties.num_cpus // 0),
memory: (
if .value.properties.mem_size then
(.value.properties.mem_size | gsub("[^0-9]"; "") | tonumber)
elif .value.properties.memory_preferred then
.value.properties.memory_preferred
else
0
end
),
gpu: (
if (.value.properties.gpu_model or .value.properties.gpu_count or .value.type | contains("GPU")) then
true
else
false
end
)
}'
done | jq -s '
"Total Nodes: \(length)\n\n" +

"CPU Statistics:\n" +
"  Total CPUs: \([.[] | .cpus] | add)\n" +
"  Average: \([.[] | .cpus] | add / length | floor)\n" +
"  Min: \([.[] | select(.cpus > 0) | .cpus] | min)\n" +
"  Max: \([.[] | .cpus] | max)\n\n" +

"Memory Statistics:\n" +
"  Total Memory: \([.[] | .memory] | add) GB\n" +
"  Average: \([.[] | select(.memory > 0) | .memory] | add / length | floor) GB\n" +
"  Min: \([.[] | select(.memory > 0) | .memory] | min) GB\n" +
"  Max: \([.[] | .memory] | max) GB\n\n" +

"GPU-Enabled Nodes:\n" +
"  Count: \([.[] | select(.gpu == true)] | length)\n" +
"  Percentage: \(([.[] | select(.gpu == true)] | length / length * 100 | floor))%\n\n" +

"By Datastore:\n" +
(group_by(.datastore) | .[] | "  \(.[0].datastore): \(length) nodes\n") +

"\nTop Resource Consumers:\n" +
([.[] | select(.memory > 0)] | sort_by(.memory) | reverse | .[0:5][] |
"  \(.template) / \(.node): \(.cpus) CPUs, \(.memory) GB RAM\n")
'
```

**Sample Output:**
```
=== Resource Statistics Across All Templates ===

Total Nodes: 15

CPU Statistics:
Total CPUs: 197
Average: 13.13
Min: 4
Max: 64

Memory Statistics:
Total Memory: 512 GB
Average: 34.13 GB
Min: 16 GB
Max: 128 GB

GPU-Enabled Nodes:
Count: 2
Percentage: 13%

By Datastore:
dsswres: 6 nodes
dssrres: 2 nodes
dssires: 3 nodes
dssares: 2 nodes

Top Resource Consumers:
ApplicationRequirements-MLTrainingWorkload / training_compute: 64 CPUs, 128 GB RAM
EdgeCluster-CapacityProfile / edge_compute_node: 32 CPUs, 128 GB RAM
HybridInfrastructure-SwarmDeployment / on_premise_kubernetes: 16 CPUs, 64 GB RAM
HybridInfrastructure-SwarmDeployment / azure_kubernetes_cluster: 4 CPUs, 16 GB RAM
HybridInfrastructure-SwarmDeployment / aws_kubernetes_cluster: 4 CPUs, 16 GB RAM
```

---

### Query 5.2: Port Distribution and Security Analysis

**Use Case:** Security audit - comprehensive port exposure analysis.

**PowerShell:**
```powershell
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

# Extract all ports with analysis
$portData = $response.data | ForEach-Object {
$template = $_

if ($template.topology_template.node_templates) {
$template.topology_template.node_templates.PSObject.Properties |
Where-Object { $_.Value.properties.ports } |
ForEach-Object {
$node = $_

$node.Value.properties.ports | ForEach-Object {
$portStr = $_

# Parse port number
if ($portStr -match ':(\d+)') {
$port = [int]$matches[1]
} elseif ($portStr -match '^(\d+)$') {
$port = [int]$matches[1]
} else {
$port = 0
}

# Classify port
$category = if ($port -eq 80) { "HTTP" }
elseif ($port -eq 443) { "HTTPS" }
elseif ($port -eq 5432) { "Database-PostgreSQL" }
elseif ($port -eq 6379) { "Cache-Redis" }
elseif ($port -ge 1 -and $port -le 1023) { "Well-Known" }
elseif ($port -ge 1024 -and $port -le 49151) { "Registered" }
else { "Dynamic" }

$security = if ($port -eq 443) { "Secure" }
elseif ($port -eq 80 -or $port -eq 5000) { "Insecure" }
else { "Review Required" }

[PSCustomObject]@{
Template = $template.metadata.template_name
Node = $node.Name
Port = $port
PortString = $portStr
Category = $category
Security = $security
}
}
}
}
}

Write-Host "`n=== Port Distribution and Security Analysis ===" -ForegroundColor Cyan
Write-Host ""

Write-Host "Total Exposed Ports: $($portData.Count)" -ForegroundColor Green

Write-Host "`nBy Category:" -ForegroundColor Yellow
$portData | Group-Object Category | Sort-Object Count -Descending | ForEach-Object {
Write-Host "  $($_.Name): $($_.Count) ports"
}

Write-Host "`nSecurity Assessment:" -ForegroundColor Yellow
$portData | Group-Object Security | ForEach-Object {
$color = if ($_.Name -eq "Secure") { "Green" }
elseif ($_.Name -eq "Insecure") { "Red" }
else { "Yellow" }
Write-Host "  $($_.Name): $($_.Count) ports" -ForegroundColor $color
}

Write-Host "`nPort Details:" -ForegroundColor Yellow
$portData | Sort-Object Port | Format-Table Template, Node, Port, Category, Security -AutoSize

Write-Host "`n⚠️  Security Recommendations:" -ForegroundColor Red
$insecure = $portData | Where-Object { $_.Security -eq "Insecure" }
if ($insecure.Count -gt 0) {
Write-Host "  • $($insecure.Count) insecure ports detected (HTTP)"
Write-Host "  • Recommend enabling SSL/TLS for:"
$insecure | ForEach-Object {
Write-Host "    - $($_.Node) (Port $($_.Port))"
}
}
```

**Bash:**
```bash
echo "=== Port Distribution and Security Analysis ==="

curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d '{"method":{"cmd":"crudget","argcnt":1},"dstype":"dsswres","criteria":[]}' | \
jq -r '
[.data[] |
select(.topology_template.node_templates != null) |
.metadata.template_name as $template |
.topology_template.node_templates | to_entries[] |
select(.value.properties.ports != null) |
.value.properties.ports[] as $port |
{
template: $template,
node: .key,
port_str: $port,
port: (
if ($port | contains(":")) then
($port | split(":") | .[-1] | tonumber)
else
($port | tonumber)
end
),
category: (
if ($port | contains("80:80") or $port == "80") then "HTTP"
elif ($port | contains("443")) then "HTTPS"
elif ($port | contains("5432")) then "Database-PostgreSQL"
elif ($port | contains("6379")) then "Cache-Redis"
elif ($port | contains("5000")) then "App-HTTP"
else "Other"
end
),
security: (
if ($port | contains("443")) then "Secure"
elif ($port | contains("80") or $port | contains("5000")) then "Insecure"
else "Review Required"
end
)
}
] |

"Total Exposed Ports: \(length)\n\n" +

"By Category:\n" +
(group_by(.category) | map("  \(.[0].category): \(length) ports") | join("\n")) +
"\n\n" +

"Security Assessment:\n" +
(group_by(.security) | map("  \(.[0].security): \(length) ports") | join("\n")) +
"\n\n" +

"Port Details:\n" +
(sort_by(.port)[] |
"  Port \(.port) (\(.category)) - \(.node) - Security: \(.security)\n") +

"\n⚠️  Security Recommendations:\n" +
(
[.[] | select(.security == "Insecure")] |
if length > 0 then
"  • \(length) insecure ports detected (HTTP)\n" +
"  • Recommend enabling SSL/TLS for:\n" +
(map("    - \(.node) (Port \(.port))\n") | join(""))
else
"  • No critical security issues detected\n"
end
)
'
```

**Sample Output:**
```
=== Port Distribution and Security Analysis ===

Total Exposed Ports: 6

By Category:
HTTPS: 2 ports
HTTP: 1 port
Database-PostgreSQL: 1 port
Cache-Redis: 1 port
App-HTTP: 1 port

Security Assessment:
Secure: 2 ports
Insecure: 2 ports
Review Required: 2 ports

Port Details:
Port 80 (HTTP) - frontend_container - Security: Insecure
Port 443 (HTTPS) - frontend_container - Security: Secure
Port 443 (HTTPS) - web_application - Security: Secure
Port 5000 (App-HTTP) - backend_container - Security: Insecure
Port 5432 (Database-PostgreSQL) - database_container - Security: Review Required
Port 6379 (Cache-Redis) - cache_container - Security: Review Required

⚠️  Security Recommendations:
• 2 insecure ports detected (HTTP)
• Recommend enabling SSL/TLS for:
- frontend_container (Port 80)
- backend_container (Port 5000)
```

---

### Query 5.3: Policy and Workflow Distribution

**Use Case:** Governance overview - analyze policies and automation across templates.

**PowerShell:**
```powershell
$datastores = @("dsswres", "dssrres", "dssires", "dssares")
$policyData = @()
$workflowData = @()

foreach ($ds in $datastores) {
$body = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = $ds
criteria = @()
} | ConvertTo-Json

$response = Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body

$response.data | ForEach-Object {
$template = $_

# Policies
if ($template.topology_template.policies) {
$template.topology_template.policies | ForEach-Object {
$policy = $_
$policyName = $policy.PSObject.Properties.Name
$policyValue = $policy.PSObject.Properties.Value

$policyData += [PSCustomObject]@{
Template = $template.metadata.template_name
Datastore = $ds
PolicyName = $policyName
PolicyType = $policyValue.type
}
}
}

# Workflows
if ($template.workflows) {
$template.workflows.PSObject.Properties | ForEach-Object {
$workflow = $_
$stepCount = ($workflow.Value.steps | Measure-Object).Count

$workflowData += [PSCustomObject]@{
Template = $template.metadata.template_name
Datastore = $ds
WorkflowName = $workflow.Name
Description = $workflow.Value.description
Steps = $stepCount
}
}
}
}
}

Write-Host "`n=== Policy and Workflow Distribution ===" -ForegroundColor Cyan
Write-Host ""

Write-Host "Policy Analysis:" -ForegroundColor Yellow
Write-Host "  Total Policies: $($policyData.Count)"

Write-Host "`n  By Type:" -ForegroundColor Magenta
$policyData | Group-Object PolicyType | Sort-Object Count -Descending | ForEach-Object {
Write-Host "    $($_.Name): $($_.Count)"
}

Write-Host "`n  By Datastore:" -ForegroundColor Magenta
$policyData | Group-Object Datastore | ForEach-Object {
Write-Host "    $($_.Name): $($_.Count) policies"
}

Write-Host "`n  Top Policy Users:" -ForegroundColor Magenta
$policyData | Group-Object Template | Sort-Object Count -Descending | Select-Object -First 3 | ForEach-Object {
Write-Host "    $($_.Name): $($_.Count) policies"
}

Write-Host "`nWorkflow Analysis:" -ForegroundColor Yellow
Write-Host "  Total Workflows: $($workflowData.Count)"
Write-Host "  Total Steps: $(($workflowData | Measure-Object -Property Steps -Sum).Sum)"

if ($workflowData.Count -gt 0) {
Write-Host "`n  Workflows Detail:" -ForegroundColor Magenta
$workflowData | ForEach-Object {
Write-Host "    $($_.WorkflowName) ($($_.Template)): $($_.Steps) steps"
}
}

Write-Host "`nGovernance Coverage:" -ForegroundColor Yellow
$templatesWithPolicies = ($policyData | Select-Object -ExpandProperty Template -Unique).Count
$templatesWithWorkflows = ($workflowData | Select-Object -ExpandProperty Template -Unique).Count
Write-Host "  Templates with Policies: $templatesWithPolicies / 5"
Write-Host "  Templates with Workflows: $templatesWithWorkflows / 5"
Write-Host "  Automation Coverage: $([math]::Round(($templatesWithWorkflows / 5) * 100, 2))%"
```

**Bash:**
```bash
echo "=== Policy and Workflow Distribution ==="

# Collect all data
for ds in dsswres dssrres dssires dssares; do
curl -s -X POST http://localhost:18001/swarmkb/command \
-H "Content-Type: application/json" \
-d "{\"method\":{\"cmd\":\"crudget\",\"argcnt\":1},\"dstype\":\"$ds\",\"criteria\":[]}"
done | jq -s '
# Flatten all responses
[.[] | .data[]] |

{
policies: [
.[] |
select(.topology_template.policies != null) |
.metadata.template_name as $template |
.topology_template.policies[] | to_entries[] |
{
template: $template,
policy_name: .key,
policy_type: .value.type
}
],
workflows: [
.[] |
select(.workflows != null) |
.metadata.template_name as $template |
.workflows | to_entries[] |
{
template: $template,
workflow_name: .key,
description: .value.description,
steps: (.value.steps | length)
}
]
} |

"Policy Analysis:\n" +
"  Total Policies: \(.policies | length)\n\n" +

"  By Type:\n" +
(.policies | group_by(.policy_type) |
map("    \(.[0].policy_type): \(length)") | join("\n")) +
"\n\n" +

"  Top Policy Users:\n" +
(.policies | group_by(.template) |
sort_by(length) | reverse | .[0:3][] |
"    \(.[0].template): \(length) policies\n") +

"\nWorkflow Analysis:\n" +
"  Total Workflows: \(.workflows | length)\n" +
"  Total Steps: \([.workflows[].steps] | add)\n\n" +

(if (.workflows | length) > 0 then
"  Workflows Detail:\n" +
(.workflows[] |
"    \(.workflow_name) (\(.template)): \(.steps) steps\n")
else
"  No workflows found\n"
end) +

"\nGovernance Coverage:\n" +
"  Templates with Policies: \(.policies | map(.template) | unique | length) / 5\n" +
"  Templates with Workflows: \(.workflows | map(.template) | unique | length) / 5\n" +
"  Automation Coverage: \((.workflows | map(.template) | unique | length / 5 * 100 | floor))%"
'
```

**Sample Output:**
```
=== Policy and Workflow Distribution ===

Policy Analysis:
Total Policies: 18

By Type:
tosca.policies.Scaling: 6
tosca.policies.Monitoring: 4
tosca.policies.Performance: 4
tosca.policies.Placement: 2
tosca.policies.Security: 2

Top Policy Users:
ApplicationRequirements-MLTrainingWorkload: 6 policies
WebApp-MicroservicesApplication: 3 policies
EdgeCluster-CapacityProfile: 3 policies

Workflow Analysis:
Total Workflows: 2
Total Steps: 16

Workflows Detail:
deploy_workflow (DeploymentPlan-WebApp-Release-v1.2.3): 9 steps
rollback_workflow (DeploymentPlan-WebApp-Release-v1.2.3): 7 steps

Governance Coverage:
Templates with Policies: 5 / 5
Templates with Workflows: 1 / 5
Automation Coverage: 20%
```

---

### Query 5.4: Template Comparison

**Use Case:** Compare two templates side-by-side for migration or optimization.

**PowerShell:**
```powershell
# Compare two templates
$template1Name = "WebApp-MicroservicesApplication"
$template2Name = "DeploymentPlan-WebApp-Release-v1.2.3"

# Get both templates
$body1 = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @(@{"metadata.template_name" = $template1Name})
} | ConvertTo-Json -Depth 10

$body2 = @{
method = @{cmd = "crudget"; argcnt = 1}
dstype = "dsswres"
criteria = @(@{"metadata.template_name" = $template2Name})
} | ConvertTo-Json -Depth 10

$t1 = (Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body1).data[0]

$t2 = (Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" `
-Method Post `
-ContentType "application/json" `
-Body $body2).data[0]

# Compare
Write-Host "`n=== Template Comparison ===" -ForegroundColor Cyan
Write-Host ""

# Basic info
$comparison = @(
[PSCustomObject]@{
Metric = "Template Name"
Template1 = $t1.metadata.template_name
Template2 = $t2.metadata.template_name
},
[PSCustomObject]@{
Metric = "Version"
Template1 = $t1.metadata.template_version
Template2 = $t2.metadata.template_version
},
[PSCustomObject]@{
Metric = "TOSCA Version"
Template1 = $t1.tosca_definitions_version
Template2 = $t2.tosca_definitions_version
},
[PSCustomObject]@{
Metric = "Node Count"
Template1 = ($t1.topology_template.node_templates.PSObject.Properties | Measure-Object).Count
Template2 = ($t2.topology_template.node_templates.PSObject.Properties | Measure-Object).Count
},
[PSCustomObject]@{
Metric = "Container Nodes"
Template1 = ($t1.topology_template.node_templates.PSObject.Properties |
Where-Object { $_.Value.type -like "*Container*" } | Measure-Object).Count
Template2 = ($t2.topology_template.node_templates.PSObject.Properties |
Where-Object { $_.Value.type -like "*Container*" } | Measure-Object).Count
},
[PSCustomObject]@{
Metric = "Policies"
Template1 = ($t1.topology_template.policies | Measure-Object).Count
Template2 = ($t2.topology_template.policies | Measure-Object).Count
},
[PSCustomObject]@{
Metric = "Workflows"
Template1 = if ($t1.workflows) { ($t1.workflows.PSObject.Properties | Measure-Object).Count } else { 0 }
Template2 = if ($t2.workflows) { ($t2.workflows.PSObject.Properties | Measure-Object).Count } else { 0 }
},
[PSCustomObject]@{
Metric = "Exposed Ports"
Template1 = ($t1.topology_template.node_templates.PSObject.Properties |
Where-Object { $_.Value.properties.ports } |
ForEach-Object { $_.Value.properties.ports.Count } |
Measure-Object -Sum).Sum
Template2 = ($t2.topology_template.node_templates.PSObject.Properties |
Where-Object { $_.Value.properties.ports } |
ForEach-Object { $_.Value.properties.ports.Count } |
Measure-Object -Sum).Sum
}
)

$comparison | Format-Table -AutoSize

Write-Host "`nKey Differences:" -ForegroundColor Yellow

# Workflows difference
if ($t2.workflows -and -not $t1.workflows) {
Write-Host "  ✓ $template2Name includes deployment workflows (9 + 7 steps)" -ForegroundColor Green
}

# Container count difference
$diff = ($comparison | Where-Object {$ _.Metric -eq "Container Nodes"})
if ($diff.Template1 -ne $diff.Template2) {
Write-Host "  • Container difference: Template1 has $($diff.Template1), Template2 has $($diff.Template2)"
}
```

**Sample Output:**
```
=== Template Comparison ===

Metric            Template1                           Template2
------            ---------                           ---------
Template Name     WebApp-MicroservicesApplication    DeploymentPlan-WebApp-Release-v1.2.3
Version           1.0.0                              1.2.3
TOSCA Version     tosca_simple_yaml_1_3              tosca_simple_yaml_1_3
Node Count        4                                  2
Container Nodes   4                                  1
Policies          3                                  3
Workflows         0                                  2
Exposed Ports     5                                  1

Key Differences:
✓ DeploymentPlan-WebApp-Release-v1.2.3 includes deployment workflows (9 + 7 steps)
• Container difference: Template1 has 4, Template2 has 1
```

---

## 🎯 Summary and Best Practices

### Multi-Datastore Design

OptimusDB supports logical separation of concerns through multiple datastores:

| Datastore | Purpose | Example Templates |
|-----------|---------|-------------------|
| **dsswres** | Workflows & Applications | webapp_adt, deployment_plan |
| **dssrres** | Resource Requirements | capacity_profile |
| **dssires** | Infrastructure | opentofu_hybrid |
| **dssares** | Application Requirements | app_requirements |

**Benefits:**
- ✅ Logical separation
- ✅ Easier queries (target specific domain)
- ✅ Better organization
- ✅ Performance optimization

---

### Query Best Practices

#### 1. **Start Simple, Build Complex**
```
Simple (Server-side) → Intermediate (Client-filter) → Complex (Multi-criteria) → Analytical (Cross-datastore)
```

#### 2. **Know When to Filter Client-Side**

**Server-side (OrbitDB) when:**
- Exact field match
- Top-level fields only
- Small result sets

**Client-side (PowerShell/Bash) when:**
- Wildcard/substring matching
- Nested property access
- Complex AND/OR logic
- Array element searching

#### 3. **Use Appropriate Datastores**

**Don't query all datastores for specific info:**
```powershell
# Bad: Query all datastores
for ds in dsswres dssrres dssires dssares { ... }

# Good: Query relevant datastore
Query dsswres for applications
Query dssires for Kubernetes clusters
```

#### 4. **Cache Results for Analysis**

**For analytical queries, fetch once, analyze multiple times:**
```powershell
# Fetch once
$allData = Get-AllTemplates

# Analyze multiple aspects
Analyze-Ports $allData
Analyze-Resources $allData
Analyze-Policies $allData
```

---

### Performance Tips

1. **Minimize API Calls**
- Fetch all, filter client-side for multiple criteria
- Don't query same datastore repeatedly

2. **Use Targeted Queries**
- Query specific datastore when possible
- Use exact matches when you know IDs

3. **Parallel Queries**
```powershell
# PowerShell parallel
$jobs = $datastores | ForEach-Object {
Start-Job -ScriptBlock { Query-Datastore $_ }
}
```

4. **Result Pagination**
- For large result sets, consider pagination
- Process results incrementally

---

### Security Considerations

1. **Sensitive Data in Environment Variables**
- Passwords, API keys visible in queries
- Consider encryption for production

2. **Port Exposure**
- Regular audits via port distribution queries
- Identify insecure protocols (HTTP)

3. **Access Control**
- Different datastores = different access levels
- Implement RBAC policies

---

## 📚 Conclusion

This guide demonstrated OptimusDB as a **TOSCA-native knowledge base** enabling:

✅ **Deep Querying** - Search nested YAML structures
✅ **Infrastructure-Aware** - Understand containers, GPUs, networks
✅ **Multi-Datastore** - Logical separation of concerns
✅ **Analytical Insights** - Resource statistics, security audits
✅ **Platform-Agnostic** - PowerShell (Windows) + Bash (Linux/macOS)

**OptimusDB transforms static TOSCA files into queryable, analyzable infrastructure knowledge.**

---

**Project:** OptimusDB - EU Horizon Europe Grant 101135012
**Guide Version:** 1.0
**Date:** December 20, 2025
**Status:** Production Ready
