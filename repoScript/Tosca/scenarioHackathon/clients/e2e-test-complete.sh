#!/bin/bash

################################################################################
# OptimusDB End-to-End Testing Script with Complete Debug (Bash)
################################################################################
#
# SYNOPSIS
#     Comprehensive E2E testing for OptimusDB with built-in response display,
#     all fixes applied for TOSCA 1.3 standard compliance.
#
#     Project: OptimusDB - EU Horizon Europe Grant 101135012
#
# USAGE
#     ./e2e-test-debug.sh [base_url] [files_path]
#
# EXAMPLES
#     ./e2e-test-debug.sh
#     ./e2e-test-debug.sh http://localhost:18002 /path/to/tosca
#
# NOTES
#     Fixes Applied:
#     - Container filter (was Docker, now Container per TOSCA 1.3)
#     - Workflows at top level (was topology_template.workflows)
#     - Complete response display for all tests
#
################################################################################

set -o pipefail

################################################################################
# Configuration
################################################################################

BASE_URL="${1:-http://localhost:18001}"
FILES_PATH="${2:-../}"

# TOSCA files
declare -A TOSCA_FILES=(
    ["webapp_adt.yaml"]="WebApp Microservices Application"
    ["capacity_profile.yaml"]="Edge Cluster Capacity Profile"
    ["opentofu_hybrid.yaml"]="Hybrid Infrastructure with OpenTofu"
    ["deployment_plan.yaml"]="Deployment Plan with Workflows"
    ["app_requirements.yaml"]="ML Training Application Requirements"
)

# Test results
declare -a TEST_RESULTS=()
declare -a TEMPLATE_IDS=()
TEST_START_TIME=$(date +%s)

# Report files
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
REPORT_FILE="test_report_${TIMESTAMP}.txt"
JSON_REPORT="test_report_${TIMESTAMP}.json"

# Temporary file for responses
TEMP_RESPONSE=$(mktemp)
trap "rm -f $TEMP_RESPONSE" EXIT

################################################################################
# Color Codes
################################################################################

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
GRAY='\033[0;90m'
DARK_CYAN='\033[0;36m'
DARK_GRAY='\033[1;30m'
NC='\033[0m' # No Color
BG_BLACK='\033[40m'

################################################################################
# Helper Functions
################################################################################

write_banner() {
    local text="$1"
    echo ""
    echo -e "${CYAN}$(printf '═%.0s' {1..80})${NC}"
    printf "${CYAN}${BG_BLACK}%*s${NC}\n" $(((${#text}+80)/2)) "$text"
    echo -e "${CYAN}$(printf '═%.0s' {1..80})${NC}"
    echo ""
}

write_section() {
    local text="$1"
    echo ""
    echo -e "${BLUE}$(printf '─%.0s' {1..80})${NC}"
    echo -e "${BLUE}${BG_BLACK}${text}${NC}"
    echo -e "${BLUE}$(printf '─%.0s' {1..80})${NC}"
    echo ""
}

write_test_header() {
    local scenario="$1"
    local description="$2"
    echo ""
    echo -e "${MAGENTA}${BG_BLACK}TEST SCENARIO: ${scenario}${NC}"
    echo -e "${CYAN}Description: ${description}${NC}"
}

write_expected() {
    local expected="$1"
    echo -e "${YELLOW}Expected: ${expected}${NC}"
}

write_command() {
    local command="$1"
    echo -e "${GRAY}Command: ${command}${NC}"
}

write_payload() {
    local payload="$1"
    echo -e "${MAGENTA}Payload:${NC}"
    echo "$payload" | jq '.' 2>/dev/null | while IFS= read -r line; do
        echo -e "${DARK_GRAY}  $line${NC}"
    done
}

write_response() {
    local response="$1"
    local max_lines="${2:-50}"

    echo -e "${CYAN}Response:${NC}"

    # Pretty print JSON and count lines
    local formatted=$(echo "$response" | jq '.' 2>/dev/null)
    local line_count=$(echo "$formatted" | wc -l)

    if [ "$line_count" -gt "$max_lines" ]; then
        echo "$formatted" | head -n "$max_lines" | while IFS= read -r line; do
            echo -e "${DARK_CYAN}  $line${NC}"
        done
        echo -e "${GRAY}  ... (truncated, $((line_count - max_lines)) more lines)${NC}"
    else
        echo "$formatted" | while IFS= read -r line; do
            echo -e "${DARK_CYAN}  $line${NC}"
        done
    fi

    # Show summary info
    local status=$(echo "$response" | jq -r '.status // empty' 2>/dev/null)
    if [ -n "$status" ]; then
        if [ "$status" = "200" ]; then
            echo -e "${GREEN}  Status Code: ${status}${NC}"
        else
            echo -e "${YELLOW}  Status Code: ${status}${NC}"
        fi
    fi

    local data_count=$(echo "$response" | jq '.data | length' 2>/dev/null)
    if [ -n "$data_count" ] && [ "$data_count" != "null" ]; then
        echo -e "${GRAY}  Data Count: ${data_count} items${NC}"
    fi
}

write_test_result() {
    local passed="$1"
    local message="$2"

    if [ "$passed" = "true" ]; then
        echo -e "${GREEN}✅ PASS: ${message}${NC}"
    else
        echo -e "${RED}❌ FAIL: ${message}${NC}"
    fi
}

record_test() {
    local scenario="$1"
    local description="$2"
    local expected="$3"
    local command="$4"
    local passed="$5"
    local actual="$6"
    local exec_time="$7"

    local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    # Escape quotes for JSON
    scenario=$(echo "$scenario" | sed 's/"/\\"/g')
    description=$(echo "$description" | sed 's/"/\\"/g')
    expected=$(echo "$expected" | sed 's/"/\\"/g')
    command=$(echo "$command" | sed 's/"/\\"/g')
    actual=$(echo "$actual" | sed 's/"/\\"/g')

    local result=$(cat <<EOF
{
  "Scenario": "$scenario",
  "Description": "$description",
  "Expected": "$expected",
  "Command": "$command",
  "Passed": $passed,
  "ActualResult": "$actual",
  "ExecutionTime": $exec_time,
  "Timestamp": "$timestamp"
}
EOF
)

    TEST_RESULTS+=("$result")
}

################################################################################
# Upload Functions
################################################################################

upload_tosca_file() {
    local filename="$1"
    local description="$2"
    local filepath="${FILES_PATH}/${filename}"

    if [ ! -f "$filepath" ]; then
        return 1
    fi

    # Convert to base64
    local base64_content=$(base64 -w 0 "$filepath" 2>/dev/null || base64 "$filepath")

    # Prepare request body
    local body=$(cat <<EOF
{
  "file": "$base64_content",
  "filename": "$filename",
  "store_full_structure": true
}
EOF
)

    # Upload to OptimusDB
    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/upload" \
        -H "Content-Type: application/json" \
        -d "$body" \
        --max-time 60)

    local status=$(echo "$response" | jq -r '.status // empty' 2>/dev/null)

    if [ "$status" = "200" ]; then
        local template_id=$(echo "$response" | jq -r '.data.template_id // empty' 2>/dev/null)
        echo "$template_id"
        return 0
    fi

    return 1
}

upload_all_tosca_files() {
    write_section "PHASE 1: Upload TOSCA Files"

    local success_count=0
    local total_count=${#TOSCA_FILES[@]}

    for filename in "${!TOSCA_FILES[@]}"; do
        local description="${TOSCA_FILES[$filename]}"

        echo -n "Uploading: ${description}..."

        local template_id=$(upload_tosca_file "$filename" "$description")

        if [ -n "$template_id" ]; then
            TEMPLATE_IDS+=("$template_id")
            local id_preview="${template_id:0:20}"
            echo -e " ${GREEN}✅ Success${NC} ${GRAY}(ID: ${id_preview}...)${NC}"
            ((success_count++))
        else
            echo -e " ${RED}❌ Failed${NC}"
        fi
    done

    echo ""
    echo "Upload Summary: ${success_count}/${total_count} successful"

    if [ "$success_count" -eq "$total_count" ]; then
        return 0
    else
        return 1
    fi
}

################################################################################
# Test Functions
################################################################################

test_get_all_templates() {
    local scenario="Get All TOSCA Templates"
    local description="Retrieve all templates from dsswres"
    local expected="Returns array with ${#TOSCA_FILES[@]}+ templates"

    local payload=$(cat <<'EOF'
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": []
}
EOF
)

    local command="POST ${BASE_URL}/swarmkb/command"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"
    write_payload "$payload"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    local count=$(echo "$response" | jq '.data | length' 2>/dev/null)

    if [ -n "$count" ] && [ "$count" -ge "${#TOSCA_FILES[@]}" ]; then
        local actual="Returned ${count} templates"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
    else
        local actual="Returned ${count:-0} templates"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
    fi
}

test_find_by_template_id() {
    if [ ${#TEMPLATE_IDS[@]} -eq 0 ]; then
        echo -e "${YELLOW}Skipping: No template IDs available${NC}"
        return
    fi

    local scenario="Find Template by ID"
    local description="Retrieve specific template using its ID"
    local expected="Returns exactly 1 template with matching ID"

    local test_id="${TEMPLATE_IDS[0]}"

    local payload=$(cat <<EOF
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": [{"_id": "$test_id"}]
}
EOF
)

    local command="POST ${BASE_URL}/swarmkb/command"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"
    write_payload "$payload"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    local count=$(echo "$response" | jq '.data | length' 2>/dev/null)
    local returned_id=$(echo "$response" | jq -r '.data[0]._id // empty' 2>/dev/null)

    if [ "$count" = "1" ] && [ "$returned_id" = "$test_id" ]; then
        local actual="Returned ${count} template(s), ID match: True"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
    else
        local actual="Returned ${count:-0} template(s), ID match: False"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
    fi
}

test_find_by_tosca_version() {
    local scenario="Find by TOSCA Version"
    local description="Find all templates using tosca_simple_yaml_1_3"
    local expected="Returns ${#TOSCA_FILES[@]} templates"

    local payload=$(cat <<'EOF'
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": [{"tosca_definitions_version": "tosca_simple_yaml_1_3"}]
}
EOF
)

    local command="POST ${BASE_URL}/swarmkb/command"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"
    write_payload "$payload"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    local count=$(echo "$response" | jq '.data | length' 2>/dev/null)

    if [ -n "$count" ] && [ "$count" -ge "${#TOSCA_FILES[@]}" ]; then
        local actual="Returned ${count} templates"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
    else
        local actual="Returned ${count:-0} templates"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
    fi
}

test_find_container_nodes() {
    local scenario="Find Templates with Container Nodes"
    local description="Find all templates containing Container node types"
    local expected="Returns 2+ templates (webapp_adt, deployment_plan)"

    local payload=$(cat <<'EOF'
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": []
}
EOF
)

    local command="POST ${BASE_URL}/swarmkb/command (with client-side filtering for Container nodes)"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"
    write_payload "$payload"
    echo -e "${YELLOW}Note: Filtering for node types containing 'Container' (client-side)${NC}"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    # Client-side filtering for Container nodes (FIXED: was Docker, now Container)
    local container_count=$(echo "$response" | jq '[.data[] |
        select(.topology_template.node_templates != null) |
        select(
            .topology_template.node_templates |
            to_entries[] |
            .value.type |
            contains("Container")
        )
    ] | length' 2>/dev/null)

    # Get template names
    local container_templates=$(echo "$response" | jq -r '[.data[] |
        select(.topology_template.node_templates != null) |
        select(
            .topology_template.node_templates |
            to_entries[] |
            .value.type |
            contains("Container")
        ) | .metadata.template_name
    ] | join(", ")' 2>/dev/null)

    if [ -n "$container_count" ] && [ "$container_count" -ge 2 ]; then
        local actual="Found ${container_count} templates with Container nodes (${container_templates})"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
    else
        local actual="Found ${container_count:-0} templates with Container nodes"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
    fi
}

test_find_gpu_resources() {
    local scenario="Find Templates with GPU Resources"
    local description="Find all templates containing GPU nodes or requirements"
    local expected="Returns 2+ templates (capacity_profile, app_requirements)"

    local payload=$(cat <<'EOF'
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": []
}
EOF
)

    local command="POST ${BASE_URL}/swarmkb/command (with client-side GPU filtering)"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"
    write_payload "$payload"
    echo -e "${YELLOW}Note: Filtering for GPU nodes/properties (client-side)${NC}"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    # Client-side filtering for GPU
    local gpu_count=$(echo "$response" | jq '[.data[] |
        select(.topology_template.node_templates != null) |
        select(
            .topology_template.node_templates |
            to_entries[] |
            (.value.type | contains("GPU")) or
            (.value.properties.gpu_model != null) or
            (.value.properties.gpu_count_preferred != null) or
            (.value.properties.gpu_memory != null)
        )
    ] | length' 2>/dev/null)

    # Get template names
    local gpu_templates=$(echo "$response" | jq -r '[.data[] |
        select(.topology_template.node_templates != null) |
        select(
            .topology_template.node_templates |
            to_entries[] |
            (.value.type | contains("GPU")) or
            (.value.properties.gpu_model != null) or
            (.value.properties.gpu_count_preferred != null) or
            (.value.properties.gpu_memory != null)
        ) | .metadata.template_name
    ] | join(", ")' 2>/dev/null)

    if [ -n "$gpu_count" ] && [ "$gpu_count" -ge 2 ]; then
        local actual="Found ${gpu_count} templates with GPU resources (${gpu_templates})"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
    else
        local actual="Found ${gpu_count:-0} templates with GPU resources"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
    fi
}

test_find_by_port() {
    local scenario="Find Templates with Specific Ports"
    local description="Find templates exposing port 443 (HTTPS)"
    local expected="Returns 1+ templates with HTTPS endpoints"

    local payload=$(cat <<'EOF'
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": []
}
EOF
)

    local command="POST ${BASE_URL}/swarmkb/command (filtering for port 443)"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"
    write_payload "$payload"
    echo -e "${YELLOW}Note: Filtering for port 443 in node properties (client-side)${NC}"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    # Client-side filtering for port 443
    local port_count=$(echo "$response" | jq '[.data[] |
        select(.topology_template.node_templates != null) |
        select(
            .topology_template.node_templates |
            to_entries[] |
            .value.properties.ports[]? |
            contains("443")
        )
    ] | length' 2>/dev/null)

    if [ -n "$port_count" ] && [ "$port_count" -ge 1 ]; then
        local actual="Found ${port_count} templates with port 443"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
    else
        local actual="Found ${port_count:-0} templates with port 443"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
    fi
}

test_find_workflows() {
    local scenario="Find Templates with Workflows"
    local description="Find templates containing deployment or operational workflows"
    local expected="Returns 1+ templates (deployment_plan with 2 workflows)"

    local payload=$(cat <<'EOF'
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": []
}
EOF
)

    local command="POST ${BASE_URL}/swarmkb/command (filtering for workflows)"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"
    write_payload "$payload"
    echo -e "${YELLOW}Note: Filtering for workflows at top level (client-side)${NC}"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    # Client-side filtering for workflows (FIXED: at top level, not in topology_template)
    local workflow_count=$(echo "$response" | jq '[.data[] |
        select(.workflows != null)
    ] | length' 2>/dev/null)

    # Get workflow details
    local workflow_details=$(echo "$response" | jq -r '[.data[] |
        select(.workflows != null) |
        "\(.metadata.template_name): \(.workflows | length) workflows"
    ] | join("; ")' 2>/dev/null)

    if [ -n "$workflow_count" ] && [ "$workflow_count" -ge 1 ]; then
        local actual="Found ${workflow_count} templates with workflows (${workflow_details})"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
    else
        local actual="Found ${workflow_count:-0} templates with workflows (${workflow_details})"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
    fi
}

test_find_policies() {
    local scenario="Find Templates with Policies"
    local description="Find templates containing scaling, monitoring, or cost policies"
    local expected="Returns 2+ templates with policy definitions"

    local payload=$(cat <<'EOF'
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": []
}
EOF
)

    local command="POST ${BASE_URL}/swarmkb/command (filtering for policies)"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"
    write_payload "$payload"
    echo -e "${YELLOW}Note: Filtering for topology_template.policies (client-side)${NC}"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    # Client-side filtering for policies
    local policy_count=$(echo "$response" | jq '[.data[] |
        select(.topology_template.policies != null)
    ] | length' 2>/dev/null)

    # Get policy details
    local policy_details=$(echo "$response" | jq -r '[.data[] |
        select(.topology_template.policies != null) |
        "\(.metadata.template_name): \(.topology_template.policies | length) policies"
    ] | join("; ")' 2>/dev/null)

    if [ -n "$policy_count" ] && [ "$policy_count" -ge 2 ]; then
        local actual="Found ${policy_count} templates with policies (${policy_details})"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
    else
        local actual="Found ${policy_count:-0} templates with policies (${policy_details})"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
    fi
}

test_find_high_memory_nodes() {
    local scenario="Find Templates with High Memory Requirements"
    local description="Find templates requiring nodes with >64 GB memory"
    local expected="Returns 2+ templates (capacity_profile: 128GB, app_requirements: 64-128GB)"

    local payload=$(cat <<'EOF'
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": []
}
EOF
)

    local command="POST ${BASE_URL}/swarmkb/command (filtering for memory >64GB)"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"
    write_payload "$payload"
    echo -e "${YELLOW}Note: Filtering for memory specifications >64GB (client-side)${NC}"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    # Client-side filtering for high memory (>64GB)
    local highmem_count=$(echo "$response" | jq '[.data[] |
        select(.topology_template.node_templates != null) |
        select(
            .topology_template.node_templates |
            to_entries[] |
            (.value.properties.mem_size // "" | test("[0-9]+") and (. | capture("(?<num>[0-9]+)") | .num | tonumber) > 64) or
            (.value.properties.total_memory // "" | test("[0-9]+") and (. | capture("(?<num>[0-9]+)") | .num | tonumber) > 64) or
            (.value.properties.memory_preferred // 0 | tonumber > 64)
        )
    ] | length' 2>/dev/null)

    if [ -n "$highmem_count" ] && [ "$highmem_count" -ge 2 ]; then
        local actual="Found ${highmem_count} templates with >64GB memory requirements"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
    else
        local actual="Found ${highmem_count:-0} templates with >64GB memory requirements"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
    fi
}

test_crud_insert() {
    local scenario="CRUD - INSERT"
    local description="Insert a test renewable energy resource document"
    local expected="Document inserted successfully with confirmation message"

    local test_id="test_solar_farm_$(date +%Y%m%d%H%M%S)"

    local payload=$(cat <<EOF
{
  "method": {"cmd": "crudput", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": [{
    "_id": "$test_id",
    "name": "Athens Solar Farm Test",
    "type": "solar",
    "capacity_mw": 500,
    "location": {
      "country": "Greece",
      "region": "Attica",
      "coordinates": {"lat": 37.9838, "lon": 23.7275}
    },
    "status": "operational",
    "commissioned_date": "2024-06-15"
  }]
}
EOF
)

    local command="POST ${BASE_URL}/swarmkb/command"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"
    write_payload "$payload"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    local message=$(echo "$response" | jq -r '.data // empty' 2>/dev/null)

    if [[ "$message" == *"inserted"* ]] || [[ "$message" == *"success"* ]]; then
        local actual="Response: ${message}"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
        echo "$test_id"
    else
        local actual="Response: ${message}"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
        echo ""
    fi
}

test_crud_query() {
    local test_id="$1"

    local scenario="CRUD - QUERY"
    local description="Query the test document we just inserted"
    local expected="Returns exactly 1 document with matching _id"

    local payload=$(cat <<EOF
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": [{"_id": "$test_id"}]
}
EOF
)

    local command="POST /swarmkb/command with crudget, _id: ${test_id}"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    local count=$(echo "$response" | jq '.data | length' 2>/dev/null)
    local returned_id=$(echo "$response" | jq -r '.data[0]._id // empty' 2>/dev/null)

    if [ "$count" = "1" ] && [ "$returned_id" = "$test_id" ]; then
        local actual="Returned ${count} document(s), _id match: True"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
        return 0
    else
        local actual="Returned ${count:-0} document(s), _id match: False"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
        return 1
    fi
}

test_crud_update() {
    local test_id="$1"

    local scenario="CRUD - UPDATE"
    local description="Update test document with new values"
    local expected="Document updated successfully, _id preserved"

    local payload=$(cat <<EOF
{
  "method": {"cmd": "crudupdate", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": [{"_id": "$test_id"}],
  "UpdateData": [{
    "status": "maintenance",
    "maintenance_reason": "Scheduled panel cleaning",
    "capacity_mw": 550
  }]
}
EOF
)

    local command="POST /swarmkb/command with crudupdate, _id: ${test_id}"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    local message=$(echo "$response" | jq -r '.data // empty' 2>/dev/null)

    if [[ "$message" == *"updated"* ]] || [[ "$message" == *"success"* ]]; then
        local actual="Response: ${message}"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
        return 0
    else
        local actual="Response: ${message}"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
        return 1
    fi
}

test_crud_verify_update() {
    local test_id="$1"

    local scenario="CRUD - VERIFY UPDATE (CRITICAL)"
    local description="Verify update applied correctly and _id was preserved"
    local expected="_id preserved, status='maintenance', capacity_mw=550, has _updated_at"

    local payload=$(cat <<EOF
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": [{"_id": "$test_id"}]
}
EOF
)

    local command="POST /swarmkb/command with crudget, verify _id preserved"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    local count=$(echo "$response" | jq '.data | length' 2>/dev/null)

    if [ "$count" = "1" ]; then
        local doc_id=$(echo "$response" | jq -r '.data[0]._id // empty' 2>/dev/null)
        local status=$(echo "$response" | jq -r '.data[0].status // empty' 2>/dev/null)
        local capacity=$(echo "$response" | jq -r '.data[0].capacity_mw // empty' 2>/dev/null)
        local updated_at=$(echo "$response" | jq -r '.data[0]._updated_at // empty' 2>/dev/null)

        local id_preserved="False"
        [ "$doc_id" = "$test_id" ] && id_preserved="True"

        local has_timestamp="False"
        [ -n "$updated_at" ] && [ "$updated_at" != "null" ] && has_timestamp="True"

        if [ "$id_preserved" = "True" ] && [ "$status" = "maintenance" ] && [ "$capacity" = "550" ] && [ "$has_timestamp" = "True" ]; then
            local actual="_id preserved: ${id_preserved}, status: ${status}, capacity: ${capacity}, has _updated_at: ${has_timestamp}"
            write_test_result "true" "$actual"
            echo -e "${GREEN}   🎉 CRITICAL TEST PASSED - UPDATE fix working correctly!${NC}"
            record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
            return 0
        else
            local actual="_id preserved: ${id_preserved}, status: ${status}, capacity: ${capacity}, has _updated_at: ${has_timestamp}"
            write_test_result "false" "$actual"
            echo -e "${RED}   ⚠️  CRITICAL TEST FAILED - UPDATE may have issues!${NC}"
            record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
            return 1
        fi
    else
        local actual="Expected 1 document, got ${count:-0}"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
        return 1
    fi
}

test_crud_delete() {
    local test_id="$1"

    local scenario="CRUD - DELETE"
    local description="Delete the test document"
    local expected="Document deleted successfully"

    local payload=$(cat <<EOF
{
  "method": {"cmd": "cruddelete", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": [{"_id": "$test_id"}]
}
EOF
)

    local command="POST /swarmkb/command with cruddelete, _id: ${test_id}"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    local message=$(echo "$response" | jq -r '.data // empty' 2>/dev/null)

    if [[ "$message" == *"deleted"* ]] || [[ "$message" == *"success"* ]]; then
        local actual="Response: ${message}"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
        return 0
    else
        local actual="Response: ${message}"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
        return 1
    fi
}

test_crud_verify_delete() {
    local test_id="$1"

    local scenario="CRUD - VERIFY DELETE"
    local description="Verify document was deleted"
    local expected="Query returns empty array (0 results)"

    local payload=$(cat <<EOF
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": [{"_id": "$test_id"}]
}
EOF
)

    local command="POST /swarmkb/command with crudget, should return empty"

    write_test_header "$scenario" "$description"
    write_expected "$expected"
    write_command "$command"

    local start_time=$(date +%s.%N)

    local response=$(curl -s -X POST "${BASE_URL}/swarmkb/command" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        --max-time 30)

    local end_time=$(date +%s.%N)
    local exec_time=$(echo "$end_time - $start_time" | bc)

    # Display response
    write_response "$response"

    local count=$(echo "$response" | jq '.data | length' 2>/dev/null)

    if [ "$count" = "0" ]; then
        local actual="Returned ${count} document(s)"
        write_test_result "true" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "true" "$actual" "$exec_time"
        return 0
    else
        local actual="Returned ${count:-0} document(s)"
        write_test_result "false" "$actual"
        record_test "$scenario" "$description" "$expected" "$command" "false" "$actual" "$exec_time"
        return 1
    fi
}

run_crud_tests() {
    write_section "PHASE 3: CRUD Operations Testing"

    # INSERT
    local test_id=$(test_crud_insert)

    if [ -z "$test_id" ]; then
        echo -e "${RED}CRUD tests aborted - INSERT failed${NC}"
        return 1
    fi

    sleep 1

    # QUERY
    test_crud_query "$test_id"
    sleep 1

    # UPDATE
    test_crud_update "$test_id"
    sleep 1

    # VERIFY UPDATE (CRITICAL)
    test_crud_verify_update "$test_id"
    sleep 1

    # DELETE
    test_crud_delete "$test_id"
    sleep 1

    # VERIFY DELETE
    test_crud_verify_delete "$test_id"
}

################################################################################
# Report Generation
################################################################################

generate_report() {
    write_section "PHASE 4: Test Report Generation"

    local end_time=$(date +%s)
    local total_duration=$((end_time - TEST_START_TIME))

    local passed=0
    local failed=0

    for result in "${TEST_RESULTS[@]}"; do
        if echo "$result" | jq -e '.Passed == true' >/dev/null 2>&1; then
            ((passed++))
        else
            ((failed++))
        fi
    done

    local total=${#TEST_RESULTS[@]}

    # Console report
    echo ""
    echo -e "${CYAN}$(printf '═%.0s' {1..80})${NC}"
    echo -e "${CYAN}${BG_BLACK}TEST EXECUTION SUMMARY${NC}"
    echo -e "${CYAN}$(printf '═%.0s' {1..80})${NC}"
    echo ""

    echo "Total Tests:     ${total}"
    echo -e "Passed:          ${GREEN}${passed}${NC}"
    echo -e "Failed:          ${RED}${failed}${NC}"
    echo "Duration:        ${total_duration}s"

    local success_rate=0
    if [ "$total" -gt 0 ]; then
        success_rate=$(echo "scale=1; $passed * 100 / $total" | bc)
    fi
    echo "Success Rate:    ${success_rate}%"
    echo ""

    # Detailed results
    echo -e "${CYAN}DETAILED RESULTS:${NC}"
    echo ""

    local i=1
    for result in "${TEST_RESULTS[@]}"; do
        local scenario=$(echo "$result" | jq -r '.Scenario')
        local expected_val=$(echo "$result" | jq -r '.Expected')
        local actual_val=$(echo "$result" | jq -r '.ActualResult')
        local exec_time=$(echo "$result" | jq -r '.ExecutionTime')
        local passed_val=$(echo "$result" | jq -r '.Passed')

        if [ "$passed_val" = "true" ]; then
            echo -e "${i}. ${GREEN}✅ PASS${NC} - ${scenario}"
        else
            echo -e "${i}. ${RED}❌ FAIL${NC} - ${scenario}"
        fi

        echo -e "${GRAY}   Expected: ${expected_val}${NC}"
        echo -e "${GRAY}   Actual:   ${actual_val}${NC}"
        echo -e "${GRAY}   Time:     ${exec_time}s${NC}"
        echo ""

        ((i++))
    done

    # Save text report
    cat > "$REPORT_FILE" <<EOF
OptimusDB End-to-End Test Report
$(printf '=%.0s' {1..80})

Test Session: $(date +"%Y-%m-%d %H:%M:%S")
Base URL: $BASE_URL
Total Tests: $total
Passed: $passed
Failed: $failed
Duration: ${total_duration}s
Success Rate: ${success_rate}%

Detailed Results:
$(printf -- '-%.0s' {1..80})

EOF

    i=1
    for result in "${TEST_RESULTS[@]}"; do
        local scenario=$(echo "$result" | jq -r '.Scenario')
        local description=$(echo "$result" | jq -r '.Description')
        local expected_val=$(echo "$result" | jq -r '.Expected')
        local actual_val=$(echo "$result" | jq -r '.ActualResult')
        local command=$(echo "$result" | jq -r '.Command')
        local exec_time=$(echo "$result" | jq -r '.ExecutionTime')
        local timestamp=$(echo "$result" | jq -r '.Timestamp')
        local passed_val=$(echo "$result" | jq -r '.Passed')

        local status="FAIL"
        [ "$passed_val" = "true" ] && status="PASS"

        cat >> "$REPORT_FILE" <<EOF
${i}. ${status} - ${scenario}
   Description: ${description}
   Expected: ${expected_val}
   Actual: ${actual_val}
   Command: ${command}
   Execution Time: ${exec_time}s
   Timestamp: ${timestamp}

EOF
        ((i++))
    done

    echo -e "${GREEN}✅ Text report saved to: ${REPORT_FILE}${NC}"

    # Save JSON report
    local json_results=$(printf '%s\n' "${TEST_RESULTS[@]}" | jq -s '.')

    cat > "$JSON_REPORT" <<EOF
{
  "session": {
    "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "base_url": "$BASE_URL",
    "total_tests": $total,
    "passed": $passed,
    "failed": $failed,
    "duration": $total_duration,
    "success_rate": $success_rate
  },
  "test_results": $json_results
}
EOF

    echo -e "${GREEN}✅ JSON report saved to: ${JSON_REPORT}${NC}"

    # Final status
    echo ""
    if [ "$failed" -eq 0 ]; then
        echo -e "${GREEN}${BG_BLACK}🎉 ALL TESTS PASSED! 🎉${NC}"
        return 0
    else
        echo -e "${YELLOW}${BG_BLACK}⚠️  ${failed} TEST(S) FAILED${NC}"
        return 1
    fi
}

################################################################################
# Main Execution
################################################################################

main() {
    write_banner "OptimusDB End-to-End Test Suite with Complete Debug"

    echo "Configuration:"
    echo "  Base URL: $BASE_URL"
    echo "  Files Directory: $FILES_PATH"
    echo "  Test Report: $REPORT_FILE"
    echo ""
    echo -e "${YELLOW}Fixes Applied:${NC}"
    echo "  ✅ Container filter (was Docker, now Container per TOSCA 1.3)"
    echo "  ✅ Workflows at top level (was topology_template.workflows)"
    echo "  ✅ Complete response display for all tests"
    echo ""

    # Check dependencies
    if ! command -v jq &> /dev/null; then
        echo -e "${RED}Error: jq is not installed. Please install jq to run this script.${NC}"
        echo "Ubuntu/Debian: sudo apt-get install jq"
        echo "macOS: brew install jq"
        exit 1
    fi

    if ! command -v bc &> /dev/null; then
        echo -e "${RED}Error: bc is not installed. Please install bc to run this script.${NC}"
        echo "Ubuntu/Debian: sudo apt-get install bc"
        echo "macOS: brew install bc"
        exit 1
    fi

    # Phase 1: Upload TOSCA files
    if ! upload_all_tosca_files; then
        echo -e "${RED}Upload phase failed - aborting tests${NC}"
        return 1
    fi

    sleep 2  # Wait for replication

    # Phase 2: Query Tests
    write_section "PHASE 2: Query Operation Tests"

    test_get_all_templates
    sleep 1

    test_find_by_template_id
    sleep 1

    test_find_by_tosca_version
    sleep 1

    test_find_container_nodes
    sleep 1

    test_find_gpu_resources
    sleep 1

    test_find_by_port
    sleep 1

    test_find_workflows
    sleep 1

    test_find_policies
    sleep 1

    test_find_high_memory_nodes
    sleep 1

    # Phase 3: CRUD Tests
    run_crud_tests

    # Phase 4: Generate Report
    generate_report

    return $?
}

# Execute
main
exit $?