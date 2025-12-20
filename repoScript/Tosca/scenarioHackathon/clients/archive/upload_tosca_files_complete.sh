#!/bin/bash

################################################################################
# OptimusDB TOSCA Upload Script (Bash)
################################################################################
# Uploads TOSCA YAML files to OptimusDB with base64 encoding
# Persists template IDs to a JSON file for later use
#
# Usage: ./upload_tosca_files_complete.sh [base_url] [files_directory]
#
# Project: OptimusDB - EU Horizon Europe Grant 101135012
################################################################################

set -e  # Exit on error

# Color codes
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly CYAN='\033[0;36m'
readonly GRAY='\033[0;90m'
readonly NC='\033[0m' # No Color

# Configuration
BASE_URL="${1:-http://localhost:18001}"
FILES_DIR="${2:-.}"
OUTPUT_FILE="uploaded_tosca_templates.json"
LOG_FILE="upload_log_$(date +%Y%m%d_%H%M%S).txt"

# TOSCA files configuration
declare -A TOSCA_FILES
TOSCA_FILES=(
    ["webapp_adt.yaml"]="WebApp Microservices Application"
    ["capacity_profile.yaml"]="Edge Cluster Capacity Profile"
    ["opentofu_hybrid.yaml"]="Hybrid Infrastructure with OpenTofu"
    ["deployment_plan.yaml"]="Deployment Plan with Workflows"
    ["app_requirements.yaml"]="ML Training Application Requirements"
)

# Statistics
TOTAL_FILES=${#TOSCA_FILES[@]}
UPLOADED_COUNT=0
FAILED_COUNT=0

################################################################################
# Helper Functions
################################################################################

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

print_header() {
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
    log "SUCCESS: $1"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
    log "ERROR: $1"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
    log "WARNING: $1"
}

print_info() {
    echo -e "${CYAN}ℹ️  $1${NC}"
}

print_detail() {
    echo -e "${GRAY}   $1${NC}"
}

# Check dependencies
check_dependencies() {
    local deps=("curl" "jq" "base64")
    local missing=()

    for dep in "${deps[@]}"; do
        if ! command -v "$dep" &> /dev/null; then
            missing+=("$dep")
        fi
    done

    if [ ${#missing[@]} -ne 0 ]; then
        print_error "Missing required dependencies: ${missing[*]}"
        echo ""
        echo "Please install missing dependencies:"
        echo "  Ubuntu/Debian: sudo apt-get install curl jq coreutils"
        echo "  CentOS/RHEL:   sudo yum install curl jq coreutils"
        echo "  macOS:         brew install curl jq coreutils"
        exit 1
    fi
}

# Test API connectivity
test_connectivity() {
    print_info "Testing connection to $BASE_URL..."

    if curl -s -f -m 5 "$BASE_URL/health" > /dev/null 2>&1; then
        print_success "API is reachable"
        return 0
    else
        print_warning "Health endpoint not responding (this may be normal)"
        print_info "Attempting to continue anyway..."
        return 0
    fi
}

# Convert file to base64
convert_to_base64() {
    local file="$1"

    if command -v base64 &> /dev/null; then
        # Linux (with -w option) or macOS (without -w)
        if base64 -w 0 "$file" 2>/dev/null; then
            return 0
        else
            base64 "$file" | tr -d '\n'
            return 0
        fi
    else
        print_error "base64 command not found"
        return 1
    fi
}

# Upload a single TOSCA file
upload_tosca_file() {
    local filename="$1"
    local description="$2"
    local filepath="$FILES_DIR/$filename"

    echo ""
    print_info "Processing: $description"
    print_detail "File: $filename"

    # Check file exists
    if [ ! -f "$filepath" ]; then
        print_error "File not found: $filepath"
        return 1
    fi

    # Get file size
    local size
    size=$(du -h "$filepath" | cut -f1)
    print_detail "Size: $size"

    # Convert to base64
    print_detail "Converting to base64..."
    local base64_content
    if ! base64_content=$(convert_to_base64 "$filepath"); then
        print_error "Failed to convert file to base64"
        return 1
    fi

    # Prepare JSON payload
    local json_payload
    json_payload=$(jq -n \
        --arg file "$base64_content" \
        --arg filename "$filename" \
        '{
            file: $file,
            filename: $filename,
            store_full_structure: true
        }')

    # Upload to OptimusDB
    print_detail "Uploading to $BASE_URL/swarmkb/upload..."

    local response
    local http_code

    response=$(curl -s -w "\n%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$json_payload" \
        "$BASE_URL/swarmkb/upload" 2>&1)

    http_code=$(echo "$response" | tail -n1)
    response=$(echo "$response" | sed '$d')

    # Check HTTP status
    if [ "$http_code" != "200" ]; then
        print_error "Upload failed with HTTP $http_code"
        print_detail "Response: $response"
        return 1
    fi

    # Parse response
    local status template_id queryable storage_location

    status=$(echo "$response" | jq -r '.status // empty')
    template_id=$(echo "$response" | jq -r '.data.template_id // empty')
    queryable=$(echo "$response" | jq -r '.data.queryable // false')
    storage_location=$(echo "$response" | jq -r '.data.storage_location // empty')

    if [ "$status" != "200" ]; then
        local error_msg
        error_msg=$(echo "$response" | jq -r '.message // "Unknown error"')
        print_error "Upload failed: $error_msg"
        return 1
    fi

    if [ -z "$template_id" ]; then
        print_error "No template ID returned in response"
        print_detail "Response: $response"
        return 1
    fi

    # Success!
    print_success "Upload successful"
    print_detail "Template ID: $template_id"
    print_detail "Queryable: $queryable"
    print_detail "Storage: $storage_location"

    # Store result
    echo "$response" | jq --arg desc "$description" \
        '.data | {
            filename: .filename,
            description: $desc,
            template_id: .template_id,
            queryable: .queryable,
            storage_location: .storage_location,
            uploaded_at: (now | strftime("%Y-%m-%dT%H:%M:%SZ"))
        }'

    return 0
}

# Save results to JSON file
save_results() {
    local results="$1"

    echo "$results" | jq -s '{
        upload_session: {
            timestamp: (now | strftime("%Y-%m-%dT%H:%M:%SZ")),
            base_url: env.BASE_URL,
            total_files: env.TOTAL_FILES | tonumber,
            uploaded: env.UPLOADED_COUNT | tonumber,
            failed: env.FAILED_COUNT | tonumber
        },
        templates: .
    }' > "$OUTPUT_FILE" \
        BASE_URL="$BASE_URL" \
        TOTAL_FILES="$TOTAL_FILES" \
        UPLOADED_COUNT="$UPLOADED_COUNT" \
        FAILED_COUNT="$FAILED_COUNT"

    print_success "Results saved to: $OUTPUT_FILE"
}

# Query uploaded templates
verify_uploads() {
    print_info "Verifying uploads..."

    local query_payload='{
        "method": {"cmd": "crudget", "argcnt": 1},
        "dstype": "dsswres",
        "criteria": []
    }'

    local response
    response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -d "$query_payload" \
        "$BASE_URL/swarmkb/command" 2>&1)

    local count
    count=$(echo "$response" | jq '.data | length')

    if [ -n "$count" ] && [ "$count" -gt 0 ]; then
        print_success "Verified: $count total templates in database"

        # Show TOSCA templates
        local tosca_count
        tosca_count=$(echo "$response" | jq '[.data[] | select(.tosca_definitions_version != null)] | length')
        print_detail "TOSCA templates: $tosca_count"

        return 0
    else
        print_warning "Could not verify uploads (query returned no results)"
        return 1
    fi
}

################################################################################
# Main Script
################################################################################

main() {
    print_header "OptimusDB TOSCA Upload Script"

    echo "Configuration:"
    echo "  Base URL: $BASE_URL"
    echo "  Files Directory: $FILES_DIR"
    echo "  Output File: $OUTPUT_FILE"
    echo "  Log File: $LOG_FILE"
    echo ""

    # Check dependencies
    print_info "Checking dependencies..."
    check_dependencies
    print_success "All dependencies available"

    # Test connectivity
    test_connectivity

    # Process each file
    print_header "Uploading TOSCA Files"

    local all_results=""

    for filename in "${!TOSCA_FILES[@]}"; do
        local description="${TOSCA_FILES[$filename]}"

        if result=$(upload_tosca_file "$filename" "$description"); then
            all_results="${all_results}${result}"$'\n'
            ((UPLOADED_COUNT++))
        else
            ((FAILED_COUNT++))
        fi

        # Brief pause between uploads
        sleep 1
    done

    # Summary
    echo ""
    print_header "Upload Summary"

    echo "Total Files:     $TOTAL_FILES"
    echo -e "Uploaded:        ${GREEN}$UPLOADED_COUNT${NC}"
    echo -e "Failed:          ${RED}$FAILED_COUNT${NC}"
    echo ""

    # Save results if any succeeded
    if [ "$UPLOADED_COUNT" -gt 0 ]; then
        save_results "$all_results"

        echo ""
        print_info "Template IDs saved to $OUTPUT_FILE"
        echo ""

        # Show uploaded templates
        echo "Uploaded Templates:"
        echo "$all_results" | jq -r 'select(.template_id != null) | "  ✓ \(.description)\n    ID: \(.template_id)"'

        # Verify uploads
        echo ""
        verify_uploads
    fi

    # Final status
    echo ""
    if [ "$FAILED_COUNT" -eq 0 ]; then
        print_header "✅ All Uploads Successful!"
        exit 0
    elif [ "$UPLOADED_COUNT" -gt 0 ]; then
        print_header "⚠️  Partial Success - Some Uploads Failed"
        exit 1
    else
        print_header "❌ All Uploads Failed"
        exit 1
    fi
}

# Run main function
main "$@"