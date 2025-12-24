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

        echo -e "${GREEN} Uploaded successfully${NC}"
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
    echo -e "${CYAN} Template IDs saved to: uploaded_tosca_ids.json${NC}"
fi