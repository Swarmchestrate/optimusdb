# OptimusDB API Testing Guide

Quick testing examples using curl for all OptimusDB endpoints.

## Prerequisites

```bash
# Set base URL as environment variable
export OPTIMUSDB_URL="http://localhost:8089"
export CONTEXT="swarmkb"
```

---

## Core Commands

### Execute GET Command
```bash
curl -X POST "${OPTIMUSDB_URL}/${CONTEXT}/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "GET", "argCnt": 1},
"args": ["mykey"],
"dstype": "docstore"
}'
```

### Execute QUERY Command
```bash
curl -X POST "${OPTIMUSDB_URL}/${CONTEXT}/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "QUERY", "argCnt": 1},
"args": ["temperature > 25"],
"dstype": "docstore"
}'
```

### Execute SQL SELECT
```bash
curl -X POST "${OPTIMUSDB_URL}/${CONTEXT}/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "SQLSELECT", "argCnt": 1},
"sqldml": "SELECT * FROM renewable_energy.solar_telemetry LIMIT 10"
}'
```

---

## TOSCA Management

### Upload TOSCA Template
```bash
# From YAML file
TOSCA_B64=$(base64 -w 0 template.yaml)
curl -X POST "${OPTIMUSDB_URL}/${CONTEXT}/upload" \
-H "Content-Type: application/json" \
-d "{
\"file\": \"${TOSCA_B64}\",
\"filename\": \"template.yaml\"
}"

# Alternative: inline base64
curl -X POST "${OPTIMUSDB_URL}/${CONTEXT}/upload" \
-H "Content-Type: application/json" \
-d '{"file": "dG9zY2FfZGVmaW5pdGlvbnNfdmVyc2lvbjogdG9zY2Ffc2ltcGxlX3lhbWxfMV8z"}'
```

---

## Peer Discovery

### List All Peers
```bash
curl "${OPTIMUSDB_URL}/${CONTEXT}/peers"
```

### Pretty Print with jq
```bash
curl -s "${OPTIMUSDB_URL}/${CONTEXT}/peers" | jq '.'
```

---

## EMS (Event Management)

### Get EMS Info
```bash
curl "${OPTIMUSDB_URL}/${CONTEXT}/ems"
```

### Get Recent EMS Logs
```bash
# Last 50 ERROR logs from last hour
curl "${OPTIMUSDB_URL}/${CONTEXT}/ems/logs?limit=50&level=ERROR&since_min=60"

# All logs (last 100)
curl "${OPTIMUSDB_URL}/${CONTEXT}/ems/logs?limit=100"

# DEBUG logs from last 30 minutes
curl "${OPTIMUSDB_URL}/${CONTEXT}/ems/logs?limit=200&level=DEBUG&since_min=30"
```

### Get EMS Events
```bash
# Last 50 events
curl "${OPTIMUSDB_URL}/${CONTEXT}/ems/events?limit=50"

# Events from last 2 hours
curl "${OPTIMUSDB_URL}/${CONTEXT}/ems/events?limit=100&since_min=120"
```

### Execute SQL on EMS Database (GET)
```bash
# URL-encoded SQL query
curl "${OPTIMUSDB_URL}/${CONTEXT}/ems/sql?q=SELECT%20*%20FROM%20optimusLogger%20WHERE%20level=%27ERROR%27%20LIMIT%2010"

# Using URL encoding from command line
SQL_QUERY="SELECT * FROM ems_events WHERE action='UPDATE' LIMIT 5"
ENCODED=$(echo -n "$SQL_QUERY" | jq -sRr @uri)
curl "${OPTIMUSDB_URL}/${CONTEXT}/ems/sql?q=${ENCODED}"
```

### Execute SQL on EMS Database (POST)
```bash
curl -X POST "${OPTIMUSDB_URL}/${CONTEXT}/ems/sql" \
-H "Content-Type: application/json" \
-d '{
"sql": "SELECT * FROM optimusLogger WHERE level='\''ERROR'\'' ORDER BY id DESC LIMIT 10"
}'
```

---

## Application Logging

### Get Logs for Specific Date/Hour
```bash
# Today's logs at 14:00
DATE=$(date +%Y-%m-%d)
curl "${OPTIMUSDB_URL}/${CONTEXT}/log?date=${DATE}&hour=14"

# Specific date
curl "${OPTIMUSDB_URL}/${CONTEXT}/log?date=2025-12-03&hour=09"
```

---

## Credentials (DID)

### Store a Verifiable Credential
```bash
curl -X POST "${OPTIMUSDB_URL}/${CONTEXT}/credentials" \
-H "Content-Type: application/json" \
-d '{
"@context": ["https://www.w3.org/2018/credentials/v1"],
"id": "http://example.edu/credentials/3732",
"type": ["VerifiableCredential"],
"issuer": "did:example:issuer123",
"issuanceDate": "2025-01-01T00:00:00Z",
"credentialSubject": {
"id": "did:example:subject456",
"name": "John Doe",
"degree": "Bachelor of Science"
},
"proof": {
"type": "Ed25519Signature2020",
"created": "2025-01-01T00:00:00Z",
"verificationMethod": "did:example:issuer123#key-1",
"proofPurpose": "assertionMethod",
"proofValue": "z3FXQi..."
}
}'
```

### List All Credentials
```bash
# Default pagination
curl "${OPTIMUSDB_URL}/${CONTEXT}/credentials"

# With pagination
curl "${OPTIMUSDB_URL}/${CONTEXT}/credentials?limit=20&offset=0"

# Next page
curl "${OPTIMUSDB_URL}/${CONTEXT}/credentials?limit=20&offset=20"
```

### Get Specific Credential
```bash
# URL encode the credential ID
CRED_ID="http://example.edu/credentials/3732"
ENCODED_ID=$(echo -n "$CRED_ID" | jq -sRr @uri)
curl "${OPTIMUSDB_URL}/${CONTEXT}/credentials/get/${ENCODED_ID}"
```

### Query Credentials (Advanced Search)
```bash
curl -X POST "${OPTIMUSDB_URL}/${CONTEXT}/credentials/query" \
-H "Content-Type: application/json" \
-d '{
"issuer": "did:example:issuer123",
"type": "VerifiableCredential",
"issuedAfter": "2025-01-01T00:00:00Z",
"issuedBefore": "2025-12-31T23:59:59Z",
"revoked": false
}'
```

### Get Credentials by Issuer
```bash
ISSUER_ID="did:example:issuer123"
ENCODED_ISSUER=$(echo -n "$ISSUER_ID" | jq -sRr @uri)
curl "${OPTIMUSDB_URL}/${CONTEXT}/credentials/issuer/${ENCODED_ISSUER}"
```

### Get Credentials by Subject
```bash
SUBJECT_ID="did:example:subject456"
ENCODED_SUBJECT=$(echo -n "$SUBJECT_ID" | jq -sRr @uri)
curl "${OPTIMUSDB_URL}/${CONTEXT}/credentials/subject/${ENCODED_SUBJECT}"
```

### Revoke Credential
```bash
curl -X POST "${OPTIMUSDB_URL}/${CONTEXT}/credentials/revoke" \
-H "Content-Type: application/json" \
-d '{
"credentialId": "http://example.edu/credentials/3732",
"reason": "Credential expired or no longer valid"
}'
```

### Verify Credential
```bash
curl -X POST "${OPTIMUSDB_URL}/${CONTEXT}/credentials/verify" \
-H "Content-Type: application/json" \
-d @credential.json
```

---

## Metadata Enrichment

### Enrich Single Dataset
```bash
curl -X POST "${OPTIMUSDB_URL}/api/v1/metadata/enrich" \
-H "Content-Type: application/json" \
-d '{
"database": "renewable_energy",
"table": "solar_telemetry",
"max_rows": 200
}'
```

### Enrich Multiple Datasets (Batch)
```bash
curl -X POST "${OPTIMUSDB_URL}/api/v1/metadata/enrich-batch" \
-H "Content-Type: application/json" \
-d '{
"datasets": [
{"database": "renewable_energy", "table": "solar_telemetry"},
{"database": "renewable_energy", "table": "wind_telemetry"},
{"database": "renewable_energy", "table": "hydro_telemetry"}
]
}'
```

### Profile Dataset (Statistical Analysis)
```bash
curl "${OPTIMUSDB_URL}/api/v1/metadata/profile?db=renewable_energy&table=solar_telemetry&max_rows=100"
```

### Get Metadata Service Metrics
```bash
curl "${OPTIMUSDB_URL}/api/v1/metadata/metrics" | jq '.'
```

### Health Check
```bash
curl "${OPTIMUSDB_URL}/api/v1/metadata/health"
```

### Clear Metadata Cache
```bash
curl -X DELETE "${OPTIMUSDB_URL}/api/v1/metadata/cache"
```

---

## Benchmarking

### Get Peer Benchmarks
```bash
# Note: Requires -benchmark flag to be enabled
curl "${OPTIMUSDB_URL}/${CONTEXT}/benchmarks" | jq '.'
```

---

## Advanced Testing Scenarios

### Test Complete Workflow: TOSCA Upload + Metadata Enrichment
```bash
#!/bin/bash

# 1. Upload TOSCA template
echo "1. Uploading TOSCA template..."
TOSCA_B64=$(base64 -w 0 mytemplate.yaml)
UPLOAD_RESULT=$(curl -s -X POST "${OPTIMUSDB_URL}/${CONTEXT}/upload" \
-H "Content-Type: application/json" \
-d "{\"file\": \"${TOSCA_B64}\", \"filename\": \"mytemplate.yaml\"}")

echo "Upload Result: ${UPLOAD_RESULT}"

# 2. Verify it's in the system (via SQL)
echo "2. Querying TOSCA metadata..."
curl -s -X POST "${OPTIMUSDB_URL}/${CONTEXT}/ems/sql" \
-H "Content-Type: application/json" \
-d '{"sql": "SELECT * FROM tosca_metadata ORDER BY uploaded_at DESC LIMIT 1"}' \
| jq '.'

# 3. Enrich related datasets
echo "3. Enriching datasets..."
curl -s -X POST "${OPTIMUSDB_URL}/api/v1/metadata/enrich" \
-H "Content-Type: application/json" \
-d '{"database": "renewable_energy", "table": "solar_telemetry", "max_rows": 200}' \
| jq '.'
```

### Monitor System Health
```bash
#!/bin/bash

echo "=== OptimusDB System Health Check ==="
echo ""

# Check metadata service
echo "1. Metadata Service Health:"
curl -s "${OPTIMUSDB_URL}/api/v1/metadata/health" | jq '.'
echo ""

# Check peer connectivity
echo "2. Connected Peers:"
PEER_COUNT=$(curl -s "${OPTIMUSDB_URL}/${CONTEXT}/peers" | jq '. | length')
echo "Total Peers: ${PEER_COUNT}"
echo ""

# Check recent errors
echo "3. Recent Errors (last hour):"
curl -s "${OPTIMUSDB_URL}/${CONTEXT}/ems/logs?limit=10&level=ERROR&since_min=60" \
| jq '.records | length'
echo ""

# Check metadata metrics
echo "4. Metadata Service Metrics:"
curl -s "${OPTIMUSDB_URL}/api/v1/metadata/metrics" | jq '.'
```

### Credential Lifecycle Test
```bash
#!/bin/bash

# 1. Store credential
echo "1. Storing credential..."
CRED_ID="http://test.example/cred/$(date +%s)"
curl -s -X POST "${OPTIMUSDB_URL}/${CONTEXT}/credentials" \
-H "Content-Type: application/json" \
-d "{
\"@context\": [\"https://www.w3.org/2018/credentials/v1\"],
\"id\": \"${CRED_ID}\",
\"type\": [\"VerifiableCredential\"],
\"issuer\": \"did:test:issuer\",
\"issuanceDate\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
\"credentialSubject\": {
\"id\": \"did:test:subject\",
\"name\": \"Test Subject\"
}
}" | jq '.'

# 2. Retrieve credential
echo "2. Retrieving credential..."
ENCODED_ID=$(echo -n "$CRED_ID" | jq -sRr @uri)
curl -s "${OPTIMUSDB_URL}/${CONTEXT}/credentials/get/${ENCODED_ID}" | jq '.'

# 3. Query by issuer
echo "3. Querying by issuer..."
curl -s "${OPTIMUSDB_URL}/${CONTEXT}/credentials/issuer/did%3Atest%3Aissuer" | jq '.count'

# 4. Revoke credential
echo "4. Revoking credential..."
curl -s -X POST "${OPTIMUSDB_URL}/${CONTEXT}/credentials/revoke" \
-H "Content-Type: application/json" \
-d "{
\"credentialId\": \"${CRED_ID}\",
\"reason\": \"Test revocation\"
}" | jq '.'
```

---

## Performance Testing

### Benchmark Metadata Enrichment
```bash
#!/bin/bash

echo "Testing metadata enrichment performance..."

# Run 10 enrichments and measure time
for i in {1..10}; do
START=$(date +%s%N)
curl -s -X POST "${OPTIMUSDB_URL}/api/v1/metadata/enrich" \
-H "Content-Type: application/json" \
-d '{"database": "renewable_energy", "table": "solar_telemetry", "max_rows": 200}' \
> /dev/null
END=$(date +%s%N)
ELAPSED=$((($END - $START) / 1000000))
echo "Request $i: ${ELAPSED}ms"
done
```

### Stress Test Peer Discovery
```bash
#!/bin/bash

echo "Monitoring peer discovery..."
for i in {1..60}; do
PEER_COUNT=$(curl -s "${OPTIMUSDB_URL}/${CONTEXT}/peers" | jq '. | length')
echo "$(date +%H:%M:%S) - Peers: ${PEER_COUNT}"
sleep 5
done
```

---

## Troubleshooting

### Check if Service is Running
```bash
curl -f "${OPTIMUSDB_URL}/${CONTEXT}/peers" && echo "✓ Service is UP" || echo "✗ Service is DOWN"
```

### Test CORS Configuration
```bash
curl -H "Origin: http://example.com" \
-H "Access-Control-Request-Method: POST" \
-H "Access-Control-Request-Headers: Content-Type" \
-X OPTIONS \
--verbose \
"${OPTIMUSDB_URL}/${CONTEXT}/peers"
```

### Validate JSON Responses
```bash
# Check if response is valid JSON
curl -s "${OPTIMUSDB_URL}/${CONTEXT}/peers" | jq empty && echo "✓ Valid JSON" || echo "✗ Invalid JSON"
```

### Get Detailed Error Information
```bash
# Enable verbose output
curl -v -X POST "${OPTIMUSDB_URL}/${CONTEXT}/command" \
-H "Content-Type: application/json" \
-d '{"method": {"cmd": "GET", "argCnt": 1}}'
```

---

## Environment-Specific URLs

### Local Development
```bash
export OPTIMUSDB_URL="http://localhost:8089"
```

### Docker Container
```bash
export OPTIMUSDB_URL="http://172.17.0.2:8089"  # Replace with actual container IP
```

### Kubernetes Service
```bash
export OPTIMUSDB_URL="http://swarmkb-service:8089"
```

### Remote Server
```bash
export OPTIMUSDB_URL="http://192.168.1.100:8089"  # Replace with actual server IP
```

---

## Tips & Best Practices

1. **Use jq for JSON formatting:**
```bash
curl -s "${OPTIMUSDB_URL}/api/v1/metadata/health" | jq '.'
```

2. **Save responses to files:**
```bash
curl "${OPTIMUSDB_URL}/${CONTEXT}/peers" > peers.json
```

3. **Use variables for repeated values:**
```bash
DB="renewable_energy"
TABLE="solar_telemetry"
curl "${OPTIMUSDB_URL}/api/v1/metadata/profile?db=${DB}&table=${TABLE}"
```

4. **Include timestamps in logs:**
```bash
echo "[$(date)] Starting test..." >> test.log
curl "${OPTIMUSDB_URL}/${CONTEXT}/peers" >> test.log 2>&1
```

5. **Test error handling:**
```bash
# Send invalid JSON
curl -X POST "${OPTIMUSDB_URL}/${CONTEXT}/command" \
-H "Content-Type: application/json" \
-d 'invalid json'
```

---

## Quick Reference Commands

```bash
# Health check
curl "${OPTIMUSDB_URL}/api/v1/metadata/health"

# List peers
curl "${OPTIMUSDB_URL}/${CONTEXT}/peers"

# Recent errors
curl "${OPTIMUSDB_URL}/${CONTEXT}/ems/logs?limit=10&level=ERROR"

# List credentials
curl "${OPTIMUSDB_URL}/${CONTEXT}/credentials?limit=10"

# Enrich metadata
curl -X POST "${OPTIMUSDB_URL}/api/v1/metadata/enrich" \
-H "Content-Type: application/json" \
-d '{"database":"renewable_energy","table":"solar_telemetry","max_rows":200}'
```

---

**Testing Date:** December 3, 2025
**OptimusDB Version:** LSA Release
