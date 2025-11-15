# OptimusDB Complete API Endpoints

**Base URL:** `http://localhost:18001/...contextUrl...`
**Default Context:** `swarm`
**Agent Ports:** 18001-18008 (mapped to internal 8089)

---

## Table of Contents

1. [Main Query Endpoint](#main-query-endpoint)
2. [TOSCA Upload Endpoint](#tosca-upload-endpoint)
3. [Peers Discovery](#peers-discovery)
4. [EMS Integration Endpoints](#ems-integration-endpoints)
5. [Metadata Enrichment Endpoints](#metadata-enrichment-endpoints)
6. [Credentials (DID) Endpoints](#credentials-did-endpoints)
7. [Monitoring Endpoints](#monitoring-endpoints)
8. [Data Models](#data-models)

---

## Main Query Endpoint

### POST `/...contextUrl.../command`

The primary endpoint for all database operations (queries, inserts, updates, deletes).

**Request Body:**
```json
{
"method": {
"cmd": "METHOD_NAME",
"argcnt": 0
},
"args": [],
"dstype": "DATASTORE_TYPE",
"criteria": [],
"UpdateData": [],
"sqldml": "",
"graph_Traversal": [],
"options": {
"strategy": "LOCAL_THEN_REMOTE_MERGE",
"consistency": "BEST_EFFORT",
"time_budget_ms": 2000,
"quorum_n": 3,
"min_rows": 10,
"stale_ok_ttl_ms": 5000,
"max_peers": 5,
"include_local": true,
"annotate_source": true
}
}
```

**Available Methods:**

| Method | cmd | argcnt | Description |
|--------|-----|--------|-------------|
| `GET` | `"get"` | 1 | Get file from IPFS (needs IPFS filepath) |
| `POST` | `"post"` | 1 | Post file to IPFS (needs base64 encoded file) |
| `CONNECT` | `"connect"` | 1 | Connect to peer (needs peer address) |
| `QUERY` | `"query"` | 0 | Execute query with criteria |
| `QUERYKBDATA` | `"querykbdata"` | 2 | Query knowledge base data |
| `SQLSELECT` | `"sqlselect"` | 1 | Execute SQL SELECT statement |
| `SQLDML` | `"sqldml"` | 1 | Execute SQL DML (INSERT/UPDATE/DELETE) |
| `CRUDGET` | `"crudget"` | 1 | CRUD read operation |
| `CRUDPUT` | `"crudput"` | 1 | CRUD create operation |
| `CRUDUPDATE` | `"crudupdate"` | 1 | CRUD update operation |
| `CRUDDELETE` | `"cruddelete"` | 1 | CRUD delete operation |
| `CONTRI` | `"contri"` | 1 | Add contribution |
| `BENCHMARK` | `"benchmark"` | 0 | Get benchmark data |
| `HELP` | `"help"` | 0 | Get help information |

**Query Strategies:**

- `"LOCAL_ONLY"` - Query only local OrbitDB store
- `"REMOTE_ONLY"` - Query only remote peers
- `"LOCAL_THEN_REMOTE_MERGE"` - Query local first, then remote, merge results
- `"PARALLEL_MERGE"` - Query local and remote concurrently, merge and dedupe
- `"QUORUM"` - Query until quorum_n peers respond

**Consistency Levels:**

- `"BEST_EFFORT"` - Return as much data as available within time budget
- `"QUORUM"` - Honor quorum_n requirement
- `"ALL"` - Wait for all peers (bounded by time budget)

**Example 1: SQL SELECT Query**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {
"cmd": "sqlselect",
"argcnt": 1
},
"args": ["SELECT * FROM renewable_assets WHERE capacity_kw > 1000 LIMIT 10"]
}'
```

**Example 2: Criteria-Based Query with Options**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {
"cmd": "query",
"argcnt": 0
},
"criteria": [
{
"Field": "asset_type",
"Operator": "=",
"Value": "solar"
},
{
"Field": "capacity_kw",
"Operator": ">",
"Value": 2000
}
],
"options": {
"strategy": "LOCAL_THEN_REMOTE_MERGE",
"time_budget_ms": 2000,
"include_local": true,
"annotate_source": true,
"max_peers": 5
}
}'
```

**Example 3: SQL INSERT (DML)**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {
"cmd": "sqldml",
"argcnt": 1
},
"sqldml": "INSERT INTO renewable_assets (asset_id, asset_name, asset_type, capacity_kw) VALUES ('\''solar_099'\'', '\''Test Farm'\'', '\''solar'\'', 2500)"
}'
```

**Response Structure:**
```json
{
"status": "success",
"data": [
{
"_id": "asset_solar_001",
"asset_name": "Solar Farm Alpha",
"asset_type": "solar",
"capacity_kw": 5000,
"_source": {
"type": "local",
"peer_id": "",
"path": ["12D3KooW..."]
},
"_trace": {
"id": "trace_abc123",
"path": ["12D3KooW..."]
}
}
],
"count": 10,
"execution_time_ms": 45
}
```

---

## TOSCA Upload Endpoint

### POST `/...contextUrl.../upload`

Upload TOSCA (Topology and Orchestration Specification for Cloud Applications) templates.

**Request Body:**
```json
{
"file": "BASE64_ENCODED_YAML_CONTENT",
"filename": "my-tosca-template.yaml"
}
```

**Example:**
```bash
# Encode TOSCA file
BASE64_TOSCA=$(base64 -w 0 my-template.yaml)

curl -X POST "http://localhost:18001/swarm/upload" \
-H "Content-Type: application/json" \
-d "{
\"file\": \"$BASE64_TOSCA\",
\"filename\": \"my-template.yaml\"
}"
```

**Response:**
```json
{
"status": "success",
"message": "TOSCA uploaded successfully",
"template_id": "5f7b1e4c3d8a9b2e1f6c8d4a",
"node_count": 5,
"filename": "my-template.yaml",
"filesize": 2048,
"sha256": "a3c5f8d9e2b4f7a1c3d5e7f9b2c4d6e8f0a2b4c6d8e0f2a4b6c8d0e2f4a6b8c0"
}
```

**What Gets Stored:**

1. **OrbitDB (DsTOSCA_Imported):**
```json
{
"_id": "template_id_hash",
"type": "tosca_template",
"description": "Template description from YAML",
"nodeCount": 5,
"yaml": "original YAML content",
"createdAt": "2025-11-11T12:00:00Z"
}
```

2. **SQLite (toscametadata table):**
```sql
INSERT INTO toscametadata (
template_id, description, node_templates_count,
filename, filesize_bytes, content_sha256, ipfs_path,
uploader, source_pod, source_ip
) VALUES (...)
```

---

## Peers Discovery

### GET `/...contextUrl.../peers`

Get list of discovered peers in the P2P network.

**Example:**
```bash
curl -X GET "http://localhost:18001/swarm/peers"
```

**Response:**
```json
[
{
"ID": "12D3KooWBhXv9F3wQzKjPy8xL5mN7oR9sT1uV3wX5yZ7aB9cD1eF2",
"Addrs": [
"/ip4/192.168.1.100/tcp/14001",
"/ip4/192.168.1.100/udp/13001/quic"
]
},
{
"ID": "12D3KooWGhIj3K4lMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUv",
"Addrs": [
"/ip4/192.168.1.101/tcp/14002"
]
}
]
```

---

## EMS Integration Endpoints

### GET `/...contextUrl.../ems`

Get EMS connection status.

**Example:**
```bash
curl -X GET "http://localhost:18001/swarm/ems"
```

**Response:**
```json
{
"status": "connected",
"broker": "ems-broker.messaging.svc.cluster.local:61610",
"client_id": "optimusdb-agent-1",
"topic": "/topic/>",
"durable": true,
"last_message": "2025-11-11T12:00:00Z"
}
```

### GET `/...contextUrl.../ems/logs`

Retrieve EMS event logs.

**Query Parameters:**
- `limit` (default: 50, max: 1000) - Maximum log entries to return
- `level` (optional) - Filter by log level: `INFO`, `WARN`, `ERROR`, `DEBUG`
- `since_min` (optional, max: 1440) - Time window in minutes

**Example:**
```bash
curl -X GET "http://localhost:18001/swarm/ems/logs?limit=100&level=ERROR&since_min=60"
```

**Response:**
```json
{
"success": true,
"records": [
{
"id": 1247,
"timestamp": "2025-11-11T11:58:30Z",
"level": "INFO",
"source": "ems",
"message": "EMS recv action=update resource=capacity body={...}"
},
{
"id": 1248,
"timestamp": "2025-11-11T11:59:15Z",
"level": "ERROR",
"source": "ems",
"message": "EMS recv (unmarshal failed): {invalid json structure}"
}
]
}
```

### GET `/...contextUrl.../ems/events`

Retrieve detailed EMS events with full message payloads.

**Query Parameters:**
- `limit` (default: 50, max: 1000) - Maximum events to return
- `since_min` (optional, max: 1440) - Time window in minutes

**Example:**
```bash
curl -X GET "http://localhost:18001/swarm/ems/events?limit=50&since_min=120"
```

**Response:**
```json
{
"success": true,
"records": [
{
"id": 523,
"received_at": "2025-11-11T11:58:30Z",
"client_id": "optimusdb-agent-1",
"topic": "/topic/capacity.updates",
"action": "update",
"resource": "renewable_capacity",
"params": "{\"asset_id\":\"solar_042\",\"new_capacity\":5200}",
"raw": "{\"action\":\"update\",\"resource\":\"renewable_capacity\",\"params\":{\"asset_id\":\"solar_042\",\"new_capacity\":5200}}"
}
]
}
```

### GET/POST `/...contextUrl.../ems/sql`

Execute SQL queries against the EMS events database.

**GET Request:**
```bash
curl -X GET "http://localhost:18001/swarm/ems/sql?q=SELECT%20COUNT(*)%20FROM%20ems_events%20WHERE%20action='update'"
```

**POST Request:**
```bash
curl -X POST "http://localhost:18001/swarm/ems/sql" \
-H "Content-Type: application/json" \
-d '{
"sql": "SELECT action, COUNT(*) as count FROM ems_events GROUP BY action ORDER BY count DESC"
}'
```

**Response:**
```json
{
"success": true,
"records": [
{
"action": "update",
"count": 1247
},
{
"action": "create",
"count": 523
},
{
"action": "delete",
"count": 89
}
]
}
```

**EMS Message Format (what to send to EMS broker):**
```json
{
"action": "update",
"resource": "renewable_capacity",
"params": {
"asset_id": "solar_042",
"new_capacity": 5200,
"timestamp": "2025-11-11T12:00:00Z"
}
}
```

---

## Metadata Enrichment Endpoints

All metadata endpoints use the `/api/v1` prefix.

### POST `/api/v1/metadata/enrich`

Trigger AI-powered contextual metadata generation for a specific dataset.

**Request Body:**
```json
{
"database": "swarmkb",
"table": "renewable_assets",
"max_rows": 200
}
```

**Example:**
```bash
curl -X POST "http://localhost:18001/api/v1/metadata/enrich" \
-H "Content-Type: application/json" \
-d '{
"database": "swarmkb",
"table": "renewable_assets",
"max_rows": 200
}'
```

**Response:**
```json
{
"status": "success",
"metadata": {
"table_name": "renewable_assets",
"description": "Collection of renewable energy generation assets including solar, wind, and battery storage facilities with capacity and operational metadata",
"key_columns": ["asset_id", "asset_name", "asset_type", "capacity_kw"],
"domain": "renewable_energy",
"suggested_queries": [
"SELECT * FROM renewable_assets WHERE asset_type = 'solar' AND capacity_kw > 1000",
"SELECT asset_type, AVG(capacity_kw) FROM renewable_assets GROUP BY asset_type"
],
"data_quality_score": 0.89,
"completeness": 0.96
},
"elapsed_ms": 1523,
"timestamp": "2025-11-11T12:00:00Z",
"cache_duration": "24h"
}
```

### POST `/api/v1/metadata/enrich-batch`

Batch metadata enrichment for multiple tables.

**Request Body:**
```json
{
"datasets": [
{
"database": "swarmkb",
"table": "renewable_assets"
},
{
"database": "swarmkb",
"table": "tosca_templates"
}
]
}
```

**Example:**
```bash
curl -X POST "http://localhost:18001/api/v1/metadata/enrich-batch" \
-H "Content-Type: application/json" \
-d '{
"datasets": [
{"database": "swarmkb", "table": "renewable_assets"},
{"database": "swarmkb", "table": "tosca_templates"}
]
}'
```

**Response:**
```json
{
"status": "completed",
"total": 2,
"successful": 2,
"failed": 0,
"elapsed_ms": 3045,
"results": [
{
"database": "swarmkb",
"table": "renewable_assets",
"status": "success",
"metadata": { "..." }
},
{
"database": "swarmkb",
"table": "tosca_templates",
"status": "success",
"metadata": { "..." }
}
],
"timestamp": "2025-11-11T12:00:00Z",
"cache_duration": "24h"
}
```

### GET `/api/v1/metadata/profile`

Get data profiling statistics for a dataset.

**Query Parameters:**
- `db` (required) - Database name
- `table` (required) - Table name
- `max_rows` (optional, default: 200) - Rows to profile

**Example:**
```bash
curl -X GET "http://localhost:18001/api/v1/metadata/profile?db=swarmkb&table=renewable_assets&max_rows=500"
```

**Response:**
```json
{
"status": "success",
"profile": {
"row_count": 487,
"column_count": 18,
"columns": [
{
"name": "asset_id",
"type": "string",
"non_null_count": 487,
"unique_count": 487,
"completeness": 1.0
},
{
"name": "capacity_kw",
"type": "number",
"non_null_count": 485,
"min": 100,
"max": 5500,
"mean": 2345.7,
"median": 2100,
"stddev": 892.3,
"completeness": 0.996
}
],
"data_quality_score": 0.88
},
"domain": "renewable_energy",
"elapsed_ms": 245,
"timestamp": "2025-11-11T12:00:00Z"
}
```

### GET `/api/v1/metadata/metrics`

Get metadata enrichment system metrics.

**Example:**
```bash
curl -X GET "http://localhost:18001/api/v1/metadata/metrics"
```

**Response:**
```json
{
"status": "success",
"metrics": {
"total_enrichments": 15247,
"cache_hits": 11892,
"cache_misses": 3355,
"cache_hit_rate": 0.78,
"average_enrichment_time_ms": 265,
"llm_requests_total": 3355,
"llm_requests_failed": 10,
"llm_error_rate": 0.003,
"last_enrichment": "2025-11-11T11:59:45Z"
},
"timestamp": "2025-11-11T12:00:00Z"
}
```

### GET `/api/v1/metadata/health`

Health check for metadata enrichment service.

**Example:**
```bash
curl -X GET "http://localhost:18001/api/v1/metadata/health"
```

**Response:**
```json
{
"status": "healthy",
"llm_status": "healthy",
"llm_error": "",
"timestamp": "2025-11-11T12:00:00Z"
}
```

### DELETE `/api/v1/metadata/cache`

Clear the metadata enrichment cache.

**Example:**
```bash
curl -X DELETE "http://localhost:18001/api/v1/metadata/cache"
```

**Response:**
```json
{
"status": "success",
"message": "cache cleared",
"timestamp": "2025-11-11T12:00:00Z"
}
```

---

## Credentials (DID) Endpoints

All credential endpoints follow W3C Verifiable Credentials standard.

### POST `/...contextUrl.../credentials`

Store a new Verifiable Credential.

**Request Body:**
```json
{
"@context": [
"https://www.w3.org/2018/credentials/v1",
"https://www.w3.org/2018/credentials/examples/v1"
],
"id": "http://example.edu/credentials/3732",
"type": ["VerifiableCredential", "AgentCredential"],
"issuer": "did:example:issuer123",
"issuanceDate": "2025-11-11T12:00:00Z",
"expirationDate": "2026-11-11T12:00:00Z",
"credentialSubject": {
"id": "did:example:agent1",
"name": "OptimusDB Agent 1",
"role": "coordinator",
"trustScore": 0.95
},
"proof": {
"type": "Ed25519Signature2020",
"created": "2025-11-11T12:00:00Z",
"proofPurpose": "assertionMethod",
"verificationMethod": "did:example:issuer123#keys-1",
"proofValue": "base64_encoded_signature..."
}
}
```

**Example:**
```bash
curl -X POST "http://localhost:18001/swarm/credentials" \
-H "Content-Type: application/json" \
-d @credential.json
```

**Response:**
```json
{
"success": true,
"message": "Credential stored successfully",
"credential_id": "http://example.edu/credentials/3732",
"orbitdb_hash": "zdpuB2Wgjkz8fHqGHvP3nF4kL7mR9sT1xY5aZ6bC8dE2gF3hI",
"timestamp": "2025-11-11T12:00:15Z"
}
```

### GET `/...contextUrl.../credentials`

List all stored credentials.

**Example:**
```bash
curl -X GET "http://localhost:18001/swarm/credentials"
```

**Response:**
```json
{
"success": true,
"count": 25,
"credentials": [
{
"id": "http://example.edu/credentials/3732",
"type": ["VerifiableCredential", "AgentCredential"],
"issuer": "did:example:issuer123",
"subject": "did:example:agent1",
"issuanceDate": "2025-11-11T12:00:00Z",
"status": "valid"
}
]
}
```

### GET `/...contextUrl.../credentials/get/...credentialID...`

Get a specific credential by ID.

**Example:**
```bash
curl -X GET "http://localhost:18001/swarm/credentials/get/http://example.edu/credentials/3732"
```

**Response:**
```json
{
"success": true,
"credential": {
"@context": [...],
"id": "http://example.edu/credentials/3732",
"type": ["VerifiableCredential", "AgentCredential"],
"issuer": "did:example:issuer123",
"credentialSubject": {...},
"proof": {...}
}
}
```

### POST `/...contextUrl.../credentials/query`

Advanced query for credentials with filters.

**Request Body:**
```json
{
"issuer": "did:example:issuer123",
"type": "AgentCredential",
"subject": "did:example:agent1",
"status": "valid",
"limit": 50
}
```

**Example:**
```bash
curl -X POST "http://localhost:18001/swarm/credentials/query" \
-H "Content-Type: application/json" \
-d '{
"type": "AgentCredential",
"status": "valid"
}'
```

### GET `/...contextUrl.../credentials/issuer/...issuerID...`

Get all credentials issued by a specific issuer.

**Example:**
```bash
curl -X GET "http://localhost:18001/swarm/credentials/issuer/did:example:issuer123"
```

### GET `/...contextUrl.../credentials/subject/...subjectID...`

Get all credentials for a specific subject.

**Example:**
```bash
curl -X GET "http://localhost:18001/swarm/credentials/subject/did:example:agent1"
```

### POST `/...contextUrl.../credentials/revoke`

Revoke a credential.

**Request Body:**
```json
{
"credential_id": "http://example.edu/credentials/3732",
"reason": "Agent decommissioned"
}
```

**Example:**
```bash
curl -X POST "http://localhost:18001/swarm/credentials/revoke" \
-H "Content-Type: application/json" \
-d '{
"credential_id": "http://example.edu/credentials/3732",
"reason": "Agent decommissioned"
}'
```

### POST `/...contextUrl.../credentials/verify`

Verify a credential's authenticity and validity.

**Request Body:**
```json
{
"@context": [...],
"id": "http://example.edu/credentials/3732",
"type": ["VerifiableCredential"],
"issuer": "did:example:issuer123",
"proof": {...}
}
```

**Example:**
```bash
curl -X POST "http://localhost:18001/swarm/credentials/verify" \
-H "Content-Type: application/json" \
-d @credential_to_verify.json
```

**Response:**
```json
{
"success": true,
"verified": true,
"errors": [],
"credential": {
"id": "http://example.edu/credentials/3732",
"type": ["VerifiableCredential", "AgentCredential"],
"issuer": "did:example:issuer123"
},
"checks": {
"signature_valid": true,
"not_expired": true,
"issuer_trusted": true,
"not_revoked": true
}
}
```

---

## Monitoring Endpoints

### GET `/...contextUrl.../log`

Get application logs for a specific date and hour.

**Query Parameters:**
- `date` (required) - Date in YYYY-MM-DD format
- `hour` (required) - Hour in HH format (00-23)

**Example:**
```bash
curl -X GET "http://localhost:18001/swarm/log?date=2025-11-11&hour=12"
```

**Response:**
```json
{
"success": true,
"logs": [
{
"id": 54321,
"timestamp": "2025-11-11T12:15:30Z",
"date": "2025-11-11",
"hour": "12",
"level": "INFO",
"message": "Query executed successfully",
"source": "query_engine"
}
]
}
```

### GET `/...contextUrl.../benchmarks`

Get performance benchmarking data (requires `--benchmark` flag).

**Example:**
```bash
curl -X GET "http://localhost:18001/swarm/benchmarks"
```

**Response:**
```json
{
"success": true,
"agent_id": "Agent-1",
"region": "us-west-2",
"benchmark": {
"bootstrap": 12.45,
"averagec": 0.342,
"minc": 0.085,
"maxc": 1.523,
"samples": [
{
"ts": "2025-11-11T12:00:00Z",
"membytes": 2147483648,
"cpupercent": 35.2
}
]
}
}
```

---

## Data Models

### FilterCriterion

Used in query criteria arrays.

```typescript
{
"Field": string,      // Field name to filter on
"Operator": string,   // "=", ">", "<", ">=", "<=", "!="
"Value": any          // Value to compare against
}
```

### Method

Defines the operation to perform.

```typescript
{
"cmd": string,        // Method command name
"argcnt": number      // Number of required arguments
}
```

### QueryOptions

Advanced query configuration.

```typescript
{
"strategy": "LOCAL_THEN_REMOTE_MERGE",
"consistency": "BEST_EFFORT",
"time_budget_ms": 2000,
"quorum_n": 3,
"min_rows": 10,
"stale_ok_ttl_ms": 5000,
"max_peers": 5,
"include_local": true,
"annotate_source": true
}
```

### VerifiableCredential (W3C Standard)

```typescript
{
"@context": string[],
"id": string,
"type": string[],
"issuer": string | object,
"issuanceDate": string,     // ISO 8601
"expirationDate": string,   // ISO 8601, optional
"credentialSubject": object,
"proof": {
"type": string,
"created": string,
"proofPurpose": string,
"verificationMethod": string,
"proofValue": string,
"jws": string,
"challenge": string,
"domain": string
}
}
```

### EMSMessage

EMS event message format.

```typescript
{
"action": "create" | "update" | "delete",
"resource": string,
"params": {
[key: string]: any
}
}
```

### Contribution

Contribution tracking structure.

```typescript
{
"agentname": string,
"path": string,           // IPFS path
"contributor": string,    // IPFS node ID
"creationTS": string,     // ISO 8601
"localip": string,
"nodeip": string,
"remoteIPs": string[]
}
```

---

## Complete Integration Example

```bash
#!/bin/bash

# 1. Check system health
echo "1. Checking system health..."
curl -s http://localhost:18001/swarm/log?date=2025-11-11&hour=12 | jq '.logs | length'

# 2. Query renewable assets
echo -e "\n2. Querying renewable assets..."
curl -s -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqlselect", "argcnt": 1},
"args": ["SELECT * FROM renewable_assets WHERE asset_type = '\''solar'\'' LIMIT 5"]
}' | jq '.'

# 3. Insert new asset
echo -e "\n3. Inserting new asset..."
curl -s -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqldml", "argcnt": 1},
"sqldml": "INSERT INTO renewable_assets (asset_id, asset_name, asset_type, capacity_kw) VALUES ('\''solar_test_001'\'', '\''Test Solar Farm'\'', '\''solar'\'', 3000)"
}' | jq '.'

# 4. Enrich metadata
echo -e "\n4. Enriching metadata..."
curl -s -X POST "http://localhost:18001/api/v1/metadata/enrich" \
-H "Content-Type: application/json" \
-d '{
"database": "swarmkb",
"table": "renewable_assets",
"max_rows": 200
}' | jq '.status, .elapsed_ms'

# 5. Get EMS events
echo -e "\n5. Getting EMS events..."
curl -s "http://localhost:18001/swarm/ems/events?limit=5" | jq '.records | length'

# 6. Check metadata health
echo -e "\n6. Checking metadata service health..."
curl -s http://localhost:18001/api/v1/metadata/health | jq '.llm_status'

# 7. Get peers
echo -e "\n7. Getting peer list..."
curl -s http://localhost:18001/swarm/peers | jq 'length'

echo -e "\n✅ Integration test complete!"
```

---

## Error Response Format

All endpoints return errors in this format:

```json
{
"status": "error",
"message": "Human-readable error description",
"code": "ERROR_CODE",
"details": {
"field": "specific_field_if_applicable"
}
}
```

---

## Notes

1. **Port Configuration:** All examples use Agent 1 on port 18001. For other agents, use ports 18002-18008.

2. **Context Path:** Default context is `swarm` but can be changed via `--context` flag.

3. **Authentication:** Currently operates in trusted cluster mode. For production, implement mTLS or JWT.

4. **Rate Limiting:** No built-in rate limiting. Implement via reverse proxy if needed.

5. **CORS:** Not configured by default. Add middleware if needed for web applications.

6. **Timeouts:** Default query timeout is 2000ms. Adjust via `time_budget_ms` option.

---


**OptimusDB Version:** Latest (November 2025)
**EU Horizon Europe Grant:** 101135012 (Swarmchestrate Project)