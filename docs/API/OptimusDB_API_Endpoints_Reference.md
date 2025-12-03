# OptimusDB API Endpoints Reference

**Generated:** December 3, 2025
**Context Path:** `/{context}` (default: `/swarmkb`)

---

## Table of Contents

1. [Core Command Endpoints](#core-command-endpoints)
2. [TOSCA Management](#tosca-management)
3. [Peer Discovery & Management](#peer-discovery--management)
4. [EMS (Event Management System)](#ems-event-management-system)
5. [Logging & Monitoring](#logging--monitoring)
6. [Credentials Management (DID)](#credentials-management-did)
7. [Metadata Enrichment](#metadata-enrichment)
8. [Benchmarking](#benchmarking)

---

## Core Command Endpoints

### POST `/{context}/command`

**Purpose:** Universal command handler for all data store operations

**Request Body:**
```json
{
"method": {
"cmd": "COMMAND_NAME",
"argCnt": 1
},
"args": ["arg1", "arg2"],
"dstype": "docstore|eventlog|keyvalue",
"criteria": [{"field": "value"}],
"UpdateData": [{"field": "new_value"}],
"graph_Traversal": [{"node": "value"}],
"sqldml": "SELECT * FROM table",
"file": "base64_encoded_content"
}
```

**Supported Methods:**
- `GET` - Retrieve data from stores
- `POST` - Add data to stores (requires base64-encoded file)
- `CONNECT` - Connect to peers
- `QUERY` - Query data
- `BENCHMARK` - Run benchmarks
- `SQLSELECT` - Execute SQL queries
- `HELP` - Get help information
- `QUERYKBDATA` - Query knowledge base data
- `CRUDGET` - CRUD get operation
- `CRUDPUT` - CRUD put operation

**Response:**
```json
{
"status": 200,
"data": { /* response data */ }
}
```

---

## TOSCA Management

### POST `/{context}/upload`

**Purpose:** Upload and parse TOSCA YAML templates

**Request Body:**
```json
{
"file": "base64_encoded_yaml_content",
"filename": "optional_filename.yaml"
}
```

**Process:**
1. Base64 decodes the YAML content
2. Parses TOSCA template
3. Computes template ID and node count
4. Persists to OrbitDB (DsTOSCA_Imported)
5. Adds raw YAML to IPFS
6. Indexes metadata in SQLite

**Response:**
```json
{
"status": 200,
"data": {
"message": "TOSCA uploaded successfully",
"template_id": "computed_hash",
"node_count": 5,
"filename": "mytosca.yaml",
"filesize": 1234,
"sha256": "file_hash"
}
}
```

---

## Peer Discovery & Management

### GET `/{context}/peers`

**Purpose:** Get list of all discovered peers in the network

**Response:**
```json
[
{
"ID": "QmPeerID123...",
"Addrs": [
"/ip4/192.168.1.100/tcp/4001",
"/ip6/::1/tcp/4001"
]
}
]
```

**Features:**
- Returns all peers tracked during discovery (mDNS, DHT, PubSub)
- Includes peer IDs and their multiaddresses
- Updated in real-time as peers are discovered

---

## EMS (Event Management System)

### GET `/{context}/ems`

**Purpose:** EMS info endpoint (returns available sub-endpoints)

**Response:**
```json
{
"status": 200,
"data": {
"hint": "Try /swarmkb/ems/logs and /swarmkb/ems/events"
}
}
```

---

### GET `/{context}/ems/logs`

**Purpose:** Retrieve EMS logs with filtering

**Query Parameters:**
- `limit` (int, default: 50, max: 1000) - Number of records to return
- `level` (string) - Filter by log level: INFO, WARN, ERROR, DEBUG
- `since_min` (int, max: 1440) - Logs from last N minutes

**Example:**
```
GET /{context}/ems/logs?limit=100&level=ERROR&since_min=60
```

**Response:**
```json
{
"records": [
{
"id": 1,
"timestamp": "2025-12-03T10:00:00Z",
"level": "ERROR",
"source": "ems",
"message": "Connection failed"
}
]
}
```

---

### GET `/{context}/ems/events`

**Purpose:** Retrieve EMS events (from ems_events table)

**Query Parameters:**
- `limit` (int, default: 50, max: 1000) - Number of events to return
- `since_min` (int, max: 1440) - Events from last N minutes

**Example:**
```
GET /{context}/ems/events?limit=50&since_min=30
```

**Response:**
```json
{
"records": [
{
"id": 1,
"received_at": "2025-12-03T10:00:00Z",
"client_id": "client123",
"topic": "/topic/events",
"action": "UPDATE",
"resource": "sensor_data",
"params": "{\"sensor_id\":\"temp01\"}",
"raw": "{\"full_event_data\":\"...\"}"
}
]
}
```

---

### GET/POST `/{context}/ems/sql`

**Purpose:** Execute arbitrary SQL queries against the EMS logger database

**GET Request:**
```
GET /{context}/ems/sql?q=SELECT%20*%20FROM%20ems_events%20LIMIT%2010
```

**POST Request:**
```json
{
"sql": "SELECT * FROM optimusLogger WHERE level = 'ERROR' LIMIT 10"
}
```

**Response:**
```json
{
"records": [
{ /* query results */ }
]
}
```

---

## Logging & Monitoring

### GET `/{context}/log`

**Purpose:** Retrieve application logs for a specific date and hour

**Query Parameters:**
- `date` (string, required) - Format: YYYY-MM-DD
- `hour` (string, required) - Format: HH (00-23)

**Example:**
```
GET /{context}/log?date=2025-12-03&hour=14
```

**Response:**
```json
[
{
"timestamp": "2025-12-03T14:30:00Z",
"level": "INFO",
"message": "System started successfully"
}
]
```

---

## Credentials Management (DID)

All credential endpoints support W3C Verifiable Credentials format.

### POST `/{context}/credentials`

**Purpose:** Store a new verifiable credential

**Request Body:**
```json
{
"@context": [
"https://www.w3.org/2018/credentials/v1"
],
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
}
```

**Response:**
```json
{
"success": true,
"message": "Credential stored successfully",
"credentialId": "http://example.edu/credentials/3732",
"storedAt": "2025-12-03T10:00:00Z"
}
```

---

### GET `/{context}/credentials`

**Purpose:** List all credentials with pagination

**Query Parameters:**
- `limit` (int, default: 50) - Number of credentials per page
- `offset` (int, default: 0) - Starting offset

**Example:**
```
GET /{context}/credentials?limit=20&offset=0
```

**Response:**
```json
{
"success": true,
"count": 20,
"credentials": [
{
"id": "credential_id",
"type": ["VerifiableCredential"],
"issuer": "did:example:issuer",
"subject": "did:example:subject",
"issuanceDate": "2025-01-01T00:00:00Z"
}
]
}
```

---

### GET `/{context}/credentials/get/{credentialID}`

**Purpose:** Retrieve a specific credential by ID

**Example:**
```
GET /{context}/credentials/get/http%3A%2F%2Fexample.edu%2Fcredentials%2F3732
```

**Response:**
```json
{
"success": true,
"credential": {
/* Full credential object */
},
"metadata": {
"storedAt": "2025-12-03T10:00:00Z",
"verifiedAt": null,
"revoked": false
}
}
```

---

### POST `/{context}/credentials/query`

**Purpose:** Advanced credential query with multiple filters

**Request Body:**
```json
{
"issuer": "did:example:issuer123",
"subject": "did:example:subject456",
"type": "VerifiableCredential",
"issuedAfter": "2025-01-01T00:00:00Z",
"issuedBefore": "2025-12-31T23:59:59Z",
"revoked": false
}
```

**Response:**
```json
{
"success": true,
"count": 5,
"query": { /* echoes query params */ },
"results": [
{ /* matching credentials */ }
]
}
```

---

### GET `/{context}/credentials/issuer/{issuerID}`

**Purpose:** Get all credentials issued by a specific issuer

**Example:**
```
GET /{context}/credentials/issuer/did%3Aexample%3Aissuer123
```

**Response:**
```json
{
"success": true,
"issuerId": "did:example:issuer123",
"count": 10,
"credentials": [
{ /* credential objects */ }
]
}
```

---

### GET `/{context}/credentials/subject/{subjectID}`

**Purpose:** Get all credentials for a specific subject

**Example:**
```
GET /{context}/credentials/subject/did%3Aexample%3Asubject456
```

**Response:**
```json
{
"success": true,
"subjectId": "did:example:subject456",
"count": 3,
"credentials": [
{ /* credential objects */ }
]
}
```

---

### POST `/{context}/credentials/revoke`

**Purpose:** Revoke a credential

**Request Body:**
```json
{
"credentialId": "http://example.edu/credentials/3732",
"reason": "Credential no longer valid"
}
```

**Response:**
```json
{
"success": true,
"message": "Credential revoked successfully",
"credentialId": "http://example.edu/credentials/3732"
}
```

---

### POST `/{context}/credentials/verify`

**Purpose:** Verify the authenticity of a credential

**Request Body:**
```json
{
/* Full verifiable credential object */
}
```

**Response:**
```json
{
"success": true,
"verified": true,
"errors": [],
"credential": {
"id": "credential_id",
"type": ["VerifiableCredential"],
"issuer": "did:example:issuer"
}
}
```

---

## Metadata Enrichment

All metadata endpoints are under `/api/v1/metadata` prefix.

### POST `/api/v1/metadata/enrich`

**Purpose:** Enrich a single dataset with AI-generated metadata

**Request Body:**
```json
{
"database": "renewable_energy",
"table": "solar_telemetry",
"max_rows": 200
}
```

**Response:**
```json
{
"status": "success",
"metadata": {
"table_name": "solar_telemetry",
"description": "Solar panel telemetry data",
"domain": "Renewable Energy",
"columns": [
{
"name": "timestamp",
"type": "TIMESTAMP",
"description": "Measurement timestamp"
}
],
"tags": ["solar", "renewable", "energy"],
"sample_queries": [
"SELECT * FROM solar_telemetry WHERE timestamp > '2025-01-01'"
]
},
"elapsed_ms": 1234,
"timestamp": "2025-12-03T10:00:00Z",
"cache_duration": "24h"
}
```

**Note:** Results are cached for 24 hours

---

### POST `/api/v1/metadata/enrich-batch`

**Purpose:** Enrich multiple datasets in parallel

**Request Body:**
```json
{
"datasets": [
{
"database": "renewable_energy",
"table": "solar_telemetry"
},
{
"database": "renewable_energy",
"table": "wind_telemetry"
}
]
}
```

**Response:**
```json
{
"status": "completed",
"total": 2,
"successful": 2,
"failed": 0,
"elapsed_ms": 2500,
"results": [
{
"database": "renewable_energy",
"table": "solar_telemetry",
"status": "success",
"metadata": { /* enriched metadata */ }
},
{
"database": "renewable_energy",
"table": "wind_telemetry",
"status": "success",
"metadata": { /* enriched metadata */ }
}
],
"timestamp": "2025-12-03T10:00:00Z",
"cache_duration": "24h"
}
```

---

### GET `/api/v1/metadata/profile`

**Purpose:** Profile a dataset (statistics without AI enrichment)

**Query Parameters:**
- `db` (string, required) - Database name
- `table` (string, required) - Table name
- `max_rows` (int, default: 200) - Maximum rows to sample

**Example:**
```
GET /api/v1/metadata/profile?db=renewable_energy&table=solar_telemetry&max_rows=100
```

**Response:**
```json
{
"status": "success",
"profile": {
"table_name": "solar_telemetry",
"row_count": 1000,
"columns": [
{
"name": "power_output",
"type": "REAL",
"null_count": 5,
"distinct_count": 987,
"min": 0.0,
"max": 5000.0,
"mean": 2500.5
}
]
},
"domain": "Renewable Energy",
"elapsed_ms": 45,
"timestamp": "2025-12-03T10:00:00Z"
}
```

---

### GET `/api/v1/metadata/metrics`

**Purpose:** Get metadata enrichment service metrics

**Response:**
```json
{
"status": "success",
"metrics": {
"total_enrichments": 150,
"cache_hits": 45,
"cache_misses": 105,
"avg_enrichment_time_ms": 1200,
"errors": 2,
"last_enrichment": "2025-12-03T09:55:00Z"
},
"timestamp": "2025-12-03T10:00:00Z"
}
```

---

### GET `/api/v1/metadata/health`

**Purpose:** Health check for metadata service and LLM connectivity

**Response:**
```json
{
"status": "healthy",
"llm_status": "healthy",
"llm_error": "",
"timestamp": "2025-12-03T10:00:00Z"
}
```

**LLM Status Values:**
- `healthy` - LLM service is responsive
- `available` - LLM client exists but not HTTP type
- `unavailable` - No LLM client configured
- `error` - LLM health check failed

---

### DELETE `/api/v1/metadata/cache`

**Purpose:** Clear the metadata cache (forces re-enrichment)

**Response:**
```json
{
"status": "success",
"message": "cache cleared",
"timestamp": "2025-12-03T10:00:00Z"
}
```

---

## Benchmarking

### GET `/{context}/benchmarks`

**Purpose:** Collect benchmark data from all connected peers

**Requirements:**
- Only available when benchmark flag is enabled (`-benchmark`)

**Response:**
```json
[
{
"peer_id": "QmPeer1...",
"cpu_usage": 45.2,
"memory_usage": 67.8,
"query_latency_ms": 23,
"throughput_qps": 150
},
{
"peer_id": "QmPeer2...",
"cpu_usage": 38.5,
"memory_usage": 55.2,
"query_latency_ms": 19,
"throughput_qps": 180
}
]
```

**Process:**
1. Queries all connected IPFS swarm peers
2. Sends benchmark request to each peer's command endpoint
3. Aggregates and returns results including local peer

---

## Authentication & Headers

### Common Headers

All endpoints support:
- `Content-Type: application/json`
- `Access-Control-Allow-Origin: *` (CORS enabled)
- `Access-Control-Allow-Methods: *`
- `Access-Control-Allow-Headers: Content-Type`

### Optional Headers

- `X-User` - Set uploader/user identity (used in TOSCA uploads)

---

## Error Handling

### Standard Error Response

```json
{
"status": "error",
"message": "Error description",
"details": "Additional error details"
}
```

### HTTP Status Codes

- `200 OK` - Successful request
- `201 Created` - Resource created successfully
- `400 Bad Request` - Invalid request parameters
- `404 Not Found` - Resource not found
- `405 Method Not Allowed` - HTTP method not supported
- `500 Internal Server Error` - Server-side error
- `503 Service Unavailable` - Service not ready

---

## Notes

1. **Context Path:** The `{context}` parameter is configurable via the `-context` flag (default: `swarmkb`)
2. **Port Configuration:** HTTP port is configurable via the `-httpport` flag (default: `8089`)
3. **Caching:** Metadata enrichment results are cached for 24 hours
4. **Rate Limiting:** Not currently implemented but recommended for production
5. **Authentication:** Currently not enforced - consider implementing for production deployments
6. **Logging:** All operations are logged to SQLite logger database

---

## Example Usage

### Using curl

```bash
# Get list of peers
curl http://localhost:8089/swarmkb/peers

# Upload TOSCA template
curl -X POST http://localhost:8089/swarmkb/upload \
-H "Content-Type: application/json" \
-d '{"file": "'$(base64 -w 0 template.yaml)'"}'

# Query EMS logs
curl "http://localhost:8089/swarmkb/ems/logs?limit=50&level=ERROR&since_min=60"

# Store a credential
curl -X POST http://localhost:8089/swarmkb/credentials \
-H "Content-Type: application/json" \
-d @credential.json

# Enrich dataset metadata
curl -X POST http://localhost:8089/api/v1/metadata/enrich \
-H "Content-Type: application/json" \
-d '{"database":"renewable_energy","table":"solar_telemetry","max_rows":200}'
```

---

**Document Version:** 1.0
**Last Updated:** December 3, 2025
**OptimusDB Version:** LSA Release
