# OptimusDB API Quick Reference

## Endpoint Summary Table

| Category | Method | Endpoint | Purpose |
|----------|--------|----------|---------|
| **Core Commands** | POST | `/{context}/command` | Universal command handler for data operations |
| **TOSCA** | POST | `/{context}/upload` | Upload and parse TOSCA templates |
| **Peers** | GET | `/{context}/peers` | List all discovered peers |
| **EMS Info** | GET | `/{context}/ems` | EMS endpoints information |
| **EMS Logs** | GET | `/{context}/ems/logs` | Retrieve EMS logs with filtering |
| **EMS Events** | GET | `/{context}/ems/events` | Retrieve EMS events |
| **EMS SQL** | GET/POST | `/{context}/ems/sql` | Execute SQL on logger DB |
| **Application Logs** | GET | `/{context}/log` | Get logs by date/hour |
| **Store Credential** | POST | `/{context}/credentials` | Store verifiable credential |
| **List Credentials** | GET | `/{context}/credentials` | List all credentials (paginated) |
| **Get Credential** | GET | `/{context}/credentials/get/{id}` | Get specific credential |
| **Query Credentials** | POST | `/{context}/credentials/query` | Advanced credential search |
| **Credentials by Issuer** | GET | `/{context}/credentials/issuer/{id}` | Get credentials by issuer |
| **Credentials by Subject** | GET | `/{context}/credentials/subject/{id}` | Get credentials by subject |
| **Revoke Credential** | POST | `/{context}/credentials/revoke` | Revoke a credential |
| **Verify Credential** | POST | `/{context}/credentials/verify` | Verify credential authenticity |
| **Enrich Dataset** | POST | `/api/v1/metadata/enrich` | AI-powered metadata enrichment |
| **Batch Enrich** | POST | `/api/v1/metadata/enrich-batch` | Enrich multiple datasets |
| **Profile Dataset** | GET | `/api/v1/metadata/profile` | Statistical profiling |
| **Metadata Metrics** | GET | `/api/v1/metadata/metrics` | Enrichment service metrics |
| **Metadata Health** | GET | `/api/v1/metadata/health` | Service health check |
| **Clear Cache** | DELETE | `/api/v1/metadata/cache` | Clear metadata cache |
| **Benchmarks** | GET | `/{context}/benchmarks` | Collect peer benchmarks |

## Command Types (via /command endpoint)

| Command | Purpose | Args |
|---------|---------|------|
| GET | Retrieve data | [key/query] |
| POST | Add data | [base64_file] |
| CONNECT | Connect to peer | [peer_address] |
| QUERY | Query data | [query_params] |
| BENCHMARK | Run benchmark | [] |
| SQLSELECT | Execute SQL | [sql_query] |
| HELP | Get help | [] |
| QUERYKBDATA | Query KB data | [params] |
| CRUDGET | CRUD get | [key] |
| CRUDPUT | CRUD put | [key, value] |

## Common Query Parameters

### EMS Logs & Events
- `limit` (1-1000, default: 50) - Results per page
- `level` (INFO|WARN|ERROR|DEBUG) - Filter by log level
- `since_min` (1-1440) - Time window in minutes

### Credentials
- `limit` (default: 50) - Results per page
- `offset` (default: 0) - Starting offset

### Metadata Profile
- `db` (required) - Database name
- `table` (required) - Table name
- `max_rows` (default: 200) - Sample size

### Application Logs
- `date` (required, YYYY-MM-DD) - Log date
- `hour` (required, 00-23) - Log hour

## Request/Response Formats

### Standard Success Response
```json
{
"status": 200,
"data": { /* response content */ }
}
```

### Standard Error Response
```json
{
"status": "error",
"message": "Error description",
"details": "Additional details"
}
```

## Configuration

- **Default Context:** `swarmkb` (configurable via `-context` flag)
- **Default Port:** `8089` (configurable via `-httpport` flag)
- **CORS:** Enabled for all origins
- **Metadata Cache TTL:** 24 hours

## Base URLs by Environment

| Environment | Base URL |
|-------------|----------|
| Local Development | `http://localhost:8089` |
| Docker Container | `http://container_ip:8089` |
| Kubernetes | `http://service_name:8089` |

## Authentication

Currently, no authentication is enforced. Consider implementing:
- API keys for production
- JWT tokens for user sessions
- DID-based authentication for credential operations

## Rate Limiting

Not currently implemented. Recommended limits for production:
- Command endpoint: 100 req/min per IP
- Metadata enrichment: 10 req/min per IP
- Query endpoints: 1000 req/min per IP

## Notes

1. All POST requests require `Content-Type: application/json`
2. File uploads use base64 encoding
3. Credentials follow W3C Verifiable Credentials spec
4. Metadata enrichment uses TinyLlama for AI generation
5. All timestamps are in ISO 8601 format (UTC)

---

**Quick Start:**
```bash
# Health check
curl http://localhost:8089/api/v1/metadata/health

# List peers
curl http://localhost:8089/swarmkb/peers

# Get recent EMS logs
curl "http://localhost:8089/swarmkb/ems/logs?limit=10"
```
