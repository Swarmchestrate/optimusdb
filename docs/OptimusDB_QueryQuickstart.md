# OptimusDB Query Guide
## How to Query by Context, Datastore, and Input Type

**Version:** November 11, 2025
**EU Horizon Europe Grant:** 101135012 (Swarmchestrate Project)

---

## Table of Contents

1. [Overview](#overview)
2. [Understanding OptimusDB Architecture](#understanding-optimusdb-architecture)
3. [Available Datastores](#available-datastores)
4. [Query Methods](#query-methods)
5. [Query by Input Type](#query-by-input-type)
6. [Query by Datastore (DSType)](#query-by-datastore-dstype)
7. [Query Strategies](#query-strategies)
8. [Complete Query Examples](#complete-query-examples)
9. [Advanced Query Patterns](#advanced-query-patterns)
10. [Performance Optimization](#performance-optimization)

---

## Overview

OptimusDB is a decentralized knowledge base system that uses **OrbitDB** (CRDT-based) for distributed data storage and **SQLite** for local indexing. Understanding how to query effectively requires knowledge of:

1. **Datastores** - Where your data lives (doc stores + SQLite)
2. **Query Methods** - How to access the data (SQL, Criteria, CRUD)
3. **Query Strategies** - Where to fetch from (local, remote, both)
4. **Input Types** - What format to send your query in

---

## Understanding OptimusDB Architecture

### Data Storage Layers

```
┌─────────────────────────────────────────────────────────┐
│              Application Layer (HTTP API)                │
│              Port 18001-18008 → Internal 8089            │
└─────────────────────────────────────────────────────────┘
↓
┌─────────────────────────────────────────────────────────┐
│            Query Engine & Strategy Router                │
│   (LOCAL_ONLY, REMOTE_ONLY, LOCAL_THEN_REMOTE_MERGE)   │
└─────────────────────────────────────────────────────────┘
↓
┌──────────────────┴──────────────────┐
↓                                      ↓
┌──────────────────┐                  ┌──────────────────┐
│   Doc Stores     │                  │  SQLite Database │
│   (Distributed)  │                  │     (Local)      │
├──────────────────┤                  ├──────────────────┤
│ • Contributions  │                  │ • datacatalog    │
│ • Validations    │                  │ • toscametadata  │
│ • KBdata         │                  │ • ems_events     │
│ • KBMetadata     │                  │ • optimusLogger  │
│ • DsSWres        │                  └──────────────────┘
│ • DsSWresaloc    │
│ • DsTOSCA_*      │
└──────────────────┘
↓
┌──────────────────┐
│  IPFS (Storage)  │
│  Replication &   │
│  Content Addr.   │
└──────────────────┘
```

### Key Concepts

1. **Context Path**: The URL prefix (default: `swarm`)
- Example: `/swarm/command`, `/swarm/upload`
- Configured via `--context` flag

2. **Datastore (DSType)**: Specifies which Doc store to query
- Each store has different access controls and purposes
- Defaults to `DsSWres` if not specified

3. **Query Strategy**: Determines query execution path
- `LOCAL_ONLY`: Query only local data-store replica
- `REMOTE_ONLY`: Query remote peers only
- `LOCAL_THEN_REMOTE_MERGE`: Local first, then remote if needed
- `PARALLEL_MERGE`: Concurrent local + remote
- `QUORUM`: Wait for N peers to respond

---

## Available Datastores

### 1. **Contributions** (EventLog)
- **Type**: EventLog (immutable, append-only)
- **Access**: Write: *, Read: * (public)
- **Purpose**: Track all contributions (file uploads) to the knowledge base
- **Replication**: Yes (distributed across all agents)
- **DSType Value**: N/A (accessed via special methods)

**Structure:**
```json
{
"agentname": "Agent-1",
"path": "/ipfs/QmXx...",
"contributor": "12D3KooW...",
"creationTS": "2025-11-11T12:00:00Z",
"localip": "192.168.1.100",
"nodeip": "10.0.0.5",
"remoteIPs": ["192.168.1.101", "192.168.1.102"]
}
```

**Query Example:**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "contri", "argcnt": 1},
"args": []
}'
```

---

### 2. **Validations** (DocumentStore)
- **Type**: DocumentStore (CRDT key-value)
- **Access**: Write: Owner only, Read: Owner only (private)
- **Purpose**: Store validation results for contributions (peer voting)
- **Replication**: No (local only)
- **DSType Value**: `"validations"`

**Structure:**
```json
{
"_id": "unique_validation_id",
"path": "/ipfs/QmXx...",
"isValid": true,
"voteCnt": 5
}
```

**Query Example:**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "validations",
"criteria": [
{"Field": "isValid", "Operator": "=", "Value": true}
]
}'
```

---

### 3. **KBdata** (DocumentStore)
- **Type**: DocumentStore
- **Access**: Write: *, Read: * (public)
- **Purpose**: Store renewable energy asset data, metrics, measurements
- **Replication**: Yes
- **DSType Value**: `"kbdata"` (implicitly used)

**Structure:**
```json
{
"_id": "asset_solar_001",
"asset_name": "Solar Farm Alpha",
"asset_type": "solar",
"capacity_kw": 5000,
"location": {
"lat": 37.7749,
"lon": -122.4194
},
"metadata": {
"description": "Large-scale solar facility",
"tags": ["solar", "renewable", "grid-connected"]
}
}
```

**Query Example:**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"dstype": "kbdata",
"criteria": [
{"Field": "asset_type", "Operator": "=", "Value": "solar"},
{"Field": "capacity_kw", "Operator": ">", "Value": 2000}
]
}'
```

---

### 4. **KBMetadata** (DocumentStore)
- **Type**: DocumentStore
- **Access**: Write: *, Read: * (public)
- **Purpose**: Store AI-generated contextual metadata for datasets
- **Replication**: Yes
- **DSType Value**: N/A (accessed via `/api/v1/metadata/*`)

**Structure:**
```json
{
"_id": "metadata_renewable_assets",
"table_name": "renewable_assets",
"description": "AI-generated description...",
"key_columns": ["asset_id", "asset_name"],
"domain": "renewable_energy",
"data_quality_score": 0.89
}
```

---

### 5. **DsSWres** (DocumentStore) - Default
- **Type**: DocumentStore
- **Access**: Write: *, Read: * (public)
- **Purpose**: General-purpose data store for resources, assets, generic documents
- **Replication**: Yes
- **DSType Value**: `""` (empty/default) or `"dsswres"`

**This is the DEFAULT datastore** - if you don't specify `dstype`, queries go here.

**Structure:**
```json
{
"_id": "resource_wind_042",
"resource_type": "wind_turbine",
"resource_name": "Offshore Wind 42",
"resource_grpname": "Northern Wind Farm",
"resource_tags": ["offshore", "wind", "3MW"],
"resource_def": "turbine_model_xyz"
}
```

**Query Example:**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqlselect", "argcnt": 1},
"args": ["SELECT * FROM dsswres WHERE resource_type = '\''wind_turbine'\''"]
}'
```

---

### 6. **DsSWresaloc** (DocumentStore)
- **Type**: DocumentStore
- **Access**: Write: *, Read: * (public)
- **Purpose**: Resource allocation tracking
- **Replication**: Yes
- **DSType Value**: `"dsswresaloc"`

---

### 7. **DsTOSCA_Imported** (DocumentStore)
- **Type**: DocumentStore
- **Access**: Write: *, Read: * (public)
- **Purpose**: Store uploaded TOSCA templates (full YAML content)
- **Replication**: Yes
- **DSType Value**: `"tosca"` or accessed via `/swarm/upload` endpoint

**Structure:**
```json
{
"_id": "template_hash_abc123",
"type": "tosca_template",
"description": "Kubernetes deployment template",
"nodeCount": 5,
"yaml": "tosca_definitions_version: ...",
"createdAt": "2025-11-11T12:00:00Z"
}
```

**Query Example:**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "tosca",
"criteria": [
{"Field": "nodeCount", "Operator": ">", "Value": 3}
]
}'
```

---

### 8. **SQLite Databases** (Local Only)

#### **datacatalog** Table
- **Purpose**: Local index of knowledge base metadata
- **Access**: SQL queries only
- **Replication**: No (local to each agent)

**Schema:**
```sql
CREATE TABLE datacatalog (
_id VARCHAR(36) PRIMARY KEY,
author VARCHAR(255),
metadata_type VARCHAR(255),
component VARCHAR(255),
behaviour VARCHAR(255),
relationships TEXT,
associated_id VARCHAR(36),
name VARCHAR(255),
description TEXT,
tags VARCHAR(255),
status VARCHAR(50),
created_by VARCHAR(255),
created_at TIMESTAMP,
updated_at TIMESTAMP,
related_ids VARCHAR(255),
priority VARCHAR(50),
scheduling_info VARCHAR(255),
sla_constraints VARCHAR(255),
ownership_details VARCHAR(255),
audit_trail VARCHAR(255)
);
```

**Query Example:**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqlselect", "argcnt": 1},
"args": ["SELECT * FROM datacatalog WHERE metadata_type = '\''RenewableAsset'\'' AND status = '\''Active'\'' LIMIT 10"]
}'
```

#### **toscametadata** Table
- **Purpose**: TOSCA template metadata index
- **Access**: SQL queries only

**Schema:**
```sql
CREATE TABLE toscametadata (
id INTEGER PRIMARY KEY AUTOINCREMENT,
template_id TEXT NOT NULL UNIQUE,
description TEXT,
node_templates_count INTEGER,
created_at TEXT,
filename TEXT,
filesize_bytes INTEGER,
content_sha256 TEXT,
ipfs_path TEXT,
uploader TEXT,
source_pod TEXT,
source_ip TEXT
);
```

**Query Example:**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqlselect", "argcnt": 1},
"args": ["SELECT template_id, description, node_templates_count, filename FROM toscametadata WHERE node_templates_count > 5 ORDER BY created_at DESC"]
}'
```

---

## Query Methods

OptimusDB supports multiple query methods, each optimized for different use cases:

### 1. **SQL SELECT** (Method: `sqlselect`)
- **Best For**: Complex queries, joins, aggregations, sorting
- **Executes On**: SQLite database (local only by default)
- **Returns**: Result rows as JSON array

**Example:**
```json
{
"method": {"cmd": "sqlselect", "argcnt": 1},
"args": ["SELECT asset_type, COUNT(*) as count, AVG(capacity_kw) as avg_capacity FROM datacatalog WHERE metadata_type = 'RenewableAsset' GROUP BY asset_type ORDER BY count DESC"]
}
```

---

### 2. **SQL DML** (Method: `sqldml`)
- **Best For**: INSERT, UPDATE, DELETE operations
- **Executes On**: SQLite database
- **Returns**: Affected rows count

**Insert Example:**
```json
{
"method": {"cmd": "sqldml", "argcnt": 1},
"sqldml": "INSERT INTO datacatalog (_id, author, metadata_type, name, description, status, created_at) VALUES ('asset_123', 'Agent-1', 'RenewableAsset', 'Solar Farm', 'New solar installation', 'Active', datetime('now'))"
}
```

**Update Example:**
```json
{
"method": {"cmd": "sqldml", "argcnt": 1},
"sqldml": "UPDATE datacatalog SET status = 'Inactive', updated_at = datetime('now') WHERE _id = 'asset_123'"
}
```

**Delete Example:**
```json
{
"method": {"cmd": "sqldml", "argcnt": 1},
"sqldml": "DELETE FROM datacatalog WHERE status = 'Archived' AND updated_at < datetime('now', '-1 year')"
}
```

---

### 3. **Criteria-Based Query** (Method: `query`)
- **Best For**: Simple filters, distributed queries across peers
- **Executes On**: Data Store (can query remote peers)
- **Returns**: Matching documents with source annotations

**Example:**
```json
{
"method": {"cmd": "query", "argcnt": 0},
"criteria": [
{"Field": "asset_type", "Operator": "=", "Value": "solar"},
{"Field": "capacity_kw", "Operator": ">=", "Value": 1000},
{"Field": "status", "Operator": "!=", "Value": "Decommissioned"}
],
"options": {
"strategy": "LOCAL_THEN_REMOTE_MERGE",
"include_local": true,
"max_peers": 5,
"time_budget_ms": 2000
}
}
```

**Supported Operators:**
- `=` - Equal
- `!=` - Not equal
- `>` - Greater than
- `<` - Less than
- `>=` - Greater than or equal
- `<=` - Less than or equal

---

### 4. **CRUD Operations**

#### **CRUDGET** (Method: `crudget`)
- **Best For**: Fetching documents from Data stores
- **Executes On**: Specified datastore

```json
{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "validations",
"criteria": [
{"Field": "isValid", "Operator": "=", "Value": true}
]
}
```

#### **CRUDPUT** (Method: `crudput`)
- **Best For**: Inserting documents into Data stores
- **Executes On**: DsSWres (default) and KBMetadata (auto-generated)

```json
{
"method": {"cmd": "crudput", "argcnt": 1},
"criteria": [
{
"_id": "resource_wind_099",
"resource_type": "wind_turbine",
"resource_name": "Wind Turbine 99",
"resource_grpname": "Eastern Wind Farm",
"resource_tags": ["wind", "onshore", "2.5MW"],
"resource_def": "turbine_gen3"
}
]
}
```

**Note:** CRUDPUT automatically generates metadata and stores it in KBMetadata store!

#### **CRUDUPDATE** (Method: `crudupdate`)
- **Best For**: Updating existing documents

```json
{
"method": {"cmd": "crudupdate", "argcnt": 1},
"dstype": "dsswres",
"UpdateData": [
{
"_id": "resource_wind_099",
"status": "Maintenance",
"updated_at": "2025-11-11T12:00:00Z"
}
]
}
```

#### **CRUDDELETE** (Method: `cruddelete`)
- **Best For**: Removing documents

```json
{
"method": {"cmd": "cruddelete", "argcnt": 1},
"dstype": "dsswres",
"criteria": [
{"Field": "_id", "Operator": "=", "Value": "resource_wind_099"}
]
}
```

---

### 5. **IPFS Operations**

#### **GET** (Method: `get`)
- **Best For**: Retrieving files from IPFS
- **Returns**: File content

```json
{
"method": {"cmd": "get", "argcnt": 1},
"args": ["/ipfs/QmXx7vK8fP9mNxZkJ4Y3bL2wR5tH8gD9cA1fE6hM3nB4jP7"]
}
```

#### **POST** (Method: `post`)
- **Best For**: Uploading files to IPFS
- **Input**: Base64-encoded file content

```json
{
"method": {"cmd": "post", "argcnt": 1},
"file": "BASE64_ENCODED_CONTENT_HERE"
}
```

---

## Query by Input Type

### Input Type 1: SQL Queries

**Use When:**
- Need complex queries (JOINs, GROUP BY, aggregations)
- Working with SQLite indexed data
- Need SQL-specific functions (date functions, string operations)

**Example: Complex Aggregation**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqlselect", "argcnt": 1},
"args": ["
SELECT
asset_type,
COUNT(*) as total_assets,
AVG(capacity_kw) as avg_capacity,
MAX(capacity_kw) as max_capacity,
MIN(capacity_kw) as min_capacity
FROM datacatalog
WHERE metadata_type = '\''RenewableAsset'\''
AND status = '\''Active'\''
GROUP BY asset_type
ORDER BY total_assets DESC
"]
}'
```

**Response:**
```json
{
"status": "success",
"data": [
{
"asset_type": "solar",
"total_assets": 150,
"avg_capacity": 2345.6,
"max_capacity": 5500,
"min_capacity": 100
},
{
"asset_type": "wind",
"total_assets": 89,
"avg_capacity": 3102.4,
"max_capacity": 6000,
"min_capacity": 500
}
]
}
```

---

### Input Type 2: Criteria Arrays

**Use When:**
- Need distributed queries across peer network
- Want automatic source annotation (`_source`, `_trace`)
- Building dynamic filters programmatically

**Example: Multi-Condition Filter**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"criteria": [
{"Field": "asset_type", "Operator": "=", "Value": "solar"},
{"Field": "capacity_kw", "Operator": ">=", "Value": 2000},
{"Field": "location.state", "Operator": "=", "Value": "California"}
],
"options": {
"strategy": "PARALLEL_MERGE",
"include_local": true,
"annotate_source": true,
"max_peers": 8,
"time_budget_ms": 3000
}
}'
```

**Response with Source Annotation:**
```json
{
"status": "success",
"data": [
{
"_id": "asset_solar_042",
"asset_type": "solar",
"capacity_kw": 5200,
"location": {"state": "California"},
"_source": {
"type": "local",
"peer_id": "",
"path": ["12D3KooWAgent1..."]
},
"_trace": {
"id": "trace_abc123",
"path": ["12D3KooWAgent1..."]
}
},
{
"_id": "asset_solar_088",
"asset_type": "solar",
"capacity_kw": 3100,
"location": {"state": "California"},
"_source": {
"type": "peer",
"peer_id": "12D3KooWAgent3...",
"path": ["12D3KooWAgent1...", "12D3KooWAgent3..."]
},
"_trace": {
"id": "trace_abc123",
"path": ["12D3KooWAgent1...", "12D3KooWAgent3..."]
}
}
],
"count": 2
}
```

---

### Input Type 3: JSON Documents (CRUD)

**Use When:**
- Inserting/updating complete documents
- Working with Datastores directly
- Need automatic metadata generation

**Example: Bulk Insert**
```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudput", "argcnt": 1},
"criteria": [
{
"_id": "asset_battery_001",
"resource_type": "battery_storage",
"resource_name": "Grid Battery Alpha",
"resource_grpname": "Energy Storage Network",
"resource_tags": ["battery", "lithium-ion", "10MWh"],
"capacity_kwh": 10000,
"charge_rate_kw": 2500,
"discharge_rate_kw": 2500,
"efficiency_percent": 92.5
},
{
"_id": "asset_battery_002",
"resource_type": "battery_storage",
"resource_name": "Grid Battery Beta",
"resource_grpname": "Energy Storage Network",
"resource_tags": ["battery", "flow-battery", "20MWh"],
"capacity_kwh": 20000,
"charge_rate_kw": 5000,
"discharge_rate_kw": 5000,
"efficiency_percent": 85.0
}
]
}'
```

**What Happens:**
1. Documents inserted into **DsSWres** datastore
2. Metadata auto-generated for each document
3. Metadata inserted into **KBMetadata** datastore
4. Both replicated to other peers
5. Response includes OrbitDB hashes

---

## Query by Datastore (DSType)

### Pattern 1: Default Datastore Query (DsSWres)

**No DSType specified → Queries DsSWres**

```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"criteria": [
{"Field": "resource_type", "Operator": "=", "Value": "wind_turbine"}
]
}'
```

---

### Pattern 2: Validations Datastore Query

**DSType: `"validations"`**

```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "validations",
"criteria": [
{"Field": "isValid", "Operator": "=", "Value": true},
{"Field": "voteCnt", "Operator": ">=", "Value": 3}
]
}'
```

---

### Pattern 3: TOSCA Datastore Query

**DSType: `"tosca"`**

```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "tosca",
"criteria": [
{"Field": "nodeCount", "Operator": ">", "Value": 5},
{"Field": "type", "Operator": "=", "Value": "tosca_template"}
]
}'
```

---

### Pattern 4: Resource Allocation Datastore

**DSType: `"dsswresaloc"`**

```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "dsswresaloc",
"criteria": [
{"Field": "allocation_status", "Operator": "=", "Value": "active"}
]
}'
```

---

## Query Strategies

### Strategy 1: LOCAL_ONLY

**Use When:**
- Fast response more important than completeness
- Working with known local data
- Testing or debugging

**Example:**
```json
{
"method": {"cmd": "query", "argcnt": 0},
"criteria": [
{"Field": "asset_type", "Operator": "=", "Value": "solar"}
],
"options": {
"strategy": "LOCAL_ONLY",
"include_local": true,
"time_budget_ms": 500
}
}
```

**Execution Flow:**
```
Query → Local Datastore Replica → Return Results
```

**Performance:** ~50-100ms
**Completeness:** Only local data

---

### Strategy 2: REMOTE_ONLY

**Use When:**
- Need data from specific peers
- Local data known to be incomplete
- Load balancing queries across cluster

**Example:**
```json
{
"method": {"cmd": "query", "argcnt": 0},
"criteria": [
{"Field": "asset_type", "Operator": "=", "Value": "wind"}
],
"options": {
"strategy": "REMOTE_ONLY",
"include_local": false,
"max_peers": 5,
"time_budget_ms": 2000
}
}
```

**Execution Flow:**
```
Query → Peer 1, Peer 2, ... Peer N (parallel) → Merge Results
```

**Performance:** ~200-500ms
**Completeness:** High (across N peers)

---

### Strategy 3: LOCAL_THEN_REMOTE_MERGE (Default)

**Use When:**
- Want best of both: speed + completeness
- Acceptable to get remote data only if local insufficient
- **Most Common Strategy**

**Example:**
```json
{
"method": {"cmd": "query", "argcnt": 0},
"criteria": [
{"Field": "capacity_kw", "Operator": ">", "Value": 5000}
],
"options": {
"strategy": "LOCAL_THEN_REMOTE_MERGE",
"include_local": true,
"max_peers": 5,
"time_budget_ms": 2000,
"min_rows": 10
}
}
```

**Execution Flow:**
```
Query → Local Datastore
↓
Results < min_rows?
↓
Yes → Query Remote Peers → Merge & Dedupe → Return
No  → Return Local Results
```

**Performance:** ~100-400ms
**Completeness:** High (falls back to remote if needed)

---

### Strategy 4: PARALLEL_MERGE

**Use When:**
- Need maximum completeness quickly
- Can afford higher resource usage
- Data highly distributed across peers

**Example:**
```json
{
"method": {"cmd": "query", "argcnt": 0},
"criteria": [
{"Field": "status", "Operator": "=", "Value": "Active"}
],
"options": {
"strategy": "PARALLEL_MERGE",
"include_local": true,
"max_peers": 8,
"time_budget_ms": 3000,
"annotate_source": true
}
}
```

**Execution Flow:**
```
┌→ Local Datastore →┐
Query →      │                  │→ Merge & Dedupe → Return
└→ Remote Peers  →┘
(concurrent)
```

**Performance:** ~150-350ms
**Completeness:** Very High (all sources simultaneously)

---

### Strategy 5: QUORUM

**Use When:**
- Need consensus from multiple peers
- Data consistency critical
- Can wait for N confirmations

**Example:**
```json
{
"method": {"cmd": "query", "argcnt": 0},
"criteria": [
{"Field": "critical_metric", "Operator": ">", "Value": 0.95}
],
"options": {
"strategy": "QUORUM",
"consistency": "QUORUM",
"quorum_n": 5,
"include_local": true,
"max_peers": 8,
"time_budget_ms": 5000
}
}
```

**Execution Flow:**
```
Query → Peer 1, Peer 2, ... Peer N
↓
Wait for quorum_n responses
↓
Merge & Return (or timeout)
```

**Performance:** ~500-2000ms
**Completeness:** Guaranteed N confirmations

---

## Complete Query Examples

### Example 1: Find All High-Capacity Solar Assets Across Cluster

**Scenario:** You need all solar assets with capacity > 3000 kW from any agent in the cluster.

```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"criteria": [
{"Field": "asset_type", "Operator": "=", "Value": "solar"},
{"Field": "capacity_kw", "Operator": ">", "Value": 3000}
],
"options": {
"strategy": "PARALLEL_MERGE",
"include_local": true,
"annotate_source": true,
"max_peers": 8,
"time_budget_ms": 2000
}
}'
```

**Response:**
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
"peer_id": ""
}
},
{
"_id": "asset_solar_042",
"asset_name": "Solar Mega Complex",
"asset_type": "solar",
"capacity_kw": 5500,
"_source": {
"type": "peer",
"peer_id": "12D3KooWAgent5..."
}
}
],
"count": 2,
"execution_time_ms": 287
}
```

---

### Example 2: SQL Query with Complex Join and Aggregation

**Scenario:** Generate a report of renewable assets by type with averages.

```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqlselect", "argcnt": 1},
"args": ["
SELECT
d.metadata_type,
d.component as asset_type,
COUNT(*) as total,
ROUND(AVG(CAST(d.sla_constraints as INTEGER)), 2) as avg_sla,
GROUP_CONCAT(d.tags) as all_tags
FROM datacatalog d
WHERE d.status = '\''Active'\''
AND d.created_at > datetime('\''now'\'', '\''-30 days'\'')
GROUP BY d.metadata_type, d.component
HAVING COUNT(*) >= 5
ORDER BY total DESC
"]
}'
```

---

### Example 3: Insert Asset with Auto-Generated Metadata

**Scenario:** Add a new wind turbine, OptimusDB will auto-generate metadata.

```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudput", "argcnt": 1},
"criteria": [
{
"_id": "asset_wind_250",
"resource_type": "wind_turbine",
"resource_name": "Offshore Wind 250",
"resource_grpname": "Atlantic Wind Farm",
"resource_tags": ["offshore", "wind", "6MW", "deep-water"],
"resource_def": "turbine_gen5_6mw",
"capacity_kw": 6000,
"hub_height_m": 120,
"rotor_diameter_m": 180,
"location": {
"lat": 40.5,
"lon": -73.8,
"depth_m": 45
},
"commissioning_date": "2025-06-15",
"status": "Active"
}
]
}'
```

**What Happens:**
1. Document stored in **DsSWres**
2. AI metadata generated:
```json
{
"_id": "meta_asset_wind_250",
"associated_id": "asset_wind_250",
"metadata_type": "Resource",
"name": "Metadata for Offshore Wind 250",
"description": "Metadata auto-generated for resource: Offshore Wind 250",
"component": "wind_turbine-turbine_gen5_6mw",
"behaviour": "Auto-Generated",
"tags": ["offshore", "wind", "6MW", "deep-water"],
"status": "Active",
"created_at": "2025-11-11T12:00:00Z"
}
```
3. Metadata stored in **KBMetadata**
4. Both replicated across cluster

---

### Example 4: Query TOSCA Templates by Node Count

**Scenario:** Find all TOSCA templates with more than 10 node templates.

```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudget", "argcnt": 1},
"dstype": "tosca",
"criteria": [
{"Field": "nodeCount", "Operator": ">", "Value": 10},
{"Field": "type", "Operator": "=", "Value": "tosca_template"}
],
"options": {
"strategy": "LOCAL_THEN_REMOTE_MERGE",
"include_local": true,
"max_peers": 5
}
}'
```

---

### Example 5: Query with Quorum for Critical Data

**Scenario:** Get validation results that have been confirmed by at least 5 peers.

```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "query", "argcnt": 0},
"criteria": [
{"Field": "isValid", "Operator": "=", "Value": true}
],
"options": {
"strategy": "QUORUM",
"consistency": "QUORUM",
"quorum_n": 5,
"include_local": true,
"max_peers": 8,
"time_budget_ms": 5000
}
}'
```

---

## Advanced Query Patterns

### Pattern 1: Paginated Results

**For Large Datasets:**

```bash
# Page 1 (first 50 results)
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqlselect", "argcnt": 1},
"args": ["SELECT * FROM datacatalog WHERE status = '\''Active'\'' ORDER BY created_at DESC LIMIT 50 OFFSET 0"]
}'

# Page 2 (next 50 results)
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqlselect", "argcnt": 1},
"args": ["SELECT * FROM datacatalog WHERE status = '\''Active'\'' ORDER BY created_at DESC LIMIT 50 OFFSET 50"]
}'
```

---

### Pattern 2: Time-Series Queries

**Query by Date Range:**

```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqlselect", "argcnt": 1},
"args": ["
SELECT
DATE(created_at) as date,
COUNT(*) as assets_created
FROM datacatalog
WHERE created_at BETWEEN '\''2025-01-01'\'' AND '\''2025-11-11'\''
AND metadata_type = '\''RenewableAsset'\''
GROUP BY DATE(created_at)
ORDER BY date DESC
"]
}'
```

---

### Pattern 3: Full-Text Search Simulation

**Using SQLite LIKE:**

```bash
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqlselect", "argcnt": 1},
"args": ["
SELECT * FROM datacatalog
WHERE (description LIKE '\''%solar%'\'' OR name LIKE '\''%solar%'\'')
AND status = '\''Active'\''
LIMIT 20
"]
}'
```

---

### Pattern 4: Conditional Inserts (Upsert Simulation)

**Insert only if not exists:**

```bash
# Step 1: Check if exists
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqlselect", "argcnt": 1},
"args": ["SELECT COUNT(*) as count FROM datacatalog WHERE _id = '\''asset_123'\''"]
}'

# Step 2: If count = 0, insert
curl -X POST "http://localhost:18001/swarm/command" \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "sqldml", "argcnt": 1},
"sqldml": "INSERT INTO datacatalog (_id, name, status) VALUES ('\''asset_123'\'', '\''New Asset'\'', '\''Active'\'')"
}'
```

---

## Performance Optimization

### Tip 1: Choose the Right Strategy

| Scenario | Recommended Strategy | Expected Latency |
|----------|---------------------|------------------|
| Quick local lookup | LOCAL_ONLY | 50-100ms |
| Need completeness | PARALLEL_MERGE | 150-350ms |
| Balance speed/completeness | LOCAL_THEN_REMOTE_MERGE | 100-400ms |
| Critical consensus | QUORUM | 500-2000ms |
| Remote-only data | REMOTE_ONLY | 200-500ms |

---

### Tip 2: Optimize Time Budget

```json
{
"options": {
"time_budget_ms": 2000,  // 2 second timeout
"min_rows": 10,           // Stop early if >= 10 results
"stale_ok_ttl_ms": 5000   // Use cached remote results within 5s
}
}
```

---

### Tip 3: Limit Peer Fan-Out

```json
{
"options": {
"max_peers": 5  // Query only top 5 peers by reputation
}
}
```

---

### Tip 4: Use SQLite for Complex Operations

**✅ Good: Use SQL for aggregations**
```sql
SELECT asset_type, AVG(capacity_kw) FROM datacatalog GROUP BY asset_type
```

**❌ Bad: Fetch all, aggregate in app**
```json
// Don't do this - inefficient
{"method": {"cmd": "crudget"}, "criteria": []}
// Then aggregate in your application
```

---

### Tip 5: Index Important Fields in SQLite

```sql
-- Add indexes for frequently queried fields
CREATE INDEX IF NOT EXISTS idx_datacatalog_type ON datacatalog(metadata_type);
CREATE INDEX IF NOT EXISTS idx_datacatalog_status ON datacatalog(status);
CREATE INDEX IF NOT EXISTS idx_datacatalog_created ON datacatalog(created_at);
```

---

## Summary: Decision Tree

```
Need to query OptimusDB?
↓
├─ Complex query (joins, aggregates)?
│  → Use SQL (sqlselect/sqldml)
│  → Executes on SQLite
│
├─ Simple filters across distributed peers?
│  → Use Criteria (query method)
│  → Executes on Data Store
│  → Choose strategy:
│     • LOCAL_ONLY (fast, local)
│     • PARALLEL_MERGE (complete, all peers)
│     • LOCAL_THEN_REMOTE_MERGE (balanced)
│     • QUORUM (consensus required)
│
├─ Insert/Update/Delete documents?
│  → Use CRUD (crudput/crudupdate/cruddelete)
│  → Specify dstype:
│     • "" or "dsswres" (default)
│     • "validations" (private)
│     • "tosca" (TOSCA templates)
│     • "dsswresaloc" (allocations)
│
└─ Work with files?
→ Use IPFS (get/post methods)
→ Returns IPFS CID
```

---

## Additional Resources

- **OptimusDB Complete API Endpoints:** See `OptimusDB_Complete_API_Endpoints.md`
- **Quick Reference:** See `OptimusDB_API_QuickReference.md`
- **CENTERIS 2025 Paper:** Full technical details on architecture

---

**Document Version:** 1.0
**Date:** November 11, 2025
**Prepared for:** EU Horizon Europe Grant 101135012 (Swarmchestrate Project)