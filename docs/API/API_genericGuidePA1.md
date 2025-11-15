# OptimusDB API Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Getting Started](#getting-started)
4. [API Endpoints](#api-endpoints)
5. [Document Operations (CRUD)](#document-operations-crud)
6. [Advanced Queries](#advanced-queries)
7. [SQL Operations](#sql-operations)
8. [Metadata Enrichment](#metadata-enrichment)
9. [Cluster Management](#cluster-management)
10. [Error Handling](#error-handling)
11. [Best Practices](#best-practices)

---

## Overview

OptimusDB is a distributed, peer-to-peer database system combining:
- **OrbitDB**: Distributed document store (P2P, eventually consistent)
- **SQLite**: Local relational database (ACID compliant)
- **TinyLlama AI**: Automatic metadata enrichment
- **IPFS/LibP2P**: Decentralized networking layer

### Key Features
- ✅ Distributed data storage across 8-node cluster
- ✅ Automatic data replication via P2P gossip
- ✅ AI-powered metadata generation
- ✅ SQL and NoSQL query support
- ✅ Automatic peer discovery (mDNS, DHT, PubSub)
- ✅ TOSCA template management

---

## Architecture
```
┌─────────────────────────────────────────────┐
│           OptimusDB Node (x8)               │
├─────────────────────────────────────────────┤
│  HTTP API (Port 18001-18008)                │
│    - /swarmkb/* (configurable context)      │
│    - /api/v1/metadata/* (fixed)             │
├─────────────────────────────────────────────┤
│  OrbitDB (Distributed P2P Storage)          │
│    - DsSWres (main docstore)                │
│    - KBMetadata (metadata catalog)          │
│    - Contributions (eventlog)               │
├─────────────────────────────────────────────┤
│  SQLite (Local Relational DB)               │
│    - products, customers, orders...         │
│    - metadata_catalog                       │
├─────────────────────────────────────────────┤
│  TinyLlama AI (Internal: 127.0.0.1:8080)    │
│    - Description generation                 │
│    - Keyword extraction                     │
│    - Tag creation                           │
├─────────────────────────────────────────────┤
│  IPFS/LibP2P Networking                     │
│    - Peer discovery                         │
│    - Content addressing                     │
│    - P2P replication                        │
└─────────────────────────────────────────────┘
```

### Port Mapping

| Node | HTTP API | IPFS Swarm | IPFS API | P2P | Internal TinyLlama |
|------|----------|------------|----------|-----|-------------------|
| 1    | 18001    | 14001      | 15001    | 13001 | 127.0.0.1:8080 |
| 2    | 18002    | 14002      | 15002    | 13002 | 127.0.0.1:8080 |
| 3    | 18003    | 14003      | 15003    | 13003 | 127.0.0.1:8080 |
| 4    | 18004    | 14004      | 15004    | 13004 | 127.0.0.1:8080 |
| 5    | 18005    | 14005      | 15005    | 13005 | 127.0.0.1:8080 |
| 6    | 18006    | 14006      | 15006    | 13006 | 127.0.0.1:8080 |
| 7    | 18007    | 14007      | 15007    | 13007 | 127.0.0.1:8080 |
| 8    | 18008    | 14008      | 15008    | 13008 | 127.0.0.1:8080 |

---

## Getting Started

### Prerequisites
- OptimusDB cluster running (8 nodes)
- Network connectivity between nodes
- TinyLlama model loaded (optional, for metadata enrichment)

### Base URLs
```
Node 1: http://localhost:18001
Node 2: http://localhost:18002
...
Node 8: http://localhost:18008
```

### API Context
All main endpoints use the configurable context path:
```
/swarmkb/*           # Main API (default context)
/api/v1/metadata/*   # Metadata API (fixed path)
```

### Authentication
Currently, OptimusDB does not require authentication. **In production, add authentication middleware.**

---

## API Endpoints

### Main Command Endpoint

**All document and query operations use a single endpoint:**
```
POST /swarmkb/command
```

#### Request Structure
```json
{
"method": {
"cmd": "command_name",
"argcnt": number_of_arguments
},
"dstype": "store_type",
"criteria": [...],
"UpdateData": [...],
"options": {...}
}
```

#### Response Structure
```json
{
"status": 200,
"data": <result_data>
    }
    ```

    ---

    ## Document Operations (CRUD)

    ### CREATE - Insert Documents

    **Endpoint:** `POST /swarmkb/command`

    **Method:** `crudput`

    #### Request
    ```json
    {
    "method": {"cmd": "crudput", "argcnt": 1},
    "dstype": "dsswres",
    "criteria": [
    {
    "_id": "unique_document_id",
    "name": "Product Name",
    "description": "Product description",
    "price": 99.99,
    "nested": {
    "field": "value"
    },
    "array_field": ["item1", "item2"]
    }
    ]
    }
    ```

    #### Response
    ```json
    {
    "status": 200,
    "data": "OK: Successfully inserted records"
    }
    ```

    #### Process
    1. Document inserted into OrbitDB
    2. **Automatic metadata generation triggered**
    3. System metadata created (ID, timestamps)
    4. TinyLlama generates contextual metadata (if available)
    5. Metadata stored in KBMetadata OrbitDB store
    6. Metadata indexed in SQLite
    7. Replication across cluster begins

    #### Notes
    - `_id` field is **required** and must be unique
    - Supports nested objects and arrays
    - Replication is asynchronous (eventually consistent)
    - Typically takes 1-3 seconds for cluster-wide propagation

    ---

    ### READ - Query Documents

    **Endpoint:** `POST /swarmkb/command`

    **Method:** `crudget`

    #### Simple Query
    ```json
    {
    "method": {"cmd": "crudget", "argcnt": 1},
    "dstype": "dsswres",
    "criteria": [
    {"category": "Electronics"}
    ]
    }
    ```

    #### Multiple Criteria (OR logic)
    ```json
    {
    "method": {"cmd": "crudget", "argcnt": 1},
    "dstype": "dsswres",
    "criteria": [
    {"category": "Electronics"},
    {"category": "Accessories"}
    ]
    }
    ```

    #### Multiple Fields (AND logic)
    ```json
    {
    "method": {"cmd": "crudget", "argcnt": 1},
    "dstype": "dsswres",
    "criteria": [
    {
    "category": "Electronics",
    "price": 1999.99,
    "stock": 10
    }
    ]
    }
    ```

    #### Response
    ```json
    {
    "status": 200,
    "data": [
    {"_id": "prod_001", "name": "Laptop", "price": 1999.99},
    {"_id": "prod_002", "name": "Monitor", "price": 499.99}
    ]
    }
    ```

    ---

    ### UPDATE - Modify Documents

    **Endpoint:** `POST /swarmkb/command`

    **Method:** `crudupdate`

    #### Request
    ```json
    {
    "method": {"cmd": "crudupdate", "argcnt": 1},
    "dstype": "dsswres",
    "criteria": [
    {"_id": "prod_001"}
    ],
    "UpdateData": [
    {
    "price": 1799.99,
    "stock": 5,
    "status": "on_sale"
    }
    ]
    }
    ```

    #### Response
    ```json
    {
    "status": 200,
    "data": "SUCCESS! 1 document(s) updated"
    }
    ```

    #### Notes
    - Only fields in `UpdateData` are modified
    - Other fields remain unchanged
    - Multiple documents can be updated if multiple match criteria

    ---

    ### DELETE - Remove Documents

    **Endpoint:** `POST /swarmkb/command`

    **Method:** `cruddelete`

    #### Request
    ```json
    {
    "method": {"cmd": "cruddelete", "argcnt": 1},
    "dstype": "dsswres",
    "criteria": [
    {"_id": "prod_001"}
    ]
    }
    ```

    #### Response
    ```json
    {
    "status": 200,
    "data": "SUCCESS! 1 document(s) deleted"
    }
    ```

    #### ⚠️ Warning
    - Deletions are **permanent**
    - Cannot be undone
    - Replicated immediately to all peers

    ---

    ## Advanced Queries

    ### Distributed Query

    **Endpoint:** `POST /swarmkb/command`

    **Method:** `query`

    #### Request with Options
    ```json
    {
    "method": {"cmd": "query", "argcnt": 0},
    "criteria": [
    {
    "category": "Electronics",
    "price": {"$gte": 1000}
    }
    ],
    "options": {
    "strategy": "LOCAL_THEN_REMOTE_MERGE",
    "consistency": "BEST_EFFORT",
    "time_budget_ms": 1200,
    "min_rows": 5,
    "include_local": true,
    "annotate_source": true
    }
    }
    ```

    ### Query Strategies

    | Strategy | Description | Use Case |
    |----------|-------------|----------|
    | `LOCAL_ONLY` | Query only local node | Fast, node-specific data |
    | `REMOTE_ONLY` | Query only peers | Distributed aggregation |
    | `LOCAL_THEN_REMOTE_MERGE` | Local first, then peers | **Default, balanced** |
    | `PARALLEL_MERGE` | Local and peers simultaneously | Fast, large clusters |
    | `QUORUM` | Wait for N peer responses | Consistency-critical |

    ### Consistency Levels

    | Level | Description | Trade-off |
    |-------|-------------|-----------|
    | `BEST_EFFORT` | Return ASAP within budget | **Fast, may be incomplete** |
    | `QUORUM` | Wait for N peers | Consistent, slower |
    | `ALL` | Wait for all peers | Most consistent, slowest |

    ### Query Options

    | Option | Type | Default | Description |
    |--------|------|---------|-------------|
    | `strategy` | string | `LOCAL_THEN_REMOTE_MERGE` | Query strategy |
    | `consistency` | string | `BEST_EFFORT` | Consistency level |
    | `time_budget_ms` | int | 1200 | Max query time (ms) |
    | `min_rows` | int | 0 | Stop early if threshold met |
    | `quorum_n` | int | 0 | Peers required for quorum |
    | `max_peers` | int | 0 | Limit to top N peers |
    | `include_local` | bool | true | Include local results |
    | `annotate_source` | bool | true | Add `_source` field |

    ### Response with Annotations
    ```json
    {
    "status": 200,
    "data": [
    {
    "_id": "prod_001",
    "name": "Laptop",
    "_source": {
    "type": "local",
    "peer_id": ""
    },
    "_trace": {
    "strategy": "LOCAL_THEN_REMOTE_MERGE"
    }
    },
    {
    "_id": "prod_002",
    "name": "Monitor",
    "_source": {
    "type": "peer",
    "peer_id": "QmXXX..."
    }
    }
    ]
    }
    ```

    ### Filter Operators

    OptimusDB supports advanced filtering operators:

    | Operator | Example | Description |
    |----------|---------|-------------|
    | `$gte` | `{"price": {"$gte": 100}}` | Greater than or equal |
    | `$gt` | `{"price": {"$gt": 100}}` | Greater than |
    | `$lte` | `{"price": {"$lte": 1000}}` | Less than or equal |
    | `$lt` | `{"price": {"$lt": 1000}}` | Less than |
    | `$ne` | `{"status": {"$ne": "deleted"}}` | Not equal |
    | `$regex` | `{"name": {"$regex": "^Gaming.*"}}` | Pattern match |

    #### Complex Filter Example
    ```json
    {
    "method": {"cmd": "query", "argcnt": 0},
    "criteria": [
    {
    "category": "Electronics",
    "price": {"$gte": 500, "$lte": 2000},
    "brand": {"$regex": "Tech.*"},
    "stock": {"$gt": 0}
    }
    ]
    }
    ```

    ---

    ## SQL Operations

    ### SQL Execution

    **Endpoint:** `POST /swarmkb/command`

    **Method:** `sqldml`

    #### SELECT Query
    ```json
    {
    "method": {"cmd": "sqldml", "argcnt": 1},
    "sqldml": "SELECT * FROM products WHERE price > 1000 ORDER BY price DESC LIMIT 10"
    }
    ```

    #### Response
    ```json
    {
    "status": 200,
    "data": {
    "records": [
    {"id": 1, "name": "Laptop", "price": 1999.99},
    {"id": 2, "name": "Monitor", "price": 1499.99}
    ]
    }
    }
    ```

    ### SQL with Automatic Peer Fallback

    **Process:**
    1. Execute SQL on local SQLite
    2. If no results found, **automatically query remote peers**
    3. Deduplicate merged results
    4. Return combined dataset

    **Example:** Query local and remote
    ```json
    {
    "method": {"cmd": "sqldml", "argcnt": 1},
    "sqldml": "SELECT * FROM customers WHERE country = 'Greece'"
    }
    ```

    If local DB has no Greek customers, OptimusDB will:
    1. Query all 7 other nodes
    2. Collect their results
    3. Deduplicate by row hash
    4. Return merged dataset

    ### INSERT with Metadata Generation

    **Request:**
    ```json
    {
    "method": {"cmd": "sqldml", "argcnt": 1},
    "sqldml": "INSERT INTO products (name, description, category, price) VALUES ('Wireless Headphones', 'Premium wireless headphones with active noise cancellation, 30-hour battery life, and superior sound quality', 'Audio', 249.99)"
    }
    ```

    **Automatic Metadata Process:**
    1. SQL INSERT executed in SQLite
    2. **System metadata generated:**
    - Unique ID (hash)
    - Timestamps (created_at, updated_at)
    - Source (node ID, IP address)
    - Creator info
    3. **Contextual metadata (TinyLlama):**
    - Description summary
    - Keywords: ["wireless", "headphones", "noise-cancellation", "premium"]
    - Tags: ["audio", "wireless", "premium"]
    - Inferred name/title
    4. **Storage:**
    - OrbitDB KBMetadata store (distributed)
    - SQLite metadata_catalog table (local index)
    5. **Replication** to cluster

    **Generated Metadata:**
    ```json
    {
    "id": "meta_abc123def456",
    "metadata_type": "dataset",
    "component": "products",
    "description": "Premium audio device with noise cancellation and long battery",
    "name": "Wireless Headphones",
    "tags": ["audio", "wireless", "premium", "noise-cancellation"],
    "created_by": "optimusdb-node1",
    "created_at": "2025-01-15T10:30:00Z"
    }
    ```

    ### Supported SQL Operations

    | Operation | Supported | Notes |
    |-----------|-----------|-------|
    | SELECT | ✅ | Full support, peer fallback |
    | INSERT | ✅ | Triggers metadata generation |
    | UPDATE | ✅ | Standard SQL UPDATE |
    | DELETE | ✅ | Permanent deletion |
    | CREATE TABLE | ✅ | Schema changes local only |
    | DROP TABLE | ✅ | Use with caution |
    | ALTER TABLE | ✅ | Schema changes local only |
    | Transactions | ✅ | SQLite ACID guarantees |
    | JOINs | ✅ | Full SQL JOIN support |
    | GROUP BY | ✅ | Aggregations supported |
    | Subqueries | ✅ | Nested queries supported |

    ---

    ## Metadata Enrichment

    ### Enrich Dataset

    **Endpoint:** `POST /api/v1/metadata/enrich`

    #### Request
    ```json
    {
    "db": "optimusdb.db",
    "table": "products",
    "maxRows": 100,
    "greek": false
    }
    ```

    #### Response
    ```json
    {
    "dataset": "products",
    "rows_processed": 100,
    "metadata_entries": 100,
    "processing_time_ms": 45000,
    "tinyllama_available": true,
    "errors": []
    }
    ```

    #### Process
    1. Read rows from SQLite table
    2. For each row:
    - Send description to TinyLlama
    - Generate summary (30-50 tokens)
    - Extract keywords
    - Create tags
    3. Store metadata in OrbitDB KBMetadata
    4. Index in SQLite metadata_catalog
    5. Cache results (24h TTL)

    #### Notes
    - **Requires TinyLlama** running and healthy
    - Processing time: ~450ms per row on average
    - 100 rows ≈ 45 seconds total
    - Cached results speed up subsequent enrichments

    ### Batch Enrichment

    **Endpoint:** `POST /api/v1/metadata/enrich-batch`

    #### Request
    ```json
    {
    "datasets": [
    {"db": "optimusdb.db", "table": "products", "maxRows": 50},
    {"db": "optimusdb.db", "table": "customers", "maxRows": 50},
    {"db": "optimusdb.db", "table": "orders", "maxRows": 100}
    ],
    "greek": false
    }
    ```

    #### Use Cases
    - Initial database setup
    - Bulk metadata generation
    - Multi-table enrichment

    ### Profile Dataset

    **Endpoint:** `GET /api/v1/metadata/profile?db=optimusdb.db&table=products`

    #### Response
    ```json
    {
    "dataset": "products",
    "row_count": 1250,
    "columns": [
    {"name": "id", "type": "INTEGER", "nullable": false},
    {"name": "name", "type": "TEXT", "nullable": false},
    {"name": "price", "type": "REAL", "nullable": true}
    ],
    "metadata_coverage": 0.85,
    "last_enriched": "2025-01-15T10:30:00Z",
    "sample_rows": [...]
    }
    ```

    ### Metadata Endpoints

    | Endpoint | Method | Description |
    |----------|--------|-------------|
    | `/api/v1/metadata/enrich` | POST | Enrich single dataset |
    | `/api/v1/metadata/enrich-batch` | POST | Enrich multiple datasets |
    | `/api/v1/metadata/profile` | GET | Dataset statistics |
    | `/api/v1/metadata/metrics` | GET | Performance metrics |
    | `/api/v1/metadata/health` | GET | Service health check |
    | `/api/v1/metadata/cache` | DELETE | Clear metadata cache |

    ---

    ## Cluster Management

    ### List Peers

    **Endpoint:** `GET /swarmkb/peers`

    #### Response
    ```json
    [
    {
    "ID": "12D3KooWAbC123XyZ...",
    "Addrs": [
    "/ip4/192.168.1.101/tcp/4001",
    "/ip4/203.0.113.50/tcp/4001"
    ]
    },
    {
    "ID": "12D3KooWDeF456UvW...",
    "Addrs": [
    "/ip4/192.168.1.102/tcp/4001"
    ]
    }
    ]
    ```

    ### Peer Discovery Methods

    OptimusDB uses three discovery mechanisms:

    1. **mDNS (Multicast DNS)**
    - Local network discovery
    - Zero-configuration
    - Fast (1-2 seconds)

    2. **DHT (Distributed Hash Table)**
    - Global peer discovery
    - Kademlia routing
    - Slower but works across networks

    3. **PubSub (Topic-based)**
    - Active announcements
    - Topic: "optimusdb"
    - Reliable mesh formation

    ### Get Benchmarks

    **Endpoint:** `GET /swarmkb/benchmarks`

    **Requires:** `-benchmark` flag on startup

    #### Response
    ```json
    [
    {
    "node_id": "12D3KooWXXX...",
    "cpu_usage_percent": 15.3,
    "memory_usage_mb": 512.5,
    "disk_usage_mb": 2048.7,
    "uptime_seconds": 3600,
    "queries_processed": 1250,
    "avg_query_latency_ms": 45,
    "replication_lag_ms": 120
    }
    ]
    ```

    ---

    ## Error Handling

    ### Error Response Format
    ```json
    {
    "status": "error",
    "message": "Error description"
    }
    ```

    ### Common Error Codes

    | HTTP Status | Meaning | Common Causes |
    |-------------|---------|---------------|
    | 400 | Bad Request | Invalid JSON, missing fields |
    | 404 | Not Found | Endpoint doesn't exist |
    | 405 | Method Not Allowed | Wrong HTTP method (GET vs POST) |
    | 500 | Internal Server Error | Database error, OrbitDB failure |
    | 503 | Service Unavailable | TinyLlama unavailable |

    ### Example Error
    ```json
    {
    "status": 400,
    "message": "Invalid JSON payload"
    }
    ```

    ---

    ## Best Practices

    ### 1. Document Design

    **DO:**
    - Always include a unique `_id` field
    - Use consistent naming conventions
    - Keep documents reasonably sized (<1MB)
    - Use nested objects for related data

    **DON'T:**
    - Store binary data directly (use IPFS paths instead)
    - Create documents without `_id`
    - Use extremely large arrays

    ### 2. Querying

    **DO:**
    - Use distributed queries for cluster-wide searches
    - Set reasonable `time_budget_ms` (1000-2000ms)
    - Use `LOCAL_ONLY` when possible for speed
    - Leverage query result caching

    **DON'T:**
    - Query with no filters (returns everything)
    - Set `time_budget_ms` too low (<500ms)
    - Use `consistency: ALL` unless absolutely necessary

    ### 3. SQL Operations

    **DO:**
    - Use prepared statements in application code
    - Create indexes on frequently queried columns
    - Use transactions for multiple related operations
    - Let INSERT trigger automatic metadata generation

    **DON'T:**
    - Execute raw user input (SQL injection risk)
    - Create unnecessary indexes (slows writes)
    - Use SELECT * in production code

    ### 4. Metadata Enrichment

    **DO:**
    - Enrich in batches (50-200 rows at a time)
    - Use caching (24h TTL default)
    - Monitor TinyLlama health
    - Handle TinyLlama unavailability gracefully

    **DON'T:**
    - Enrich the same dataset repeatedly
    - Process thousands of rows in one request
    - Assume TinyLlama is always available

    ### 5. Cluster Operations

    **DO:**
    - Monitor peer connectivity regularly
    - Handle replication delays (1-3 seconds)
    - Use quorum queries for critical data
    - Plan for network partitions

    **DON'T:**
    - Assume instant replication
    - Expect strong consistency by default
    - Ignore peer discovery failures

    ---

    ## Performance Tips

    ### Query Optimization

    1. **Use specific filters:** Narrow results early
    2. **Leverage distributed queries:** Parallel > Sequential
    3. **Cache results:** Especially for repeated queries
    4. **Index SQLite:** CREATE INDEX on common WHERE clauses
    5. **Batch operations:** Insert/update in batches

    ### Metadata Optimization

    1. **Batch enrichment:** Process 50-200 rows at once
    2. **Use cache:** 24h TTL reduces TinyLlama load
    3. **Monitor health:** Check `/api/v1/metadata/health`
    4. **Async processing:** Don't block on metadata generation

    ### Cluster Optimization

    1. **Peer selection:** Use `max_peers` to limit queries
    2. **Time budgets:** Balance speed vs completeness
    3. **Consistency levels:** Use `BEST_EFFORT` when possible
    4. **Network:** Ensure low-latency connectivity

    ---

    ## Troubleshooting

    ### TinyLlama Not Responding

    **Symptoms:**
    - Metadata enrichment fails
    - INSERT doesn't generate metadata
    - `/api/v1/metadata/health` shows unhealthy

    **Solutions:**
    1. Check TinyLlama process: `docker exec optimusdb1 ps aux | grep llama`
    2. Check TinyLlama logs: `docker exec optimusdb1 tail -f /var/log/supervisor/tinyllama.log`
    3. Verify model loaded: Look for "model loaded" in logs
    4. Restart if needed: `docker restart optimusdb1`

    ### Replication Not Working

    **Symptoms:**
    - Data not appearing on other nodes
    - Distributed queries return incomplete results

    **Solutions:**
    1. Check peer connectivity: `GET /swarmkb/peers`
    2. Verify network: `docker exec optimusdb1 ping optimusdb2`
    3. Check GossipSub mesh: Look for "GRAFT" in logs
    4. Wait longer: Replication can take 1-5 seconds

    ### SQL Queries Failing

    **Symptoms:**
    - SQL commands return errors
    - Peer fallback not working

    **Solutions:**
    1. Check SQL syntax: Test in SQLite directly
    2. Verify table exists: Run `PRAGMA table_info(table_name)`
    3. Check logs: `docker exec optimusdb1 tail -f /var/log/supervisor/optimusdb.log`
    4. Verify peers responding: Check `/swarmkb/peers`

    ---

    ## Security Considerations

    ### Current State
    - ⚠️ **No authentication** - All endpoints publicly accessible
    - ⚠️ **No authorization** - No role-based access control
    - ⚠️ **No encryption** - HTTP traffic unencrypted
    - ⚠️ **SQL injection** - User input not sanitized

    ### Production Recommendations

    1. **Add Authentication:**
    - JWT tokens
    - API keys
    - OAuth 2.0

    2. **Add Authorization:**
    - Role-based access control
    - Resource-level permissions

    3. **Enable TLS:**
    - HTTPS for all endpoints
    - Certificate management

    4. **Input Validation:**
    - Sanitize SQL inputs
    - Validate JSON payloads
    - Rate limiting

    5. **Network Security:**
    - Firewall rules
    - VPN for P2P traffic
    - Private IPFS network

    ---

    ## Appendix A: Full Method Reference

    ### Command Methods

    | Method | Arg Count | Description |
    |--------|-----------|-------------|
    | `crudput` | 1 | Insert documents |
    | `crudget` | 1                     |
    | `crudupdate` | 1 | Update documents |
    | `cruddelete` | 1 | Delete documents |
    | `query` | 0 | Distributed query |
    | `sqldml` | 1 | Execute SQL statement |
    | `contri` | 1 | Query contributions |
    | `benchmark` | 0 | Get benchmark data |
    | `cachestats` | 0 | Get cache statistics |
    | `clearcache` | 0 | Clear query cache |

    ### Document Store Types

    | Store Type | Description | Use Case |
    |------------|-------------|----------|
    | `dsswres` | Main document store | Primary data storage |
    | `dsswresaloc` | Allocation document store | Resource allocation |
    | `kbdata` | Knowledge base data | KB documents |
    | `kbmetadata` | Metadata catalog | Generated metadata |
    | `validations` | Validation records | Data validation |
    | `tosca_imported` | TOSCA templates | Infrastructure templates |

    ---

    ## Appendix B: Query Strategy Decision Tree
    ```
    ┌─────────────────────────────────────┐
    │  What are your requirements?        │
    └─────────────┬───────────────────────┘
    │
    ├─ Need fast response, local data OK
    │  └─> Use: LOCAL_ONLY
    │
    ├─ Need all cluster data, time not critical
    │  └─> Use: LOCAL_THEN_REMOTE_MERGE
    │
    ├─ Need fastest cluster-wide query
    │  └─> Use: PARALLEL_MERGE
    │
    ├─ Need consistency guarantee (N peers)
    │  └─> Use: QUORUM with quorum_n
    │
    └─ Only want remote data (exclude local)
    └─> Use: REMOTE_ONLY
    ```

    ---

    ## Appendix C: Metadata Generation Flow
    ```
    ┌─────────────────────────────────────────────────┐
    │  SQL INSERT or CRUDPUT Operation                │
    └───────────────┬─────────────────────────────────┘
    │
    ▼
    ┌─────────────────────────────────────────────────┐
    │  1. Execute Data Operation                      │
    │     - SQLite: INSERT INTO products...           │
    │     - OrbitDB: Put document                     │
    └───────────────┬─────────────────────────────────┘
    │
    ▼
    ┌─────────────────────────────────────────────────┐
    │  2. Generate System Metadata                    │
    │     - ID: Hash-based unique identifier          │
    │     - Timestamps: created_at, updated_at        │
    │     - Source: Node ID, IP address               │
    │     - Creator: Agent name                       │
    └───────────────┬─────────────────────────────────┘
    │
    ▼
    ┌─────────────────────────────────────────────────┐
    │  3. Attempt Contextual Metadata (TinyLlama)     │
    │     ├─ Check TinyLlama availability             │
    │     ├─ Send data to http://127.0.0.1:8080       │
    │     ├─ Generate description summary             │
    │     ├─ Extract keywords                         │
    │     └─ Create tags                              │
    └───────────────┬─────────────────────────────────┘
    │
    ├─ Success ──────────────┐
    │                        │
    └─ Failure (TinyLlama    │
    unavailable)          │
    └─ Use fallback       │
    metadata           │
    │
    ┌───────────────────────┘
    │
    ▼
    ┌─────────────────────────────────────────────────┐
    │  4. Store Metadata                              │
    │     ├─ OrbitDB KBMetadata (distributed)         │
    │     └─ SQLite metadata_catalog (local index)    │
    └───────────────┬─────────────────────────────────┘
    │
    ▼
    ┌─────────────────────────────────────────────────┐
    │  5. Replicate to Cluster                        │
    │     - P2P gossip propagation                    │
    │     - Eventually consistent (1-5 seconds)       │
    └─────────────────────────────────────────────────┘
    ```

    ---

    ## Appendix D: Example Workflows

    ### Workflow 1: Complete Product Lifecycle
    ```bash
    # 1. Create product with auto-metadata
    POST /swarmkb/command
    {
    "method": {"cmd": "sqldml", "argcnt": 1},
    "sqldml": "INSERT INTO products (name, description, price)
    VALUES ('Gaming Mouse X1',
    'Professional gaming mouse with 16000 DPI sensor',
    79.99)"
    }
    # → System generates metadata automatically
    # → TinyLlama creates: description, keywords, tags
    # → Stored in both OrbitDB and SQLite
    # → Replicated to all 8 nodes

    # 2. Query from any node (distributed)
    POST /swarmkb/command
    {
    "method": {"cmd": "query", "argcnt": 0},
    "criteria": [{"name": "Gaming Mouse X1"}],
    "options": {
    "strategy": "LOCAL_THEN_REMOTE_MERGE",
    "annotate_source": true
    }
    }
    # → Searches local first, then peers if needed
    # → Returns with source annotations

    # 3. Update price
    POST /swarmkb/command
    {
    "method": {"cmd": "sqldml", "argcnt": 1},
    "sqldml": "UPDATE products SET price = 69.99
    WHERE name = 'Gaming Mouse X1'"
    }

    # 4. Verify on different node
    POST http://localhost:18005/swarmkb/command
    {
    "method": {"cmd": "sqldml", "argcnt": 1},
    "sqldml": "SELECT * FROM products WHERE name = 'Gaming Mouse X1'"
    }
    # → Should show updated price after replication (1-3s)
    ```

    ### Workflow 2: Batch Data Import with Enrichment
    ```bash
    # 1. Bulk insert products via SQL
    POST /swarmkb/command
    {
    "method": {"cmd": "sqldml", "argcnt": 1},
    "sqldml": "INSERT INTO products (name, description, category, price) VALUES
    ('Product 1', 'Description 1', 'Electronics', 99.99),
    ('Product 2', 'Description 2', 'Electronics', 149.99),
    ('Product 3', 'Description 3', 'Accessories', 29.99)"
    }
    # → Each INSERT triggers metadata generation
    # → May take 1-2 seconds per product

    # 2. Batch enrich entire dataset
    POST /api/v1/metadata/enrich-batch
    {
    "datasets": [
    {"db": "optimusdb.db", "table": "products", "maxRows": 100}
    ]
    }
    # → Processes up to 100 products
    # → Generates enhanced metadata via TinyLlama
    # → ~45 seconds for 100 products

    # 3. Verify metadata coverage
    GET /api/v1/metadata/profile?db=optimusdb.db&table=products
    # → Shows statistics: row count, metadata coverage, etc.

    # 4. Query enriched data
    POST /swarmkb/command
    {
    "method": {"cmd": "crudget", "argcnt": 1},
    "dstype": "kbmetadata",
    "criteria": [{"component": "products"}]
    }
    # → Returns all metadata for products table
    ```

    ### Workflow 3: Cross-Node Replication Test
    ```bash
    # 1. Insert on Node 1
    POST http://localhost:18001/swarmkb/command
    {
    "method": {"cmd": "crudput", "argcnt": 1},
    "dstype": "dsswres",
    "criteria": [
    {
    "_id": "test_replication_001",
    "test": "cross-node replication",
    "timestamp": "2025-01-15T10:30:00Z",
    "source_node": "node1"
    }
    ]
    }
    # Response: {"status": 200, "data": "OK: Successfully inserted records"}

    # 2. Wait for replication (3 seconds)
    # (Replication typically takes 1-3 seconds)

    # 3. Query from Node 5
    POST http://localhost:18005/swarmkb/command
    {
    "method": {"cmd": "crudget", "argcnt": 1},
    "dstype": "dsswres",
    "criteria": [{"_id": "test_replication_001"}]
    }
    # Expected: Document appears on Node 5

    # 4. Distributed query to see all copies
    POST http://localhost:18003/swarmkb/command
    {
    "method": {"cmd": "query", "argcnt": 0},
    "criteria": [{"_id": "test_replication_001"}],
    "options": {
    "strategy": "PARALLEL_MERGE",
    "annotate_source": true
    }
    }
    # Expected: Multiple results with different peer_id in _source field
    ```

    ### Workflow 4: TOSCA Template Management
    ```bash
    # 1. Prepare TOSCA template (bash)
    base64 -w 0 infrastructure.yaml > tosca_base64.txt

    # 2. Upload via API
    POST /swarmkb/upload
    {
    "file": "<contents_of_tosca_base64.txt>",
        "filename": "infrastructure.yaml"
        }
        # Response:
        # {
        #   "status": 200,
        #   "data": {
        #     "message": "TOSCA uploaded successfully",
        #     "template_id": "tosca_abc123",
        #     "node_count": 12,
        #     "filesize": 4096,
        #     "sha256": "def456..."
        #   }
        # }

        # 3. Query TOSCA templates
        POST /swarmkb/command
        {
        "method": {"cmd": "crudget", "argcnt": 1},
        "dstype": "tosca_imported",
        "criteria": [{"type": "tosca_template"}]
        }

        # 4. Query metadata about TOSCA templates
        POST /swarmkb/command
        {
        "method": {"cmd": "sqldml", "argcnt": 1},
        "sqldml": "SELECT * FROM tosca_metadata
        WHERE node_count > 10
        ORDER BY created_at DESC"
        }
        ```

        ---

        ## Appendix E: TinyLlama Integration Details

        ### TinyLlama Configuration

        **Internal Endpoint:** `http://127.0.0.1:8080/v1/completions`

        **Model:** TinyLlama-1.1B-Chat-v1.0 (Q4_K_M quantization)

        **Startup Configuration:**
        ```bash
        /usr/local/bin/llama-server \
        -m /models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf \
        -c 2048 \
        --host 127.0.0.1 \
        --port 8080 \
        --n-gpu-layers 0
        ```

        ### Request Format
        ```json
        {
        "prompt": "Summarize this product: High-performance gaming laptop...",
        "max_tokens": 50,
        "temperature": 0.7,
        "stop": ["\n\n"]
        }
        ```

        ### Response Format
        ```json
        {
        "id": "cmpl-xxx",
        "object": "text_completion",
        "created": 1234567890,
        "model": "tinyllama",
        "choices": [
        {
        "text": " A powerful gaming laptop with RTX 4090 GPU and advanced cooling.",
        "index": 0,
        "logprobs": null,
        "finish_reason": "stop"
        }
        ],
        "usage": {
        "prompt_tokens": 25,
        "completion_tokens": 15,
        "total_tokens": 40
        }
        }
        ```

        ### Health Check
        ```bash
        # From inside container
        curl http://127.0.0.1:8080/health

        # Check if running
        ps aux | grep llama-server

        # View logs
        tail -f /var/log/supervisor/tinyllama.log
        ```

        ### Performance Characteristics

        | Metric | Value |
        |--------|-------|
        | Model Size | ~1.1B parameters |
        | Quantization | Q4_K_M (4-bit) |
        | Memory Usage | ~700-800MB RAM |
        | Generation Speed | ~450ms per completion |
        | Context Window | 2048 tokens |
        | Concurrent Requests | Limited (sequential processing) |

        ### Fallback Behavior

        When TinyLlama is unavailable:
        ```json
        {
        "description": "Autogenerated description (TinyLlama disabled)",
        "name": "SampleDataset_20250115_143000",
        "tags": ["auto", "metadata", "fallback"]
        }
        ```

        ---

        ## Appendix F: Docker Commands Reference

        ### Container Management
        ```bash
        # Start all 8 nodes
        docker run -d --network=swarmnet --name=optimusdb1 \
        -p 18001:8089 -p 14001:4001 -p 15001:5001 -p 13001:4002 \
        optimusdb

        # Repeat for optimusdb2-8 with incremented ports

        # Stop all nodes
        docker stop optimusdb{1..8}

        # Remove all nodes
        docker rm optimusdb{1..8}

        # View logs
        docker logs optimusdb1 -f

        # Execute commands inside container
        docker exec optimusdb1 ps aux
        docker exec -it optimusdb1 bash

        # Check resource usage
        docker stats optimusdb1
        ```

        ### Log Access
        ```bash
        # OptimusDB logs
        docker exec optimusdb1 tail -f /var/log/supervisor/optimusdb.log

        # TinyLlama logs
        docker exec optimusdb1 tail -f /var/log/supervisor/tinyllama.log

        # Supervisor logs
        docker exec optimusdb1 tail -f /var/log/supervisor/supervisord.log

        # Search for errors
        docker exec optimusdb1 grep -i error /var/log/supervisor/optimusdb.log
        ```

        ### Troubleshooting Commands
        ```bash
        # Check if services are running
        docker exec optimusdb1 ps aux | grep -E "supervisor|optimusdb|llama"

        # Check listening ports
        docker exec optimusdb1 netstat -tlnp

        # Check peer connectivity
        docker exec optimusdb1 ping optimusdb2

        # Test TinyLlama
        docker exec optimusdb1 curl http://127.0.0.1:8080/health

        # Check OrbitDB stores
        docker exec optimusdb1 ls -la /data/orbitdb/
        ```

        ---

        ## Appendix G: Performance Benchmarks

        ### Query Performance

        | Query Type | Avg Latency | Notes |
        |------------|-------------|-------|
        | Local CRUDGET | 5-15ms | Single document by ID |
        | Local CRUDGET (filtered) | 20-50ms | 100-1000 documents |
        | Distributed Query (LOCAL_THEN_REMOTE) | 200-500ms | 8-node cluster |
        | Distributed Query (QUORUM, N=3) | 300-800ms | Waits for 3 peers |
        | SQL SELECT (local) | 10-30ms | Simple query |
        | SQL SELECT (peer fallback) | 400-1000ms | Queries 7 peers |

        ### Metadata Generation

        | Operation | Avg Time | Notes |
        |-----------|----------|-------|
        | System metadata only | 5-10ms | No TinyLlama |
        | + TinyLlama generation | 450ms | Single product |
        | Batch enrichment (100 rows) | 45s | ~450ms per row |
        | Batch enrichment (cached) | 5s | Cache hit rate 90% |

        ### Replication

        | Metric | Typical Value | Notes |
        |--------|---------------|-------|
        | Initial propagation | 1-3s | First peer sees change |
        | Full cluster propagation | 3-5s | All 8 nodes synchronized |
        | Large document (1MB) | 5-10s | Slower for big data |

        ### Resource Usage (per node)

        | Resource | Idle | Active | Under Load |
        |----------|------|--------|------------|
        | CPU | 0.5-2% | 5-15% | 20-40% |
        | Memory | 180-220MB | 200-250MB | 250-350MB |
        | Disk I/O | Minimal | Moderate | High |
        | Network | 1-10KB/s | 50-200KB/s | 1-5MB/s |

        ---

        ## Appendix H: Common Patterns

        ### Pattern 1: Pagination
        ```json
        // Page 1 (first 20 results)
        {
        "method": {"cmd": "sqldml", "argcnt": 1},
        "sqldml": "SELECT * FROM products ORDER BY id LIMIT 20 OFFSET 0"
        }

        // Page 2 (next 20 results)
        {
        "method": {"cmd": "sqldml", "argcnt": 1},
        "sqldml": "SELECT * FROM products ORDER BY id LIMIT 20 OFFSET 20"
        }

        // Better: Cursor-based pagination
        {
        "method": {"cmd": "sqldml", "argcnt": 1},
        "sqldml": "SELECT * FROM products WHERE id > 20 ORDER BY id LIMIT 20"
        }
        ```

        ### Pattern 2: Search with Filters
        ```json
        {
        "method": {"cmd": "query", "argcnt": 0},
        "criteria": [
        {
        "category": "Electronics",
        "price": {"$gte": 100, "$lte": 1000},
        "stock": {"$gt": 0},
        "brand": {"$regex": "Tech.*"}
        }
        ],
        "options": {
        "strategy": "LOCAL_THEN_REMOTE_MERGE",
        "min_rows": 10
        }
        }
        ```

        ### Pattern 3: Aggregation
        ```json
        {
        "method": {"cmd": "sqldml", "argcnt": 1},
        "sqldml": "SELECT
        category,
        COUNT(*) as product_count,
        AVG(price) as avg_price,
        SUM(stock) as total_stock
        FROM products
        GROUP BY category
        HAVING product_count > 5
        ORDER BY avg_price DESC"
        }
        ```

        ### Pattern 4: Upsert (Insert or Update)
        ```json
        // 1. Try to get existing document
        {
        "method": {"cmd": "crudget", "argcnt": 1},
        "dstype": "dsswres",
        "criteria": [{"_id": "prod_001"}]
        }

        // 2a. If exists, update
        {
        "method": {"cmd": "crudupdate", "argcnt": 1},
        "dstype": "dsswres",
        "criteria": [{"_id": "prod_001"}],
        "UpdateData": [{"price": 99.99}]
        }

        // 2b. If not exists, insert
        {
        "method": {"cmd": "crudput", "argcnt": 1},
        "dstype": "dsswres",
        "criteria": [
        {"_id": "prod_001", "name": "Product", "price": 99.99}
        ]
        }
        ```

        ### Pattern 5: Bulk Operations
        ```json
        // Bulk insert (OrbitDB)
        {
        "method": {"cmd": "crudput", "argcnt": 1},
        "dstype": "dsswres",
        "criteria": [
        {"_id": "prod_001", "name": "Product 1", "price": 99.99},
        {"_id": "prod_002", "name": "Product 2", "price": 149.99},
        {"_id": "prod_003", "name": "Product 3", "price": 199.99}
        ]
        }

        // Bulk insert (SQL - better for large datasets)
        {
        "method": {"cmd": "sqldml", "argcnt": 1},
        "sqldml": "INSERT INTO products (name, price) VALUES
        ('Product 1', 99.99),
        ('Product 2', 149.99),
        ('Product 3', 199.99)"
        }
        ```

        ---

        ## Appendix I: Environment Variables

        ### Configuration via Environment
        ```bash
        # Metadata enrichment settings
        METADATA_ENRICHMENT_ENABLED=true        # Enable/disable metadata generation
        METADATA_CACHE_TTL=24h                  # Cache TTL (24 hours default)
        METADATA_AUTO_ENRICH=true               # Enable background enricher
        METADATA_ENRICH_INTERVAL=1h             # Background enrichment interval

        # TinyLlama settings
        TINYLLAMA_ENDPOINT=http://127.0.0.1:8080/v1/completions
        TINYLLAMA_MODEL=/models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf

        # Startup flags
        -context=swarmkb                        # HTTP API context path (default: optimusdb)
        -http                                   # Enable HTTP API
        -httpport=8089                          # HTTP server port
        -benchmark                              # Enable benchmarking
        -shell                                  # Enable interactive shell
        -autodiscovery                          # Enable peer discovery
        -autodiscovery-mdns                     # Enable mDNS discovery
        -autodiscovery-dht                      # Enable DHT discovery
        -autodiscovery-pubsub                   # Enable PubSub discovery
        ```

        ### Docker Run Example with Full Configuration
        ```bash
        docker run -d \
        --network=swarmnet \
        --name=optimusdb1 \
        -p 18001:8089 \
        -p 14001:4001 \
        -p 15001:5001 \
        -p 13001:4002 \
        -e METADATA_ENRICHMENT_ENABLED=true \
        -e METADATA_CACHE_TTL=24h \
        -e METADATA_AUTO_ENRICH=true \
        -e METADATA_ENRICH_INTERVAL=1h \
        -e POD_NAME=optimusdb-node1 \
        optimusdb \
        -context=swarmkb \
        -http \
        -httpport=8089 \
        -benchmark \
        -autodiscovery \
        -autodiscovery-mdns \
        -autodiscovery-pubsub
        ```

        ---

        ## Appendix J: Glossary

        | Term | Definition |
        |------|------------|
        | **OrbitDB** | Distributed, peer-to-peer database built on IPFS |
        | **IPFS** | InterPlanetary File System - content-addressed storage |
        | **LibP2P** | Modular peer-to-peer networking library |
        | **TinyLlama** | Small language model for metadata generation |
        | **GossipSub** | Pubsub protocol for message propagation in P2P networks |
        | **DHT** | Distributed Hash Table for peer discovery |
        | **mDNS** | Multicast DNS for local network discovery |
        | **Quorum** | Minimum number of peers needed for consensus |
        | **Eventually Consistent** | Data becomes consistent across nodes over time |
        | **Peer** | Another OptimusDB node in the cluster |
        | **Replication** | Copying data across multiple nodes |
        | **Metadata Enrichment** | AI-generated descriptions, tags, keywords |
        | **TOSCA** | Topology and Orchestration Specification for Cloud Applications |
        | **Context Path** | Base URL path for API endpoints (e.g., `/swarmkb`) |

        ---

        ## Appendix K: FAQ

        ### Q: How long does replication take?
        **A:** Typically 1-3 seconds for small documents across an 8-node cluster. Large documents (>1MB) may take 5-10 seconds.

        ### Q: Is OptimusDB strongly consistent?
        **A:** No, OptimusDB is **eventually consistent**. Use quorum queries when you need stronger consistency guarantees.

        ### Q: Can I disable metadata generation?
        **A:** Yes, set `METADATA_ENRICHMENT_ENABLED=false`. System metadata will still be created, but TinyLlama won't be called.

        ### Q: What happens if TinyLlama is down?
        **A:** INSERT operations continue normally with fallback metadata. System metadata is always generated.

        ### Q: Can I query across different document stores?
        **A:** No, each query targets one store type specified in `dstype`. Run multiple queries if needed.

        ### Q: How do I backup OptimusDB data?
        **A:** Back up both SQLite databases and OrbitDB data directories:
        ```bash
        # SQLite
        cp /path/to/*.db /backup/

        # OrbitDB
        cp -r /data/orbitdb /backup/
        ```

        ### Q: Can I use OptimusDB with less than 8 nodes?
        **A:** Yes, OptimusDB works with any number of nodes (1-N). Adjust quorum settings accordingly.

        ### Q: What's the maximum document size?
        **A:** Recommended maximum is 1MB per document. Larger documents slow replication.

        ### Q: Can I use custom TinyLlama prompts?
        **A:** Not via API currently. Prompts are hardcoded in the metadata service.

        ### Q: How do I monitor cluster health?
        **A:** Use `/swarmkb/peers` to check connectivity and `/swarmkb/benchmarks` (with `-benchmark` flag) for performance metrics.

        ---

        ## Support & Resources

        ### Documentation
        - **This API Documentation**: Complete reference guide
        - **Postman Collection**: `OptimusDB-SwarmKB-Complete.postman_collection.json`

        ### Logs
        - **OptimusDB**: `docker exec optimusdb1 tail -f /var/log/supervisor/optimusdb.log`
        - **TinyLlama**: `docker exec optimusdb1 tail -f /var/log/supervisor/tinyllama.log`
        - **Supervisor**: `docker exec optimusdb1 tail -f /var/log/supervisor/supervisord.log`

        ### Monitoring Endpoints
        - **Peers**: `GET /swarmkb/peers`
        - **Benchmarks**: `GET /swarmkb/benchmarks`
        - **Metadata Health**: `GET /api/v1/metadata/health`
        - **Metadata Metrics**: `GET /api/v1/metadata/metrics`

        ### Community
        - **GitHub**: (Add your repository URL)
        - **Issues**: (Add your issues URL)
        - **Discussions**: (Add your discussions URL)

        ---

        ## License

        (Add your license information here)

        ---

        ## Changelog

        ### Version 1.0 (2025-01-15)
        - Initial API documentation
        - Complete Postman collection
        - 8-node cluster support
        - TinyLlama metadata enrichment
        - Distributed query strategies
        - SQL with peer fallback
        - TOSCA template management

        ---

        **End of Documentation**

        *Last Updated: 2025-01-15*
        *API Version: 1.0*
        *OptimusDB Version: SwarmKB Edition*

