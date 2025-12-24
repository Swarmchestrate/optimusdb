# OptimusDB Hackathon - Postman Collection

**Complete testing suite for OptimusDB distributed knowledge base system**

[![Project](https://img.shields.io/badge/Project-EU%20Horizon%20Europe-blue)](https://cordis.europa.eu/project/id/101135012)
[![Grant](https://img.shields.io/badge/Grant-101135012-green)](https://swarmchestrate.eu)
[![Version](https://img.shields.io/badge/Version-1.0-orange)](https://github.com/swarmchestrate/optimusdb)
[![Status](https://img.shields.io/badge/Status-Active-success)](https://github.com/swarmchestrate/optimusdb)

---

## 📋 Table of Contents

- [Overview](#overview)
- [What is OptimusDB?](#what-is-optimusdb)
- [Quick Start](#quick-start)
- [Collection Structure](#collection-structure)
- [Feature Showcase](#feature-showcase)
- [Common Workflows](#common-workflows)
- [Configuration Guide](#configuration-guide)
- [Troubleshooting](#troubleshooting)
- [Advanced Usage](#advanced-usage)
- [Support](#support)

---

## Overview

This Postman collection provides **comprehensive testing and demonstration** of OptimusDB capabilities for the **EU Horizon Europe Swarmchestrate project (Grant 101135012)**. It includes 70+ requests organized into 11 functional categories covering everything from basic CRUD operations to advanced distributed query strategies.

**Project:** Swarmchestrate - Decentralized Knowledge Base for Renewable Energy Metadata
**Technology:** LibP2P, pubsub, ipfs, SQLite, TinyLlama AI, W3C Verifiable Credentials
**Version:** 1.0
**Date:** December 2025

---

## What is OptimusDB?

OptimusDB is a **peer-to-peer decentralized knowledge base** designed for the Swarmchestrate project. It combines:

🔗 **LibP2P** - Peer-to-peer networking with GossipSub mesh topology
📦 **CRUD Store** - decentralized databases (DocumentStore, EventLog)
🗄️ **RDBMS** - Local caching and relational queries
🤖 **TinyLlama** - AI-powered metadata enrichment
🔐 **W3C DID** - Decentralized identity and verifiable credentials
⚡ **Distributed Queries** - 10 different query strategies for performance optimization

### Key Features:

- ✅ **Autonomous metadata generation** using AI (TinyLlama LLM)
- ✅ **Distributed consensus** with coordinator/follower election
- ✅ **Automatic replication** across agent clusters
- ✅ **Nested JSON queries** with operators ($gte, $regex, $contains, etc.)
- ✅ **TOSCA template management** for infrastructure orchestration
- ✅ **SQL operations** with automatic peer fallback
- ✅ **Event management** with comprehensive logging
- ✅ **Verifiable credentials** for decentralized identity

---

## Quick Start

### Prerequisites

**Required:**
- ✅ Postman (Desktop or Web)
- ✅ OptimusDB agent running (default: http://localhost:18001)

**Optional:**
- 🔶 Multiple OptimusDB agents (for replication testing)
- 🔶 TinyLlama LLM (for AI metadata enrichment)

### Installation

**1. Import Collection**
```
File → Import → OptimusDB_Hackathon.postman_collection.json
```

**2. Create Environment**
```
Name: OptimusDB Local
```

**3. Set Variables**
```javascript
base_url:   http://localhost:18001
context:    swarmkb
dataStore:  kbdata
```

**4. Start OptimusDB**
```bash
./optimusdb -port 18001 -context swarmkb -benchmark
```

**5. Run Your First Request**
```
7. System Related → Check Agent Status
```

**Expected Response:**
```json
{
"role": "coordinator",
"health_score": 95.5,
"connected_peers": 7,
"status": "healthy"
}
```

✅ **If you get this response, you're ready to go!**

---

## Collection Structure

### 📁 11 Main Folders | 70+ Requests

```
OptimusDB - Hackathon/
│
├── 1. Upload TOSCA Files (5 requests)
│   └── Upload infrastructure templates to system
│
├── 2. TOSCA Queries (7 requests)
│   ├── Scenario Small - Basic queries
│   ├── Scenario Moderate - Multi-criteria queries
│   └── Scenario Complex - Advanced matching
│
├── 3. Agent Queries Strategy (10 requests)
│   └── Test 10 different distributed query strategies
│
├── 4. CRUD Operations (8 requests)
│   └── Complete Create, Read, Update, Delete lifecycle
│
├── 5. Batch Operations (3 requests)
│   └── Multi-document insert and delete
│
├── 6. Replication Tests (6 requests)
│   └── Verify data sync across cluster agents
│
├── 7. System Related (9 requests)
│   └── Health checks, benchmarks, logs, cache
│
├── 8. EMS - Event Management (6 requests)
│   └── Access logs and events via SQL queries
│
├── 9. Credentials - DID (8 requests)
│   └── W3C Verifiable Credentials management
│
├── 10. Metadata (6 requests)
│   └── AI-powered metadata enrichment
│
└── 11. SQL Operations (9 requests)
└── SQL queries with automatic peer fallback
```

---

## Feature Showcase

### 🎯 Feature 1: TOSCA Template Management

**What it does:** Store and query infrastructure-as-code templates

**Requests:**
- Upload WebApp ADT
- Upload Capacity Profile (GPU, CPU, storage specs)
- Upload Deployment Plans
- Query by metadata, author, resources

**Use Case:** Infrastructure orchestration for edge computing

**Example Query:**
```json
{
"method": {"cmd": "query"},
"dstype": "kbdata",
"criteria": [{
"metadata.status": "available",
"topology.gpu_accelerator_01.properties.available": true,
"topology.edge_compute_node_01.properties.available_cpu_cores": {"$gte": 20}
}]
}
```

**Returns:** Available capacity nodes with GPUs and sufficient CPU

---

### 🎯 Feature 2: Distributed Query Strategies

**What it does:** 10 different strategies for querying distributed data

| Strategy | Speed | Use Case |
|----------|-------|----------|
| **LOCAL_ONLY** | ~50ms | Baseline test |
| **REMOTE_ONLY** | ~200ms | Network test |
| **LOCAL_THEN_REMOTE_MERGE** | ~300ms | Standard mode |
| **PARALLEL_MERGE** | ~150ms | Speed priority |
| **QUORUM** | ~500ms | Consistency priority |
| **STALE_OK (cache)** | ~20ms | Read-heavy workloads |

**Example:**
```json
{
"options": {
"strategy": "PARALLEL_MERGE",
"time_budget_ms": 2000,
"annotate_source": true
}
}
```

**Result:** Fast queries with source tracking (local vs peer data)

---

### 🎯 Feature 3: Nested JSON Queries

**What it does:** Query deeply nested document structures

**Supports:**
- ✅ Nested paths: `metadata.template.version`
- ✅ Numeric operators: `$gte`, `$lte`, `$gt`, `$lt`
- ✅ String operators: `$regex`, `$contains`
- ✅ Array operators: `$contains`, `$all`
- ✅ Logical operators: `$and`, `$or`

**Example:**
```json
{
"criteria": [{
"$and": [
{"metadata.region": {"$regex": "eu-.*"}},
{"topology.edge_compute_node_01.properties.available_cpu_cores": {"$gte": 16}},
{"node_types": {"$contains": "tosca.nodes.Compute.GPU"}}
]
}]
}
```

**Returns:** EU nodes with 16+ CPUs and GPU support

---

### 🎯 Feature 4: Automatic Replication

**What it does:** Sync data across cluster in ~3 seconds

**Test Sequence:**
1. Insert on Agent 1 (port 18001)
2. Wait 3 seconds
3. Query on Agent 2 (port 18002)
4. Verify data replicated

**Use Case:** High availability, fault tolerance

---

### 🎯 Feature 5: AI Metadata Enrichment

**What it does:** TinyLlama generates descriptions, tags, keywords

**Process:**
1. SQL INSERT into products table
2. TinyLlama analyzes content
3. Auto-generates:
- Description summary
- Keywords
- Tags
- Component type

**Example:**
```sql
INSERT INTO products (name, description, price)
VALUES ('Mechanical Keyboard Pro', 'Professional mechanical keyboard with Cherry MX switches...', 179.99)
```

**Generated Metadata:**
```json
{
"description": "Professional mechanical keyboard for gaming and productivity",
"tags": ["keyboard", "mechanical", "gaming", "rgb"],
"keywords": ["cherry-mx", "switches", "backlight"],
"component": "products"
}
```

---

### 🎯 Feature 6: W3C Verifiable Credentials

**What it does:** Store and verify decentralized identity credentials

**Operations:**
- Store credentials
- Query by issuer/subject
- Verify authenticity
- Revoke credentials

**Example:**
```json
{
"@context": ["https://www.w3.org/2018/credentials/v1"],
"id": "http://example.edu/credentials/3732",
"type": ["VerifiableCredential"],
"issuer": "did:example:issuer123",
"credentialSubject": {
"id": "did:example:subject456",
"degree": "Bachelor of Science"
}
}
```

**Use Case:** Academic credentials, access control, trust systems

---

### 🎯 Feature 7: SQL with Peer Fallback

**What it does:** Execute SQL queries with automatic distributed fallback

**Process:**
1. Query local SQLite database
2. If empty → Automatically query remote peers
3. Merge results from all sources
4. Deduplicate rows
5. Return combined dataset

**Example:**
```sql
SELECT * FROM products WHERE price > 1000 ORDER BY price DESC LIMIT 10
```

**Result:** Gets data from local DB + all connected peers automatically

---

## Common Workflows

### ✅ Workflow 1: Basic System Validation (5 min)

**Verify OptimusDB is working correctly**

```
Step 1: System Related → Check Agent Status
Expected: role=coordinator/follower, peers>0

Step 2: System Related → Discovered Peers
Expected: List of peer IDs

Step 3: CRUD Operations → INSERT Test Document
Expected: "Successfully inserted"

Step 4: CRUD Operations → QUERY#1 Test Document
Expected: Returns inserted document

Step 5: CRUD Operations → DELETE Test Document
Expected: "deleted successfully"
```

**Success:** All 5 requests succeed ✅

---

### ✅ Workflow 2: TOSCA Template Testing (10 min)

**Complete template lifecycle**

```
Step 1: TOSCA Queries (Moderate) → INSERT Test Document
Inserts nested WebApp template to kbdata

Step 2: TOSCA Queries (Moderate) → Query 1: Find ADT Applications
Expected: 1 result (WebApp template)

Step 3: TOSCA Queries (Moderate) → Query 2: Find by Author
Expected: 1-3 results

Step 4: TOSCA Queries (Moderate) → Query 3: Find GPU Capacity
Expected: 1 result (if capacity template exists)

Step 5: Agent Queries Strategy → LOCAL_THEN_REMOTE_MERGE
Expected: Results with source annotation
```

**Success:** All queries return expected documents ✅

---

### ✅ Workflow 3: Distributed System Testing (15 min)

**Verify replication and distributed queries**

**Prerequisites:** Multiple OptimusDB nodes running

```
Step 1: Replication Tests → Insert on Agent 1
URL: http://localhost:18001
Expected: Insert successful

Step 2: Wait 3 seconds (replication delay)

Step 3: Replication Tests → Verify on Agent 2
URL: http://localhost:18002
Expected: Document present (replicated!)

Step 4: Replication Tests → Verify on Agent 3
URL: http://localhost:18003
Expected: Document present

Step 5: Replication Tests → Distributed Query Test
Expected: Shows which Agents have data

Step 6: Agent Queries Strategy → PARALLEL_MERGE
Expected: Fast merged results

Step 7: Agent Queries Strategy → QUORUM
Expected: Consistent results from majority
```

**Success:** Data replicates within 3 seconds ✅

---

### ✅ Workflow 4: Performance Benchmarking (20 min)

**Compare query strategy performance**

```
Run each strategy and record query_time_ms:

1. LOCAL_ONLY           → Baseline (~50ms)
2. STALE_OK (cache)     → Fastest (~20ms)
3. PARALLEL_MERGE       → Good balance (~150ms)
4. LOCAL_THEN_REMOTE    → Standard (~300ms)
5. QUORUM               → Consistency (~500ms)
6. Consistency=ALL      → Slowest (~800ms)
```

**Success:** STALE_OK < LOCAL_ONLY < PARALLEL_MERGE < QUORUM ✅

---

### ✅ Workflow 5: AI Metadata Enrichment (10 min)

**Test TinyLlama integration**

**Prerequisites:**
- TinyLlama running on port 11434
- Metadata service enabled

```
Step 1: SQL Operations → SQL CREATE TABLE
Creates products table

Step 2: SQL Operations → SQL INSERT with Metadata
Inserts product + triggers AI enrichment

Step 3: SQL Operations → SQL Query Data Catalog
Expected: Shows AI-generated metadata

Step 4: Metadata → Profile Dataset
Expected: Statistical profile

Step 5: Metadata → Enrich Single Dataset
Expected: AI description, tags, keywords

Step 6: Metadata → Get Metadata Metrics
Expected: Enrichment statistics
```

**Success:** Metadata auto-generated with AI descriptions ✅

---

## Configuration Guide

### Environment Variables

**Required:**
```javascript
base_url:   http://localhost:18001  // Agent URL
context:    swarmkb                 // API context path
dataStore:  kbdata                  // Default datastore
```

**Optional:**
```javascript
webapp_template_id:       ""  // Auto-set by upload requests
capacity_template_id:     ""  // Auto-set by upload requests
opentofu_template_id:     ""  // Auto-set by upload requests
deployment_template_id:   ""  // Auto-set by upload requests
requirements_template_id: ""  // Auto-set by upload requests
```

### Multi-Node Setup

**For replication testing:**

```javascript
// Create environment variables for each node
agent1_url: http://localhost:18001
agent2_url: http://localhost:18002
agent3_url: http://localhost:18003
// ... up to agent8_url
```

**Start multiple agents:**
```bash
# Terminal 1
./optimusdb -port 18001 -context swarmkb -peerid node1

# Terminal 2
./optimusdb -port 18002 -context swarmkb -peerid node2 -bootstrap /ip4/127.0.0.1/tcp/18001/p2p/<node1_peer_id></node1_peer_id>>

    # Terminal 3
    ./optimusdb -port 18003 -context swarmkb -peerid node3 -bootstrap /ip4/127.0.0.1/tcp/18001/p2p/<node1_peer_id></node1_peer_id>>
        ```

        ### TinyLlama Setup

        **For AI metadata enrichment:**

        ```bash
        # Start TinyLlama (Ollama)
        ollama serve

        # Pull TinyLlama model
        ollama pull tinyllama

        # Verify
        curl http://localhost:11434/api/health
        ```

        **Enable in OptimusDB:**
        ```bash
        ./optimusdb -port 18001 -metadata-enabled -llm-url http://localhost:11434
        ```

        ---

        ## Troubleshooting

        ### ❌ Issue: "Failed to connect to localhost:18001"

        **Cause:** OptimusDB not running

        **Solution:**
        ```bash
        # Start OptimusDB
        ./optimusdb -port 18001 -context swarmkb -benchmark

        # Verify
        curl http://localhost:18001/swarmkb/agent/status
        ```

        ---

        ### ❌ Issue: Queries return empty []

        **Common Causes:**

        **1. Wrong datastore**
        ```json
        // Check you're using the right store
        "dstype": "kbdata"    // TOSCA templates
        "dstype": "dsswres"   // General documents
        ```

        **2. No data inserted**
        ```bash
        # Verify data exists
        {
        "method": {"cmd": "crudget"},
        "dstype": "kbdata",
        "criteria": []  // Returns all documents
        }
        ```

        **3. Flattened keys instead of nested JSON**
        ```json
        // ❌ WRONG (flattened)
        {
        "metadata_status": "available"
        }

        // ✅ CORRECT (nested)
        {
        "metadata": {
        "status": "available"
        }
        }
        ```

        ---

        ### ❌ Issue: Nested path queries don't work

        **Cause:** Using flattened keys in document structure

        **Solution:** Insert with proper nested structure

        ```json
        // ✅ CORRECT INSERT
        {
        "metadata": {
        "status": "available",
        "region": "eu-central-1"
        },
        "topology": {
        "edge_compute_node_01": {
        "properties": {
        "available_cpu_cores": 24
        }
        }
        }
        }

        // Then query with paths
        {
        "criteria": [{
        "metadata.status": "available",
        "topology.edge_compute_node_01.properties.available_cpu_cores": {"$gte": 20}
        }]
        }
        ```

        ---

        ### ❌ Issue: Replication not working

        **Causes:**
        - Agents not in same peer network
        - Firewall blocking ports
        - Different swarm IDs

        **Solutions:**
        ```bash
        # Check peers
        curl http://localhost:18001/swarmkb/peers

        # Should show connected peers
        # If empty, check bootstrap nodes

        # Verify connectivity
        ping 'peer_ip'
            telnet 'peer_ip' 18001
                ```

                ---

                ### ❌ Issue: UPDATE doesn't preserve _id

                **Cause:** Old bug in crudUpdate function

                **Solution:** Ensure you're using OptimusDB version with _id preservation fix

                **Verify:**
                ```json
                // After UPDATE, query and check
                {
                "_id": "test_doc_001",  // ✅ Should be preserved
                "status": "updated",
                "_updated_at": "2025-12-25T..."
                }
                ```

                ---

                ### ❌ Issue: Metadata enrichment fails

                **Causes:**
                - TinyLlama not running
                - Wrong port configuration
                - Metadata service not enabled

                **Solutions:**
                ```bash
                # Check TinyLlama
                curl http://localhost:11434/api/health

                # Check OptimusDB metadata health
                curl http://localhost:18001/api/v1/metadata/health

                # Start OptimusDB with metadata enabled
                ./optimusdb -port 18001 -metadata-enabled -llm-url http://localhost:11434
                ```

                ---

                ### ❌ Issue: TOSCA uploads fail (base64 error)

                **Cause:** Collection expects base64-encoded YAML files

                **Solution:** Use nested JSON inserts instead

                ```
                Use: TOSCA Queries (Moderate) → INSERT Test Document
                Instead of: Upload TOSCA Files folder

                This inserts proper nested JSON without base64 encoding
                ```

                ---

                ## Advanced Usage

                ### Custom Query Operators

                **Numeric Comparisons:**
                ```json
                {"price": {"$gte": 100}}        // Greater than or equal
                {"price": {"$lte": 1000}}       // Less than or equal
                {"price": {"$gt": 100}}         // Greater than
                {"price": {"$lt": 1000}}        // Less than
                {"price": {"$ne": 500}}         // Not equal
                ```

                **String Operations:**
                ```json
                {"region": {"$regex": "eu-.*"}}               // Pattern match
                {"name": {"$regex": "^Gaming.*"}}             // Starts with
                {"description": {"$regex": ".*keyboard.*"}}   // Contains
                ```

                **Array Operations:**
                ```json
                {"tags": {"$contains": "gaming"}}             // Has element
                {"tags": {"$all": ["gaming", "rgb"]}}         // Has all elements
                ```

                **Logical Operators:**
                ```json
                {
                "$and": [
                {"price": {"$gte": 100}},
                {"stock": {"$gt": 0}}
                ]
                }

                {
                "$or": [
                {"category": "Gaming"},
                {"category": "Accessories"}
                ]
                }
                ```

                **Nested Combinations:**
                ```json
                {
                "$and": [
                {"metadata.status": "available"},
                {
                "$or": [
                {"metadata.region": "eu-central-1"},
                {"metadata.region": "eu-west-1"}
                ]
                },
                {"topology.gpu.properties.available": true}
                ]
                }
                ```

                ---

                ### Query Strategy Selection Guide

                **Choose strategy based on your needs:**

                | Requirement | Strategy | Why |
                |-------------|----------|-----|
                | **Fastest response** | STALE_OK (cache) | Uses cache if available |
                | **Local-only data** | LOCAL_ONLY | No network overhead |
                | **Eventual consistency** | PARALLEL_MERGE | Queries all nodes fast |
                | **Strong consistency** | QUORUM | Waits for majority |
                | **Complete dataset** | Consistency=ALL | Waits for all nodes |
                | **Network testing** | REMOTE_ONLY | Forces peer queries |
                | **Limited bandwidth** | max_peers=2 | Reduces fanout |
                | **Standard mode** | LOCAL_THEN_REMOTE | Good default |

                **Performance vs Consistency Trade-off:**

                ```
                Fast                                    Slow
                ├────┬────┬────┬────┬────┬────┬────┬────┤
                STALE_OK  LOCAL  PARALLEL  LOCAL_THEN  QUORUM  ALL
                ↑                                            ↑
                Low Consistency                    High Consistency
                ```

                ---

                ### SQL Advanced Features

                **Joins:**
                ```sql
                SELECT p.name, c.name as category_name
                FROM products p
                JOIN categories c ON p.category_id = c.id
                WHERE p.price > 100
                ```

                **Aggregations:**
                ```sql
                SELECT category,
                COUNT(*) as count,
                AVG(price) as avg_price,
                SUM(stock) as total_stock
                FROM products
                GROUP BY category
                HAVING COUNT(*) > 5
                ```

                **Subqueries:**
                ```sql
                SELECT * FROM products
                WHERE price > (SELECT AVG(price) FROM products)
                ```

                **Peer Fallback:**
                ```
                1. Execute query on local SQLite
                2. If result set is empty → Query all connected peers
                3. Merge results from all sources
                4. Deduplicate by unique columns
                5. Return combined dataset
                ```

                ---

                ### EMS SQL Queries

                **Query logs by level:**
                ```sql
                SELECT timestamp, level, message, source
                FROM optimusLogger
                WHERE level = 'ERROR'
                AND date = '2025-12-25'
                ORDER BY timestamp DESC
                LIMIT 50
                ```

                **Check coordinator status:**
                ```sql
                SELECT leader_id, term, timestamp, health_score
                FROM optimusLogger
                WHERE message LIKE '%election%'
                ORDER BY timestamp DESC
                LIMIT 1
                ```

                **Performance metrics:**
                ```sql
                SELECT
                AVG(query_time_ms) as avg_query_time,
                MAX(query_time_ms) as max_query_time,
                COUNT(*) as total_queries
                FROM ems_events
                WHERE event_type = 'query_executed'
                AND timestamp > datetime('now', '-1 hour')
                ```

                ---

                ## Support

                ### 📚 Documentation

                - **OptimusDB Docs:** https://github.com/swarmchestrate/optimusdb
                - **Swarmchestrate Project:** https://swarmchestrate.eu
                - **EU Grant Portal:** https://cordis.europa.eu/project/id/101135012

                ### 🐛 Issues & Bugs

                - **GitHub Issues:** https://github.com/swarmchestrate/optimusdb/issues
                - **Email:** support@swarmchestrate.eu

                ### 💬 Community

                - **Discussions:** https://github.com/swarmchestrate/optimusdb/discussions
                - **Slack:** swarmchestrate.slack.com

                ---

                ## Collection Metadata

                **Project:** EU Horizon Europe Grant 101135012 (Swarmchestrate)
                **Version:** 1.0
                **Last Updated:** December 2025
                **Total Requests:** 70+
                **Categories:** 11
                **Maintainer:** Swarmchestrate Team

                ---

                ## License

                This collection is part of the Swarmchestrate project funded by the European Union's Horizon Europe research and innovation programme under grant agreement No 101135012.

                ---

                ## Quick Reference Card

                ### Essential Requests (Top 10)

                ```
                1. System Related → Check Agent Status
                Verify OptimusDB is running

                2. System Related → Discovered Peers
                See connected nodes

                3. CRUD Operations → INSERT Test Document
                Insert sample data

                4. CRUD Operations → QUERY#1 Test Document
                Verify insert worked

                5. TOSCA Queries (Moderate) → INSERT Test Document
                Insert nested TOSCA template

                6. TOSCA Queries (Moderate) → Query 3: Find GPU Capacity
                Complex nested query example

                7. Agent Queries Strategy → PARALLEL_MERGE
                Fast distributed query

                8. Agent Queries Strategy → QUORUM
                Consistent distributed query

                9. SQL Operations → SQL INSERT with Metadata
                AI metadata generation

                10. Replication Tests → Distributed Query Test
                See data distribution
                ```

                ### Key Endpoints

                ```
                Base URL: http://localhost:18001

                GET  /swarmkb/agent/status       - Agent health
                GET  /swarmkb/peers               - Connected peers
                POST /swarmkb/command             - CRUD & Query
                GET  /swarmkb/ems/logs            - System logs
                GET  /api/v1/metadata/health      - AI service health
                ```

                ### Datastores

                ```
                dsswres     - General documents
                kbdata      - TOSCA templates, KB data
                kbmetadata  - Auto-generated metadata
                validations - Validation records
                ```

                ---

                **🎉 Ready to explore OptimusDB? Start with "Check Agent Status"!**
