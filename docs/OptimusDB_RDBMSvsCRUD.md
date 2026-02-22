# SQLite Datastore Decentralization Analysis — OptimusDB

## Executive Summary

**SQLite in OptimusDB is _distributed_ but _not fully decentralized_.** Each peer maintains an independent, local SQLite instance with no automatic replication or CRDT-based synchronization. Cross-node data access relies on request-response streaming over libp2p — fundamentally different from the OrbitDB/IPFS stores which achieve true decentralization through automatic CRDT replication.

The system implements a **federated query model** for SQLite: queries fan out to peers on demand, results merge and deduplicate at the requester, but data ownership remains local. This is a deliberate architectural choice that trades eventual consistency for SQL queryability and performance.

---

## 1. How SQLite Is Initialized (Per-Node Isolation)

Each OptimusDB agent creates its **own isolated SQLite database file** during peer initialization.

**File: `app/initPeer.go` — lines ~235-250**

```go
rdbmsCache := filepath.Join(cache, *config.FlagRDBMSDB+".db")
rdbms, err = InitSQLite(rdbmsCache)  // Each node gets its own file
```

The path resolves to something like:

```
~/.cache/optimusdb/..repo../orbitdb/..db_name.db
        ```

        In a Docker cluster (agents 1–8), each container has its own filesystem, so each agent operates on a completely separate `.db` file. There is **no shared filesystem, no shared SQLite WAL, and no multi-writer access**.

        **File: `app/app.go` — `InitSQLite()`**

        ```go
        func InitSQLite(dbPath string) (*KnowledgeBaseSQLite, error) {
        db, err := sql.Open("sqlite3", dbPath)
        // Creates tables: datacatalog, tosca_metadata, metadata_catalog
        GlobalKBSQLite = &KnowledgeBaseSQLite{DB: db}
        ...
        }
        ```

        The `GlobalKBSQLite` singleton means each process has exactly **one SQLite connection** — purely local.

        ---

        ## 2. Contrast: OrbitDB Stores Are Truly Decentralized

        OrbitDB stores are initialized with `Replicate: boolPtr(true)` and open access controllers:

        ```go
        // From initPeer.go — DsSWres store
        dbopts = orbitdb.CreateDBOptions{
        Replicate:        boolPtr(true),    // ← Automatic CRDT replication
        AccessController: fullRW,           // ← Open write access
        ...
        }
        ```

        OrbitDB achieves decentralization through:

        | Property | OrbitDB Stores | SQLite |
        |----------|---------------|--------|
        | **Data location** | IPFS content-addressed blocks | Local filesystem `.db` file |
        | **Replication** | Automatic CRDT-based (EventReplicated events) | None — data stays local |
        | **Write model** | Any peer with access can write; conflicts resolve via CRDTs | Only the local process writes |
        | **Consistency** | Eventual consistency across all peers | Strong local consistency only |
        | **Discovery** | Stores are addressed by IPFS CIDs | No address — file path only |

        The `awaitReplicateEvent()` function in `service.go` subscribes to replication events for OrbitDB stores (DsSWres, KBMetadata, KBdata) but **there is no equivalent for SQLite**.

        ---

        ## 3. How SQLite Achieves Cross-Node Access

        Instead of replication, OptimusDB uses **on-demand peer querying** for SQLite:

        ### 3.1 The `/sqldml/1.0.0` Stream Protocol

        **File: `app/service.go` — `AwaitRegisterSQLDMLStreamHandler()`**

        ```go
        hostCID.SetStreamHandler("/sqldml/1.0.0", func(s network.Stream) {
        var req Request
        json.NewDecoder(s).Decode(&req)
        result, err := GlobalKBSQLite.SqlDML(req.SQLDML, logChan)  // Execute on LOCAL SQLite
        json.NewEncoder(s).Encode(records)
        })
        ```

        When Agent-1 registers this handler, it is saying: *"If another peer sends me a SQL query, I'll execute it against MY local SQLite and return results."*

        ### 3.2 The Fallback Query Chain

        **File: `app/service.go` — `SQLDMLWithPeerFallback()`**

        The query flow is:

        ```
        1. Execute SQL on LOCAL SQLite
        2. If no results → queryRemoteSQLitePeers()
        3. Fan out to all connected peers via /sqldml/1.0.0
        4. Merge results from all peers
        5. Deduplicate using DedupSQLResults() (SHA-256 hash)
        6. Return combined results
        ```

        ```go
        func SQLDMLWithPeerFallback(req Request, logChan chan Log, db *KnowledgeBaseDB) (interface{}, error) {
        // Step 1: Local execution
        result, err := GlobalKBSQLite.SqlDML(req.SQLDML, logChan)

        // Step 2: Remote fallback if empty
        records, ok := result.([]map[string]interface{})
        if ok && len(records) == 0 {
        peerRecords, err := queryRemoteSQLitePeers(req, db)
        records = append(records, peerRecords...)
        }

        // Step 3: Dedup
        deduped := DedupSQLResults(records)
        return deduped, nil
        }
        ```

        ### 3.3 Remote Peer Query Mechanism

        **File: `app/service.go` — `queryRemoteSQLitePeers()`**

        ```go
        func queryRemoteSQLitePeers(req Request, db *KnowledgeBaseDB) ([]map[string]interface{}, error) {
        peers := db.Node.PeerHost.Peerstore().Peers()
        for _, peerID := range peers {
        if peerID == db.Node.Identity { continue }  // Skip self
        stream, _ := db.Node.PeerHost.NewStream(ctx, peerID, "/sqldml/1.0.0")
        json.NewEncoder(stream).Encode(req)          // Send SQL to peer
        json.NewDecoder(stream).Decode(&results)      // Read peer's local results
        }
        return allResults, nil
        }
        ```

        ---

        ## 4. What This Architecture Means

        ### 4.1 Data Sovereignty Is Per-Node

        If Agent-3 inserts a row into its SQLite `datacatalog` table, that row **only exists on Agent-3**. No other agent will have it unless:

        - Another agent explicitly queries Agent-3 (on-demand)
        - The `SQLDMLWithPeerFallback` triggers a remote query
        - The data happens to also be stored in an OrbitDB store (dual-write path)

        ### 4.2 The Dual-Write Pattern

        For INSERT operations, `SQLDMLWithPeerFallback` writes metadata to **both** systems:

        ```go
        // OrbitDB (decentralized — will replicate automatically)
        (*db.KBMetadata).Put(ctx, metadataMap)

        // SQLite (local only — will NOT replicate)
        GlobalKBSQLite.DB.Exec(insertMetaSQL, ...)
        ```

        This means:
        - **OrbitDB metadata** → eventually consistent across all peers
        - **SQLite metadata** → only on the inserting node

        ### 4.3 No Write Propagation

        When Agent-1 receives an INSERT via its HTTP API, the write goes to Agent-1's SQLite only. There is **no mechanism** to:

        - Forward the INSERT to other peers' SQLite instances
        - Replicate the transaction log
        - Sync schema changes across nodes

        ### 4.4 Query-Time Federation (Not Replication)

        The system achieves distributed reads through **query federation**:

        ```
        Agent-1: SELECT * FROM datacatalog WHERE status='active'
        ├── Execute locally → 3 rows
        ├── Stream to Agent-2 → Agent-2 executes locally → 5 rows
        ├── Stream to Agent-3 → Agent-3 executes locally → 2 rows
        └── Merge + Dedup → 8 unique rows returned
        ```

        This is fundamentally different from OrbitDB's approach where all 8 rows would eventually exist on every node.

        ---

        ## 5. Decentralization Scorecard

        | Criterion | OrbitDB Stores | SQLite Stores | Assessment |
        |-----------|---------------|---------------|------------|
        | **No single point of failure** | ✅ Any node can serve data | ⚠️ Data lost if owning node dies | Partial |
        | **No central authority** | ✅ CRDT conflict resolution | ✅ No master node | Yes |
        | **Data replication** | ✅ Automatic via IPFS/CRDTs | ❌ No replication | No |
        | **Eventual consistency** | ✅ Built-in | ❌ No convergence guarantee | No |
        | **Partition tolerance** | ✅ Operates independently | ⚠️ Local only during partition | Partial |
        | **Distributed reads** | ✅ Local reads after replication | ✅ Via query federation | Yes (different mechanism) |
        | **Distributed writes** | ✅ Any node writes, CRDTs merge | ❌ Writes are local only | No |
        | **Content addressability** | ✅ IPFS CIDs | ❌ File path based | No |
        | **Schema enforcement** | ❌ Schemaless JSON | ✅ SQL DDL | N/A (different model) |

        **Overall: SQLite scores 2/8 on decentralization criteria vs. OrbitDB's 7/8.**

        ---

        ## 6. Why This Design Makes Sense

        The hybrid architecture is **intentional and well-justified**:

        1. **SQL Queryability**: OrbitDB's query model (filter functions over all documents) cannot match SQL's expressiveness for relational queries, JOINs, aggregations, and indexed lookups.

        2. **Performance**: Local SQLite queries execute in microseconds. OrbitDB queries scan all documents in memory. For the `datacatalog` and `tosca_metadata` tables, SQLite provides dramatically faster structured queries.

        3. **Amundsen/Catalog Integration**: The `metadata_catalog` and `datacatalog` tables follow Amundsen-compatible schemas that assume SQL access patterns.

        4. **The Query Federation Bridge**: The `/sqldml/1.0.0` protocol effectively makes SQLite "network-queryable" without the complexity of distributed transaction coordination, write-ahead log shipping, or consensus protocols.

        5. **Complementary Roles**: OrbitDB handles the decentralized data catalog (metadata, TOSCA templates, contributions) while SQLite handles structured, high-performance local queries. The dual-write path in `SQLDMLWithPeerFallback` bridges both worlds.

        ---

        ## 7. Potential Improvements for Full Decentralization

        If true SQLite decentralization were required, the following approaches could be considered:

        1. **Write-Ahead Log (WAL) Replication**: Stream SQLite WAL frames to peers via libp2p, similar to Litestream or rqlite.

        2. **CRDT-Based SQL Tables**: Use CRDTs (like cr-sqlite) to enable multi-writer SQLite with automatic conflict resolution.

        3. **Raft Consensus for Writes**: Implement Raft (like rqlite) where one elected leader accepts writes and replicates to followers.

        4. **Event Sourcing**: Capture SQL mutations as events in an OrbitDB EventLog, replay them on each node's SQLite — leveraging OrbitDB's existing replication.

        5. **Periodic Sync Protocol**: Define a `/sqlite-sync/1.0.0` stream handler that periodically exchanges table checksums and synchronizes missing rows.

        ---

        ## 8. Conclusion

        SQLite in OptimusDB serves as a **high-performance local query engine** with **network-federated read access**, not as a decentralized datastore. The true decentralization layer is OrbitDB/IPFS, which provides automatic CRDT replication, content-addressed storage, and eventual consistency across all peers.

        The current design is architecturally sound for the DCS Triad: OrbitDB delivers **Decentralization** and **Consistency** (eventual), while SQLite delivers **Scalability** through performant local queries with federation. Together, they form a complementary system where each technology handles what it does best.

        **Classification**: SQLite in OptimusDB is a **distributed federated query system**, not a decentralized datastore.
