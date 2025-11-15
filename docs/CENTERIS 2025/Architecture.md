# OptimusDB TinyLlama Deployment Architecture

## System Overview
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         OptimusDB Distributed Cluster                       │
│                              (DCS Triad Implementation)                     │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                           PRODUCTION ARCHITECTURE                           │
│                              (16 Containers)                                │
└─────────────────────────────────────────────────────────────────────────────┘

Docker Network: optimusdb-network
(172.28.0.0/16)

┌─────────────────────────────────────────────────────────────────────────────┐
│                          COORDINATOR NODE (Agent-1)                         │
│                           Enhanced Resources                                │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│  Container: tinyllama-agent-1                                    │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  llama.cpp Server                                          │  │
│  │  • Model: TinyLlama-1.1B-Chat-v1.0 (Q4_K_M)                │  │
│  │  • Context Size: 4096 tokens                               │  │
│  │  • Threads: 4                                              │  │
│  │  • Resources: 8GB RAM, 4 CPU cores                         │  │
│  │  • Port: 8080                                              │  │
│  │  • Health Check: /health (30s interval)                    │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
│
│ HTTP
│ http://tinyllama-agent-1:8080/v1/completions
▼
┌──────────────────────────────────────────────────────────────────┐
│  Container: optimusdb-agent-1                                    │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  OptimusDB Agent (Coordinator)                             │  │
│  │  • Agent ID: 1                                             │  │
│  │  • Role: Coordinator (via election)                        │  │
│  │  • REST API: 9091                                          │  │
│  │  • Credentials API: 8081                                   │  │
│  │  • LibP2P: Peer-to-peer networking                         │  │
│  │  • IPFS: Content-addressed storage                         │  │
│  │  • OrbitDB: CRDT-based distributed DB                      │  │
│  │  • SQLite: Local metadata + reputation                     │  │
│  │  • Resources: 4GB RAM, 2 CPU cores                         │  │
│  │  • TinyLlama Client: HTTP (tinyllama_http.go)              │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
│
│ LibP2P + GossipSub
│ Peer-to-peer mesh
▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FOLLOWER NODES (Agents 2-8)                           │
│                          Standard Resources                                  │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┐
│  Agent-2    │  Agent-3    │  Agent-4    │  Agent-5    │  Agent-6    │  Agent-7    │  Agent-8    │
└─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘

Each Follower Node (Identical Structure):

┌──────────────────────────────────────────────────────────────────┐
│  Container: tinyllama-agent-N                                    │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  llama.cpp Server                                          │ │
│  │  • Model: TinyLlama-1.1B-Chat-v1.0 (Q4_K_M)              │ │
│  │  • Context Size: 2048 tokens                              │ │
│  │  • Threads: 2                                             │ │
│  │  • Resources: 4GB RAM, 2 CPU cores                        │ │
│  │  • Port: 8080                                             │ │
│  │  • Health Check: /health (30s interval)                   │ │
│  └────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
│
│ HTTP (localhost network)
│ http://tinyllama-agent-N:8080/v1/completions
▼
┌──────────────────────────────────────────────────────────────────┐
│  Container: optimusdb-agent-N                                    │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  OptimusDB Agent (Follower)                                │ │
│  │  • Agent ID: N (2-8)                                       │ │
│  │  • Role: Follower (participates in election)               │ │
│  │  • REST API: 909N                                          │ │
│  │  • Credentials API: 808N                                   │ │
│  │  • LibP2P: Peer-to-peer networking                         │ │
│  │  • IPFS: Content-addressed storage                         │ │
│  │  • OrbitDB: CRDT-based distributed DB                      │ │
│  │  • SQLite: Local metadata + reputation                     │ │
│  │  • Resources: 4GB RAM, 2 CPU cores                         │ │
│  │  • TinyLlama Client: HTTP (tinyllama_http.go)             │ │
│  └────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

## Component Interaction Flow
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      METADATA GENERATION WORKFLOW                            │
└─────────────────────────────────────────────────────────────────────────────┘

1. Data Ingestion
↓
User/STOMP → OptimusDB Agent-N → Needs Metadata?
│
├─ Yes → Generate Contextual Metadata
│         ↓
│   ┌─────────────────────────────┐
│   │ Build Prompt                │
│   │ • Data source context       │
│   │ • Renewable energy type     │
│   │ • Temporal information      │
│   │ • Existing metadata         │
│   └─────────────────────────────┘
│         ↓
│   HTTP POST to TinyLlama Sidecar
│   http://tinyllama-agent-N:8080/v1/completions
│         ↓
│   ┌─────────────────────────────┐
│   │ TinyLlama Processing        │
│   │ • Load model context        │
│   │ • Generate completion       │
│   │ • Return JSON response      │
│   └─────────────────────────────┘
│         ↓
│   Parse & Validate Metadata
│         ↓
└─ Store in 4-Layer Architecture:
↓
┌──────────────────┐
│ Layer 1: IPFS    │
│ (Content hash)   │
└──────────────────┘
↓
┌──────────────────┐
│ Layer 2: OrbitDB │
│ (DocumentStore)  │
└──────────────────┘
↓
┌──────────────────┐
│ Layer 3: OrbitDB │
│ (EventLog)       │
└──────────────────┘
↓
┌──────────────────┐
│ Layer 4: SQLite  │
│ (Query index)    │
└──────────────────┘
```

## Election & Consensus Flow
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LEADER ELECTION WITH GOSSIPSUB                            │
└─────────────────────────────────────────────────────────────────────────────┘

GossipSub Mesh Network
│
┌─────────────┬───────────┼───────────┬─────────────┐
│             │           │           │             │
Agent-1       Agent-2     Agent-3     Agent-4  ...  Agent-8
│             │           │           │             │
└─────────────┴───────────┴───────────┴─────────────┘
│
Election Messages Published
│
┌────────┴────────┐
│                 │
Reputation-Based    Heartbeat
Voting           Monitoring
│                 │
└────────┬────────┘
│
Coordinator Elected
│
┌────────┴────────┐
│                 │
Lease-Based      Continuous
Leadership       Health Checks
│                 │
└────────┬────────┘
│
Coordinator Operations:
• Heavy metadata generation (60-70%)
• TOSCA file processing
• ActiveMQ coordination
• Cluster health monitoring
```

## Distributed Query Flow
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     DISTRIBUTED QUERY MECHANISMS                             │
└─────────────────────────────────────────────────────────────────────────────┘

Query Request → OptimusDB Agent-N
│
├─ Strategy Selection
│
┌─────────────┼─────────────┬─────────────┬─────────────┐
│             │             │             │             │
LOCAL_ONLY   REMOTE_ONLY   PARALLEL_MERGE   QUORUM    BROADCAST
│             │             │             │             │
│             │             │             │             │
Query local   Query specific  Query all    Query N/2+1   Query all
SQLite only   remote peer     peers in     peers for     peers and
only            parallel     consensus     aggregate
│             │             │             │             │
└─────────────┴─────────────┴─────────────┴─────────────┘
│
Merge Results
│
Return to Client

Each peer independently:
1. Receives query via LibP2P stream
2. Executes on local SQLite
3. Returns results
4. Metadata generated via local TinyLlama if needed
```

## Resource Allocation
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         RESOURCE BREAKDOWN                                   │
└─────────────────────────────────────────────────────────────────────────────┘

Coordinator Node (Agent-1):
┌────────────────────────────────────────────┐
│ TinyLlama-1:    8GB RAM,  4 CPU cores      │
│ OptimusDB-1:    4GB RAM,  2 CPU cores      │
│ Total:         12GB RAM,  6 CPU cores      │
└────────────────────────────────────────────┘

Follower Nodes (Agents 2-8):
┌────────────────────────────────────────────┐
│ TinyLlama-N:    4GB RAM,  2 CPU cores      │
│ OptimusDB-N:    4GB RAM,  2 CPU cores      │
│ Total/Node:     8GB RAM,  4 CPU cores      │
│ × 7 Nodes:     56GB RAM, 28 CPU cores      │
└────────────────────────────────────────────┘

Cluster Total:
┌────────────────────────────────────────────┐
│ Production:    68GB RAM, 34 CPU cores      │
│ (16 containers)                            │
└────────────────────────────────────────────┘

Development (Single Container):
┌────────────────────────────────────────────┐
│ Agent-N Combined: 3GB RAM, 2 CPU cores     │
│ × 8 Nodes:       24GB RAM, 16 CPU cores    │
│ (8 containers)                             │
└────────────────────────────────────────────┘
```

## Network Topology
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           NETWORK ARCHITECTURE                               │
└─────────────────────────────────────────────────────────────────────────────┘

Docker Bridge Network: optimusdb-network (172.28.0.0/16)
│
┌─────────────────────────┼─────────────────────────┐
│                         │                         │
HTTP/REST API          LibP2P P2P Mesh          Internal Service
(External Access)      (Agent-to-Agent)         (Sidecar Comms)
│                         │                         │
┌─────┴─────┐         ┌─────────┴─────────┐      ┌──────┴──────┐
│           │         │                   │      │             │
Host:9091   │         Agent-1 ←→ Agent-2  │      Agent-1 →     │
Host:9092   │         Agent-1 ←→ Agent-3  │      TinyLlama-1   │
...         │         ...                 │      (Internal)    │
Host:9098   │         Agent-2 ←→ Agent-3  │                    │
│         (Full mesh)         │      Agent-2 →     │
┌───────────┴───┐     │                   │      TinyLlama-2   │
│ REST Clients  │     └─────────┬─────────┘                    │
│ • STOMP       │               │                              │
│ • ActiveMQ    │          GossipSub                           │
│ • Direct HTTP │               │                              │
└───────────────┘               │                              │
┌───────┴───────┐                      │
│ Topics:       │                      │
│ • elections   │                      │
│ • heartbeats  │                      │
│ • data-sync   │                      │
└───────────────┘                      │
│
HTTP Communication                     │
(Agent → TinyLlama)                    │
No external exposure                   │
Port 8080 internal only               │
──────────────────────────────────────┘

External Port Mapping:
Agent-1: Host:9091 → Container:9091 (REST API)
Host:8081 → Container:8081 (Credentials API)
Agent-2: Host:9092 → Container:9091
Host:8082 → Container:8081
...
Agent-8: Host:9098 → Container:9091
Host:8088 → Container:8081

Internal Communication:
• Agent ←→ TinyLlama: HTTP on Docker network
• Agent ←→ Agent: LibP2P streams
• Consensus: GossipSub pubsub
• Discovery: mDNS
```

## Data Flow Architecture
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     4-LAYER STORAGE ARCHITECTURE                             │
└─────────────────────────────────────────────────────────────────────────────┘

Data Write Path
│
┌─────────────┴─────────────┐
│                           │
With Metadata              Without Metadata
│                           │
↓                           │
┌─────────────────────┐               │
│ TinyLlama Generate  │               │
│ Contextual Metadata │               │
└─────────────────────┘               │
│                           │
└─────────────┬─────────────┘
│
↓
┌──────────────────────────────────┐
│ Layer 1: IPFS Storage            │
│ • Content-addressed              │
│ • Immutable                      │
│ • Returns CID (hash)             │
└──────────────────────────────────┘
│
↓
┌──────────────────────────────────┐
│ Layer 2: OrbitDB DocumentStore   │
│ • CRDT-based replication         │
│ • Document-oriented              │
│ • Eventual consistency           │
└──────────────────────────────────┘
│
↓
┌──────────────────────────────────┐
│ Layer 3: OrbitDB EventLog        │
│ • Append-only log                │
│ • Operation history              │
│ • Audit trail                    │
└──────────────────────────────────┘
│
↓
┌──────────────────────────────────┐
│ Layer 4: SQLite Index            │
│ • Query optimization             │
│ • Reputation tracking            │
│ • Local cache                    │
└──────────────────────────────────┘

Data Query Path
│
┌─────────────┴─────────────┐
│                           │
Local Query                 Distributed Query
│                           │
↓                           ↓
┌─────────────────────┐   ┌─────────────────────┐
│ SQLite Index        │   │ LibP2P Broadcast    │
│ (Fast local access) │   │ (Parallel queries)  │
└─────────────────────┘   └─────────────────────┘
│                           │
│                           ↓
│               ┌─────────────────────┐
│               │ Merge & Deduplicate │
│               │ (Based on CID)      │
│               └─────────────────────┘
│                           │
└─────────────┬─────────────┘
│
↓
Return Results
```

## W3C Verifiable Credentials Flow
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  VERIFIABLE CREDENTIALS LIFECYCLE                            │
└─────────────────────────────────────────────────────────────────────────────┘

Credential Creation:
User Request → OptimusDB Agent
│
↓
Generate Metadata (via TinyLlama)
│
↓
┌───────────────────────────┐
│ Create W3C Credential     │
│ • @context                │
│ • type                    │
│ • credentialSubject       │
│ • issuer (Agent DID)      │
│ • issuanceDate            │
│ • proof (digital sig)     │
└───────────────────────────┘
│
↓
Store in 4 Layers:
IPFS → OrbitDB Doc → EventLog → SQLite
│
↓
Return Credential + Verification URL

Credential Verification:
Verification Request → OptimusDB Agent
│
↓
Fetch from Storage Layers
│
↓
┌───────────────────────────┐
│ Verify:                   │
│ • Signature valid         │
│ • Not revoked             │
│ • Issuer trusted          │
│ • Schema compliant        │
└───────────────────────────┘
│
↓
Return Verification Result
```

## Fault Tolerance & Recovery
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        FAILURE SCENARIOS                                     │
└─────────────────────────────────────────────────────────────────────────────┘

Scenario 1: TinyLlama Container Fails (Agent-3)
┌──────────────────────────────────────────┐
│ tinyllama-agent-3: UNHEALTHY/DOWN        │
└──────────────────────────────────────────┘
│
↓
Agent-3 metadata generation: ❌ FAILED
Agents 1,2,4-8 metadata generation: ✅ CONTINUE
│
↓
Docker restart policy: Automatic restart
│
↓
After 60s: Agent-3 TinyLlama healthy again

Scenario 2: OptimusDB Container Fails (Agent-5)
┌──────────────────────────────────────────┐
│ optimusdb-agent-5: DOWN                  │
└──────────────────────────────────────────┘
│
↓
Agent-5 leaves LibP2P mesh
│
↓
Remaining agents detect via heartbeat timeout
│
↓
If coordinator: New election triggered
If follower: Continue with 7-agent cluster
│
↓
Docker restart: Agent-5 rejoins mesh
Data resyncs via OrbitDB CRDTs

Scenario 3: Coordinator Fails (Agent-1)
┌──────────────────────────────────────────┐
│ optimusdb-agent-1: DOWN                  │
└──────────────────────────────────────────┘
│
↓
Heartbeat timeout detected by followers
│
↓
┌─────────────────────────────────────────┐
│ ELECTION TRIGGERED                      │
│ • All followers publish candidacy       │
│ • Reputation-based voting               │
│ • Highest reputation wins               │
│ • New coordinator elected (e.g. Agent-2)│
└─────────────────────────────────────────┘
│
↓
New coordinator takes over:
• Heavy metadata generation
• TOSCA processing
• Cluster coordination
│
↓
Agent-1 restarts: Rejoins as follower

Scenario 4: Network Partition
┌──────────────────────────────────────────┐
│ Agents 1-4 │ Network Split │ Agents 5-8  │
└──────────────────────────────────────────┘
│
↓
Both partitions elect coordinator
(Split-brain scenario)
│
↓
┌─────────────────────────────────────────┐
│ Lease-Based Leadership Prevents         │
│ • Leases expire after 15s               │
│ • No heartbeats across partition        │
│ • Both coordinators step down           │
└─────────────────────────────────────────┘
│
↓
Network heals → Mesh reforms
│
↓
Single election → One coordinator
```

## DCS Triad Demonstration
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HOW ARCHITECTURE ACHIEVES DCS                             │
└─────────────────────────────────────────────────────────────────────────────┘

D - DECENTRALIZATION:
✅ No central authority
• Coordinator role rotates via election
• Each agent has dedicated TinyLlama
• No shared metadata service

✅ Peer-to-peer networking
• LibP2P for agent communication
• mDNS for automatic discovery
• Full mesh topology

✅ Distributed storage
• IPFS for content addressing
• OrbitDB for CRDT replication
• Each agent stores full dataset

✅ Independent operation
• Agent can function without others
• Local SQLite for queries
• Dedicated LLM per agent

C - CONSISTENCY:
✅ CRDT-based replication
• OrbitDB ensures convergence
• Conflict-free operations
• Eventual consistency guaranteed

✅ Consensus mechanisms
• GossipSub for elections
• Reputation-based voting
• Lease-based leadership

✅ Verifiable credentials
• W3C standard compliance
• Digital signatures
• Tamper-proof metadata

✅ Deterministic metadata
• Same prompt → Same output
• Model version pinned
• Reproducible generation

S - SCALABILITY:
✅ Horizontal scaling
• Add agents linearly
• No central bottleneck
• Distributed query load

✅ Worker pools
• Parallel query execution
• Concurrent processing
• Result caching

✅ Resource optimization
• Heterogeneous allocation
• Coordinator gets more resources
• Followers optimized for load

✅ Query strategies
• LOCAL_ONLY for single node
• PARALLEL_MERGE for cluster-wide
• QUORUM for consensus
```

## Performance Characteristics
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PERFORMANCE METRICS                                     │
└─────────────────────────────────────────────────────────────────────────────┘

Metadata Generation Latency:
┌────────────────────────────────────────┐
│ Coordinator (4096 ctx, 4 threads):     │
│   Simple prompt:    2-3 seconds        │
│   Complex prompt:   5-8 seconds        │
│                                        │
│ Follower (2048 ctx, 2 threads):        │
│   Simple prompt:    3-4 seconds        │
│   Complex prompt:   8-12 seconds       │
└────────────────────────────────────────┘

Query Performance:
┌────────────────────────────────────────┐
│ LOCAL_ONLY:       < 10ms               │
│ REMOTE_ONLY:      50-100ms             │
│ PARALLEL_MERGE:   100-200ms (8 agents) │
│ QUORUM:           75-150ms (5 agents)  │
└────────────────────────────────────────┘

Storage Operations:
┌────────────────────────────────────────┐
│ IPFS Add:         50-100ms             │
│ OrbitDB Put:      100-200ms            │
│ SQLite Insert:    < 5ms                │
│ End-to-end:       200-400ms            │
└────────────────────────────────────────┘

Replication Latency:
┌────────────────────────────────────────┐
│ OrbitDB sync:     1-5 seconds          │
│ (Depends on data size and network)     │
└────────────────────────────────────────┘

Election Time:
┌────────────────────────────────────────┐
│ Heartbeat timeout:  3× interval        │
│ Voting phase:       2-3 seconds        │
│ Lease acquisition:  < 1 second         │
│ Total:              8-12 seconds       │
└────────────────────────────────────────┘
```