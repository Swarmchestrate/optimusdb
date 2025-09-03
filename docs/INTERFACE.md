# OptimusDB — HTTP Interface & Usage

This document describes **how to interact with OptimusDB over HTTP** and the related configuration knobs.

---

## Overview

OptimusDB exposes:
- **HTTP API** on port **8089** (you may map externally, e.g. 1800x)
- **P2P/libp2p** on port **4001** (e.g. 1400x outside)
- **Gateway** (IPFS-style) on port **5001** (e.g. 1500x outside)

> Exact endpoints are defined in the source; this guide lists the current surface.

---

## Run

### Docker (3 instances)
```bash
docker network create swarmnet || true

docker run -d --network=swarmnet --name=optimusdb1   -p 18001:8089 -p 14001:4001 -p 15001:5001   ghcr.io/georgegeorgakakos/optimusdb:latest

docker run -d --network=swarmnet --name=optimusdb2   -p 18002:8089 -p 14002:4001 -p 15002:5001   ghcr.io/georgegeorgakakos/optimusdb:latest

docker run -d --network=swarmnet --name=optimusdb3   -p 18003:8089 -p 14003:4001 -p 15003:5001   ghcr.io/georgegeorgakakos/optimusdb:latest
```

### K3s (3 pods via StatefulSet)
Point your manifest to the image and apply:
```yaml
# in your K8s workload spec
image: ghcr.io/georgegeorgakakos/optimusdb:latest
```
Then:
```bash
kubectl apply -f k3smanifest/optimusdb-k3s.yaml
```

---

## Base

- **HTTP Port:** `8089` (flag: `-http-port`)
- **Path Prefix (context):** `swarmkb` (flag: `-swarmkb`)
- **Base URL:** `http://{host}:8089/swarmkb`
- **CORS:** `Access-Control-Allow-Origin: *`, `Access-Control-Allow-Methods: *`, `Access-Control-Allow-Headers: Content-Type`
- **Content-Type:** all JSON endpoints expect `Content-Type: application/json`

> You can change the port and context via flags; see **Flags** below.

---

## Configuration

### Environment variables
- `OPTIMUSDB_API_PORT` (default `8089`)
- `OPTIMUSDB_P2P_PORT` (default `4001`)
- `OPTIMUSDB_GATEWAY_PORT` (default `5001`)
- `OPTIMUSDB_LOG_LEVEL` (e.g., `info`, `debug`)
- `OPTIMUSDB_SEED_PEERS` (comma-separated `host:port` seed list; optional)

### Flags (CLI)
- `-http` (default `true`) — enable HTTP interface
- `-http-port` (default `8089`)
- `-ipfs-port` (default `4001`)
- `-swarmkb` (default `swarmkb`) — HTTP path prefix / context
- `-benchmark` (default `false`) — enable `/benchmarks`

> Combine env vars and flags as needed; env vars typically apply at container/pod level, flags are passed to the binary entrypoint.

---

## HTTP API

### 1) `POST /{context}/command`
Single RPC-style endpoint that dispatches to different commands based on `method.cmd` in the JSON body.

**Request schema (superset; individual commands use subsets):**
```json
{
"method": { "cmd": "{command}" },
"args": [],
"file": "",
"dstype": "",
"criteria": [ {} ],
"UpdateData": [ {} ],
"graph_Traversal": [ {} ],
"sqldml": ""
}
```

#### a) `get` — fetch file from IPFS path
Fetch a file (by path like `/ipfs/{cid}`) and save locally.
```bash
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d '{"method":{"cmd":"get"},"args":["/ipfs/{cid}"]}'
```
**Response:** success message string (after saving the file) or error.

---

#### b) `post` — add file bytes to IPFS & record contribution
Send file bytes as **base64** in `file`.

**Linux/macOS (bash)**:
```bash
# GNU base64 uses -w0; on macOS use: base64 -b 0 myfile.bin
b64=$(base64 -w0 myfile.bin)
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d "{"method":{"cmd":"post"},"file":"$b64"}"
```

**PowerShell**:
```powershell
$b64 = [Convert]::ToBase64String([IO.File]::ReadAllBytes("myfile.bin"))
curl.exe -X POST http://localhost:8089/swarmkb/command `
-H "Content-Type: application/json" `
-d "{`"method`":{`"cmd`":`"post`"},`"file`":`"$b64`"}"
```
**Response:** info string about stored file or error.

---

#### c) `connect` — connect to a peer (libp2p multiaddr)
```bash
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d '{"method":{"cmd":"connect"},"args":["/ip4/127.0.0.1/tcp/4001/p2p/{peerID}"]}'
```
**Response:** `"Peer id processed"` (string) or error.

---

#### d) `query` — query a document store
Queries local and peers; caches results. `criteria` is an **array** of filters (OR between entries). Within a filter, use simple equality key/value pairs.
```bash
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d '{
"method":{"cmd":"query"},
"dstype":"dsswres",
"criteria":[ { "status":"active" }, { "owner":"alice" } ]
}'
```
**Response:** JSON array of documents or `"No records found"`.

**Known doc stores (`dstype`):** `dsswres`, `dsswresaloc`, `kbdata`, `kbmetadata`, `validations`.

---

#### e) `crudput` — insert documents
Each document **must** include `_id`.
```bash
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d '{
"method":{"cmd":"crudput"},
"dstype":"dsswres",
"criteria":[
{"_id":"res-001","name":"Alice","score":0.92},
{"_id":"res-002","name":"Bob","score":0.85}
]
}'
```
**Response:** `"OK: Successfully inserted records"` or error.

---

#### f) `crudget` — select documents
```bash
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d '{
"method":{"cmd":"crudget"},
"dstype":"dsswres",
"criteria":[{"name":"Alice"}]
}'
```
**Response:** JSON array of matching documents.

---

#### g) `crudupdate` — update documents
Match with `criteria`; set fields from `UpdateData`.
```bash
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d '{
"method":{"cmd":"crudupdate"},
"dstype":"dsswres",
"criteria":[{"name":"Alice"}],
"UpdateData":[{"score":0.97}]
}'
```
**Response:** `"SUCCESS! {n} document(s) updated"` or error.

---

#### h) `cruddelete` — delete documents
```bash
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d '{
"method":{"cmd":"cruddelete"},
"dstype":"dsswres",
"criteria":[{"_id":"res-002"}]
}'
```
**Response:** `"SUCCESS! {n} document(s) deleted"` or error.

---

#### i) `sqldml` — execute SQL against embedded SQLite
Selects may fan out to peers for reads.
```bash
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d '{"method":{"cmd":"sqldml"},"sqldml":"SELECT * FROM datacatalog LIMIT 5"}'
```
**Response:** rows as an array of objects (for reads), or success/error string (for writes).

---

### 2) `POST /{context}/upload`
Upload a **TOSCA YAML** file (base64). The handler decodes it and returns success.

**Linux/macOS (bash):**
```bash
b64=$(base64 -w0 tosca.yaml)   # macOS: base64 -b 0 tosca.yaml
curl -X POST http://localhost:8089/swarmkb/upload   -H "Content-Type: application/json"   -d "{"file":"$b64"}"
```

**PowerShell:**
```powershell
$b64 = [Convert]::ToBase64String([IO.File]::ReadAllBytes("tosca.yaml"))
curl.exe -X POST http://localhost:8089/swarmkb/upload `
-H "Content-Type: application/json" `
-d "{`"file`":`"$b64`"}"
```

**Response:**
```json
{ "status": 200, "data": { "message": "TOSCA uploaded successfully" } }
```

---

### 3) `GET /{context}/peers`
Return known libp2p peers (IDs and multiaddrs).
```bash
curl http://localhost:8089/swarmkb/peers
```

### 4) `GET /{context}/ems`
Alias/placeholder; currently wired like `/peers`.

### 5) `GET /{context}/log?date=YYYY-MM-DD&hour=HH`
Fetch logs from the embedded logger for a day/hour.
```bash
curl "http://localhost:8089/swarmkb/log?date=2025-09-04&hour=14"
```

### 6) `GET /{context}/benchmarks`
Aggregated benchmark info from connected peers (enable with `-benchmark`).
```bash
curl http://localhost:8089/swarmkb/benchmarks
```

---

## Response format

Handlers return a consistent envelope:

- **Success:** `{"status": 200, "data": {payload}}`
- **Error:** HTTP status code is authoritative; body includes `{"status": {code}, "message": "{reason}"}`

> Tip: for programmatic use, check the HTTP status code first, then parse `data` or `message` from the JSON body.

---

## P2P / Discovery

- Each node listens on **4001** (inside container/pod) for libp2p traffic.
- In Kubernetes, pods resolve each other via the headless service DNS, e.g.:
`optimusdb-0.optimusdb-headless.optimusdb.svc.cluster.local:4001`
- You can bootstrap peers by setting `OPTIMUSDB_SEED_PEERS` to a comma-separated list of `host:port` entries.

---

## Data & Persistence

- Default data path: `/var/lib/optimusdb` (mounted in K8s using a PVC).
- For Docker, mount a host path if you want persistence across restarts.

---

## Quick smoke test

```bash
# List peers (should be empty at first)
curl http://localhost:8089/swarmkb/peers

# Put a document
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d '{"method":{"cmd":"crudput"},"dstype":"dsswres","criteria":[{"_id":"demo1","name":"Test"}]}'

# Get the same document
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d '{"method":{"cmd":"crudget"},"dstype":"dsswres","criteria":[{"_id":"demo1"}]}'
```

---

## Versioning & Images

- Recommended container image (personal GHCR):
`ghcr.io/georgegeorgakakos/optimusdb:latest`
- Prefer semantic tags (e.g., `v0.1.0`) alongside `latest` for reproducibility.

---

*Last updated: generated from current source tree. If you add or change handlers/routes, update this file accordingly.*
