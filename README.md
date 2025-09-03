# OptimusDB

Database management systems have been evolved and profoundly influenced by the way data is stored, retrieved, and managed
to serve end-user demands or being part of a centralized infrastructure ecosystem.
In the last decade, distributed databases have been shaped by the principles of the CAP theorem and used in enterprises
to solve a number of dependencies and limitation upon centralized data access and processing approaches.
Under this research we promote the usage of a new Decentralized Database Management system (hereinafter referred as OptimusDB),
which achieves the extension of this theorem  and merely focuses on Decentralization, Consistency and Scalability of the data
in scope among different entities of a multi-diverse ecosystem. This attempt, in a form of a prototype, aims to leverage
a combination of decentralized technologies introducing new paradigms in data storage and management, whilst, addressing
contemporary challenges such as data resilience, scalability, and collaborative access among disparate entities of a data ecosystem.
We represent the next significant milestone in this evolution, by articulating the establishment of a decentralized data store architecture
incorporating advanced data management capabilities for on-premises but also cloud oriented data.
We epitomize these advancements, and unlike traditional database systems, which rely on centralized or distributed models and coordination,
we propose an operation under a peer-to-peer environment, using decentralized data stores, ensuring transparency, and seamless synchronization
and communication across nodes. The aim of this advent is to address critical challenges in data management, thus our novel proposition for OptimusDB
is designed to redefine knowledge exposure across various industry domains and research initiatives.
By converting conceptual and operational designs of specific research principles and challenges, this proposition
is motivated by the underscore of the transformative potential of databases in fostering innovation, contributing to a possible
pathway of reshapement of a global data management and data processing practices.



# OptimusDB

**OptimusDB** is a decentralized database prototype that runs in a peer-to-peer (P2P) network using **libp2p**, **IPFS/OrbitDB-style** document stores, and a lightweight HTTP interface. It’s designed to explore a DCS triad—**Decentralization**, **Consistency**, and **Scalability**—for data sharing across diverse organizations without relying on a single central coordinator.

Key goals:
- Durable, shareable data across independently operated nodes
- Simple HTTP interface for CRUD, querying, file ingest, and SQL-like operations
- Easy deployment locally (Docker) and on lightweight clusters (K3s)

---

## Table of Contents
- [Features](#features)
- [Architecture (High Level)](#architecture-high-level)
- [Quick Start](#quick-start)
- [Prerequisites](#prerequisites)
- [Run from Source](#run-from-source)
- [Build a Binary](#build-a-binary)
- [Docker](#docker)
- [K3s Deployment](#k3s-deployment)
- [Configuration](#configuration)
- [Flags](#flags)
- [Environment Variables](#environment-variables)
- [HTTP API](#http-api)
- [Development Notes](#development-notes)
- [Contributing](#contributing)
- [License](#license)

---

## Features
- **P2P data plane** via libp2p (peer discovery and transport).
- **Document stores** with CRUD + query over HTTP.
- **File ingest & retrieval** (e.g., IPFS-style flows).
- **Optional SQL-like operations** against an embedded store (for catalog/log data).
- **Multi-instance deployment** with simple networking (Docker) or clustered (K3s).

---

## Architecture (High Level)
- **Node**: a single OptimusDB process exposing HTTP + P2P ports.
- **Stores**: decentralized/document-oriented stores replicated between peers.
- **Transport**: libp2p on port `4001` by default.
- **API**: HTTP on `8089` by default, with a path prefix context (default `swarmkb`).

> See [docs/INTERFACE.md](docs/INTERFACE.md) for the complete HTTP surface and example `curl` commands.

---

## Quick Start

### Prerequisites
- Go **1.19.13** (or compatible)
- Git
- (Optional) Docker / K3s

Make sure dependencies are downloaded and your Go proxy is set:
```bash
go mod tidy
go env GOPROXY
# If empty, set it:
go env -w GOPROXY=https://proxy.golang.org,direct
```

### Run from Source
```bash
go run main.go
# Optional flags, e.g.:
# go run main.go -http -http-port=8089 -ipfs-port=4001 -swarmkb=swarmkb
```

### Build a Binary
```bash
go build -o optimusdb main.go
# Linux:
./optimusdb
# Windows:
optimusdb.exe
```

---

## Docker
Use your published GHCR image (recommended), or build locally.

**Pull & run (3 instances like your current setup):**
```bash
docker network create swarmnet || true

docker run -d --network=swarmnet --name=optimusdb1   -p 18001:8089 -p 14001:4001 -p 15001:5001   ghcr.io/georgegeorgakakos/optimusdb:latest

docker run -d --network=swarmnet --name=optimusdb2   -p 18002:8089 -p 14002:4001 -p 15002:5001   ghcr.io/georgegeorgakakos/optimusdb:latest

docker run -d --network=swarmnet --name=optimusdb3   -p 18003:8089 -p 14003:4001 -p 15003:5001   ghcr.io/georgegeorgakakos/optimusdb:latest
```

**Build locally and tag for GHCR (optional):**
```bash
docker build -t ghcr.io/georgegeorgakakos/optimusdb:latest .
docker push ghcr.io/georgegeorgakakos/optimusdb:latest
```

---

## K3s Deployment
A ready StatefulSet + Services manifest is in `k3smanifest/optimusdb-k3s.yaml`. Adjust the image if needed:
```yaml
image: ghcr.io/georgegeorgakakos/optimusdb:latest
```

Apply and verify:
```bash
kubectl apply -f k3smanifest/optimusdb-k3s.yaml
kubectl -n optimusdb get pods,svc
```

> If your GHCR package is private, add an `imagePullSecrets` with a registry secret.

---

## Configuration

### Flags
You can configure OptimusDB using CLI flags. Common ones include:

| Flag            | Description                                        | Example               |
|-----------------|----------------------------------------------------|-----------------------|
| `-http`         | Enable the HTTP interface                          | `-http=true`          |
| `-http-port`    | HTTP port                                          | `-http-port=8089`     |
| `-ipfs-port`    | P2P/libp2p port                                    | `-ipfs-port=4001`     |
| `-swarmkb`      | HTTP path prefix (context)                         | `-swarmkb=swarmkb`    |
| `-benchmark`    | Enable benchmark endpoint                          | `-benchmark=true`     |
| `-shell`        | Enable shell interface                             | `-shell=true`         |
| `-experimental` | Enable Kubo/IPFS experimental features             | `-experimental=true`  |
| `-repo`         | Directory name for the IPFS node repository        | `-repo=optimusdb`     |
| `-devlogs`      | Verbose/dev logging                                | `-devlogs=true`       |
| `-coordinator`  | Start as a “coordinator/LSA” agent                 | `-coordinator=true`   |
| `-download-dir` | Directory to store downloaded files                | `-download-dir=~/Downloads` |
| `-full-replica` | Enable full replication (e.g., via pinning)        | `-full-replica=true`  |
| `-bootstrap`    | Bootstrap peer multiaddr to connect on startup     | `-bootstrap=/ip4/…`   |
| `-region`       | Optional region tag used in benchmark metadata     | `-region=eu-central`  |

> **Tip:** Run `./optimusdb -h` (or `go run main.go -h`) to see the exact defaults compiled into your binary.

### Environment Variables
Some setups prefer env vars (especially containers/K8s):

- `OPTIMUSDB_API_PORT` (default `8089`)
- `OPTIMUSDB_P2P_PORT` (default `4001`)
- `OPTIMUSDB_GATEWAY_PORT` (default `5001`)
- `OPTIMUSDB_LOG_LEVEL` (e.g., `info`, `debug`)
- `OPTIMUSDB_SEED_PEERS` — comma-separated `host:port` entries for bootstrap

---

## HTTP API
Full request/response shapes and example `curl` calls are documented in:

👉 **[docs/INTERFACE.md](docs/INTERFACE.md)**

Quick smoke test:
```bash
# list peers (should be empty initially)
curl http://localhost:8089/swarmkb/peers

# insert a document
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d '{"method":{"cmd":"crudput"},"dstype":"dsswres","criteria":[{"_id":"demo1","name":"Test"}]}'

# read it back
curl -X POST http://localhost:8089/swarmkb/command   -H "Content-Type: application/json"   -d '{"method":{"cmd":"crudget"},"dstype":"dsswres","criteria":[{"_id":"demo1"}]}'
```

---

## Development Notes
- Use `go mod tidy` after pulling new code.
- If you see proxy errors, set `GOPROXY` as shown in [Quick Start](#quick-start).
- Logs can be made more verbose with `-devlogs` (and consider `-benchmark` for peer stats).

---

## Contributing
Issues and PRs are welcome. Please:
- Open an issue describing the bug/feature.
- Keep PRs focused and include tests or reproduction steps when relevant.

---

## License
MIT (see [LICENSE](LICENSE)).
