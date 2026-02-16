# OptimusDB ↔ EMS Integration Guide

> Cross-VM integration between OptimusDB (Distributed Data Catalog) and EMS (Event Management System) via SSH Tunneling and ActiveMQ STOMP Protocol.

[![STOMP](https://img.shields.io/badge/Protocol-STOMP-blue)](https://stomp.github.io/)
[![ActiveMQ](https://img.shields.io/badge/Broker-ActiveMQ-red)](https://activemq.apache.org/)
[![K3s](https://img.shields.io/badge/Orchestration-K3s-yellow)](https://k3s.io/)
[![Go](https://img.shields.io/badge/Language-Go-00ADD8)](https://golang.org/)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [System Components](#system-components)
- [Connection Diagram](#connection-diagram)
- [Network Flow](#network-flow)
- [Prerequisites](#prerequisites)
- [Setup Guide](#setup-guide)
- [Step 1: EMS VM Configuration](#step-1-ems-vm-configuration)
- [Step 2: OptimusDB VM — SSH Keys](#step-2-optimusdb-vm--ssh-keys)
- [Step 3: OptimusDB VM — SSH Tunnel Service](#step-3-optimusdb-vm--ssh-tunnel-service)
- [Step 4: Kubernetes Manifest Configuration](#step-4-kubernetes-manifest-configuration)
- [Step 5: Deploy and Verify](#step-5-deploy-and-verify)
- [Code Architecture](#code-architecture)
- [Entry Point](#entry-point)
- [EMS Subscriber](#ems-subscriber)
- [MQ Package](#mq-package)
- [Message Processing Pipeline](#message-processing-pipeline)
- [Data Model](#data-model)
- [Environment Variables](#environment-variables)
- [Monitoring & Operations](#monitoring--operations)
- [Tunnel Service Commands](#tunnel-service-commands)
- [OptimusDB Pod Commands](#optimusdb-pod-commands)
- [EMS VM Commands](#ems-vm-commands)
- [Troubleshooting](#troubleshooting)
- [Security Considerations](#security-considerations)
- [API Reference](#api-reference)

---

## Overview

OptimusDB integrates with the SwarmChestrate EMS (Event Management System) to consume real-time monitoring events from a distributed infrastructure. The EMS server runs an **ActiveMQ broker** that publishes events via the **STOMP protocol** (port 61610), while OptimusDB nodes subscribe to these events for storage, processing, and analysis.

In production deployments, OptimusDB and EMS typically run on **separate virtual machines** within different Kubernetes (K3s) clusters. Since the EMS VM usually exposes only SSH (port 22) externally, this integration uses a **persistent SSH tunnel** to bridge the two environments — requiring **no firewall changes**.

### Key Features

| Feature | Description |
|---------|-------------|
| **Real-time event streaming** | STOMP subscriptions to all EMS topics (`/topic/>`) |
| **Auto-reconnection** | Exponential backoff from 5s to 5min on connection loss |
| **Message normalization** | Automatic conversion of Java-style `{key=value}` to valid JSON |
| **Persistent storage** | All events stored in SQLite `ems_events` table |
| **Health monitoring** | Periodic heartbeat pings to `/queue/optimusdb-health` |
| **Durable subscriptions** | Unique client ID per pod ensures no message loss across restarts |
| **SSH tunnel transport** | Encrypted cross-VM connectivity with no firewall changes |

---

## Architecture

### System Components

```
┌─────────────────────────────────────────┐     ┌─────────────────────────────────────────┐
│           OptimusDB VM                  │     │              EMS VM                     │
│           (epm-server)                  │     │         (193.225.251.34)                │
│                                         │     │                                         │
│  ┌──────────────────────────────────┐   │     │   ┌──────────────────────────────────┐  │
│  │  K3s Cluster (namespace:         │   │     │   │  K3s Cluster (namespace: default) │  │
│  │              optimusddc)          │   │     │   │                                  │  │
│  │                                  │   │     │   │  ┌────────────────────────────┐   │  │
│  │  ┌────────────┐ ┌────────────┐   │   │     │   │  │  emsserver-ems-server     │   │  │
│  │  │ optimusdb1 │ │ optimusdb2 │   │   │     │   │  │  (ActiveMQ Broker)        │   │  │
│  │  └─────┬──────┘ └──────┬─────┘   │   │     │   │  │  Ports: 8111, 61616,     │   │  │
│  │        │               │         │   │     │   │  │         61610 (STOMP)     │   │  │
│  │  ┌─────┴──────────────┐│         │   │     │   │  └────────────────────────────┘   │  │
│  │  │ optimusdb3         ││         │   │     │   │                                  │  │
│  │  └────────┬───────────┘│         │   │     │   │  ┌────────────────────────────┐   │  │
│  └───────────┼────────────┼─────────┘   │     │   │  │  ems-client-daemonset     │   │  │
│              │            │             │     │   │  │  (Netdata → ActiveMQ)      │   │  │
│         hostIP:61610      │             │     │   │  └────────────────────────────┘   │  │
│              │            │             │     │   │                                  │  │
│  ┌───────────┴────────────┘             │     │   │  ┌────────────────────────────┐   │  │
│  │                                      │     │   │  │  stomp-listener           │   │  │
│  │  ┌──────────────────────────────┐    │     │   │  │  (Event processor)        │   │  │
│  │  │  autossh (ems-tunnel.service)│    │     │   │  └────────────────────────────┘   │  │
│  │  │  0.0.0.0:61610 ─────────────┼────┼─SSH─┼───┼──► localhost:61610               │  │
│  │  └──────────────────────────────┘    │  22 │   │                                  │  │
│  │                                      │     │   └──────────────────────────────────┘  │
│  │                                      │     │                                         │
└──┴──────────────────────────────────────┘     └─────────────────────────────────────────┘
```

### Connection Diagram

```
OptimusDB Pod ──► Node hostIP:61610 ──► autossh tunnel ──► SSH (port 22) ──► EMS VM localhost:61610 ──► ActiveMQ STOMP
```

### Network Flow

1. **EMS clients** collect Netdata metrics from K3s nodes and publish to the ActiveMQ broker
2. **ActiveMQ** listens on `*:61610` (STOMP) on the EMS VM host
3. **autossh tunnel** on the OptimusDB VM forwards `0.0.0.0:61610` → EMS VM `localhost:61610` via SSH
4. **OptimusDB pods** connect to the tunnel via their node's host IP (`status.hostIP:61610`)
5. **Messages** are parsed, normalized, stored in `ems_events`, and processed by domain logic

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| **EMS VM** | K3s cluster with `emsserver-ems-server` deployment (ActiveMQ), SSH on port 22 |
| **OptimusDB VM** | K3s cluster with OptimusDB deployments (`optimusdb1`, `optimusdb2`, `optimusdb3`) |
| **Network** | SSH access (port 22) from OptimusDB VM → EMS VM |
| **Software** | `autossh` on OptimusDB VM, `kubectl` on both VMs |
| **SSH Keys** | RSA/ED25519 keypair on OptimusDB VM, public key authorized on EMS VM |
| **Credentials** | STOMP login (default: `aaa`/`111`) |

---

## Setup Guide

### Step 1: EMS VM Configuration

#### Verify EMS pods are running

```bash
sudo kubectl get pods -o wide
```

Expected output:
```
NAME                                    READY   STATUS    RESTARTS   AGE
emsserver-ems-server-774cc9594f-xxxxx   1/1     Running   0          10d
ems-client-daemonset-xxxxx              1/1     Running   0          10d
ems-client-daemonset-xxxxx              1/1     Running   0          10d
stomp-listener-69db95f8f8-xxxxx         1/1     Running   0          12d
```

#### Verify STOMP port is listening on host

```bash
sudo ss -tlnp | grep 61610
```

Expected:
```
LISTEN 0 4096 *:61610 *:* users:(("java",pid=XXXXX,fd=XXX))
```

> **Note:** If port 61610 is not on the host, check the service type with `sudo kubectl get svc emsserver-ems-server -o wide`. A `ClusterIP` service means the port is internal only.

#### Check service type (should confirm ports)

```bash
sudo kubectl get svc emsserver-ems-server -o yaml | grep -A5 "type:\|ports:"
```

#### Authorize OptimusDB VM's SSH key

```bash
# Paste the OptimusDB VM's public key
echo "ssh-rsa AAAA...your-key... ubuntu@epm-server" >> /home/ubuntu/.ssh/authorized_keys
chmod 600 /home/ubuntu/.ssh/authorized_keys
```

---

### Step 2: OptimusDB VM — SSH Keys

#### Generate keypair (if not existing)

```bash
ls ~/.ssh/id_rsa.pub 2>/dev/null || ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
```

#### Display public key (copy to EMS VM)

```bash
cat ~/.ssh/id_rsa.pub
```

#### Test SSH connectivity

```bash
ssh ubuntu@193.225.251.34 'hostname && echo SSH OK'
```

---

### Step 3: OptimusDB VM — SSH Tunnel Service

#### Install autossh

```bash
sudo apt install autossh -y
```

#### Create systemd service

```bash
sudo tee /etc/systemd/system/ems-tunnel.service << 'EOF'
[Unit]
Description=SSH Tunnel to EMS Server (STOMP 61610)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
ExecStart=/usr/bin/autossh -M 0 -N \
-o "ServerAliveInterval=30" \
-o "ServerAliveCountMax=3" \
-o "ExitOnForwardFailure=yes" \
-L 0.0.0.0:61610:localhost:61610 ubuntu@193.225.251.34
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
```

> ⚠️ **Critical:** The tunnel MUST bind on `0.0.0.0:61610` (not `127.0.0.1`) so K8s pods can reach it via the host's node IP.

#### Enable and start

```bash
sudo systemctl daemon-reload
sudo systemctl enable ems-tunnel
sudo systemctl start ems-tunnel
```

#### Verify

```bash
# Check service status
sudo systemctl status ems-tunnel

# Verify listening on all interfaces
ss -tlnp | grep 61610
# Should show *:61610 or 0.0.0.0:61610

# Test connectivity
nc -zv 127.0.0.1 61610
# Expected: Connection to 127.0.0.1 61610 port [tcp/*] succeeded!
```

---

### Step 4: Kubernetes Manifest Configuration

Add the following environment variables to **each** OptimusDB deployment (`optimusdb1`, `optimusdb2`, `optimusdb3`):

```yaml
env:
# ... existing env vars ...

# EMS integration via SSH tunnel (ActiveMQ STOMP)
- name: HOST_IP
valueFrom: { fieldRef: { fieldPath: status.hostIP } }
- name: EMS_SERVICE_NAME
valueFrom: { fieldRef: { fieldPath: status.hostIP } }  # pod reaches host via node IP
- name: EMS_STOMP_PORT
value: "61610"
- name: EMS_TOPIC
value: "/topic/>"       # subscribe to ALL topics
- name: MQ_USER
value: "aaa"
- name: MQ_PASS
value: "111"
- name: MQ_CLIENT_ID
value: "$(POD_NAME)"    # unique per pod for durable subscriptions
```

> **Why `status.hostIP` instead of `localhost`?**
> Inside a K8s pod, `localhost` refers to the pod's own network namespace — not the host machine where the SSH tunnel runs. Using `status.hostIP` resolves to the node's actual IP, allowing pods to reach the tunnel.

---

### Step 5: Deploy and Verify

```bash
# Apply the updated manifest
kubectl apply -f optimusddc-k3s-manifest.yaml

# Watch pods restart
kubectl get pods -n optimusddc -w

# Verify EMS environment in a pod
kubectl exec -n optimusddc deploy/optimusdb1 -- env | grep EMS

# Check EMS connection logs
kubectl logs -n optimusddc deploy/optimusdb1 | grep -i "ems\|stomp\|connect"
```

---

## Code Architecture

### Entry Point

**`main.go:268-281`**

```go
// EMS subscriber (ActiveMQ/STOMP)
emsCtx, emsCancel := context.WithCancel(termCtx)
cleanupEMS, err := knowledgeBaseDB.StartEMSSubscriber(emsCtx)
if err != nil {
log.Printf("[ERROR] EMS init failed: %v", err)
} else {
go func() {
<-termCtx.Done()
_ = cleanupEMS()
emsCancel()
}()
logger.Info("[INFO] EMS service started (auto-reconnect enabled)")
}
```

### EMS Subscriber

**`app/ems_subscriber.go`** — Main integration entry point

| Function | Description |
|----------|-------------|
| `StartEMSSubscriber(ctx)` | Reads env vars, creates `mq.EMSService`, registers callbacks, starts background loop |
| `handleEMSMessage(body)` | Parses JSON, normalizes Java-style format, persists to SQLite, calls domain logic |
| `ProcessEMS(action, resource, params)` | Domain logic hook (extensible) |
| `EMSSend(dest, contentType, body)` | Publishes messages back to EMS |
| `normalizeEMSMessage(s)` | Converts `{key=value}` to `{"key":"value"}` |

#### Configuration Loading

```go
func (db *KnowledgeBaseDB) StartEMSSubscriber(ctx context.Context) (cleanup func() error, err error) {
host := os.Getenv("EMS_SERVICE_NAME")
if host == "" {
host = "ems-broker.default.svc.cluster.local"  // default for same-cluster
}
// ... port, user, pass, topic from env vars

cfg := mq.Config{
Host:     host,      // status.hostIP when cross-VM
Port:     stompPort, // 61610
User:     user,      // "aaa"
Pass:     pass,      // "111"
ClientID: clientID,  // $(POD_NAME)
Topic:    topic,     // "/topic/>"
}

service := mq.NewEMSService(cfg, 10*time.Second)
// ... register callbacks and start
}
```

### MQ Package

**`mq/`** — STOMP client implementations

| File | Class | Description |
|------|-------|-------------|
| `activemq.go` | `Client` | Core STOMP client with DNS resolution, `NewClient()`, `PublishJSON()`, `SubscribeJSON()`, `SubscribeJSONDurable()` |
| `ems_service.go` | `EMSService` | **Active implementation.** Auto-reconnect loop with exponential backoff (5s→10s→20s→40s→80s→160s→5min cap). Health checks via `/queue/optimusdb-health`. Connected/disconnected callbacks. |
| `reconnect_stomp.go` | `ReconnectingClient` | Alternative implementation with subscription memory and auto-resubscribe on reconnect |

#### EMSService Reconnection Logic

```
Initial retry:  5s → 10s → 20s → 40s → 80s → 160s → 5min (cap)
After success:  Delay resets to 5 seconds
After failure:  Delay doubles until 5 minute maximum
```

```go
func (s *EMSService) loop() {
retryDelay := 5 * time.Second

for {
if !s.isConnected() {
if err := s.connect(); err != nil {
logger.Warn("[WARN] EMS connect failed: %v (retry in %s)", err, retryDelay)
time.Sleep(retryDelay)
retryDelay = nextDelay(retryDelay, 5*time.Minute) // exponential backoff
continue
}
retryDelay = 5 * time.Second // reset on success
}

// Health check
time.Sleep(retryDelay)
if err := s.Send("/queue/optimusdb-health", "text/plain", []byte("ping")); err != nil {
s.disconnect()
}
}
}
```

### Message Processing Pipeline

```
┌─────────────────┐
│ STOMP Message    │ Raw bytes from ActiveMQ subscription
│ Received         │
└────────┬────────┘
▼
┌─────────────────┐
│ JSON Parse       │ json.Unmarshal → EMSMessage{action, resource, params}
│ Attempt          │
└────────┬────────┘
▼ (on failure)
┌─────────────────┐
│ Normalize        │ normalizeEMSMessage(): {key=value} → {"key":"value"}
│ Java Format      │ Single quotes → double quotes, unquoted keys → quoted
└────────┬────────┘
▼
┌─────────────────┐
│ SQLite           │ InsertEMSEvent(): received_at, node_id, client_id,
│ Persistence      │ topic, action, resource, params_json, raw_json
└────────┬────────┘
▼
┌─────────────────┐
│ Domain Logic     │ ProcessEMS(action, resource, params)
│ Processing       │ Currently logs; extensible for custom logic
└────────┬────────┘
▼
┌─────────────────┐
│ Logging          │ Success/failure logged via OptimusDB logger
└─────────────────┘
```

### Data Model

#### EMSMessage

```go
type EMSMessage struct {
Action   string                 `json:"action"`
Resource string                 `json:"resource"`
Params   map[string]interface{} `json:"params"`
}
```

#### ems_events Table

```sql
CREATE TABLE IF NOT EXISTS ems_events (
id            INTEGER PRIMARY KEY AUTOINCREMENT,
received_at   TEXT,     -- UTC RFC3339
node_id       TEXT,     -- libp2p host id
client_id     TEXT,     -- MQ_CLIENT_ID
topic         TEXT,     -- destination topic
action        TEXT,     -- parsed from payload
resource      TEXT,     -- parsed from payload
params_json   TEXT,     -- marshaled params
raw_json      TEXT      -- original message body
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_ems_events_time ON ems_events(received_at);
CREATE INDEX IF NOT EXISTS idx_ems_events_act_res ON ems_events(action, resource);
```

#### Query stored events via API

```bash
# Get recent EMS events
curl http://localhost:8089/swarmkb/agent/inventory?table=ems_events
```

---

## Environment Variables

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `EMS_SERVICE_NAME` | `ems-broker.default.svc.cluster.local` | **Yes** (cross-VM) | STOMP broker hostname or IP. Set to `status.hostIP` for SSH tunnel. |
| `EMS_STOMP_PORT` | `61610` | No | STOMP protocol port |
| `EMS_TOPIC` | `/topic/>` | No | Subscription topic. `>` is ActiveMQ wildcard for all subtopics. |
| `MQ_USER` | `aaa` | No | STOMP login username |
| `MQ_PASS` | `111` | No | STOMP login password |
| `MQ_CLIENT_ID` | `(hostname)` | Recommended | Unique client ID for durable subscriptions. Use `$(POD_NAME)`. |
| `EMS_NAMESPACE` | `default` | No | K8s namespace (same-cluster deployments only) |
| `EMS_DURABLE` | `true` | No | Enable durable subscriptions (requires `MQ_CLIENT_ID`) |
| `EMS_SUB_NAME` | `optimusdb-ems` | No | Durable subscription name |
| `EMS_USE_IP` | `false` | No | Resolve broker DNS to IP before connecting |

---

## Monitoring & Operations

### Tunnel Service Commands

```bash
# Service management
sudo systemctl status ems-tunnel        # Check status
sudo systemctl start ems-tunnel         # Start tunnel
sudo systemctl stop ems-tunnel          # Stop tunnel
sudo systemctl restart ems-tunnel       # Restart tunnel

# Logs
sudo journalctl -u ems-tunnel -f        # Stream logs
sudo journalctl -u ems-tunnel -n 50     # Last 50 entries

# Connectivity
ss -tlnp | grep 61610                   # Verify listening
nc -zv 127.0.0.1 61610                  # Test port
```

### OptimusDB Pod Commands

```bash
# Check EMS connection status
kubectl logs -n optimusddc deploy/optimusdb1 | grep -i "ems\|stomp"

# Verify environment variables
kubectl exec -n optimusddc deploy/optimusdb1 -- env | grep EMS

# Check all nodes
for n in 1 2 3; do
echo "=== optimusdb$n ==="
kubectl logs -n optimusddc deploy/optimusdb$n --tail=5 | grep -i ems
done

# Query stored EMS events
kubectl exec -n optimusddc deploy/optimusdb1 -- \
curl -s localhost:8089/swarmkb/agent/inventory?table=ems_events | head -20
```

### EMS VM Commands

```bash
# Check EMS pods
sudo kubectl get pods -o wide

# Check EMS server logs
sudo kubectl logs deploy/emsserver-ems-server --tail=50

# Check STOMP listener
sudo kubectl logs deploy/stomp-listener --tail=50

# Verify ActiveMQ is serving STOMP
sudo ss -tlnp | grep 61610

# Check EMS service
sudo kubectl get svc emsserver-ems-server -o wide
```

---

## Troubleshooting

### Connection Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| `dial tcp: lookup ems-broker...no such host` | `EMS_SERVICE_NAME` not set; defaults to K8s internal DNS | Set `EMS_SERVICE_NAME` to `status.hostIP` in manifest |
| `Connection refused` on `localhost:61610` | Tunnel not running or bound to `127.0.0.1` only | Ensure tunnel binds on `0.0.0.0:61610`; restart service |
| Tunnel keeps restarting (exit 255) | Bad SSH config option | Fix `ServerAliveCountsec` → `ServerAliveCountMax` |
| `Permission denied (publickey)` | Public key not in EMS VM `authorized_keys` | Copy public key to EMS VM |
| Port already in use on EMS VM | ActiveMQ already bound to host port | Skip `kubectl port-forward`; tunnel directly |

### Message Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| EMS connects but no messages | Wrong topic or EMS not publishing | Verify `EMS_TOPIC="/topic/>"` and check EMS client daemonset is running |
| `unmarshal failed` in logs | Java-style message format | `normalizeEMSMessage()` should handle this; check raw_json in ems_events |
| Duplicate messages after restart | Non-durable subscription | Ensure `MQ_CLIENT_ID` is set for durable subscriptions |

### Diagnostic Flowchart

```
Is the tunnel service running?
├── No  → sudo systemctl start ems-tunnel
└── Yes
Is port 61610 listening on 0.0.0.0?
├── No  → Fix -L 0.0.0.0:61610:... in service file; restart
└── Yes
Can nc reach 127.0.0.1:61610?
├── No  → Check SSH keys and EMS VM connectivity
└── Yes
Is EMS_SERVICE_NAME set in pod env?
├── No  → Add to manifest and redeploy
└── Yes
Do pod logs show "EMS connected"?
├── No  → Check MQ_USER/MQ_PASS credentials
└── Yes → Integration is working ✓
```

---

## Security Considerations

### Transport Encryption

All STOMP traffic between VMs is encrypted via the SSH tunnel. No plaintext monitoring data crosses the network.

### No Firewall Changes Required

The integration uses only port 22 (SSH), which is already open. No additional ports need to be exposed on the EMS VM.

### Key-Based Authentication

The tunnel uses SSH public key authentication. No passwords are stored in configuration files or environment variables for the tunnel.

### Credential Management

STOMP credentials are currently passed as plain environment variables. For production, use **Kubernetes Secrets**:

```bash
# Create secret
kubectl create secret generic ems-credentials -n optimusddc \
--from-literal=MQ_USER=aaa \
--from-literal=MQ_PASS=111

# Reference in manifest
env:
- name: MQ_USER
valueFrom:
secretKeyRef:
name: ems-credentials
key: MQ_USER
- name: MQ_PASS
valueFrom:
secretKeyRef:
name: ems-credentials
key: MQ_PASS
```

### Tunnel Binding

The tunnel listens on `0.0.0.0` to allow pod access. In a multi-tenant environment, consider binding to the cluster network interface only.

### Network Policies

Restrict which pods can access port 61610 on the host:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
name: allow-ems-access
namespace: optimusddc
spec:
podSelector:
matchLabels:
app: optimusdb
policyTypes:
- Egress
egress:
- ports:
- port: 61610
protocol: TCP
```

---

## API Reference

### Go Functions

#### StartEMSSubscriber

```go
func (db *KnowledgeBaseDB) StartEMSSubscriber(ctx context.Context) (cleanup func() error, err error)
```

Starts the EMS subscriber service with auto-reconnection. Returns a cleanup function for graceful shutdown.

#### EMSSend

```go
func (db *KnowledgeBaseDB) EMSSend(dest, contentType string, body []byte) error
```

Sends a message to an EMS destination. Returns error if the service is not connected.

```go
// Example: publish a status event
msg := []byte(`{"action":"status","resource":"optimusdb","params":{"status":"healthy"}}`)
err := db.EMSSend("/topic/optimusdb.status", "application/json", msg)
```

#### handleEMSMessage

```go
func (db *KnowledgeBaseDB) handleEMSMessage(body []byte) error
```

Internal handler for incoming STOMP messages. Parses, normalizes, persists, and routes to `ProcessEMS()`.

#### ProcessEMS

```go
func (db *KnowledgeBaseDB) ProcessEMS(action, resource string, params map[string]interface{}) error
```

Domain-specific message processing hook. Override this function to implement custom event handling logic.

### REST Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/swarmkb/agent/inventory?table=ems_events` | Query stored EMS events |

---

## File Structure

```
optimusdb-lsa/
├── main.go                          # EMS init at line 268
├── app/
│   ├── ems_subscriber.go            # StartEMSSubscriber, handleEMSMessage, ProcessEMS
│   ├── app.go                       # EMSMessage struct, publishEvent, ems_events table
│   └── service.go                   # ProcessEMS implementation
├── mq/
│   ├── activemq.go                  # Core Client, Config, DNS resolution
│   ├── ems_service.go               # EMSService with auto-reconnect (active)
│   └── reconnect_stomp.go           # ReconnectingClient (alternative)
├── k3smanifest/
│   └── optimusdb-k3s.yaml           # K8s manifest with EMS env vars
└── logger/
└── logger.go                    # Logging system
```

---

## License

This integration is part of the [OptimusDB](https://github.com/georgeGeorgakakos/optimusdb) project. See the main repository for license information.
