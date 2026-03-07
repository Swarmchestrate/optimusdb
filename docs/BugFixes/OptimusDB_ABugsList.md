# OptimusDB — Bug Fix Index

> **Project:** Swarmchestrate — EU Horizon Europe, Grant Agreement No. 101135012
> **Component:** OptimusDB — Decentralised Knowledge Base & Data Catalog
> **Repository:** `optimusdb-main`

This document is the central index of all resolved defects in the OptimusDB codebase. Each entry links to a dedicated fix document that describes the problem, root cause, solution, and tests. The index is organised by functional area.

---

## Table of Contents

1. [KB / RA Integration](#1-kb--ra-integration)
2. [CRUD Layer — OrbitDB Document Store](#2-crud-layer--orbitdb-document-store)
3. [SQL & SQLite Layer](#3-sql--sqlite-layer)
4. [Leader Election & Mesh Healing](#4-leader-election--mesh-healing)
5. [Query Engine](#5-query-engine)
6. [Fix Status Legend](#fix-status-legend)

---

## 1. KB / RA Integration

Fixes related to the Knowledge Base (KB) and Resource Agent (RA) integration, covering SAT (Swarmchestrate Application Template) upload, storage, and retrieval across the distributed OrbitDB and SQLite layers.

| ID | Title | Affected Files | Status | Detail |
|---|---|---|---|---|
| **BF-001** | SAT Deduplication — Same-content files with different swarmIDs silently overwrite each other in OrbitDB | `tosca/toscaparser.go` `api/http.go` | ✅ Resolved | [BF-001_SAT_Deduplication_Fix.md](BugFixes/BF-001_SAT_Deduplication_Fix.md) |

---

## 2. CRUD Layer — OrbitDB Document Store

Fixes to the core create, read, update, and delete operations against the OrbitDB DocumentStore, covering context management, replication timeouts, bulk insert verification, and index integrity.

| ID | Title | Affected Files | Status | Detail |
|---|---|---|---|---|
| **BF-010** | CRUDPUT — HTTP request context cancellation causes Bad Gateway on insert | `app/service.go` | ✅ Resolved | [BF-010_CRUDPUT_Context_Fix.md](BugFixes/BF-010_CRUDPUT_Context_Fix.md) *(pending)* |
| **BF-011** | CRUDPUT — Missing replication timeout causes indefinite block on Load() | `app/service.go` | ✅ Resolved | [BF-011_CRUDPUT_Replication_Timeout.md](BugFixes/BF-011_CRUDPUT_Replication_Timeout.md) *(pending)* |
| **BF-012** | CRUDGET — Nested field paths not resolved; all nested criteria silently ignored | `app/service.go` | ✅ Resolved | [BF-012_CRUDGET_Nested_Path.md](BugFixes/BF-012_CRUDGET_Nested_Path.md) *(pending)* |
| **BF-013** | CRUDDELETE — Complete rewrite; previous implementation failed silently on multi-store clusters | `app/service.go` | ✅ Resolved | [BF-013_CRUDDELETE_Rewrite.md](BugFixes/BF-013_CRUDDELETE_Rewrite.md) *(pending)* |
| **BF-014** | CRUDUPDATE — Complete rewrite; partial update left documents in inconsistent state | `app/service.go` | ✅ Resolved | [BF-014_CRUDUPDATE_Rewrite.md](BugFixes/BF-014_CRUDUPDATE_Rewrite.md) *(pending)* |
| **BF-015** | Bulk insert (5+ files) — documents missing after upload on 8-node clusters; no retry or verification | `app/service.go` | ✅ Resolved | [BF-015_BulkInsert_Retry.md](BugFixes/BF-015_BulkInsert_Retry.md) *(pending)* |
| **BF-016** | OrbitDB index not rebuilt after bulk operations; queries return stale results | `app/service.go` | ✅ Resolved | [BF-016_IndexRebuild.md](BugFixes/BF-016_IndexRebuild.md) *(pending)* |
| **BF-017** | DSType not propagated to query routing; all queries fall back to default `dsswres` store | `app/service.go` | ✅ Resolved | [BF-017_DSType_Routing.md](BugFixes/BF-017_DSType_Routing.md) *(pending)* |

---

## 3. SQL & SQLite Layer

Fixes to the embedded SQL engine and the dual-database SQLite architecture (KnowledgeBaseSQLite / LoggerSQLite), covering reserved word handling, query sanitisation, and SQL routing.

| ID | Title | Affected Files | Status | Detail |
|---|---|---|---|---|
| **BF-020** | SQLite reserved words in SELECT cause syntax errors (`Alias`, `Component`, `Status`, `Type`, etc.) | `app/app.go` | ✅ Resolved | [BF-020_SQLite_ReservedWords.md](BugFixes/BF-020_SQLite_ReservedWords.md) *(pending)* |
| **BF-021** | Unescaped quotes in `_tuplegetter` strings break query execution | `app/app.go` | ✅ Resolved | [BF-021_Tuplegetter_Quotes.md](BugFixes/BF-021_Tuplegetter_Quotes.md) *(pending)* |
| **BF-022** | SQL DML routed to wrong SQLite database — KB tables (metadata_catalog, toscametadata) hit LoggerSQLite | `api/http.go` | ✅ Resolved | [BF-022_SQL_DB_Routing.md](BugFixes/BF-022_SQL_DB_Routing.md) *(pending)* |

---

## 4. Leader Election & Mesh Healing

Fixes to the LibP2P GossipSub-based leader election and mesh healing subsystem. All fixes are tracked directly in `election/reputationBasedElection.go` with inline changelogs.

| ID | Title | Affected Files | Version | Status | Detail |
|---|---|---|---|---|---|
| **BF-030** | Election retry loop blocked forever by `isElecting` atomic flag — zero successful retries | `election/reputationBasedElection.go` | v2.4.0 | ✅ Resolved | [BF-030_Election_RetryLoop.md](BugFixes/BF-030_Election_RetryLoop.md) *(pending)* |
| **BF-031** | Default coordinator never assigned — `optimusdb1` does not auto-promote on isolated start | `election/reputationBasedElection.go` | v2.4.0 | ✅ Resolved | [BF-031_DefaultCoordinator.md](BugFixes/BF-031_DefaultCoordinator.md) *(pending)* |
| **BF-032** | Fallback election unreachable after 3 failed attempts; `consecutiveElectionFailures` never triggers self-promotion | `election/reputationBasedElection.go` | v2.4.0 | ✅ Resolved | [BF-032_FallbackElection.md](BugFixes/BF-032_FallbackElection.md) *(pending)* |
| **BF-033** | GossipSub topic subscription cancel instead of full recreation causes persistent mesh splits | `election/reputationBasedElection.go` | v2.3.1 | ✅ Resolved | [BF-033_TopicRecreation.md](BugFixes/BF-033_TopicRecreation.md) *(pending)* |
| **BF-034** | Mesh heal trigger too slow (30 s) and term divergence threshold too high (>20) — split-brain persists | `election/reputationBasedElection.go` | v2.3.1 | ✅ Resolved | [BF-034_MeshHeal_Timing.md](BugFixes/BF-034_MeshHeal_Timing.md) *(pending)* |
| **BF-035** | Startup with stale high-term value causes node to refuse valid leaders indefinitely | `election/reputationBasedElection.go` | v2.3.1 | ✅ Resolved | [BF-035_StartupTermValidation.md](BugFixes/BF-035_StartupTermValidation.md) *(pending)* |
| **BF-036** | ListPeers() unreliable as sole mesh health indicator — false isolation detection blocks elections | `election/reputationBasedElection.go` | v2.3 | ✅ Resolved | [BF-036_MeshHealthIndicator.md](BugFixes/BF-036_MeshHealthIndicator.md) *(pending)* |
| **BF-037** | Split-brain — nodes elect separate leaders simultaneously due to missing term reconciliation | `election/reputationBasedElection.go` | v2.3 | ✅ Resolved | [BF-037_SplitBrain.md](BugFixes/BF-037_SplitBrain.md) *(pending)* |

---

## 5. Query Engine

Fixes to the multi-strategy distributed query engine (local, peer, parallel merge, quorum, local-then-remote).

| ID | Title | Affected Files | Status | Detail |
|---|---|---|---|---|
| **BF-040** | queryLocalDB ignores nested document paths — criteria on `a.b.c` always returns empty | `app/service.go` | ✅ Resolved | [BF-040_QueryLocalDB_NestedPath.md](BugFixes/BF-040_QueryLocalDB_NestedPath.md) *(pending)* |

---

## Fix Status Legend

| Symbol | Meaning |
|---|---|
| ✅ Resolved | Fix implemented, tested, and merged |
| 🔄 In Progress | Fix under development |
| 🔍 Investigating | Root cause analysis ongoing |
| *(pending)* | Detail document not yet written |

---

## How to Add a New Fix

1. Assign the next available ID in the relevant section.
2. Create a detail document under `docs/BugFixes/` following the naming convention `BF-NNN_Short_Title.md`.
3. Use `BF-001_SAT_Deduplication_Fix.md` as the reference template.
4. Add the entry to this index.

---

*OptimusDB is developed in the context of the [Swarmchestrate](https://swarmchestrate.eu) project, funded by the European Union under the Horizon Europe programme, Grant Agreement No. 101135012.*