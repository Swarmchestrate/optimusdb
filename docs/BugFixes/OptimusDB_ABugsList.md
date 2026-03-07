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
| **BF-001** | SAT Deduplication — Same-content files with different swarmIDs silently overwrite each other in OrbitDB | `tosca/toscaparser.go` `api/http.go` | ✅ Resolved | [BF-001_SAT_Deduplication_Fix.md](BF-001_SAT_Deduplication_Fix.md) |

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