# BF-001 — SAT Deduplication Fix

| Field | Value |
|---|---|
| **ID** | BF-001 |
| **Title** | SAT Deduplication — Same-content files with different swarmIDs silently overwrite each other in OrbitDB |
| **Project** | Swarmchestrate — EU Horizon Europe, Grant Agreement No. 101135012 |
| **Component** | Knowledge Base (KB) / Resource Agent (RA) Integration — v1 |
| **Severity** | High — silent data loss, no error returned to caller |
| **Status** | ✅ Resolved |
| **Files changed** | `tosca/toscaparser.go`, `api/http.go` |
| **Tests added** | `tosca/toscaparser_test.go`, `repoScript/Test-SAT-Dedup-Fix.ps1` |

---

## Table of Contents

1. [Swarmchestrate Context](#1-swarmchestrate-context)
2. [Problem Statement](#2-problem-statement)
3. [Root Cause Analysis](#3-root-cause-analysis)
4. [Impact Per Storage Layer](#4-impact-per-storage-layer)
5. [Fix Design](#5-fix-design)
6. [Code Changes](#6-code-changes)
7. [Testing](#7-testing)
8. [v2 Migration Note](#8-v2-migration-note)

---

## 1. Swarmchestrate Context

### Architecture overview

Swarmchestrate is an EU-funded orchestration platform for managing distributed workloads across heterogeneous cloud and edge infrastructure. OptimusDB is the platform's **decentralised Knowledge Base (KB)** — a peer-to-peer database built on LibP2P, OrbitDB, IPFS, and SQLite that stores and replicates orchestration metadata across all participating agents.

The KB stores five categories of **TOSCA** (Topology and Orchestration Specification for Cloud Applications) documents:

| TOSCA Type | Datastore | Direction |
|---|---|---|
| Application Description | ADT | Orchestration System ingress/egress |
| Capacity Description | Capacity Descriptions | Capacity Provider ingress |
| OpenTofu/TOSCA Templates | OpenTofu/TOSCA | Orchestration System ingress/egress |
| Deployment / Release Plans | Deployment Plans | Orchestration System ingress/egress |
| Application Requirements | Requirements | Application Owner → Orchestration System |

### KB v1 integration — filename as swarmID

In the first iteration of the KB/RA integration, the **filename** of an uploaded SAT file is used as the **swarmID** — the logical identifier that uniquely identifies a SAT across the swarm.

The upload path from the Resource Agent to OptimusDB is:

```
Resource Agent
│
│  POST /swarmkb/upload
│  { file: base64, filename: "swarm-eu-west-1.yaml" }
    ▼
    api/http.go → uploadTOSCAHandler()
    │
    ├── Computes templateID  ←──── BUG IS HERE (v1)
    │
    ├── OrbitDB PUT (kbdata / dsswres)       _id = templateID
    ├── SQLite INSERT (toscametadata)         key = templateID
    ├── OrbitDB PUT (KBMetadata)              associated_id = templateID  [goroutine]
    └── SQLite INSERT (metadata_catalog)      associated_id = templateID  [goroutine]
    ```

    ---

    ## 2. Problem Statement

    When the Resource Agent uploads two SAT files that have **different filenames (swarmIDs) but identical YAML content**, the second upload silently destroys the first. The system returns HTTP 200 for both uploads with no indication of the collision.

    **Reproduction scenario:**

    ```
    Upload 1:  filename = "swarm-eu-west-1.yaml"     content = YAML bytes Ω
        Upload 2:  filename = "swarm-eu-central-1.yaml"  content = YAML bytes Ω  ← same content
            ```

            **Expected:** Two independent OrbitDB documents, each retrievable by its swarmID.

            **Actual:** One document in OrbitDB. SAT `swarm-eu-west-1` is permanently lost. All four storage layers (OrbitDB kbdata, OrbitDB KBMetadata, SQLite toscametadata, SQLite metadata_catalog) reflect only the second upload. Metadata entries for the first upload remain in some layers but now point to a document that no longer exists.

            **Practical impact for the orchestration team:**

            - A Coordinator that already cached a reference to `swarm-eu-west-1`'s `template_id` will receive stale or incorrect data when it queries the KB.
            - Deployment plans that depended on `swarm-eu-west-1` being independently retrievable will silently use `swarm-eu-central-1`'s document instead.
            - There is no error, no log warning, and no way to detect the loss without manually querying OrbitDB before and after the second upload.

            ---

            ## 3. Root Cause Analysis

            ### Primary cause — OrbitDB `_id` collision

            The `templateID` that becomes the OrbitDB document's `_id` was computed by `tosca.ComputeTemplateID()`:

            ```go
            // tosca/toscaparser.go  (before fix)
            func ComputeTemplateID(yamlContent []byte) string {
            hash := sha256.Sum256(yamlContent)
            return fmt.Sprintf("%x", hash[:8])
            }
            ```

            This is a **content-only hash**. The filename (swarmID) is never consulted. Two files with identical bytes always produce the same 16-character hex string regardless of their names.

            OrbitDB's DocumentStore `Put()` is keyed on `_id`. It applies **last-write-wins** CRDT semantics: if a document with the same `_id` already exists, `Put()` replaces it without error. This is the correct and expected behaviour for a CRDT store — the bug is that the wrong key was being used.

            ```
            templateID = sha256(YAML bytes Ω)[:8]  →  "a3f9c1b2de4f..."

                Upload 1 PUT(_id="a3f9c1b2de4f...", _filename="swarm-eu-west-1.yaml")     → document created
                Upload 2 PUT(_id="a3f9c1b2de4f...", _filename="swarm-eu-central-1.yaml")  → document OVERWRITTEN
                ```

                ### Secondary factor — IPFS content-addressing

                IPFS stores data blocks by **CID** (Content Identifier), which is a cryptographic hash of the block's bytes. Two files with identical content map to the same IPFS CID. This means the `ipfs_cid` metadata field will be identical for both uploads. However, this does not cause data loss on its own — two separate OrbitDB documents can legitimately reference the same IPFS block. The destructive overwrite is caused entirely by the `_id` collision described above.

                ---

                ## 4. Impact Per Storage Layer

                ### Before the fix

                | Layer | Key | Behaviour | Result |
                |---|---|---|---|
                | **OrbitDB `kbdata`** | `_id` = `templateID` | `Put()` last-write-wins on same key | ❌ SAT A document permanently overwritten |
                | **OrbitDB `KBMetadata`** | `_id` = `metaID` (includes timestamp) | Two distinct `metaID` values are created | ⚠️ Both entries exist but `associated_id` on both equals the same `templateID` — pointing to the now-overwritten document |
                | **SQLite `toscametadata`** | `template_id UNIQUE` + `ON CONFLICT DO UPDATE` | Conflict triggers on second insert | ❌ SAT A's row overwritten — filename updated to SAT B's name |
                | **SQLite `metadata_catalog`** | `id` = `metaID` (timestamp-seeded) | Two rows inserted normally | ⚠️ Both rows have `associated_id` pointing to the same dead document |
                | **In-memory `MetadataStore`** | `entry.ID` = `metaID` | Two map entries inserted | ⚠️ Both entries have `AssociatedID` pointing to the overwritten document |

                ### After the fix

                | Layer | Key | Behaviour | Result |
                |---|---|---|---|
                | **OrbitDB `kbdata`** | `_id` = `templateID` (seeded) | Unique key per swarmID | ✅ Two independent documents |
                | **OrbitDB `KBMetadata`** | `_id` = `metaID` | Two distinct `metaID` values | ✅ Each `associated_id` points to its own live document |
                | **SQLite `toscametadata`** | `template_id UNIQUE` | No conflict — two distinct keys | ✅ Two rows, each with the correct filename |
                | **SQLite `metadata_catalog`** | `id` = `metaID` | Two rows | ✅ Each `associated_id` is correct |
                | **In-memory `MetadataStore`** | `entry.ID` = `metaID` | Two map entries | ✅ Each `AssociatedID` is correct |

                ### Fields intentionally identical after the fix

                | Field | Value | Reason |
                |---|---|---|
                | `content_hash` | `sha256(fileBytes)` — identical for same-content files | Preserved as the pure content fingerprint for deduplication auditing — lets the orchestration team detect when two swarmIDs carry the same YAML without that detection causing an OrbitDB collision |
                | `ipfs_cid` | Same CID for same bytes | IPFS content-addressing is unchanged; both documents legitimately reference the same block |

                ---

                ## 5. Fix Design

                The fix introduces `ComputeTemplateIDWithSeed()`, a variant of the ID generation function that incorporates the filename (swarmID) into the hash:

                ```
                templateID = sha256( filename + ":" + content )[:8]
                ```

                Two files with the same bytes but different filenames now produce different hashes and therefore different OrbitDB `_id` values. No collision, no overwrite.

                The original `ComputeTemplateID()` function is left unchanged for backward compatibility. The upload handler uses the seeded variant when a filename is present and falls back to the original when it is not.

                ### Design decisions

                **Why seed with `filename + ":" + content` rather than just `filename`?**
                To preserve content sensitivity. A file with the same name but genuinely different content should still produce a different `_id`, which is the expected behaviour for a re-upload / update scenario.

                **Why keep `ComputeTemplateID()` unchanged?**
                Other parts of the codebase and any external callers that do not have a filename context must continue to work without modification.

                **Why is `content_hash` separate from `templateID`?**
                `templateID` is the OrbitDB identity key — it must be unique per swarmID. `content_hash` is the storage fingerprint — it must reflect the raw bytes regardless of identity. Conflating the two would break deduplication auditing in v2.

                ---

                ## 6. Code Changes

                ### `tosca/toscaparser.go`

                ```go
                // ComputeTemplateID generates a unique ID for a TOSCA template based on content hash.
                // NOTE: Two files with identical content will produce the same ID regardless of filename.
                // Use ComputeTemplateIDWithSeed when the filename (e.g. swarmID) must distinguish them.
                func ComputeTemplateID(yamlContent []byte) string {
                hash := sha256.Sum256(yamlContent)
                return fmt.Sprintf("%x", hash[:8])
                }

                // ComputeTemplateIDWithSeed generates a unique ID by hashing both the seed (e.g. filename /
                // swarmID) and the file content together. This guarantees that two files with identical
                // content but different seeds (filenames) produce different IDs, preventing OrbitDB
                // DocumentStore overwrites caused by _id collisions.
                //
                // Use this in v1 KB/RA integration where filename == swarmID.
                // The pure content hash is preserved in the content_hash metadata field for dedup auditing.
                func ComputeTemplateIDWithSeed(seed string, yamlContent []byte) string {
                combined := append([]byte(seed+":"), yamlContent...)
                hash := sha256.Sum256(combined)
                return fmt.Sprintf("%x", hash[:8])
                }
                ```

                ### `api/http.go` — `uploadTOSCAHandler()`

                ```go
                // Before:
                templateID := tosca.ComputeTemplateID(decoded)

                // After:
                var templateID string
                if filename != "" && filename != "unknown" {
                templateID = tosca.ComputeTemplateIDWithSeed(filename, decoded)
                } else {
                templateID = tosca.ComputeTemplateID(decoded) // legacy fallback
                }
                ```

                No other changes are required. All downstream code (`toscaDoc["_id"] = templateID`, `InsertTOSCAMetadata(templateID, ...)`, `GenerateMetadataFromTOSCA(templateID, ...)`) automatically uses the corrected value.

                ---

                ## 7. Testing

                ### Unit tests — `tosca/toscaparser_test.go`

                Pure Go tests, no cluster required.

                ```bash
                go test ./tosca/... -v
                ```

                | Test | Purpose |
                |---|---|
                | `TestComputeTemplateID_Deterministic` | Same content always returns the same ID |
                | `TestComputeTemplateID_DifferentContentProducesDifferentID` | Basic hash sanity |
                | `TestComputeTemplateID_FormatIsHex` | Output is 16-char lowercase hex |
                | `TestComputeTemplateID_SameContentSameID_KnownLimitation` | **Documents the original bug** as a permanent regression marker |
                | `TestComputeTemplateIDWithSeed_SameContentDifferentSeedsProduceDifferentIDs` | **Primary regression test for the fix** |
                | `TestComputeTemplateIDWithSeed_Deterministic` | Seeded variant is stable |
                | `TestComputeTemplateIDWithSeed_DiffersFromContentOnlyHash` | The two functions cannot be silently swapped |
                | `TestKBv1_MultipleFilesWithSameContent` | Five swarms uploading the same default SAT template — all must get unique IDs |

                ### Integration tests — `repoScript/Test-SAT-Dedup-Fix.ps1`

                End-to-end test against a live OptimusDB cluster. No YAML files required on disk — the shared SAT content is generated in memory by the script itself.

                ```powershell
                # Single agent, no replication check
                .\Test-SAT-Dedup-Fix.ps1

                # With replication verification across two agents
                .\Test-SAT-Dedup-Fix.ps1 -TestReplication -ReplicationWaitSec 8

                # Custom ports (e.g. Docker Compose cluster)
                .\Test-SAT-Dedup-Fix.ps1 -BasePort 8089 -SecondPort 8090 -TestReplication
                ```

                **Parameters:**

                | Parameter | Default | Description |
                |---|---|---|
                | `-BasePort` | `18001` | Port of Agent 1 |
                | `-SecondPort` | `18002` | Port of Agent 2 (used only with `-TestReplication`) |
                | `-Context` | `swarmkb` | API context path |
                | `-TestReplication` | `false` | Enable replication verification on Agent 2 |
                | `-ReplicationWaitSec` | `5` | Seconds to wait for OrbitDB CRDT propagation before querying Agent 2 |

                **Test scenarios:**

                | Scenario | Assertion |
                |---|---|
                | **0 — Connectivity** | Agent(s) reachable before anything runs |
                | **1 — ID uniqueness** | `template_id_A ≠ template_id_B` — core regression guard |
                | **2 — Isolation** | Both OrbitDB documents independently retrievable; SAT A re-queried after SAT B upload to confirm it was not overwritten |
                | **3 — Content hash audit** | `content_hash` identical for both KBMetadata entries — confirms the audit field is preserved |
                | **4 — Replication** *(optional)* | Both documents arrive on Agent 2 as separate entities; their `_id` values remain distinct after CRDT merge |

                The script exits with code `1` if any assertion fails, making it CI-compatible.

                ---

                ## 8. v2 Migration Note

                In KB integration v2, the swarmID will be derived from **TOSCA metadata fields** rather than the filename. The fix is already prepared for this. The only change needed at that point is the source of the seed:

                ```go
                // v1 — current
                templateID = tosca.ComputeTemplateIDWithSeed(filename, decoded)

                // v2 — planned
                templateID = tosca.ComputeTemplateIDWithSeed(metadataSwarmID, decoded)
                ```

                `ComputeTemplateIDWithSeed()` requires no modification — it is seed-agnostic by design.

                The `content_hash` field will remain meaningful in v2 for the same reason it does in v1: it reflects the raw file bytes regardless of how the swarmID is derived, and can be used to detect when two different swarmIDs carry identical TOSCA content.

                ---

                *This fix is part of the [OptimusDB Bug Fix Index](README.md).*

                *OptimusDB is developed in the context of the [Swarmchestrate](https://swarmchestrate.eu) project, funded by the European Union under the Horizon Europe programme, Grant Agreement No. 101135012.*