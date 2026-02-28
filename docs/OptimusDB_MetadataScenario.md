# OptimusDB TOSCA Metadata — End-to-End Scenario

**Version:** Post-fix (February 2026)
**Server:** `http://193.225.250.240/optimusdb1`
**Context:** `swarmkb`

---

## Prerequisites

```bash
# Verify connectivity
python optimusdb_client.py --url http://193.225.250.240/optimusdb1 --context swarmkb health
```

For all commands below, if your client defaults differ, add:
`--url http://193.225.250.240/optimusdb1 --context swarmkb`

---

## Step 1 — Upload a TOSCA file

Upload a TOSCA YAML file with full-structure mode (queryable + metadata auto-generation):

```bash
python optimusdb_client.py upload toscaSamples/webapp_adt.yaml
```

**Expected output:**
```
Uploading TOSCA file: toscaSamples/webapp_adt.yaml
File size: 5849 bytes

Response: {
"data": {
"filename": "webapp_adt.yaml",
"message": "TOSCA uploaded with full structure",
"queryable": true,
"storage_location": "dsswres",
"template_id": "tosca_webapp_microservicesapplication_v1_0_0",
"metadata_auto_generated": true
},
"status": 200
}
```

**Save the `template_id`** — you'll use it in the next steps.

---

## Step 2 — See the metadata (auto-generated after upload)

Query the auto-generated metadata using the template ID from Step 1:

```bash
python optimusdb_client.py metadata --associated-id tosca_webapp_microservicesapplication_v1_0_0
```

**Expected output (after fix):**
```
Querying metadata (KBMetadata) with 1 criteria
Executing query on kbmetadata with 1 criteria
Command 'query' executed successfully

Metadata record(s) found: 1

_id: meta-tosca-a1b2c3d4e5f6...
name: WebApp-MicroservicesApplication Metadata
associated_id: tosca_webapp_microservicesapplication_v1_0_0
metadata_type: tosca_template
description: TOSCA template for microservices web application with 4 node templates
tags: tosca, microservices, docker, webapp, nginx, python, postgres, redis
status: Generated
created_by: system
created_at: 2026-02-28T...
node_count: 4
data_domain: infrastructure
file_format: yaml
...
```

**What was fixed:** The `query` command previously hardcoded the `DsSWres` store regardless of the `dstype` parameter. Now it correctly routes to the `KBMetadata` store when `dstype=kbmetadata`.

> **NOTE:** Save the `_id` value (e.g. `meta-tosca-a1b2c3d4e5f6...`) — this is the **metadata ID** you'll need for Steps 4 and 6.

---

## Step 3 — Verify the upload in the main data store

Confirm the TOSCA template itself was stored correctly:

```bash
python optimusdb_client.py get --criteria '_id:tosca_webapp_microservicesapplication_v1_0_0'
```

**Expected output:**
```
Retrieved 1 document(s)

Document 1:
_id: tosca_webapp_microservicesapplication_v1_0_0
document_type: tosca_template
template_name: WebApp-MicroservicesApplication
template_version: 1.0.0
...
```

You can also query by content:

```bash
# Find all Docker containers
python optimusdb_client.py get --criteria 'node_templates.*.type:.*Container.*:regex'

# Find templates with PostgreSQL
python optimusdb_client.py get --criteria 'node_templates.*.properties.image:.*postgres.*:regex'
```

---

## Step 4 — Query the metadata record

### 4a. From CRUD Data Store (OrbitDB)

Query by the metadata ID you got in Step 2:

```bash
python optimusdb_client.py metadata --id meta-tosca-a1b2c3d4e5f6...
```

**Expected output:**
```
Metadata record found:

_id: meta-tosca-a1b2c3d4e5f6...
name: WebApp-MicroservicesApplication Metadata
associated_id: tosca_webapp_microservicesapplication_v1_0_0
metadata_type: tosca_template
description: TOSCA template for microservices web application...
tags: tosca, microservices, docker, webapp, nginx, python, postgres, redis
node_count: 4
content_hash: e3b0c44298fc...
file_format: yaml
status: Generated
...
```

> **FAQ:** *"Is the metadata ID the same as the metadata key?"*
> **No.** The metadata ID is the `_id` field of the metadata document (e.g. `meta-tosca-a1b2c3d4...`). The metadata "key" you see in listings refers to the OrbitDB document key. Use the `_id` value.

### 4b. From SQLite (metadata_catalog table)

```bash
python optimusdb_client.py sql "SELECT * FROM metadata_catalog WHERE associated_id = 'tosca_webapp_microservicesapplication_v1_0_0'"
```

**Expected output (after fix):**
```
SQL: SELECT * FROM metadata_catalog WHERE associated_id = '...'

Records found: 1

id: meta-tosca-a1b2c3d4e5f6...
name: WebApp-MicroservicesApplication Metadata
associated_id: tosca_webapp_microservicesapplication_v1_0_0
metadata_type: tosca_template
description: TOSCA template for microservices web application...
tags: tosca,microservices,docker,webapp,nginx,python,postgres,redis
node_count: 4
status: Generated
data_domain: infrastructure
geo_location:
...
```

**What was fixed:** The `/ems/sql` endpoint previously always queried the Logger SQLite database (which contains `optimusLogger` and `ems_events` tables). Now it auto-detects that `metadata_catalog` belongs to the KnowledgeBase SQLite database and routes accordingly.

### 4c. Other useful SQL queries

```bash
# Count all metadata entries
python optimusdb_client.py sql "SELECT COUNT(*) as total FROM metadata_catalog"

# Find metadata by type
python optimusdb_client.py sql "SELECT id, name, associated_id FROM metadata_catalog WHERE metadata_type = 'tosca_template'"

# Search by tags
python optimusdb_client.py sql "SELECT id, name, tags FROM metadata_catalog WHERE tags LIKE '%docker%'"
```

---

## Step 5 — Update an existing metadata field

Update the description of the metadata record:

```python
python -c "
from optimusdb_client import OptimusDBClient
client = OptimusDBClient()
client.update(
dstype='kbmetadata',
criteria=[{'_id': 'meta-tosca-a1b2c3d4e5f6...'}],
update_data=[{'description': 'Production-grade microservices TOSCA template with Nginx, Python, PostgreSQL and Redis'}]
)
"
```

**Expected output:**
```
Update successful: 1 document(s) modified
```

Verify the update:

```bash
python optimusdb_client.py metadata --id meta-tosca-a1b2c3d4e5f6...
```

---

## Step 6 — Add a new metadata field

Add a `geo_location` field to the metadata record:

```python
python -c "
from optimusdb_client import OptimusDBClient
client = OptimusDBClient()
client.add_metadata_field('meta-tosca-a1b2c3d4e5f6...', 'geo_location', 'Athens, Greece')
"
```

**Expected output:**
```
Field 'geo_location' added successfully to metadata record meta-tosca-a1b2c3d4e5f6...
```

> **Common mistake (from tester's email):**
> ```python
> # ❌ WRONG — 'test' is an undefined Python variable, and location split into two args
> client.add_metadata_field(test, "geo_location", "Athens", "Greece")
>
> # ✅ CORRECT — meta_id is a quoted string, location is a single string
> client.add_metadata_field('meta-tosca-a1b2c3d4e5f6...', 'geo_location', 'Athens, Greece')
> ```

Verify the field was added:

```bash
python optimusdb_client.py metadata --id meta-tosca-a1b2c3d4e5f6...
```

Should now show:
```
geo_location: Athens, Greece
```

You can also verify via SQL:

```bash
python optimusdb_client.py sql "SELECT id, geo_location FROM metadata_catalog WHERE id = 'meta-tosca-a1b2c3d4e5f6...'"
```

---

## Step 7 — Clean up (optional)

### Delete metadata only

```bash
python optimusdb_client.py delete --dstype kbmetadata --criteria '_id:meta-tosca-a1b2c3d4e5f6...'
```

### Delete the TOSCA template

```bash
python optimusdb_client.py delete --criteria '_id:tosca_webapp_microservicesapplication_v1_0_0'
```

### Delete everything (⚠️ careful)

```bash
python optimusdb_client.py delete-all --confirm
```

---

## Quick Reference — Command Summary

| Step | Command | Purpose |
|------|---------|---------|
| 1 | `upload webapp_adt.yaml` | Upload TOSCA + auto-generate metadata |
| 2 | `metadata --associated-id <template_id>` | View auto-generated metadata |
  | 3 | `get --criteria '_id:<template_id>'` | Verify TOSCA data in main store |
    | 4a | `metadata --id <meta_id>` | Query metadata by its ID (CRUD) |
      | 4b | `sql "SELECT * FROM metadata_catalog WHERE ..."` | Query metadata via SQL |
      | 5 | `update --dstype kbmetadata ...` | Update metadata fields |
      | 6 | `add_metadata_field(meta_id, field, value)` | Add new field to metadata |
      | 7 | `delete --criteria ...` | Remove records |

      ---

      ## Bugs Fixed in This Release

      ### Go Server Fixes (app/service.go, app/app.go, api/http.go)

      | # | Issue | Root Cause | Fix |
      |---|-------|-----------|-----|
      | G1 | `metadata --associated-id` returns empty | `query` command always searched `DsSWres` store, ignoring `dstype=kbmetadata` | `queryLocalDB` now resolves the correct OrbitDB store via `resolveDocStoreByType()` |
      | G2 | `sql "SELECT * FROM metadata_catalog"` → 400 | `/ems/sql` endpoint queried `GlobalLoggerDB` (wrong database) | Endpoint now auto-routes to `GlobalKBSQLite` for metadata/datacatalog tables |
      | G3 | Legacy uploads had no metadata | Metadata generation goroutine only existed in `store_full_structure` path | Added same goroutine to legacy upload path |

      ### Python Client Fixes (optimusdb_client.py)

      | # | Issue | Root Cause | Fix |
      |---|-------|-----------|-----|
      | P1 | `get_metadata()` always returns empty | Sent criteria as `[{"field": "_id", "operator": "==", "value": "xxx"}]` — Go expects `[{"_id": "xxx"}]` | Changed to direct key-value format `[{"_id": metadata_id}]` |
      | P2 | `add_metadata_field()` / `update_metadata_fields()` fails | Sent command `"put"` with `args=[json.dumps(doc)]` — Go expects `"crudput"` with `criteria=[doc]` | Changed to `crudput` with correct payload |
      | P3 | `update(dstype='kbmetadata', ...)` silently updates dsswres | `dstype` parameter was accepted but never passed to `_execute_command` | Added `dstype=dstype` to payload |
      | P4 | `delete(dstype='kbmetadata', ...)` silently deletes from dsswres | Same missing `dstype` issue | Added `dstype=dstype` to payload |

      > **P1 was the real reason the tester's `metadata --associated-id bdb857c8d4994b0b` returned empty.** Even without the Go-side fix, this Python bug would have prevented any metadata query from matching. Both G1 and P1 needed fixing together.

      ---

      **Project:** OptimusDB — EU Horizon Europe Grant 101135012 (Swarmchestrate)
      **Date:** February 28, 2026