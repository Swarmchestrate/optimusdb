# OptimusDB API Documentation - Context Update Summary

## Changes Applied

All API documentation has been updated to use **`swarmkb`** as the default context instead of `optimusdb`.

### Files Updated
- [API Endpoints Reference](OptimusDB_API_Endpoints_Reference.md)
- [API Quick Reference](OptimusDB_API_Quick_Reference.md)
- [API Testing Guide](OptimusDB_API_Documentation1.md)

### What Changed

#### Before:
```
http://localhost:8089/optimusdb/peers
http://localhost:8089/optimusdb/command
http://localhost:8089/optimusdb/credentials
```

#### After:
```
http://localhost:8089/swarmkb/peers
http://localhost:8089/swarmkb/command
http://localhost:8089/swarmkb/credentials
```

### Updated References

- **Context variable:** `optimusdb` → `swarmkb`
- **URL paths:** All `/optimusdb/` → `/swarmkb/`
- **Environment variables:** `CONTEXT="swarmkb"`
- **Postman collection variable:** Set to `swarmkb`
- **Service names:** `optimusdb-service` → `swarmkb-service`

### Verification Examples

#### Quick Test Commands (All using swarmkb context)

```bash
# Set environment
export OPTIMUSDB_URL="http://localhost:8089"
export CONTEXT="swarmkb"

# Test endpoints
curl ${OPTIMUSDB_URL}/${CONTEXT}/peers
curl ${OPTIMUSDB_URL}/${CONTEXT}/ems/logs?limit=10
curl ${OPTIMUSDB_URL}/api/v1/metadata/health
```

#### URL Pattern Examples

| Endpoint Type | URL Pattern |
|--------------|-------------|
| Core Commands | `http://localhost:8089/swarmkb/command` |
| TOSCA Upload | `http://localhost:8089/swarmkb/upload` |
| Peers | `http://localhost:8089/swarmkb/peers` |
| EMS Logs | `http://localhost:8089/swarmkb/ems/logs` |
| EMS Events | `http://localhost:8089/swarmkb/ems/events` |
| Credentials | `http://localhost:8089/swarmkb/credentials` |
| Metadata | `http://localhost:8089/api/v1/metadata/*` |

### Important Notes

1. **Metadata endpoints** use a different path structure (`/api/v1/metadata/*`) and are **NOT** affected by the context variable.

2. **Postman Collection** uses the variable `{{context}}` which is now set to `swarmkb` by default.

3. **Configuration Flag:** The context is configurable at runtime via the `-context` flag:
```bash
./optimusdb -context=swarmkb -httpport=8089
```

4. **Backward Compatibility:** If you need to use a different context, simply change the environment variable:
```bash
export CONTEXT="mycontext"
```

### Files Ready for Use

All documentation files are now consistent and ready to use with the `swarmkb` context:

- 📖 Complete API Reference with swarmkb paths
- 📋 Quick Reference Table with swarmkb endpoints
- 📦 Postman Collection configured for swarmkb
- 🧪 Testing Guide with swarmkb curl examples

---

**Update Date:** December 3, 2025
**Context:** swarmkb
**All references updated and verified**
