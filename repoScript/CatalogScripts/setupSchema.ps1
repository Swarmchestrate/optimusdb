# If using SQLite directly
sqlite3 optimusdb.db < setupSchema.sql

# Or via OptimusDB API
curl -X POST http://optimusdb1:8089/swarmkb/command \
    -H "Content-Type: application/json" \
    -d '{
        "method": {"argcnt": 2, "cmd": "sqldml"},
        "args": ["dummy1", "dummy2"],
        "dstype": "dsswres",
        "sqldml": "$(cat setupSchema.sql)",
        "graph_traversal": [{}],
        "criteria": []
      }'