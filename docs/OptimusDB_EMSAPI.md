# OptimusDB API Documentation
## Complete API Reference for EMS Integration

**Version:** 1.0
**Project:** EU Horizon Europe Grant Agreement 101135012 (Swarmchestrate)

---

## Table of Contents

1. [Overview](#overview)
2. [Base Configuration](#base-configuration)
3. [API Endpoints](#api-endpoints)
- [Capacity Descriptions](#capacity-descriptions)
- [Composite Metrics](#composite-metrics)
- [SLO Violations](#slo-violations)
- [Trust Results](#trust-results)
- [Metadata Enrichment](#metadata-enrichment)
- [Query Engine](#query-engine)
- [EMS Integration](#ems-integration)
- [Monitoring & Health](#monitoring--health)
4. [Data Models](#data-models)
5. [Composite Metrics Definitions](#composite-metrics-definitions)
6. [SLO Violations Catalog](#slo-violations-catalog)
7. [Authentication](#authentication)
8. [Error Responses](#error-responses)
9. [Integration Examples](#integration-examples)

---

## Overview

OptimusDB exposes a comprehensive REST API for accessing decentralized knowledge base data, renewable energy asset metadata, and operational metrics. The system implements a fully decentralized architecture using LibP2P, OrbitDB, and IPFS, with coordinator-follower pattern for AI-powered contextual metadata generation using TinyLlama language models.

**Key Features:**
- Fully decentralized P2P architecture without centralized coordination
- SQL-compatible query interface over distributed OrbitDB stores
- AI-powered contextual metadata generation
- Real-time composite metrics and SLO violation tracking
- EMS (Event Management System) integration via STOMP/ActiveMQ
- W3C Verifiable Credentials support (DID integration)

**Base URL:** `http://<agent-ip>:<http-port>/<context>`

            **Example for 8-agent cluster:**
            - Agent 1: `http://localhost:18001/swarm`
            - Agent 2: `http://localhost:18002/swarm`
            - Agent 8: `http://localhost:18008/swarm`

            ---

            ## Base Configuration

            ### Configuration Flags

            | Flag | Default | Description |
            |------|---------|-------------|
            | `--context` | `swarm` | API context path |
            | `--http-port` | `8089` | HTTP server port (internal) |
            | `--repo` | `swarmkb` | OrbitDB repository name |

            ### Port Configuration

            **Container Internal Port:** `8089` (all agents listen on this port inside their containers)

            **Host External Ports (8-agent cluster):**
            - **Agent 1:** `18001` → `8089`
            - **Agent 2:** `18002` → `8089`
            - **Agent 3:** `18003` → `8089`
            - **Agent 4:** `18004` → `8089`
            - **Agent 5:** `18005` → `8089`
            - **Agent 6:** `18006` → `8089`
            - **Agent 7:** `18007` → `8089`
            - **Agent 8:** `18008` → `8089`

            **Example Access:**
            ```bash
            # Agent 1 (Coordinator)
            curl http://localhost:18001/swarm/health

            # Agent 5
            curl http://localhost:18005/swarm/capacity/descriptions
            ```

            ### Environment Variables

            | Variable | Default | Description |
            |----------|---------|-------------|
            | `METADATA_AUTO_ENRICH` | `false` | Enable background metadata enrichment |
            | `METADATA_ENRICH_INTERVAL` | `1h` | Metadata enrichment interval |
            | `METADATA_CACHE_TTL` | `24h` | Metadata cache time-to-live |
            | `EMS_SERVICE_NAME` | `ems-broker` | EMS broker hostname |
            | `EMS_STOMP_PORT` | `61610` | EMS STOMP protocol port |
            | `EMS_TOPIC` | `/topic/>` | EMS topic subscription pattern |

            ---

            ## API Endpoints

            ### Capacity Descriptions

            #### GET `/<context>/capacity/descriptions`

                Retrieves capacity descriptions for renewable energy assets, including power generation capacity, storage capacity, and operational constraints.

                **Parameters:**
                - `asset_id` (optional): Filter by specific asset ID
                - `asset_type` (optional): Filter by asset type (e.g., `solar`, `wind`, `battery`)
                - `limit` (optional, default: 100): Maximum records to return
                - `include_metadata` (optional, default: true): Include AI-generated metadata

                **Response:**
                ```json
                {
                "success": true,
                "count": 25,
                "data": [
                {
                "_id": "asset_solar_001",
                "asset_name": "Solar Farm Alpha",
                "asset_type": "solar",
                "location": {
                "lat": 37.7749,
                "lon": -122.4194,
                "address": "San Francisco, CA"
                },
                "capacity": {
                "rated_power_kw": 5000,
                "peak_power_kw": 5500,
                "storage_kwh": 10000,
                "efficiency_percent": 22.5
                },
                "operational_constraints": {
                "min_irradiance": 200,
                "max_temperature_c": 45,
                "maintenance_interval_days": 90
                },
                "metadata": {
                "description": "Large-scale solar photovoltaic installation with integrated battery storage for grid stability",
                "generated_at": "2025-11-11T10:30:00Z",
                "confidence_score": 0.89,
                "enrichment_method": "tinyllama-contextual"
                },
                "created_at": "2025-01-15T08:00:00Z",
                "updated_at": "2025-11-10T14:22:00Z"
                }
                ]
                }
                ```

                **SQL Query Equivalent:**
                ```sql
                SELECT * FROM datacatalog
                WHERE metadata_type = 'RenewableAsset'
                AND component LIKE '%solar%'
                LIMIT 100;
                ```

                ---

                ### Composite Metrics

                #### GET `/<context>/metrics/composite`

                    Retrieves pre-calculated composite metrics for system performance, resource utilization, and renewable energy operations.

                    **Parameters:**
                    - `metric_type` (optional): Filter by metric type (see Composite Metrics Definitions)
                    - `time_window` (optional): Time window in minutes (default: 60)
                    - `aggregation` (optional): `avg`, `min`, `max`, `sum` (default: `avg`)

                    **Response:**
                    ```json
                    {
                    "success": true,
                    "timestamp": "2025-11-11T12:00:00Z",
                    "time_window_minutes": 60,
                    "metrics": {
                    "system_performance": {
                    "query_latency_p50_ms": 45,
                    "query_latency_p95_ms": 150,
                    "query_latency_p99_ms": 320,
                    "throughput_queries_per_second": 125.5,
                    "cache_hit_ratio": 0.78,
                    "metadata_enrichment_rate": 0.92
                    },
                    "cluster_health": {
                    "active_agents": 8,
                    "coordinator_agent": "Agent-1",
                    "peer_connectivity_score": 0.95,
                    "gossipsub_mesh_peers": 6,
                    "replication_lag_ms": 85
                    },
                    "resource_utilization": {
                    "cpu_percent_avg": 35.2,
                    "cpu_percent_max": 67.8,
                    "memory_used_mb": 2048,
                    "memory_available_mb": 6144,
                    "memory_utilization_percent": 25.0,
                    "disk_io_read_mbps": 12.5,
                    "disk_io_write_mbps": 8.3
                    },
                    "renewable_energy": {
                    "total_capacity_mw": 125.5,
                    "current_generation_mw": 98.3,
                    "capacity_factor": 0.783,
                    "availability_percent": 96.5,
                    "assets_online": 42,
                    "assets_total": 45
                    },
                    "orbitdb_performance": {
                    "documents_total": 125840,
                    "writes_per_second": 45.2,
                    "reads_per_second": 234.7,
                    "ipfs_blocks_cached": 5632,
                    "sync_operations_pending": 12
                    }
                    }
                    }
                    ```

                    ---

                    ### SLO Violations

                    #### GET `/<context>/slo/violations`

                        Retrieves Service Level Objective (SLO) violations detected across the distributed system.

                        **Parameters:**
                        - `severity` (optional): Filter by severity (`critical`, `warning`, `info`)
                        - `status` (optional): Filter by status (`active`, `resolved`, `acknowledged`)
                        - `since_minutes` (optional): Time window in minutes (default: 60)
                        - `limit` (optional, default: 50): Maximum violations to return

                        **Response:**
                        ```json
                        {
                        "success": true,
                        "timestamp": "2025-11-11T12:00:00Z",
                        "total_violations": 3,
                        "active_violations": 2,
                        "violations": [
                        {
                        "id": "slo_violation_20251111_120001",
                        "slo_type": "query_latency",
                        "severity": "warning",
                        "status": "active",
                        "threshold_value": 200,
                        "threshold_unit": "ms",
                        "actual_value": 285,
                        "deviation_percent": 42.5,
                        "first_detected": "2025-11-11T11:45:00Z",
                        "last_observed": "2025-11-11T11:58:00Z",
                        "duration_seconds": 780,
                        "affected_components": [
                        "Agent-5",
                        "Agent-7"
                        ],
                        "description": "Query latency P95 exceeded threshold for 13 minutes on 2 agents",
                        "recommended_actions": [
                        "Check network connectivity between affected agents",
                        "Review query optimization settings",
                        "Consider cache warming for frequently accessed data"
                        ],
                        "related_metrics": {
                        "cpu_utilization": 0.68,
                        "memory_pressure": 0.45,
                        "network_latency_ms": 120
                        }
                        },
                        {
                        "id": "slo_violation_20251111_113500",
                        "slo_type": "metadata_enrichment_rate",
                        "severity": "critical",
                        "status": "active",
                        "threshold_value": 0.90,
                        "threshold_unit": "ratio",
                        "actual_value": 0.65,
                        "deviation_percent": 27.8,
                        "first_detected": "2025-11-11T11:35:00Z",
                        "last_observed": "2025-11-11T11:59:00Z",
                        "duration_seconds": 1440,
                        "affected_components": [
                        "TinyLlama-Agent-1"
                        ],
                        "description": "TinyLlama metadata enrichment rate dropped below 90% threshold",
                        "recommended_actions": [
                        "Check TinyLlama service health",
                        "Review TinyLlama container resource allocation",
                        "Verify network connectivity to TinyLlama endpoint"
                        ],
                        "related_metrics": {
                        "tinyllama_response_time_ms": 3500,
                        "tinyllama_error_rate": 0.12,
                        "pending_enrichment_queue": 247
                        }
                        }
                        ]
                        }
                        ```

                        ---

                        ### Trust Results

                        #### POST `/<context>/trust/results`

                            Posts trust evaluation results from external systems (e.g., reputation scoring, validation results).

                            **Request Body:**
                            ```json
                            {
                            "trust_evaluation_id": "trust_eval_20251111_120001",
                            "evaluated_entity": "Agent-5",
                            "entity_type": "agent",
                            "trust_score": 0.87,
                            "evaluation_method": "reputation_based",
                            "evaluation_criteria": {
                            "uptime_score": 0.95,
                            "response_accuracy": 0.88,
                            "peer_consensus": 0.82,
                            "historical_reliability": 0.90
                            },
                            "confidence_level": 0.92,
                            "evaluator_id": "trust_service_001",
                            "timestamp": "2025-11-11T12:00:00Z",
                            "valid_until": "2025-11-11T18:00:00Z",
                            "metadata": {
                            "evaluation_duration_ms": 450,
                            "data_points_analyzed": 1248,
                            "baseline_comparison": "30_day_average"
                            }
                            }
                            ```

                            **Response:**
                            ```json
                            {
                            "success": true,
                            "message": "Trust result recorded successfully",
                            "result_id": "trust_eval_20251111_120001",
                            "stored_at": "2025-11-11T12:00:15Z",
                            "ipfs_cid": "QmX7vK8fP9mNxZkJ4Y3bL2wR5tH8gD9cA1fE6hM3nB4jP7",
                            "orbitdb_hash": "zdpuB2Wgjkz8fHqGHvP3nF4kL7mR9sT1xY5aZ6bC8dE2gF3hI"
                            }
                            ```

                            ---

                            ### Metadata Enrichment

                            #### POST `/api/v1/metadata/enrich`

                            Triggers AI-powered contextual metadata generation for a specific dataset or table.

                            **Request Body:**
                            ```json
                            {
                            "db": "swarmkb",
                            "table": "renewable_assets",
                            "maxRows": 200,
                            "greek": false,
                            "force_refresh": false
                            }
                            ```

                            **Response:**
                            ```json
                            {
                            "success": true,
                            "enrichment_id": "enrich_20251111_120001",
                            "status": "completed",
                            "processed_rows": 187,
                            "enriched_rows": 172,
                            "skipped_rows": 15,
                            "duration_ms": 4523,
                            "metadata_generated": [
                            {
                            "row_id": "asset_wind_042",
                            "description": "Offshore wind turbine installation with advanced pitch control and grid synchronization capabilities",
                            "confidence_score": 0.91,
                            "tags": ["wind", "offshore", "grid-connected"],
                            "relationships": "Part of Northern Wind Farm cluster",
                            "generated_at": "2025-11-11T12:00:08Z"
                            }
                            ],
                            "cache_status": {
                            "cached": 15,
                            "generated": 172
                            },
                            "llm_metrics": {
                            "average_response_time_ms": 245,
                            "token_count": 15420,
                            "model": "tinyllama-1.1b-chat"
                            }
                            }
                            ```

                            #### POST `/api/v1/metadata/enrich-batch`

                            Batch metadata enrichment for multiple tables.

                            **Request Body:**
                            ```json
                            {
                            "batch_id": "batch_20251111_001",
                            "enrichments": [
                            {
                            "db": "swarmkb",
                            "table": "renewable_assets",
                            "maxRows": 500
                            },
                            {
                            "db": "swarmkb",
                            "table": "tosca_templates",
                            "maxRows": 100
                            }
                            ],
                            "greek": false,
                            "priority": "normal"
                            }
                            ```

                            **Response:**
                            ```json
                            {
                            "success": true,
                            "batch_id": "batch_20251111_001",
                            "status": "in_progress",
                            "total_enrichments": 2,
                            "completed": 1,
                            "pending": 1,
                            "estimated_completion": "2025-11-11T12:05:00Z",
                            "results": [
                            {
                            "table": "renewable_assets",
                            "status": "completed",
                            "processed_rows": 487,
                            "enriched_rows": 451
                            },
                            {
                            "table": "tosca_templates",
                            "status": "pending",
                            "estimated_start": "2025-11-11T12:03:00Z"
                            }
                            ]
                            }
                            ```

                            #### GET `/api/v1/metadata/profile`

                            Retrieves data profiling statistics for a dataset.

                            **Parameters:**
                            - `db`: Database name (required)
                            - `table`: Table name (required)

                            **Response:**
                            ```json
                            {
                            "success": true,
                            "db": "swarmkb",
                            "table": "renewable_assets",
                            "profile": {
                            "row_count": 487,
                            "column_count": 18,
                            "enriched_rows": 451,
                            "enrichment_coverage": 0.926,
                            "data_quality_score": 0.88,
                            "columns": [
                            {
                            "name": "asset_id",
                            "type": "string",
                            "non_null_count": 487,
                            "unique_count": 487,
                            "completeness": 1.0
                            },
                            {
                            "name": "capacity_kw",
                            "type": "number",
                            "non_null_count": 485,
                            "min": 100,
                            "max": 5500,
                            "mean": 2345.7,
                            "median": 2100,
                            "stddev": 892.3,
                            "completeness": 0.996
                            }
                            ],
                            "metadata_stats": {
                            "avg_description_length": 156,
                            "avg_confidence_score": 0.87,
                            "last_enrichment": "2025-11-11T10:30:00Z"
                            }
                            }
                            }
                            ```

                            #### GET `/api/v1/metadata/metrics`

                            Retrieves metadata enrichment system metrics.

                            **Response:**
                            ```json
                            {
                            "success": true,
                            "timestamp": "2025-11-11T12:00:00Z",
                            "metrics": {
                            "total_enrichments": 15247,
                            "enrichments_today": 342,
                            "cache_hit_rate": 0.78,
                            "average_enrichment_time_ms": 265,
                            "tinyllama_health": "healthy",
                            "tinyllama_uptime_percent": 99.2,
                            "pending_queue_size": 12,
                            "error_rate_24h": 0.003
                            }
                            }
                            ```

                            #### GET `/api/v1/metadata/health`

                            Health check endpoint for metadata enrichment service.

                            **Response:**
                            ```json
                            {
                            "success": true,
                            "status": "healthy",
                            "timestamp": "2025-11-11T12:00:00Z",
                            "components": {
                            "tinyllama": {
                            "status": "healthy",
                            "response_time_ms": 185,
                            "last_checked": "2025-11-11T11:59:55Z"
                            },
                            "cache": {
                            "status": "healthy",
                            "size_mb": 245,
                            "hit_rate": 0.78
                            },
                            "orbitdb": {
                            "status": "healthy",
                            "sync_status": "synced",
                            "peers_connected": 7
                            }
                            }
                            }
                            ```

                            #### DELETE `/api/v1/metadata/cache`

                            Clears the metadata enrichment cache.

                            **Response:**
                            ```json
                            {
                            "success": true,
                            "message": "Cache cleared successfully",
                            "items_cleared": 1247,
                            "timestamp": "2025-11-11T12:00:00Z"
                            }
                            ```

                            ---

                            ### Query Engine

                            #### POST `/<context>/query`

                                Executes distributed queries across the OptimusDB cluster with query optimization and parallel execution.

                                **Request Body:**
                                ```json
                                {
                                "query": "SELECT asset_name, capacity_kw, location FROM renewable_assets WHERE asset_type = 'solar' AND capacity_kw > 1000 ORDER BY capacity_kw DESC",
                                "strategy": "LocalThenRemoteMerge",
                                "options": {
                                "includeLocal": true,
                                "timeBudgetMs": 2000,
                                "annotateSource": true,
                                "parallel": true,
                                "maxPeers": 5
                                }
                                }
                                ```

                                **Query Strategies:**
                                - `LocalOnly`: Query only local OrbitDB store
                                - `LocalThenRemoteMerge`: Query local first, then remote peers, merge results
                                - `RemoteOnly`: Query only remote peers (excludes local)
                                - `BroadcastAll`: Broadcast query to all peers simultaneously

                                **Response:**
                                ```json
                                {
                                "success": true,
                                "strategy": "LocalThenRemoteMerge",
                                "execution_time_ms": 342,
                                "result_count": 23,
                                "sources": {
                                "local": 8,
                                "remote": 15
                                },
                                "results": [
                                {
                                "asset_name": "Solar Farm Delta",
                                "capacity_kw": 5500,
                                "location": "Nevada, USA",
                                "_source": {
                                "type": "local",
                                "peer_id": ""
                                },
                                "_trace": {
                                "strategy": "LocalThenRemoteMerge"
                                }
                                },
                                {
                                "asset_name": "Solar Array Beta",
                                "capacity_kw": 4200,
                                "location": "Arizona, USA",
                                "_source": {
                                "type": "peer",
                                "peer_id": "12D3KooWBhX..."
                                },
                                "_trace": {
                                "strategy": "LocalThenRemoteMerge"
                                }
                                }
                                ],
                                "query_plan": {
                                "parsed": true,
                                "optimized": true,
                                "indexes_used": ["asset_type_idx"],
                                "estimated_cost": 342
                                }
                                }
                                ```

                                #### POST `/<context>/sql`

                                    Direct SQL execution endpoint (compatibility layer).

                                    **Request Body:**
                                    ```json
                                    {
                                    "sql": "INSERT INTO renewable_assets (asset_id, asset_name, asset_type, capacity_kw) VALUES ('solar_099', 'Test Farm', 'solar', 2500)"
                                    }
                                    ```

                                    **Response:**
                                    ```json
                                    {
                                    "success": true,
                                    "affected_rows": 1,
                                    "last_insert_id": "solar_099",
                                    "execution_time_ms": 45
                                    }
                                    ```

                                    ---

                                    ### EMS Integration

                                    #### GET `/<context>/ems/logs`

                                        Retrieves EMS event logs from the integrated Event Management System.

                                        **Parameters:**
                                        - `limit` (default: 50, max: 1000): Maximum log entries
                                        - `level` (optional): Filter by log level (`INFO`, `WARN`, `ERROR`, `DEBUG`)
                                        - `since_min` (optional, max: 1440): Time window in minutes

                                        **Response:**
                                        ```json
                                        {
                                        "success": true,
                                        "records": [
                                        {
                                        "id": 1247,
                                        "timestamp": "2025-11-11T11:58:30Z",
                                        "level": "INFO",
                                        "source": "ems",
                                        "message": "EMS recv action=update resource=capacity body={...}"
                                        },
                                        {
                                        "id": 1248,
                                        "timestamp": "2025-11-11T11:59:15Z",
                                        "level": "ERROR",
                                        "source": "ems",
                                        "message": "EMS recv (unmarshal failed): {invalid json structure}"
                                        }
                                        ]
                                        }
                                        ```

                                        #### GET `/<context>/ems/events`

                                            Retrieves detailed EMS events with full message payloads.

                                            **Parameters:**
                                            - `limit` (default: 50, max: 1000): Maximum events
                                            - `since_min` (optional, max: 1440): Time window in minutes

                                            **Response:**
                                            ```json
                                            {
                                            "success": true,
                                            "records": [
                                            {
                                            "id": 523,
                                            "received_at": "2025-11-11T11:58:30Z",
                                            "client_id": "optimusdb-agent-1",
                                            "topic": "/topic/capacity.updates",
                                            "action": "update",
                                            "resource": "renewable_capacity",
                                            "params": "{\"asset_id\":\"solar_042\",\"new_capacity\":5200}",
                                            "raw": "{\"action\":\"update\",\"resource\":\"renewable_capacity\",\"params\":{\"asset_id\":\"solar_042\",\"new_capacity\":5200}}"
                                            }
                                            ]
                                            }
                                            ```

                                            #### GET/POST `/<context>/ems/sql`

                                                Execute SQL queries against the EMS events database.

                                                **Request (GET):**
                                                ```
                                                GET /<context>/ems/sql?q=SELECT%20COUNT(*)%20FROM%20ems_events%20WHERE%20action='update'
                                                    ```

                                                    **Request (POST):**
                                                    ```json
                                                    {
                                                    "sql": "SELECT action, COUNT(*) as count FROM ems_events GROUP BY action ORDER BY count DESC"
                                                    }
                                                    ```

                                                    **Response:**
                                                    ```json
                                                    {
                                                    "success": true,
                                                    "records": [
                                                    {
                                                    "action": "update",
                                                    "count": 1247
                                                    },
                                                    {
                                                    "action": "create",
                                                    "count": 523
                                                    },
                                                    {
                                                    "action": "delete",
                                                    "count": 89
                                                    }
                                                    ]
                                                    }
                                                    ```

                                                    ---

                                                    ### Monitoring & Health

                                                    #### GET `/<context>/health`

                                                        System health check endpoint.

                                                        **Response:**
                                                        ```json
                                                        {
                                                        "success": true,
                                                        "status": "healthy",
                                                        "timestamp": "2025-11-11T12:00:00Z",
                                                        "agent": {
                                                        "id": "Agent-1",
                                                        "role": "coordinator",
                                                        "uptime_seconds": 345678
                                                        },
                                                        "components": {
                                                        "orbitdb": "healthy",
                                                        "ipfs": "healthy",
                                                        "sqlite": "healthy",
                                                        "libp2p": "healthy",
                                                        "gossipsub": "healthy",
                                                        "tinyllama": "healthy",
                                                        "ems_connection": "connected"
                                                        },
                                                        "cluster": {
                                                        "total_agents": 8,
                                                        "active_agents": 8,
                                                        "coordinator": "Agent-1"
                                                        }
                                                        }
                                                        ```

                                                        #### GET `/<context>/metrics`

                                                            Prometheus-compatible metrics endpoint.

                                                            **Response (Plain Text):**
                                                            ```
                                                            # HELP optimusdb_query_duration_seconds Query execution duration
                                                            # TYPE optimusdb_query_duration_seconds histogram
                                                            optimusdb_query_duration_seconds_bucket{le="0.05"} 1247
                                                            optimusdb_query_duration_seconds_bucket{le="0.1"} 2341
                                                            optimusdb_query_duration_seconds_bucket{le="0.5"} 3567
                                                            optimusdb_query_duration_seconds_sum 14523.45
                                                            optimusdb_query_duration_seconds_count 4521

                                                            # HELP optimusdb_cluster_agents Number of active cluster agents
                                                            # TYPE optimusdb_cluster_agents gauge
                                                            optimusdb_cluster_agents 8

                                                            # HELP optimusdb_metadata_enrichments_total Total metadata enrichments
                                                            # TYPE optimusdb_metadata_enrichments_total counter
                                                            optimusdb_metadata_enrichments_total 15247
                                                            ```

                                                            #### GET `/<context>/log`

                                                                Application logs endpoint.

                                                                **Parameters:**
                                                                - `date` (required): Date in YYYY-MM-DD format
                                                                - `hour` (required): Hour in HH format (00-23)

                                                                **Response:**
                                                                ```json
                                                                {
                                                                "success": true,
                                                                "logs": [
                                                                {
                                                                "id": 54321,
                                                                "timestamp": "2025-11-11T11:00:15Z",
                                                                "level": "INFO",
                                                                "message": "Coordinator election completed",
                                                                "source": "election"
                                                                }
                                                                ]
                                                                }
                                                                ```

                                                                #### GET `/<context>/benchmarks`

                                                                    Performance benchmarking data (requires `--benchmark` flag).

                                                                    **Response:**
                                                                    ```json
                                                                    {
                                                                    "success": true,
                                                                    "agent_id": "Agent-1",
                                                                    "region": "us-west-2",
                                                                    "benchmark": {
                                                                    "bootstrap_seconds": 12.45,
                                                                    "average_contribution_seconds": 0.342,
                                                                    "min_contribution_seconds": 0.085,
                                                                    "max_contribution_seconds": 1.523,
                                                                    "samples": [
                                                                    {
                                                                    "ts": "2025-11-11T11:00:00Z",
                                                                    "mem_bytes": 2147483648,
                                                                    "cpu_percent": 35.2
                                                                    }
                                                                    ]
                                                                    }
                                                                    }
                                                                    ```

                                                                    ---

                                                                    ## Data Models

                                                                    ### MetadataEntry

                                                                    Core metadata structure for knowledge base entries.

                                                                    ```typescript
                                                                    interface MetadataEntry {
                                                                    _id: string;
                                                                    author: string;
                                                                    metadata_type: string;
                                                                    component: string;
                                                                    behaviour: string;
                                                                    relationships: string;
                                                                    associated_id: string;
                                                                    name: string;
                                                                    description: string;
                                                                    tags: string[];
                                                                    status: string;
                                                                    created_by: string;
                                                                    created_at: string; // ISO 8601
                                                                    updated_at: string; // ISO 8601
                                                                    related_ids: string[];
                                                                    priority: string;
                                                                    scheduling_info: {
                                                                    cron_expression?: string;
                                                                    time_zone?: string;
                                                                    [key: string]: any;
                                                                    };
                                                                    sla_constraints: {
                                                                    latency_ms?: number;
                                                                    throughput_mbps?: number;
                                                                    [key: string]: any;
                                                                    };
                                                                    ownership_details: {
                                                                    owner?: string;
                                                                    organization?: string;
                                                                    [key: string]: any;
                                                                    };
                                                                    audit_trail: Array<{
                                                                    timestamp: string;
                                                                    user: string;
                                                                    action: string;
                                                                    }>;
                                                                    }
                                                                    ```

                                                                    ### RenewableAsset

                                                                    Renewable energy asset representation.

                                                                    ```typescript
                                                                    interface RenewableAsset {
                                                                    _id: string;
                                                                    asset_name: string;
                                                                    asset_type: 'solar' | 'wind' | 'hydro' | 'battery' | 'geothermal';
                                                                    location: {
                                                                    lat: number;
                                                                    lon: number;
                                                                    address: string;
                                                                    timezone?: string;
                                                                    };
                                                                    capacity: {
                                                                    rated_power_kw: number;
                                                                    peak_power_kw: number;
                                                                    storage_kwh?: number;
                                                                    efficiency_percent: number;
                                                                    };
                                                                    operational_constraints: {
                                                                    min_irradiance?: number;
                                                                    max_temperature_c?: number;
                                                                    min_wind_speed_ms?: number;
                                                                    maintenance_interval_days: number;
                                                                    };
                                                                    metadata: {
                                                                    description: string;
                                                                    generated_at: string;
                                                                    confidence_score: number;
                                                                    enrichment_method: string;
                                                                    };
                                                                    created_at: string;
                                                                    updated_at: string;
                                                                    }
                                                                    ```

                                                                    ### TOSCATemplate

                                                                    TOSCA template metadata for deployment plans.

                                                                    ```typescript
                                                                    interface TOSCATemplate {
                                                                    _id: string; // template_id (SHA256 hash)
                                                                    type: 'tosca_template';
                                                                    description: string;
                                                                    node_count: number;
                                                                    yaml: string; // Original YAML content
                                                                    created_at: string;
                                                                    filename: string;
                                                                    filesize_bytes: number;
                                                                    content_sha256: string;
                                                                    ipfs_path: string; // /ipfs/<CID>
                                                                        uploader: string;
                                                                        source_pod: string;
                                                                        source_ip: string;
                                                                        }
                                                                        ```

                                                                        ### EMSMessage

                                                                        EMS event message structure.

                                                                        ```typescript
                                                                        interface EMSMessage {
                                                                        action: 'create' | 'update' | 'delete' | string;
                                                                        resource: string;
                                                                        params: {
                                                                        [key: string]: any;
                                                                        };
                                                                        }
                                                                        ```

                                                                        ### TrustEvaluation

                                                                        Trust evaluation result.

                                                                        ```typescript
                                                                        interface TrustEvaluation {
                                                                        trust_evaluation_id: string;
                                                                        evaluated_entity: string;
                                                                        entity_type: 'agent' | 'peer' | 'service';
                                                                        trust_score: number; // 0.0 to 1.0
                                                                        evaluation_method: string;
                                                                        evaluation_criteria: {
                                                                        uptime_score?: number;
                                                                        response_accuracy?: number;
                                                                        peer_consensus?: number;
                                                                        historical_reliability?: number;
                                                                        [key: string]: number;
                                                                        };
                                                                        confidence_level: number; // 0.0 to 1.0
                                                                        evaluator_id: string;
                                                                        timestamp: string;
                                                                        valid_until: string;
                                                                        metadata: {
                                                                        evaluation_duration_ms: number;
                                                                        data_points_analyzed: number;
                                                                        baseline_comparison?: string;
                                                                        [key: string]: any;
                                                                        };
                                                                        }
                                                                        ```

                                                                        ### QueryRequest

                                                                        Distributed query request.

                                                                        ```typescript
                                                                        interface QueryRequest {
                                                                        query: string; // SQL query
                                                                        strategy: 'LocalOnly' | 'LocalThenRemoteMerge' | 'RemoteOnly' | 'BroadcastAll';
                                                                        options: {
                                                                        includeLocal: boolean;
                                                                        timeBudgetMs: number; // Query timeout
                                                                        annotateSource: boolean; // Add source metadata
                                                                        parallel: boolean; // Parallel execution
                                                                        maxPeers: number; // Max peers to query
                                                                        };
                                                                        }
                                                                        ```

                                                                        ---

                                                                        ## Composite Metrics Definitions

                                                                        OptimusDB tracks the following composite metrics:

                                                                        ### System Performance Metrics

                                                                        | Metric | Description | Unit | SLO Target |
                                                                        |--------|-------------|------|------------|
                                                                        | `query_latency_p50_ms` | 50th percentile query latency | milliseconds | < 100 |
                                                                        | `query_latency_p95_ms` | 95th percentile query latency | milliseconds | < 200 |
                                                                        | `query_latency_p99_ms` | 99th percentile query latency | milliseconds | < 500 |
                                                                        | `throughput_queries_per_second` | Query throughput | queries/sec | > 100 |
                                                                        | `cache_hit_ratio` | Cache effectiveness | ratio (0-1) | > 0.70 |
                                                                        | `metadata_enrichment_rate` | AI enrichment success rate | ratio (0-1) | > 0.90 |

                                                                        ### Cluster Health Metrics

                                                                        | Metric | Description | Unit | SLO Target |
                                                                        |--------|-------------|------|------------|
                                                                        | `active_agents` | Number of active cluster agents | count | >= 6 |
                                                                        | `coordinator_agent` | Current coordinator agent ID | string | N/A |
                                                                        | `peer_connectivity_score` | P2P network health | ratio (0-1) | > 0.85 |
                                                                        | `gossipsub_mesh_peers` | GossipSub mesh connectivity | count | >= 4 |
                                                                        | `replication_lag_ms` | OrbitDB replication delay | milliseconds | < 1000 |

                                                                        ### Resource Utilization Metrics

                                                                        | Metric | Description | Unit | SLO Target |
                                                                        |--------|-------------|------|------------|
                                                                        | `cpu_percent_avg` | Average CPU utilization | percent | < 70 |
                                                                        | `cpu_percent_max` | Peak CPU utilization | percent | < 90 |
                                                                        | `memory_used_mb` | Memory consumption | megabytes | N/A |
                                                                        | `memory_utilization_percent` | Memory usage percentage | percent | < 80 |
                                                                        | `disk_io_read_mbps` | Disk read throughput | MB/s | N/A |
                                                                        | `disk_io_write_mbps` | Disk write throughput | MB/s | N/A |

                                                                        ### Renewable Energy Metrics

                                                                        | Metric | Description | Unit | SLO Target |
                                                                        |--------|-------------|------|------------|
                                                                        | `total_capacity_mw` | Total installed capacity | megawatts | N/A |
                                                                        | `current_generation_mw` | Current power generation | megawatts | N/A |
                                                                        | `capacity_factor` | Utilization efficiency | ratio (0-1) | > 0.30 |
                                                                        | `availability_percent` | Asset availability | percent | > 95 |
                                                                        | `assets_online` | Online asset count | count | N/A |
                                                                        | `assets_total` | Total asset count | count | N/A |

                                                                        ### OrbitDB Performance Metrics

                                                                        | Metric | Description | Unit | SLO Target |
                                                                        |--------|-------------|------|------------|
                                                                        | `documents_total` | Total documents in stores | count | N/A |
                                                                        | `writes_per_second` | Write throughput | writes/sec | > 40 |
                                                                        | `reads_per_second` | Read throughput | reads/sec | > 200 |
                                                                        | `ipfs_blocks_cached` | Cached IPFS blocks | count | N/A |
                                                                        | `sync_operations_pending` | Pending sync operations | count | < 50 |

                                                                        ---

                                                                        ## SLO Violations Catalog

                                                                        ### Query Performance Violations

                                                                        #### Query Latency P95 Exceeded
                                                                        - **Type:** `query_latency_p95`
                                                                        - **Threshold:** 200 ms
                                                                        - **Severity:** Warning → Critical (if sustained > 15 min)
                                                                        - **Causes:** Network congestion, peer unavailability, cache misses, suboptimal queries
                                                                        - **Actions:** Optimize query, check peer connectivity, warm caches

                                                                        #### Query Timeout
                                                                        - **Type:** `query_timeout`
                                                                        - **Threshold:** 2000 ms (default time budget)
                                                                        - **Severity:** Critical
                                                                        - **Causes:** Unresponsive peers, network partitions, overloaded agents
                                                                        - **Actions:** Reduce query complexity, check cluster health, adjust timeout

                                                                        ### Metadata Enrichment Violations

                                                                        #### Low Enrichment Rate
                                                                        - **Type:** `metadata_enrichment_rate`
                                                                        - **Threshold:** < 0.90 (90%)
                                                                        - **Severity:** Critical
                                                                        - **Causes:** TinyLlama unavailable, resource exhaustion, API errors
                                                                        - **Actions:** Check TinyLlama health, increase resources, review error logs

                                                                        #### High Enrichment Latency
                                                                        - **Type:** `enrichment_latency`
                                                                        - **Threshold:** > 500 ms per row
                                                                        - **Severity:** Warning
                                                                        - **Causes:** TinyLlama overload, network issues, complex datasets
                                                                        - **Actions:** Batch requests, optimize prompts, scale TinyLlama

                                                                        ### Cluster Health Violations

                                                                        #### Insufficient Active Agents
                                                                        - **Type:** `cluster_agents_low`
                                                                        - **Threshold:** < 6 agents
                                                                        - **Severity:** Critical
                                                                        - **Causes:** Agent crashes, network issues, deployment problems
                                                                        - **Actions:** Restart agents, check logs, verify network

                                                                        #### Coordinator Election Failure
                                                                        - **Type:** `coordinator_election_failed`
                                                                        - **Threshold:** No coordinator for > 30 seconds
                                                                        - **Severity:** Critical
                                                                        - **Causes:** GossipSub mesh failure, split-brain, reputation issues
                                                                        - **Actions:** Check GossipSub logs, verify mesh formation, review election params

                                                                        #### Low Peer Connectivity
                                                                        - **Type:** `peer_connectivity_low`
                                                                        - **Threshold:** < 0.85 (85%)
                                                                        - **Severity:** Warning → Critical (if < 0.60)
                                                                        - **Causes:** Network partitions, firewall issues, peer failures
                                                                        - **Actions:** Check network, verify LibP2P connectivity, review peer discovery

                                                                        ### Resource Utilization Violations

                                                                        #### High CPU Utilization
                                                                        - **Type:** `cpu_utilization_high`
                                                                        - **Threshold:** > 70% sustained, > 90% peak
                                                                        - **Severity:** Warning → Critical
                                                                        - **Causes:** Query load, background tasks, insufficient resources
                                                                        - **Actions:** Scale horizontally, optimize queries, review workload

                                                                        #### High Memory Utilization
                                                                        - **Type:** `memory_utilization_high`
                                                                        - **Threshold:** > 80%
                                                                        - **Severity:** Warning → Critical (if > 95%)
                                                                        - **Causes:** Cache growth, memory leaks, large datasets
                                                                        - **Actions:** Increase memory, clear caches, investigate leaks

                                                                        #### High Replication Lag
                                                                        - **Type:** `replication_lag_high`
                                                                        - **Threshold:** > 1000 ms
                                                                        - **Severity:** Warning
                                                                        - **Causes:** Network latency, OrbitDB sync delays, peer overload
                                                                        - **Actions:** Check network, review OrbitDB health, reduce write load

                                                                        ### EMS Integration Violations

                                                                        #### EMS Connection Failure
                                                                        - **Type:** `ems_connection_failed`
                                                                        - **Threshold:** Disconnected for > 60 seconds
                                                                        - **Severity:** Critical
                                                                        - **Causes:** Broker down, network issues, credentials invalid
                                                                        - **Actions:** Check EMS broker, verify credentials, review network

                                                                        #### High EMS Event Processing Latency
                                                                        - **Type:** `ems_processing_latency`
                                                                        - **Threshold:** > 500 ms per event
                                                                        - **Severity:** Warning
                                                                        - **Causes:** Complex event processing, database contention, resource limits
                                                                        - **Actions:** Optimize handlers, increase resources, batch operations

                                                                        ---

                                                                        ## Authentication

                                                                        Currently, OptimusDB operates in a trusted cluster environment without authentication. For production deployments, consider:

                                                                        1. **W3C Verifiable Credentials** (DID integration available)
                                                                        2. **mTLS** for peer-to-peer authentication
                                                                        3. **JWT tokens** for HTTP API access
                                                                        4. **Role-based access control** (RBAC) for query permissions

                                                                        ### DID Endpoints

                                                                        OptimusDB includes W3C Verifiable Credentials support via the DID integration:

                                                                        ```
                                                                        POST /<context>/did/create
                                                                            POST /<context>/did/issue
                                                                                POST /<context>/did/verify
                                                                                    GET  /<context>/did/resolve
                                                                                        ```

                                                                                        See the DID documentation for details.

                                                                                        ---

                                                                                        ## Error Responses

                                                                                        All error responses follow this format:

                                                                                        ```json
                                                                                        {
                                                                                        "success": false,
                                                                                        "error": {
                                                                                        "code": "ERROR_CODE",
                                                                                        "message": "Human-readable error description",
                                                                                        "details": {
                                                                                        "field": "specific_field_if_applicable",
                                                                                        "value": "problematic_value"
                                                                                        }
                                                                                        },
                                                                                        "timestamp": "2025-11-11T12:00:00Z"
                                                                                        }
                                                                                        ```

                                                                                        ### Common Error Codes

                                                                                        | Code | HTTP Status | Description |
                                                                                        |------|-------------|-------------|
                                                                                        | `INVALID_REQUEST` | 400 | Malformed request body or parameters |
                                                                                        | `UNAUTHORIZED` | 401 | Authentication required |
                                                                                        | `FORBIDDEN` | 403 | Insufficient permissions |
                                                                                        | `NOT_FOUND` | 404 | Resource not found |
                                                                                        | `CONFLICT` | 409 | Resource conflict (duplicate ID) |
                                                                                        | `QUERY_ERROR` | 422 | SQL query syntax or execution error |
                                                                                        | `TIMEOUT` | 504 | Query or operation timeout |
                                                                                        | `SERVICE_UNAVAILABLE` | 503 | Service temporarily unavailable |
                                                                                        | `INTERNAL_ERROR` | 500 | Unexpected server error |

                                                                                        ---

                                                                                        ## Integration Examples

                                                                                        ### Example 1: Fetch Capacity Descriptions

                                                                                        ```bash
                                                                                        curl -X GET "http://localhost:18001/swarm/capacity/descriptions?asset_type=solar&limit=50&include_metadata=true" \
                                                                                        -H "Content-Type: application/json"
                                                                                        ```

                                                                                        ### Example 2: Post Trust Results

                                                                                        ```bash
                                                                                        curl -X POST "http://localhost:18001/swarm/trust/results" \
                                                                                        -H "Content-Type: application/json" \
                                                                                        -d '{
                                                                                        "trust_evaluation_id": "trust_eval_001",
                                                                                        "evaluated_entity": "Agent-5",
                                                                                        "entity_type": "agent",
                                                                                        "trust_score": 0.87,
                                                                                        "evaluation_method": "reputation_based",
                                                                                        "evaluator_id": "trust_service_001",
                                                                                        "timestamp": "2025-11-11T12:00:00Z"
                                                                                        }'
                                                                                        ```

                                                                                        ### Example 3: Query with Optimization

                                                                                        ```bash
                                                                                        curl -X POST "http://localhost:18001/swarm/query" \
                                                                                        -H "Content-Type: application/json" \
                                                                                        -d '{
                                                                                        "query": "SELECT * FROM renewable_assets WHERE capacity_kw > 1000",
                                                                                        "strategy": "LocalThenRemoteMerge",
                                                                                        "options": {
                                                                                        "includeLocal": true,
                                                                                        "timeBudgetMs": 2000,
                                                                                        "annotateSource": true,
                                                                                        "parallel": true,
                                                                                        "maxPeers": 5
                                                                                        }
                                                                                        }'
                                                                                        ```

                                                                                        ### Example 4: Trigger Metadata Enrichment

                                                                                        ```bash
                                                                                        curl -X POST "http://localhost:18001/api/v1/metadata/enrich" \
                                                                                        -H "Content-Type: application/json" \
                                                                                        -d '{
                                                                                        "db": "swarmkb",
                                                                                        "table": "renewable_assets",
                                                                                        "maxRows": 200,
                                                                                        "force_refresh": false
                                                                                        }'
                                                                                        ```

                                                                                        ### Example 5: Check SLO Violations

                                                                                        ```bash
                                                                                        curl -X GET "http://localhost:18001/swarm/slo/violations?severity=critical&status=active&since_minutes=60" \
                                                                                        -H "Content-Type: application/json"
                                                                                        ```

                                                                                        ### Example 6: Monitor Composite Metrics

                                                                                        ```bash
                                                                                        curl -X GET "http://localhost:18001/swarm/metrics/composite?time_window=60&aggregation=avg" \
                                                                                        -H "Content-Type: application/json"
                                                                                        ```

                                                                                        ---

                                                                                        ## Notes for CENTERIS 2025

                                                                                        This API documentation demonstrates:

                                                                                        1. **Fully Decentralized Architecture**: No centralized API gateway; each agent exposes identical endpoints
                                                                                        2. **SQL Compatibility Layer**: Standard SQL queries over distributed OrbitDB stores
                                                                                        3. **AI-Powered Enrichment**: TinyLlama integration for contextual metadata generation
                                                                                        4. **Performance Optimization**: Query strategies achieving 60-85% performance improvements
                                                                                        5. **Production-Ready Monitoring**: Comprehensive metrics, SLO tracking, and health endpoints
                                                                                        6. **Real-Time Integration**: EMS/ActiveMQ STOMP support for event-driven architectures

                                                                                        **Key Innovation**: OptimusDB solves the "DCS Triad" (Decentralization, Consistency, Scalability) by combining:
                                                                                        - LibP2P for P2P networking
                                                                                        - OrbitDB (CRDT-based) for eventual consistency
                                                                                        - Coordinator-follower pattern for resource-intensive AI operations
                                                                                        - Distributed query engine with parallel execution

                                                                                        **Academic Validation**: All endpoints support the claims in the CENTERIS 2025 paper regarding decentralized knowledge sharing for renewable energy asset management.

                                                                                        ---

                                                                                        ## Revision History

                                                                                        | Version | Date | Changes |
                                                                                        |---------|------|---------|
                                                                                        | 1.0 | 2025-11-11 | Initial comprehensive API documentation |

                                                                                        ---

                                                                                        **Contact:**
                                                                                        - Project: Swarmchestrate (EU Horizon Europe)
                                                                                        - Grant Agreement: 101135012
                                                                                        - Prepared by: George Georgakakos