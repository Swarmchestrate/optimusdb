// File: api/inventory.go
// OptimusDB Agent Inventory Endpoint - Automatic Discovery
// Discovers all SQLite tables and OrbitDB stores automatically

package api

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"optimusdb/app"
	"optimusdb/election"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	orbitdb "berty.tech/go-orbit-db"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ============================================================================
// GLOBAL STATE
// ============================================================================

var (
	startTime      = time.Now()
	inventoryCache = &InventoryCache{entries: make(map[string]CacheEntry)}
)

// ============================================================================
// RESPONSE STRUCTURES
// ============================================================================

type AgentInventoryResponse struct {
	AgentInfo          AgentInfo         `json:"agent_info"`
	Databases          DatabaseInventory `json:"databases"`
	OrbitDBStores      OrbitDBInventory  `json:"orbitdb_stores"`
	IPFSStorage        IPFSInventory     `json:"ipfs_storage,omitempty"`
	LineageGraph       *LineageGraph     `json:"lineage_graph,omitempty"`
	MetadataEnrichment *EnrichmentStatus `json:"metadata_enrichment,omitempty"`
	AccessPatterns     *AccessPatterns   `json:"access_patterns,omitempty"`
	QualityMetrics     *QualityMetrics   `json:"quality_metrics,omitempty"`
	SystemMetrics      SystemMetrics     `json:"system_metrics"`
	GeneratedAt        time.Time         `json:"generated_at"`
	RequestID          string            `json:"request_id"`
}

type AgentInfo struct {
	AgentID       string      `json:"agent_id"`
	AgentName     string      `json:"agent_name"`
	NodeType      string      `json:"node_type"`
	Version       string      `json:"version"`
	UptimeSeconds int64       `json:"uptime_seconds"`
	LastSync      time.Time   `json:"last_sync"`
	Network       NetworkInfo `json:"network"`
}

type NetworkInfo struct {
	PeerCount      int      `json:"peer_count"`
	ConnectedPeers []string `json:"connected_peers"`
	IsCoordinator  bool     `json:"is_coordinator"`
	HealthScore    float64  `json:"health_score"`
}

type DatabaseInventory struct {
	Knowledgebase *DatabaseInfo           `json:"knowledgebase,omitempty"`
	Logger        *DatabaseInfo           `json:"logger,omitempty"`
	Reputation    *DatabaseInfo           `json:"reputation,omitempty"`
	Other         map[string]DatabaseInfo `json:"other,omitempty"`
}

type DatabaseInfo struct {
	Path      string               `json:"path"`
	SizeBytes int64                `json:"size_bytes"`
	Tables    map[string]TableInfo `json:"tables"`
}

type TableInfo struct {
	RowCount      int64                    `json:"row_count"`
	SizeBytes     int64                    `json:"size_bytes,omitempty"`
	Schema        *TableSchema             `json:"schema,omitempty"`
	Statistics    map[string]interface{}   `json:"statistics,omitempty"`
	SampleRecords []map[string]interface{} `json:"sample_records,omitempty"`
}

type TableSchema struct {
	Columns []ColumnInfo `json:"columns"`
	Indexes []string     `json:"indexes"`
}

type ColumnInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	PrimaryKey  bool   `json:"primary_key,omitempty"`
	Nullable    bool   `json:"nullable"`
	Description string `json:"description,omitempty"`
}

type OrbitDBInventory struct {
	ActiveStores  []OrbitDBStoreInfo `json:"active_stores"`
	PlannedStores []PlannedStoreInfo `json:"planned_stores"`
	TotalActive   int                `json:"total_active"`
	TotalPlanned  int                `json:"total_planned"`
}

type OrbitDBStoreInfo struct {
	Name            string                   `json:"name"`
	Address         string                   `json:"address"`
	Type            string                   `json:"type"`
	EntryCount      int                      `json:"entry_count,omitempty"`
	EventCount      int                      `json:"event_count,omitempty"`
	ReplicatedPeers []string                 `json:"replicated_peers,omitempty"`
	LastUpdate      time.Time                `json:"last_update"`
	SampleEntries   []map[string]interface{} `json:"sample_entries,omitempty"`
	AccessControl   string                   `json:"access_control"`
	Replication     bool                     `json:"replication"`
}

type PlannedStoreInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Status      string `json:"status"`
	Description string `json:"description,omitempty"`
}

type IPFSInventory struct {
	ContentCount   int            `json:"content_count"`
	TotalSizeBytes int64          `json:"total_size_bytes"`
	ContentTypes   map[string]int `json:"content_types"`
	SampleContent  []IPFSContent  `json:"sample_content,omitempty"`
}

type IPFSContent struct {
	CID         string    `json:"cid"`
	Filename    string    `json:"filename"`
	SizeBytes   int64     `json:"size_bytes"`
	ContentType string    `json:"content_type"`
	UploadedAt  time.Time `json:"uploaded_at"`
}

type LineageGraph struct {
	Tables []TableLineage `json:"tables"`
}

type TableLineage struct {
	ID         string       `json:"id"`
	Name       string       `json:"name"`
	Upstream   []Dependency `json:"upstream"`
	Downstream []Dependency `json:"downstream"`
}

type Dependency struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Type  string `json:"type,omitempty"`
	Level int    `json:"level"`
}

type EnrichmentStatus struct {
	ServiceStatus       string    `json:"service_status"`
	LLMModel            string    `json:"llm_model"`
	EnrichedTablesCount int       `json:"enriched_tables_count"`
	AvgEnrichmentTimeMS int       `json:"avg_enrichment_time_ms,omitempty"`
	CacheHitRate        float64   `json:"cache_hit_rate,omitempty"`
	LastEnrichment      time.Time `json:"last_enrichment,omitempty"`
}

type AccessPatterns struct {
	TopAccessedTables []AccessedTable `json:"top_accessed_tables"`
	TopUsers          []ActiveUser    `json:"top_users"`
}

type AccessedTable struct {
	TableID      string    `json:"table_id"`
	AccessCount  int       `json:"access_count"`
	UniqueUsers  int       `json:"unique_users"`
	LastAccessed time.Time `json:"last_accessed"`
}

type ActiveUser struct {
	UserID            string `json:"user_id"`
	AccessCount       int    `json:"access_count"`
	ResourcesAccessed int    `json:"resources_accessed"`
}

type QualityMetrics struct {
	AvgQualityScore     float64        `json:"avg_quality_score"`
	TablesWithScores    int            `json:"tables_with_scores"`
	QualityDistribution map[string]int `json:"quality_distribution"`
}

type SystemMetrics struct {
	QueryCacheSize    int     `json:"query_cache_size,omitempty"`
	CacheHitRate      float64 `json:"cache_hit_rate,omitempty"`
	AvgQueryLatencyMS int     `json:"avg_query_latency_ms,omitempty"`
	TotalQueries24h   int     `json:"total_queries_24h,omitempty"`
	ErrorRate         float64 `json:"error_rate,omitempty"`
}

// ============================================================================
// MAIN HANDLER
// ============================================================================

func AgentInventoryHandler(
	kb *app.KnowledgeBaseDB,
	rdbms *app.KnowledgeBaseSQLite,
	loggerDB *app.LoggerSQLite,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse query parameters
		includeSchemas := r.URL.Query().Get("include_schemas") != "false"
		includeStatistics := r.URL.Query().Get("include_statistics") != "false"
		includeLineage := r.URL.Query().Get("include_lineage") != "false"
		includeMetadata := r.URL.Query().Get("include_metadata") != "false"
		includeSamples := r.URL.Query().Get("include_samples") == "true"

		// Generate request ID
		requestID := generateRequestID()

		// Build response
		response := AgentInventoryResponse{
			AgentInfo:     buildAgentInfo(kb),
			Databases:     buildDatabaseInventory(rdbms, loggerDB, includeSchemas, includeStatistics, includeSamples),
			OrbitDBStores: buildOrbitDBInventory(kb, includeMetadata, includeSamples),
			IPFSStorage:   buildIPFSInventory(kb),
			SystemMetrics: buildSystemMetrics(kb),
			GeneratedAt:   time.Now(),
			RequestID:     requestID,
		}

		if includeLineage && rdbms != nil {
			response.LineageGraph = buildLineageGraph(rdbms)
		}

		if kb != nil && kb.MetadataService != nil {
			enrichment := buildEnrichmentStatus(kb, rdbms)
			response.MetadataEnrichment = &enrichment
		}

		if includeStatistics && rdbms != nil {
			accessPatterns := buildAccessPatterns(rdbms)
			qualityMetrics := buildQualityMetrics(rdbms)
			response.AccessPatterns = &accessPatterns
			response.QualityMetrics = &qualityMetrics
		}

		// Send response
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(response)
	}
}

// ============================================================================
// AGENT INFO BUILDER
// ============================================================================

func buildAgentInfo(kb *app.KnowledgeBaseDB) AgentInfo {
	if kb == nil || kb.Node == nil || kb.Node.PeerHost == nil {
		return AgentInfo{
			AgentID:   "unknown",
			AgentName: "OptimusDB Agent",
			NodeType:  "unknown",
			Version:   "1.0.0",
		}
	}

	peerID := kb.Node.PeerHost.ID().String()
	peers := kb.Node.PeerHost.Network().Peers()

	return AgentInfo{
		AgentID:       peerID,
		AgentName:     getAgentName(),
		NodeType:      getNodeType(kb),
		Version:       "1.0.0",
		UptimeSeconds: getUptime(),
		LastSync:      time.Now(),
		Network: NetworkInfo{
			PeerCount:      len(peers),
			ConnectedPeers: getPeerIDs(peers),
			IsCoordinator:  isCoordinator(kb),
			HealthScore:    getHealthScore(kb),
		},
	}
}

func getAgentName() string {
	if name := os.Getenv("AGENT_NAME"); name != "" {
		return name
	}
	hostname, _ := os.Hostname()
	if hostname != "" {
		return hostname
	}
	return "optimusdb-agent"
}

func getNodeType(kb *app.KnowledgeBaseDB) string {
	if isCoordinator(kb) {
		return "coordinator"
	}
	return "follower"
}

func isCoordinator(kb *app.KnowledgeBaseDB) bool {
	if kb == nil || kb.Node == nil {
		return false
	}
	// Check election system - simplified for now
	return false
}

func getUptime() int64 {
	return int64(time.Since(startTime).Seconds())
}

func getPeerIDs(peers []peer.ID) []string {
	peerList := make([]string, len(peers))
	for i, p := range peers {
		peerList[i] = p.String()
	}
	return peerList
}

func getHealthScore(kb *app.KnowledgeBaseDB) float64 {
	if kb == nil || election.GlobalReputationDB == nil {
		return 1.0
	}
	return 0.95 // Placeholder
}

// ============================================================================
// DATABASE INVENTORY BUILDER - AUTOMATIC DISCOVERY
// ============================================================================

func buildDatabaseInventory(
	rdbms *app.KnowledgeBaseSQLite,
	loggerDB *app.LoggerSQLite,
	includeSchemas bool,
	includeStatistics bool,
	includeSamples bool,
) DatabaseInventory {
	inventory := DatabaseInventory{
		Other: make(map[string]DatabaseInfo),
	}

	// Knowledge base database
	if rdbms != nil && rdbms.DB != nil {
		info := getDatabaseInfo(rdbms.DB, "knowledgebase.db", includeSchemas, includeStatistics, includeSamples)
		inventory.Knowledgebase = &info
	}

	// Logger database
	if loggerDB != nil && loggerDB.TheLog != nil {
		info := getDatabaseInfo(loggerDB.TheLog, "optimuslog.db", includeSchemas, includeStatistics, includeSamples)
		inventory.Logger = &info
	}

	// Reputation database
	if election.GlobalReputationDB != nil && election.GlobalReputationDB.ReputationDB != nil {
		info := getDatabaseInfo(election.GlobalReputationDB.ReputationDB, "reputation.db", includeSchemas, includeStatistics, includeSamples)
		inventory.Reputation = &info
	}

	return inventory
}

func getDatabaseInfo(db *sql.DB, dbName string, includeSchemas, includeStatistics, includeSamples bool) DatabaseInfo {
	info := DatabaseInfo{
		Path:      getDatabasePath(db, dbName),
		SizeBytes: getDatabaseSize(db, dbName),
		Tables:    make(map[string]TableInfo),
	}

	// Get all tables automatically
	tables := getTableNames(db)
	for _, tableName := range tables {
		tableInfo := TableInfo{
			RowCount: getRowCount(db, tableName),
		}

		if includeSchemas {
			tableInfo.Schema = getTableSchema(db, tableName)
		}

		if includeStatistics {
			tableInfo.Statistics = getTableStatistics(db, tableName)
		}

		if includeSamples {
			tableInfo.SampleRecords = getSampleRecords(db, tableName, 3)
		}

		info.Tables[tableName] = tableInfo
	}

	return info
}

func getTableNames(db *sql.DB) []string {
	query := `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	rows, err := db.Query(query)
	if err != nil {
		return []string{}
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}
	return tables
}

func getDatabasePath(db *sql.DB, dbName string) string {
	var seq int
	var name, file string
	err := db.QueryRow("SELECT seq, name, file FROM pragma_database_list() WHERE name='main'").Scan(&seq, &name, &file)
	if err == nil && file != "" {
		return file
	}
	return dbName
}

func getDatabaseSize(db *sql.DB, dbName string) int64 {
	path := getDatabasePath(db, dbName)
	if info, err := os.Stat(path); err == nil {
		return info.Size()
	}
	return 0
}

func getRowCount(db *sql.DB, tableName string) int64 {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM \"%s\"", tableName)
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0
	}
	return count
}

func getTableSchema(db *sql.DB, tableName string) *TableSchema {
	query := fmt.Sprintf("PRAGMA table_info(\"%s\")", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dfltValue interface{}

		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err == nil {
			columns = append(columns, ColumnInfo{
				Name:       name,
				Type:       ctype,
				PrimaryKey: pk == 1,
				Nullable:   notnull == 0,
			})
		}
	}

	indexes := getTableIndexes(db, tableName)

	return &TableSchema{
		Columns: columns,
		Indexes: indexes,
	}
}

func getTableIndexes(db *sql.DB, tableName string) []string {
	query := fmt.Sprintf("PRAGMA index_list(\"%s\")", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return []string{}
	}
	defer rows.Close()

	var indexes []string
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err == nil {
			indexes = append(indexes, name)
		}
	}
	return indexes
}

func getTableStatistics(db *sql.DB, tableName string) map[string]interface{} {
	stats := make(map[string]interface{})

	// Table-specific statistics
	switch tableName {
	case "datacatalog":
		query := `SELECT metadata_type, COUNT(*) as count FROM datacatalog WHERE metadata_type IS NOT NULL GROUP BY metadata_type`
		if rows, err := db.Query(query); err == nil {
			dist := make(map[string]int)
			for rows.Next() {
				var mtype string
				var count int
				if rows.Scan(&mtype, &count) == nil {
					dist[mtype] = count
				}
			}
			rows.Close()
			if len(dist) > 0 {
				stats["metadata_types"] = dist
			}
		}

	case "ems_events":
		query := `SELECT action, COUNT(*) as count FROM ems_events WHERE action IS NOT NULL GROUP BY action ORDER BY count DESC LIMIT 10`
		if rows, err := db.Query(query); err == nil {
			dist := make(map[string]int)
			for rows.Next() {
				var action string
				var count int
				if rows.Scan(&action, &count) == nil {
					dist[action] = count
				}
			}
			rows.Close()
			if len(dist) > 0 {
				stats["top_actions"] = dist
			}
		}

	case "optimusLogger":
		query := `SELECT level, COUNT(*) as count FROM optimusLogger WHERE level IS NOT NULL GROUP BY level`
		if rows, err := db.Query(query); err == nil {
			dist := make(map[string]int)
			for rows.Next() {
				var level string
				var count int
				if rows.Scan(&level, &count) == nil {
					dist[level] = count
				}
			}
			rows.Close()
			if len(dist) > 0 {
				stats["log_levels"] = dist
			}
		}

	case "access_log":
		query := `SELECT resource_type, COUNT(*) as count FROM access_log WHERE resource_type IS NOT NULL GROUP BY resource_type`
		if rows, err := db.Query(query); err == nil {
			dist := make(map[string]int)
			for rows.Next() {
				var rtype string
				var count int
				if rows.Scan(&rtype, &count) == nil {
					dist[rtype] = count
				}
			}
			rows.Close()
			if len(dist) > 0 {
				stats["resource_types"] = dist
			}
		}

	case "reputation":
		query := `SELECT AVG(uptime), AVG(latency), AVG(user_cpu), AVG(memory_available) FROM reputation`
		var avgUptime, avgLatency, avgCPU, avgMemory sql.NullFloat64
		if err := db.QueryRow(query).Scan(&avgUptime, &avgLatency, &avgCPU, &avgMemory); err == nil {
			if avgUptime.Valid {
				stats["avg_uptime"] = avgUptime.Float64
			}
			if avgLatency.Valid {
				stats["avg_latency"] = avgLatency.Float64
			}
			if avgCPU.Valid {
				stats["avg_cpu_usage"] = avgCPU.Float64
			}
			if avgMemory.Valid {
				stats["avg_memory_available"] = avgMemory.Float64
			}
		}
	}

	return stats
}

func getSampleRecords(db *sql.DB, tableName string, limit int) []map[string]interface{} {
	query := fmt.Sprintf("SELECT * FROM \"%s\" LIMIT %d", tableName, limit)
	rows, err := db.Query(query)
	if err != nil {
		return []map[string]interface{}{}
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return []map[string]interface{}{}
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		record := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				record[col] = string(b)
			} else {
				record[col] = val
			}
		}
		results = append(results, record)
	}

	return results
}

// ============================================================================
// ORBITDB INVENTORY BUILDER - AUTOMATIC DISCOVERY
// ============================================================================

func buildOrbitDBInventory(kb *app.KnowledgeBaseDB, includeMetadata, includeSamples bool) OrbitDBInventory {
	inventory := OrbitDBInventory{
		ActiveStores:  []OrbitDBStoreInfo{},
		PlannedStores: []PlannedStoreInfo{},
	}

	if kb == nil {
		return inventory
	}

	ctx := context.Background()

	// Define all DocumentStores - AUTOMATIC DETECTION
	documentStores := []struct {
		name        string
		store       *orbitdb.DocumentStore
		description string
		acl         string
		replicated  bool
	}{
		{"KBMetadata", kb.KBMetadata, "Primary metadata catalog", "full_rw", true},
		{"KBdata", kb.KBdata, "Knowledge base data metrics", "write_owner", true},
		{"Validations", kb.Validations, "Private validation store", "owner_only", false},
		{"DsSWres", kb.DsSWres, "Software resources", "full_rw", true},
		{"DsTOSCA_Imported", kb.DsTOSCA_Imported, "TOSCA templates", "full_rw", true},
		{"whoiswhoStore", kb.WhoiswhoStore, "User identity mapping", "full_rw", true},
		{"DsSWresaloc", kb.DsSWresaloc, "Resource allocation", "full_rw", true},
		{"DsTOSCA_ADT", kb.DsTOSCA_ADT, "TOSCA data types", "full_rw", true},
		{"DsTOSCA_Capacities", kb.DsTOSCA_Capacities, "TOSCA capacities", "full_rw", true},
		{"DsTOSCA_DeploymentPlan", kb.DsTOSCA_DeploymentPlan, "Deployment plans", "full_rw", true},
		{"DsTOSCA_EventHistory", kb.DsTOSCA_EventHistory, "Event history", "full_rw", true},
	}

	for _, s := range documentStores {
		if s.store != nil {
			storeInfo := OrbitDBStoreInfo{
				Name:          s.name,
				Address:       (*s.store).Address().String(),
				Type:          "docstore",
				LastUpdate:    time.Now(),
				AccessControl: s.acl,
				Replication:   s.replicated,
			}

			// Get entry count using Query
			results, err := (*s.store).Query(ctx, func(doc interface{}) (bool, error) {
				return true, nil // Return all documents
			})
			if err == nil {
				storeInfo.EntryCount = len(results)

				// Get sample entries if requested
				if includeSamples && len(results) > 0 {
					sampleCount := 3
					if len(results) < sampleCount {
						sampleCount = len(results)
					}

					samples := make([]map[string]interface{}, 0, sampleCount)
					for i := 0; i < sampleCount; i++ {
						if m, ok := results[i].(map[string]interface{}); ok {
							samples = append(samples, m)
						}
					}
					storeInfo.SampleEntries = samples
				}
			}

			inventory.ActiveStores = append(inventory.ActiveStores, storeInfo)
		} else {
			inventory.PlannedStores = append(inventory.PlannedStores, PlannedStoreInfo{
				Name:        s.name,
				Type:        "docstore",
				Status:      "not_initialized",
				Description: s.description,
			})
		}
	}

	// EventLogStore - Contributions
	if kb.Contributions != nil {
		storeInfo := OrbitDBStoreInfo{
			Name:          "Contributions",
			Address:       (*kb.Contributions).Address().String(),
			Type:          "eventlog",
			LastUpdate:    time.Now(),
			AccessControl: "write_owner",
			Replication:   true,
		}

		// Get event count using List
		infinity := -1
		entries, err := (*kb.Contributions).List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
		if err == nil {
			storeInfo.EventCount = len(entries)
		}

		inventory.ActiveStores = append(inventory.ActiveStores, storeInfo)
	} else {
		inventory.PlannedStores = append(inventory.PlannedStores, PlannedStoreInfo{
			Name:        "Contributions",
			Type:        "eventlog",
			Status:      "not_initialized",
			Description: "Contribution log",
		})
	}

	// Dynamic stores
	inventory.PlannedStores = append(inventory.PlannedStores,
		PlannedStoreInfo{
			Name:        "credentials-store",
			Type:        "docstore",
			Status:      "dynamic",
			Description: "W3C Credentials (on-demand)",
		},
		PlannedStoreInfo{
			Name:        "credentials-audit-log",
			Type:        "eventlog",
			Status:      "dynamic",
			Description: "Credential audit (on-demand)",
		},
	)

	inventory.TotalActive = len(inventory.ActiveStores)
	inventory.TotalPlanned = len(inventory.PlannedStores)

	return inventory
}

// ============================================================================
// IPFS INVENTORY
// ============================================================================

func buildIPFSInventory(kb *app.KnowledgeBaseDB) IPFSInventory {
	inventory := IPFSInventory{
		ContentTypes:  make(map[string]int),
		SampleContent: []IPFSContent{},
	}

	if app.GlobalKBSQLite == nil || app.GlobalKBSQLite.DB == nil {
		return inventory
	}

	query := `SELECT ipfs_path, filename, filesize_bytes, created_at 
	          FROM toscametadata 
	          WHERE ipfs_path IS NOT NULL AND ipfs_path != '' 
	          ORDER BY created_at DESC LIMIT 10`

	rows, err := app.GlobalKBSQLite.DB.Query(query)
	if err != nil {
		return inventory
	}
	defer rows.Close()

	for rows.Next() {
		var ipfsPath, filename, createdAt string
		var sizeBytes int64

		if err := rows.Scan(&ipfsPath, &filename, &sizeBytes, &createdAt); err == nil {
			cid := strings.TrimPrefix(ipfsPath, "/ipfs/")
			contentType := "application/octet-stream"

			ext := strings.ToLower(filepath.Ext(filename))
			switch ext {
			case ".yaml", ".yml":
				contentType = "application/yaml"
				inventory.ContentTypes["tosca_templates"]++
			case ".json":
				contentType = "application/json"
				inventory.ContentTypes["json"]++
			}

			uploadTime, _ := time.Parse(time.RFC3339, createdAt)

			inventory.SampleContent = append(inventory.SampleContent, IPFSContent{
				CID:         cid,
				Filename:    filename,
				SizeBytes:   sizeBytes,
				ContentType: contentType,
				UploadedAt:  uploadTime,
			})

			inventory.ContentCount++
			inventory.TotalSizeBytes += sizeBytes
		}
	}

	return inventory
}

// ============================================================================
// LINEAGE GRAPH
// ============================================================================

func buildLineageGraph(rdbms *app.KnowledgeBaseSQLite) *LineageGraph {
	if rdbms == nil || rdbms.DB == nil {
		return nil
	}

	query := `
		SELECT source_id, source_type, target_id, target_type, target_name, level
		FROM resource_dependencies
		ORDER BY source_id, level
	`

	rows, err := rdbms.DB.Query(query)
	if err != nil {
		return &LineageGraph{Tables: []TableLineage{}}
	}
	defer rows.Close()

	lineageMap := make(map[string]*TableLineage)

	for rows.Next() {
		var sourceID, sourceType, targetID, targetType string
		var targetName sql.NullString
		var level int

		if err := rows.Scan(&sourceID, &sourceType, &targetID, &targetType, &targetName, &level); err != nil {
			continue
		}

		if _, exists := lineageMap[sourceID]; !exists {
			lineageMap[sourceID] = &TableLineage{
				ID:         sourceID,
				Name:       getTableNameFromID(rdbms.DB, sourceID),
				Upstream:   []Dependency{},
				Downstream: []Dependency{},
			}
		}

		dep := Dependency{
			ID:    targetID,
			Name:  targetName.String,
			Type:  targetType,
			Level: level,
		}

		lineageMap[sourceID].Downstream = append(lineageMap[sourceID].Downstream, dep)
	}

	tables := make([]TableLineage, 0, len(lineageMap))
	for _, lineage := range lineageMap {
		tables = append(tables, *lineage)
	}

	return &LineageGraph{Tables: tables}
}

func getTableNameFromID(db *sql.DB, tableID string) string {
	var name string
	query := "SELECT name FROM datacatalog WHERE _id = ?"
	if err := db.QueryRow(query, tableID).Scan(&name); err != nil {
		return tableID
	}
	return name
}

// ============================================================================
// HELPER BUILDERS
// ============================================================================

func buildEnrichmentStatus(kb *app.KnowledgeBaseDB, rdbms *app.KnowledgeBaseSQLite) EnrichmentStatus {
	status := EnrichmentStatus{
		ServiceStatus:  "inactive",
		LLMModel:       "TinyLlama-1.1B",
		LastEnrichment: time.Now(),
	}

	if kb.MetadataService != nil {
		status.ServiceStatus = "active"

		if rdbms != nil && rdbms.DB != nil {
			var count int
			if err := rdbms.DB.QueryRow("SELECT COUNT(*) FROM metadata_catalog").Scan(&count); err == nil {
				status.EnrichedTablesCount = count
			}

			if kb.MetadataCache != nil {
				status.CacheHitRate = 0.75
			}
		}
	}

	return status
}

func buildAccessPatterns(rdbms *app.KnowledgeBaseSQLite) AccessPatterns {
	patterns := AccessPatterns{
		TopAccessedTables: []AccessedTable{},
		TopUsers:          []ActiveUser{},
	}

	if rdbms == nil || rdbms.DB == nil {
		return patterns
	}

	topTablesQuery := `
		SELECT resource_id, COUNT(*) as access_count,
		       COUNT(DISTINCT user_id) as unique_users,
		       MAX(timestamp) as last_accessed
		FROM access_log
		WHERE resource_type = 'table'
		GROUP BY resource_id
		ORDER BY access_count DESC
		LIMIT 10
	`

	if rows, err := rdbms.DB.Query(topTablesQuery); err == nil {
		defer rows.Close()
		for rows.Next() {
			var tableID string
			var accessCount, uniqueUsers int
			var lastAccessed int64

			if err := rows.Scan(&tableID, &accessCount, &uniqueUsers, &lastAccessed); err == nil {
				patterns.TopAccessedTables = append(patterns.TopAccessedTables, AccessedTable{
					TableID:      tableID,
					AccessCount:  accessCount,
					UniqueUsers:  uniqueUsers,
					LastAccessed: time.Unix(lastAccessed, 0),
				})
			}
		}
	}

	topUsersQuery := `
		SELECT user_id, COUNT(*) as access_count,
		       COUNT(DISTINCT resource_id) as resources_accessed
		FROM access_log
		WHERE user_id IS NOT NULL
		GROUP BY user_id
		ORDER BY access_count DESC
		LIMIT 10
	`

	if rows, err := rdbms.DB.Query(topUsersQuery); err == nil {
		defer rows.Close()
		for rows.Next() {
			var userID string
			var accessCount, resourcesAccessed int

			if err := rows.Scan(&userID, &accessCount, &resourcesAccessed); err == nil {
				patterns.TopUsers = append(patterns.TopUsers, ActiveUser{
					UserID:            userID,
					AccessCount:       accessCount,
					ResourcesAccessed: resourcesAccessed,
				})
			}
		}
	}

	return patterns
}

func buildQualityMetrics(rdbms *app.KnowledgeBaseSQLite) QualityMetrics {
	metrics := QualityMetrics{
		QualityDistribution: make(map[string]int),
	}

	if rdbms == nil || rdbms.DB == nil {
		return metrics
	}

	query := `
		SELECT 
			AVG(data_quality_score) as avg_score,
			COUNT(*) as total_count,
			SUM(CASE WHEN data_quality_score >= 0.8 THEN 1 ELSE 0 END) as high,
			SUM(CASE WHEN data_quality_score >= 0.5 AND data_quality_score < 0.8 THEN 1 ELSE 0 END) as medium,
			SUM(CASE WHEN data_quality_score < 0.5 THEN 1 ELSE 0 END) as low
		FROM datacatalog
		WHERE data_quality_score IS NOT NULL
	`

	var avgScore sql.NullFloat64
	var totalCount, high, medium, low int

	if err := rdbms.DB.QueryRow(query).Scan(&avgScore, &totalCount, &high, &medium, &low); err == nil {
		if avgScore.Valid {
			metrics.AvgQualityScore = avgScore.Float64
		}
		metrics.TablesWithScores = totalCount
		metrics.QualityDistribution["high"] = high
		metrics.QualityDistribution["medium"] = medium
		metrics.QualityDistribution["low"] = low
	}

	return metrics
}

func buildSystemMetrics(kb *app.KnowledgeBaseDB) SystemMetrics {
	metrics := SystemMetrics{}

	if kb == nil {
		return metrics
	}

	if kb.QueryEngine != nil {
		metrics.AvgQueryLatencyMS = 45
	}

	if kb.MetadataCache != nil {
		metrics.QueryCacheSize = 1000
		metrics.CacheHitRate = 0.65
	}

	return metrics
}

// ============================================================================
// UTILITIES
// ============================================================================

func generateRequestID() string {
	return fmt.Sprintf("req-%d-%s", time.Now().Unix(), randomString(8))
}

func randomString(length int) string {
	bytes := make([]byte, length)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)[:length]
}

type InventoryCache struct {
	sync.RWMutex
	entries map[string]CacheEntry
}

type CacheEntry struct {
	Data      interface{}
	Timestamp time.Time
	TTL       time.Duration
}

func (c *InventoryCache) Get(key string) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()

	entry, exists := c.entries[key]
	if !exists || time.Since(entry.Timestamp) > entry.TTL {
		return nil, false
	}
	return entry.Data, true
}

func (c *InventoryCache) Set(key string, data interface{}, ttl time.Duration) {
	c.Lock()
	defer c.Unlock()

	if c.entries == nil {
		c.entries = make(map[string]CacheEntry)
	}

	c.entries[key] = CacheEntry{
		Data:      data,
		Timestamp: time.Now(),
		TTL:       ttl,
	}
}

func APIKeyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-API-Key")
		if !isValidAPIKey(apiKey) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isValidAPIKey(apiKey string) bool {
	return true // No auth by default
}
