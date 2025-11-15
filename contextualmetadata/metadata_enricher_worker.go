package contextualmetadata

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"optimusdb/app"
)

// MetadataEnricher automatically discovers and enriches datasets
type MetadataEnricher struct {
	Service     *Service
	KB          *app.KnowledgeBaseDB
	Cache       *MetadataCache
	Interval    time.Duration
	DBPaths     []string // Paths to SQLite databases to monitor
	stopCh      chan struct{}
	isRunning   bool
	enrichCount int64
}

// NewMetadataEnricher creates a new background enricher
func NewMetadataEnricher(
	service *Service,
	kb *app.KnowledgeBaseDB,
	cache *MetadataCache,
	dbPaths []string,
) *MetadataEnricher {
	return &MetadataEnricher{
		Service:  service,
		KB:       kb,
		Cache:    cache,
		Interval: 1 * time.Hour, // Default: scan every hour
		DBPaths:  dbPaths,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the background enrichment process
func (me *MetadataEnricher) Start() {
	if me.isRunning {
		log.Println("⚠️  Metadata enricher already running")
		return
	}

	me.isRunning = true
	log.Println("🚀 Starting metadata enricher worker...")

	go me.run()
}

// Stop gracefully stops the enricher
func (me *MetadataEnricher) Stop() {
	if !me.isRunning {
		return
	}

	log.Println("🛑 Stopping metadata enricher...")
	close(me.stopCh)
	me.isRunning = false
}

// run is the main worker loop
func (me *MetadataEnricher) run() {
	ticker := time.NewTicker(me.Interval)
	defer ticker.Stop()

	// Initial enrichment on startup
	log.Println("🔄 Running initial metadata enrichment...")
	me.enrichNewTables()

	for {
		select {
		case <-ticker.C:
			me.enrichNewTables()
		case <-me.stopCh:
			log.Println("✅ Metadata enricher stopped")
			return
		}
	}
}

// enrichNewTables discovers and enriches new tables
func (me *MetadataEnricher) enrichNewTables() {
	log.Println("🔍 Scanning for tables to enrich...")

	startTime := time.Now()
	datasets, err := me.discoverDatasets()
	if err != nil {
		log.Printf("❌ Error discovering datasets: %v", err)
		return
	}

	if len(datasets) == 0 {
		log.Println("ℹ️  No new tables found")
		return
	}

	log.Printf("📊 Found %d tables to potentially enrich", len(datasets))

	enriched := 0
	skipped := 0
	failed := 0

	for _, ds := range datasets {
		// Check if already enriched (in cache)
		if _, found := me.Cache.Get(ds.DB, ds.Table); found {
			skipped++
			continue
		}

		log.Printf("📝 Enriching %s.%s...", ds.DB, ds.Table)

		enrichStart := time.Now()
		metadata, err := me.Service.EnrichDataset(
			context.Background(),
			me.KB,
			ds.DB,
			ds.Table,
			200, // Sample 200 rows
		)
		enrichDuration := time.Since(enrichStart)

		if err != nil {
			log.Printf("❌ Error enriching %s.%s: %v", ds.DB, ds.Table, err)
			failed++
			continue
		}

		// Cache the result
		me.Cache.Set(ds.DB, ds.Table, metadata)
		enriched++
		me.enrichCount++

		log.Printf("✅ Enriched %s.%s in %v (domain: %v)",
			ds.DB, ds.Table, enrichDuration, metadata["domain"])
	}

	totalDuration := time.Since(startTime)

	log.Printf("✨ Enrichment cycle complete in %v: enriched=%d, skipped=%d, failed=%d, total=%d",
		totalDuration, enriched, skipped, failed, me.enrichCount)
}

// discoverDatasets scans databases for tables
func (me *MetadataEnricher) discoverDatasets() ([]DatasetInfo, error) {
	var allDatasets []DatasetInfo

	for _, dbPath := range me.DBPaths {
		datasets, err := me.scanDatabase(dbPath)
		if err != nil {
			log.Printf("⚠️  Error scanning %s: %v", dbPath, err)
			continue
		}
		allDatasets = append(allDatasets, datasets...)
	}

	return allDatasets, nil
}

// scanDatabase scans a single database for tables
func (me *MetadataEnricher) scanDatabase(dbPath string) ([]DatasetInfo, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database %s: %w", dbPath, err)
	}
	defer db.Close()

	// Get all user tables (excluding SQLite internal tables)
	query := `
		SELECT name 
		FROM sqlite_master 
		WHERE type='table' 
		  AND name NOT LIKE 'sqlite_%'
		  AND name NOT LIKE '_orbit_%'
		ORDER BY name
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var datasets []DatasetInfo
	dbName := filepath.Base(dbPath)

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}

		datasets = append(datasets, DatasetInfo{
			DB:    dbName,
			Table: tableName,
		})
	}

	return datasets, nil
}

// GetStats returns enrichment statistics
func (me *MetadataEnricher) GetStats() map[string]any {
	return map[string]any{
		"is_running":     me.isRunning,
		"total_enriched": me.enrichCount,
		"interval":       me.Interval.String(),
		"monitored_dbs":  len(me.DBPaths),
	}
}

// SetInterval changes the scan interval
func (me *MetadataEnricher) SetInterval(interval time.Duration) {
	me.Interval = interval
	log.Printf("🔧 Metadata enricher interval set to %v", interval)
}

// EnrichNow triggers immediate enrichment
func (me *MetadataEnricher) EnrichNow() {
	log.Println("⚡ Manual enrichment triggered")
	go me.enrichNewTables()
}
