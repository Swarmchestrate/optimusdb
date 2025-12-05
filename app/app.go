package app

import (
	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/iface"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ipfs/kubo/core"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"optimusdb/config"
	"optimusdb/logger"
	"optimusdb/mq"
	"optimusdb/queryengine"

	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var GlobalKBSQLite *KnowledgeBaseSQLite
var GlobalLoggerDB *LoggerSQLite

// Narrow ports so app doesn't depend on contextualmetadata
type MetadataCachePort interface {
	Get(db, table string) (map[string]any, bool)
	Set(db, table string, result map[string]any)
}

// If app never calls methods on the service, keep it opaque.
// If you later need methods, define them here.
type MetadataServicePort interface{}

// KnowledgeBaseDB we will try to connect to on startup
// represents the application across go routines
type KnowledgeBaseDB struct {
	// data storage
	Node          *core.IpfsNode         // TODO : only because of node.PeerHost.EventBus
	Contributions *orbitdb.EventLogStore // the log which holds all contributions
	Validations   *orbitdb.DocumentStore // the store which holds all validations
	KBdata        *orbitdb.DocumentStore // the store which holds data
	//CRDTs: For conflict-free data synchronization.
	KBMetadata    *orbitdb.DocumentStore // the store which holds metadata
	whoiswhoStore *orbitdb.DocumentStore // the store which holds data
	DsSWres       *orbitdb.DocumentStore // the store which holds data
	DsSWresaloc   *orbitdb.DocumentStore // the store which holds metadata
	// TOSCA specific datastores
	DsTOSCA_ADT            *orbitdb.DocumentStore
	DsTOSCA_Imported       *orbitdb.DocumentStore
	DsTOSCA_Capacities     *orbitdb.DocumentStore
	DsTOSCA_DeploymentPlan *orbitdb.DocumentStore
	DsTOSCA_EventHistory   *orbitdb.DocumentStore

	Orbit *iface.OrbitDB
	// mutex to control access to the eventlog db across go routines
	ContributionsMtx sync.RWMutex
	ValidationsMtx   sync.RWMutex
	// persisted config
	Config *config.Config
	// benchmarks
	Benchmark *Benchmark

	// Add below:
	discoveredPeers map[string]bool
	peersMutex      sync.Mutex

	//For EMS
	MQEMS  *mq.Client
	HostID string
	// for the watchdog service
	EMSClient  *mq.ReconnectingClient
	EMSService *mq.EMSService

	// Query engine
	QueryEngine *queryengine.OptimizedEngine

	// Add these for GossipSub
	PubSub        *pubsub.PubSub
	ElectionTopic *pubsub.Topic
	ElectionSub   *pubsub.Subscription
	// Metadata enrichment
	MetadataService MetadataServicePort
	MetadataCache   MetadataCachePort
}

// ============================================================================
// METADATA SERVICE INTERFACE METHODS
// ============================================================================
// These methods implement the KnowledgeBasePort interface required by
// contextualmetadata.InitializeMetadataService()

// SetMetadataService stores the metadata service in the knowledge base
func (kb *KnowledgeBaseDB) SetMetadataService(svc interface{}) {
	kb.MetadataService = svc.(MetadataServicePort)
}

// SetMetadataCache stores the metadata cache in the knowledge base
func (kb *KnowledgeBaseDB) SetMetadataCache(cache interface{}) {
	kb.MetadataCache = cache.(MetadataCachePort)
}

// GetMetadataCache returns the metadata cache from the knowledge base
func (kb *KnowledgeBaseDB) GetMetadataCache() interface{} {
	return kb.MetadataCache
}

// /** this is the struct for the SQL
//type KnowledgeBaseRDBMS struct {
//	Session *engine.Session
//}

type EMSMessage struct {
	Action   string                 `json:"action"`
	Resource string                 `json:"resource"`
	Params   map[string]interface{} `json:"params"`
}

type LogType uint8

const (
	RecoverableErr    LogType = 0
	NonRecoverableErr LogType = 1
	Info              LogType = 2
	Print             LogType = 3
)

type Log struct {
	Type LogType
	Data interface{}
}

// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// KnowledgeBaseSQLite manages SQLite connection
type KnowledgeBaseSQLite struct {
	DB *sql.DB
}

type LoggerSQLite struct {
	theLog *sql.DB
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//	SQLite instantiation
//
// InitSQLite initializes the SQLite database and ensures tables exist
func InitSQLite(dbPath string) (*KnowledgeBaseSQLite, error) {

	logger.Info("[INFO] Initializing RDBMS KnowledgeBase : %v", dbPath)
	//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Initializing RDBMS KnowledgeBase : %v"), runtime.GOOS)
	// Open SQLite Database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		logger.Error("[ERROR] Failed to connect to SQLite database: %v", err)
		//GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Failed to connect to SQLite database: %v", err), runtime.GOOS)
		return nil, err
	}

	// Create the KnowledgeBaseSQLite instance
	GlobalKBSQLite = &KnowledgeBaseSQLite{DB: db}

	// Create tables
	err = GlobalKBSQLite.createDataCatalog()
	if err != nil {
		logger.Error("[ERROR] Main Table creation failed for DataCatalog: %v", err)
		//GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Main Table creation failed for DataCatalog: %v", err), runtime.GOOS)
		return nil, err
	}
	err = GlobalKBSQLite.createDataCatalogSchemas()
	if err != nil {
		logger.Error("[ERROR] Tables creation failed for DataCatalog: %v", err)
		//GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Tables creation failed for DataCatalog: %v", err), runtime.GOOS)
		return nil, err
	}
	err = GlobalKBSQLite.createTOSCAMetadataTable()
	if err != nil {
		logger.Error("[ERROR] Table creation failed for TOSCA Metadata: %v", err)
		//GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Table creation failed for TOSCA Metadata: %v", err), runtime.GOOS)
		return nil, err
	}
	err = GlobalKBSQLite.CreateMetadataCatalogTable()
	if err != nil {
		logger.Error("[ERROR] Table creation failed for Contextual Metadata: %v", err)
		//GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Table creation failed for Contextual Metadata: %v", err), runtime.GOOS)
		return nil, err
	}

	logger.Info("[INFO] SQLite Database Ready at:", dbPath)
	//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("SQLite Database Ready at: %v", dbPath), runtime.GOOS)
	return GlobalKBSQLite, nil
}

// InitSQLite initializes the SQLite database and ensures tables exist
func InitLog() (*LoggerSQLite, error) {

	rdbmsCache := filepath.Join(filepath.Join(filepath.Join(os.Getenv("HOME"), ".cache"), "optimusdb", *config.FlagRepo, "optimusdb"), "optimuslog.db")
	dir := filepath.Dir(rdbmsCache)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for DB: %w", err)
	}

	//log.Printf("[INFO] Initializing RDBMS Logger : %v\n", rdbmsCache)
	//AddToOptimusLog("INFO", fmt.Sprintf("Initializing RDBMS Logger: %v", rdbmsCache), runtime.GOOS)
	// Open SQLite Database
	db, err := sql.Open("sqlite3", rdbmsCache)
	if err != nil {
		logger.Error("[ERROR] Failed to connect to SQLite database: %v", err)
		return nil, err
	}

	// Create the KnowledgeBaseSQLite instance
	GlobalLoggerDB = &LoggerSQLite{theLog: db}

	err = GlobalLoggerDB.createLogTable()
	if err != nil {
		logger.Error("[ERROR] Table creation failed for Optimus Logger: %v", err)
		return nil, err
	}

	//Create the EMS table events
	err = GlobalLoggerDB.createEMSEventsTable() // EMS
	if err != nil {
		logger.Error("[ERROR] Table creation failed for EMS events under the Optimus Logger: %v", err)
		return nil, err
	}

	//
	logger.Info("[INFO] SQLite Database Ready at:", rdbmsCache)
	//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("SQLite Database Ready at: %v", rdbmsCache), runtime.GOOS)
	return GlobalLoggerDB, nil
}

// createTables ensures the `datacatalog` table exists
func (kb *KnowledgeBaseSQLite) createDataCatalog() error {
	tableQuery :=
		`CREATE TABLE IF NOT EXISTS datacatalog (
		_id VARCHAR(36) PRIMARY KEY,
		author VARCHAR(255),
		metadata_type VARCHAR(255),
		component VARCHAR(255),
		behaviour VARCHAR(255),
		relationships TEXT,
		associated_id VARCHAR(36),
		name VARCHAR(255),
		description TEXT,
		tags VARCHAR(255),
		status VARCHAR(50),
		created_by VARCHAR(255),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		related_ids VARCHAR(255),
		priority VARCHAR(50),
		scheduling_info VARCHAR(255),
		sla_constraints VARCHAR(255),
		ownership_details VARCHAR(255),
		audit_trail VARCHAR(255)
	);`
	_, err := kb.DB.Exec(tableQuery)
	if err != nil {
		return err
	}
	logger.Info("[INFO] Table `datacatalog` created or already exists.")
	//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Table `datacatalog` created or already exists."), runtime.GOOS)
	return nil
}

func (kb *KnowledgeBaseSQLite) CreateMetadataCatalogTable() error {
	tableQuery := `
        CREATE TABLE IF NOT EXISTS metadata_catalog (
            id TEXT PRIMARY KEY,
            metadata_type TEXT,
            component TEXT,
            behaviour TEXT,
            description TEXT,
            created_by TEXT,
            created_at TEXT,
            updated_at TEXT,
            name TEXT,
            tags TEXT,
            associated_id TEXT,
            status TEXT,
            priority TEXT,
            relationships TEXT,
            related_ids TEXT,
            scheduling_info TEXT,
            sla_constraints TEXT,
            ownership_details TEXT,
            audit_trail TEXT
        );
        
        -- Add indexes for better query performance
        CREATE INDEX IF NOT EXISTS idx_metadata_type 
            ON metadata_catalog(metadata_type);
        
        CREATE INDEX IF NOT EXISTS idx_metadata_created_at 
            ON metadata_catalog(created_at);
        
        CREATE INDEX IF NOT EXISTS idx_metadata_associated_id 
            ON metadata_catalog(associated_id);
        
        CREATE INDEX IF NOT EXISTS idx_metadata_status 
            ON metadata_catalog(status);
    `
	_, err := kb.DB.Exec(tableQuery)
	if err != nil {
		return err
	}
	logger.Info("[INFO] Table `metadata_catalog` created or already exists.")
	//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Table `metadata_catalog` created or already exists."), runtime.GOOS)
	return nil
}

// createTables ensures the `datacatalog` table exists
func (kb *KnowledgeBaseSQLite) createTOSCAMetadataTable() error {
	tableQuery :=
		`CREATE TABLE IF NOT EXISTS toscametadata (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			template_id TEXT NOT NULL UNIQUE,
			description TEXT,
			node_templates_count INTEGER,
			created_at TEXT,
			-- New metadata
			filename             TEXT,           -- original filename uploaded
			filesize_bytes       INTEGER,        -- file size if known
			content_sha256       TEXT,           -- hash of the uploaded content
			ipfs_path            TEXT,           -- /ipfs/<cid> if stored in IPFS
			uploader             TEXT,           -- free-form (user/agent)
			source_pod           TEXT,           -- k8s pod that processed the upload
			source_ip            TEXT            -- client or node IP
		);`
	_, err := kb.DB.Exec(tableQuery)
	if err != nil {
		return err
	}
	logger.Info("[INFO] Table `toscametadata` created or already exists.")
	//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Table `toscametadata` created or already exists"), runtime.GOOS)

	return nil
}

// createTables ensures the `datacatalog` table exists
func (kb *LoggerSQLite) createLogTable() error {
	tableQuery :=
		`
				CREATE TABLE IF NOT EXISTS optimusLogger (
					id INTEGER PRIMARY KEY,
					timestamp TEXT,
					date TEXT,
					hour TEXT,
					level TEXT,
					message TEXT,
					source TEXT
				);
				CREATE INDEX IF NOT EXISTS idx_logs_date_hour ON optimusLogger(date, hour);
			`

	_, err := kb.theLog.Exec(tableQuery)
	if err != nil {
		return err
	}
	logger.Info("[INFO] Table `optimusLogger` created or already exists.")
	//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Table `optimusLogger` created or already exists."), runtime.GOOS)
	return nil
}

func (kb *KnowledgeBaseSQLite) createDataCatalogSchemas() error {
	// Combined SQL schema for Amundsen integration
	stmt := `
		-- ============================================================================
		-- DATACATALOG TABLE (Core Metadata Storage)
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS datacatalog (
			_id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			metadata_type TEXT NOT NULL,
			description TEXT,
			component TEXT,
			created_by TEXT,
			author TEXT,
			tags TEXT,
			badges TEXT,
			priority TEXT,
			status TEXT,
			environment TEXT,
			version TEXT,
			columns TEXT,
			lineage_upstream TEXT,
			lineage_downstream TEXT,
			owners TEXT,
			ownership_details TEXT,
			data_quality_score REAL,
			last_quality_check INTEGER,
			compliance_level TEXT,
			access_count INTEGER DEFAULT 0,
			last_accessed INTEGER,
			row_count INTEGER,
			size_bytes INTEGER,
			statistics TEXT,
			table_type TEXT,
			refresh_frequency TEXT,
			refresh_schedule TEXT,
			storage_location TEXT,
			partition_key TEXT,
			sort_key TEXT,
			documentation_url TEXT,
			wiki_url TEXT,
			slack_channel TEXT,
			related_tables TEXT,
			associated_id TEXT,
			related_ids TEXT,
			scheduling_info TEXT,
			sla_constraints TEXT,
			behaviour TEXT,
			generation_code TEXT,
			transformation_logic TEXT,
			created_at INTEGER DEFAULT (strftime('%s', 'now')),
			updated_at INTEGER DEFAULT (strftime('%s', 'now')),
			search_vector TEXT
		);
		
		CREATE INDEX IF NOT EXISTS idx_datacatalog_name ON datacatalog(name);
		CREATE INDEX IF NOT EXISTS idx_datacatalog_metadata_type ON datacatalog(metadata_type);
		CREATE INDEX IF NOT EXISTS idx_datacatalog_component ON datacatalog(component);
		CREATE INDEX IF NOT EXISTS idx_datacatalog_created_by ON datacatalog(created_by);
		CREATE INDEX IF NOT EXISTS idx_datacatalog_status ON datacatalog(status);
		CREATE INDEX IF NOT EXISTS idx_datacatalog_updated_at ON datacatalog(updated_at);
		
		-- ============================================================================
		-- USERS TABLE
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS users (
			_id TEXT PRIMARY KEY,
			user_id TEXT UNIQUE NOT NULL,
			email TEXT NOT NULL,
			display_name TEXT,
			first_name TEXT,
			last_name TEXT,
			profile_url TEXT,
			github_username TEXT,
			slack_id TEXT,
			team_name TEXT,
			department TEXT,
			role_name TEXT,
			employee_type TEXT,
			manager_id TEXT,
			manager_email TEXT,
			is_active INTEGER DEFAULT 1,
			created_at INTEGER DEFAULT (strftime('%s', 'now')),
			updated_at INTEGER DEFAULT (strftime('%s', 'now')),
			last_login INTEGER
		);
		
		CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);
		CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
		CREATE INDEX IF NOT EXISTS idx_users_team_name ON users(team_name);
		CREATE INDEX IF NOT EXISTS idx_users_is_active ON users(is_active);
		
		-- ============================================================================
		-- DASHBOARDS TABLE
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS dashboards (
			_id TEXT PRIMARY KEY,
			dashboard_id TEXT UNIQUE NOT NULL,
			name TEXT NOT NULL,
			url TEXT,
			description TEXT,
			group_name TEXT,
			group_url TEXT,
			product TEXT,
			cluster TEXT DEFAULT 'default',
			created_by TEXT,
			owners TEXT,
			tags TEXT,
			badges TEXT,
			view_count INTEGER DEFAULT 0,
			last_viewed INTEGER,
			created_timestamp INTEGER DEFAULT (strftime('%s', 'now')),
			updated_timestamp INTEGER DEFAULT (strftime('%s', 'now')),
			last_run INTEGER,
			dashboard_type TEXT,
			refresh_interval TEXT,
			data_sources TEXT
		);
		
		CREATE INDEX IF NOT EXISTS idx_dashboards_dashboard_id ON dashboards(dashboard_id);
		CREATE INDEX IF NOT EXISTS idx_dashboards_group_name ON dashboards(group_name);
		CREATE INDEX IF NOT EXISTS idx_dashboards_product ON dashboards(product);
		CREATE INDEX IF NOT EXISTS idx_dashboards_created_by ON dashboards(created_by);
		
		-- ============================================================================
		-- BADGES TABLE
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS badges (
			_id TEXT PRIMARY KEY,
			badge TEXT UNIQUE NOT NULL,
			category TEXT DEFAULT 'default',
			description TEXT,
			color TEXT,
			icon TEXT,
			badge_type TEXT,
			created_at INTEGER DEFAULT (strftime('%s', 'now'))
		);
		
		CREATE INDEX IF NOT EXISTS idx_badges_badge ON badges(badge);
		CREATE INDEX IF NOT EXISTS idx_badges_category ON badges(category);
		
		-- ============================================================================
		-- TYPE_METADATA TABLE
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS type_metadata (
			_id TEXT PRIMARY KEY,
			type_key TEXT UNIQUE NOT NULL,
			name TEXT NOT NULL,
			description TEXT,
			kind TEXT,
			created_at INTEGER DEFAULT (strftime('%s', 'now')),
			updated_at INTEGER DEFAULT (strftime('%s', 'now'))
		);
		
		CREATE INDEX IF NOT EXISTS idx_type_metadata_type_key ON type_metadata(type_key);
		
		-- ============================================================================
		-- USER_RESOURCE_RELATIONS TABLE
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS user_resource_relations (
			_id TEXT PRIMARY KEY,
			resource_id TEXT NOT NULL,
			user_id TEXT NOT NULL,
			relation_type TEXT NOT NULL,
			resource_type TEXT NOT NULL,
			created_at INTEGER DEFAULT (strftime('%s', 'now')),
			UNIQUE(resource_id, user_id, relation_type)
		);
		
		CREATE INDEX IF NOT EXISTS idx_urr_resource_id ON user_resource_relations(resource_id);
		CREATE INDEX IF NOT EXISTS idx_urr_user_id ON user_resource_relations(user_id);
		CREATE INDEX IF NOT EXISTS idx_urr_relation_type ON user_resource_relations(relation_type);
		
		-- ============================================================================
		-- USER_TABLE_RELATIONS TABLE
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS user_table_relations (
			_id TEXT PRIMARY KEY,
			table_id TEXT NOT NULL,
			user_email TEXT NOT NULL,
			relation_type TEXT NOT NULL,
			created_at INTEGER DEFAULT (strftime('%s', 'now')),
			UNIQUE(table_id, user_email, relation_type)
		);
		
		CREATE INDEX IF NOT EXISTS idx_utr_table_id ON user_table_relations(table_id);
		CREATE INDEX IF NOT EXISTS idx_utr_user_email ON user_table_relations(user_email);
		
		-- ============================================================================
		-- USER_DASHBOARD_RELATIONS TABLE
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS user_dashboard_relations (
			_id TEXT PRIMARY KEY,
			dashboard_id TEXT NOT NULL,
			user_email TEXT NOT NULL,
			relation_type TEXT NOT NULL,
			created_at INTEGER DEFAULT (strftime('%s', 'now')),
			UNIQUE(dashboard_id, user_email, relation_type)
		);
		
		CREATE INDEX IF NOT EXISTS idx_udr_dashboard_id ON user_dashboard_relations(dashboard_id);
		CREATE INDEX IF NOT EXISTS idx_udr_user_email ON user_dashboard_relations(user_email);
		
		-- ============================================================================
		-- TABLE_DASHBOARD_RELATIONS TABLE
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS table_dashboard_relations (
			_id TEXT PRIMARY KEY,
			table_uri TEXT NOT NULL,
			dashboard_id TEXT NOT NULL,
			created_at INTEGER DEFAULT (strftime('%s', 'now')),
			UNIQUE(table_uri, dashboard_id)
		);
		
		CREATE INDEX IF NOT EXISTS idx_tdr_table_uri ON table_dashboard_relations(table_uri);
		CREATE INDEX IF NOT EXISTS idx_tdr_dashboard_id ON table_dashboard_relations(dashboard_id);
		
		-- ============================================================================
		-- RESOURCE_DEPENDENCIES TABLE
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS resource_dependencies (
			_id TEXT PRIMARY KEY,
			source_id TEXT NOT NULL,
			source_type TEXT NOT NULL,
			target_id TEXT NOT NULL,
			target_type TEXT NOT NULL,
			target_name TEXT,
			dependency_type TEXT,
			level INTEGER DEFAULT 1,
			created_at INTEGER DEFAULT (strftime('%s', 'now'))
		);
		
		CREATE INDEX IF NOT EXISTS idx_rd_source_id ON resource_dependencies(source_id);
		CREATE INDEX IF NOT EXISTS idx_rd_target_id ON resource_dependencies(target_id);
		CREATE INDEX IF NOT EXISTS idx_rd_source_type ON resource_dependencies(source_type);
		
		-- ============================================================================
		-- COLUMN_METADATA TABLE
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS column_metadata (
			_id TEXT PRIMARY KEY,
			table_id TEXT NOT NULL,
			column_name TEXT NOT NULL,
			column_type TEXT,
			description TEXT,
			is_nullable INTEGER DEFAULT 1,
			is_primary_key INTEGER DEFAULT 0,
			is_foreign_key INTEGER DEFAULT 0,
			is_partition_key INTEGER DEFAULT 0,
			is_sort_key INTEGER DEFAULT 0,
			sample_value TEXT,
			min_value TEXT,
			max_value TEXT,
			avg_value TEXT,
			distinct_count INTEGER,
			null_count INTEGER,
			data_format TEXT,
			pii_flag INTEGER DEFAULT 0,
			tags TEXT,
			sort_order INTEGER,
			created_at INTEGER DEFAULT (strftime('%s', 'now')),
			updated_at INTEGER DEFAULT (strftime('%s', 'now')),
			UNIQUE(table_id, column_name)
		);
		
		CREATE INDEX IF NOT EXISTS idx_cm_table_id ON column_metadata(table_id);
		CREATE INDEX IF NOT EXISTS idx_cm_column_name ON column_metadata(column_name);
		CREATE INDEX IF NOT EXISTS idx_cm_is_primary_key ON column_metadata(is_primary_key);
		
		-- ============================================================================
		-- ACCESS_LOG TABLE
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS access_log (
			_id TEXT PRIMARY KEY,
			resource_id TEXT NOT NULL,
			resource_type TEXT NOT NULL,
			user_id TEXT,
			action TEXT,
			timestamp INTEGER DEFAULT (strftime('%s', 'now')),
			source TEXT,
			duration_ms INTEGER,
			success INTEGER DEFAULT 1
		);
		
		CREATE INDEX IF NOT EXISTS idx_al_resource_id ON access_log(resource_id);
		CREATE INDEX IF NOT EXISTS idx_al_user_id ON access_log(user_id);
		CREATE INDEX IF NOT EXISTS idx_al_timestamp ON access_log(timestamp);
		
		-- ============================================================================
		-- SEARCH_CACHE TABLE
		-- ============================================================================
		CREATE TABLE IF NOT EXISTS search_cache (
			_id TEXT PRIMARY KEY,
			query_hash TEXT UNIQUE NOT NULL,
			query_text TEXT,
			results TEXT,
			result_count INTEGER,
			created_at INTEGER DEFAULT (strftime('%s', 'now')),
			expires_at INTEGER
		);
		
		CREATE INDEX IF NOT EXISTS idx_sc_query_hash ON search_cache(query_hash);
		CREATE INDEX IF NOT EXISTS idx_sc_expires_at ON search_cache(expires_at);
		`

	// Execute the schema creation
	_, err := kb.DB.Exec(stmt)
	if err != nil {
		return fmt.Errorf("failed to create datacatalog schemas: %w", err)
	}
	logger.Info("[INFO] Catalog schemas created or already exist.")

	//GlobalLoggerDB.AddToOptimusLog("INFO", "[INFO] Catalog schemas created or already exist.", runtime.GOOS)

	// Insert default badges
	err = kb.insertDefaultBadges()
	if err != nil {
		logger.Error("[ERROR] failed to insert default badges: %w", err)
		return fmt.Errorf("failed to insert default badges: %w", err)
	}

	// Insert default test user
	err = kb.insertDefaultUser()
	if err != nil {
		logger.Error("[ERROR] failed to insert default user: %w", err)
		return fmt.Errorf("failed to insert default user: %w", err)
	}

	return nil
}

// insertDefaultBadges inserts default badge definitions
func (kb *KnowledgeBaseSQLite) insertDefaultBadges() error {
	stmt := `
	INSERT OR IGNORE INTO badges (_id, badge, category, badge_type, description) VALUES
		('badge_001', 'Verified', 'quality', 'success', 'Data quality has been verified'),
		('badge_002', 'PII', 'compliance', 'warning', 'Contains personally identifiable information'),
		('badge_003', 'Deprecated', 'status', 'danger', 'This dataset is deprecated'),
		('badge_004', 'Production', 'environment', 'success', 'Used in production systems'),
		('badge_005', 'Beta', 'status', 'info', 'Currently in beta testing'),
		('badge_006', 'High Priority', 'priority', 'danger', 'High priority dataset'),
		('badge_007', 'Real-time', 'technical', 'info', 'Real-time data updates'),
		('badge_008', 'ML Model', 'technical', 'info', 'Machine learning model output');
	`
	_, err := kb.DB.Exec(stmt)
	if err != nil {
		logger.Error("failed to insert default badges: %w", err)
		return fmt.Errorf("failed to insert default badges: %w", err)
	}
	logger.Info("[INFO] Default badges inserted.")
	//GlobalLoggerDB.AddToOptimusLog("INFO", "Default badges inserted.", runtime.GOOS)
	return nil
}

// insertDefaultUser inserts the test user for Amundsen
func (kb *KnowledgeBaseSQLite) insertDefaultUser() error {
	stmt := `
	INSERT OR REPLACE INTO users 
		(_id, user_id, email, display_name, first_name, last_name, team_name, department, 
		 role_name, employee_type, is_active, created_at, updated_at) 
	VALUES 
		('user_001', 'test_user_id', 'test@email.com', 'Test User', 'Test', 'User', 
		 'Data Science', 'Engineering', 'Data Engineer', 'full-time', 1, 
		 strftime('%s', 'now'), strftime('%s', 'now'));
	`
	_, err := kb.DB.Exec(stmt)
	if err != nil {
		logger.Error("failed to insert default user: %w", err)
		return fmt.Errorf("failed to insert default user: %w", err)
	}
	logger.Info("[INFO] Default test user inserted.")
	//GlobalLoggerDB.AddToOptimusLog("INFO", "Default test user inserted.", runtime.GOOS)
	return nil
}

// insertSampleData populates the database with sample data for testing
func (kb *KnowledgeBaseSQLite) InsertSampleData() error {
	stmt := `
	-- Sample Users
	INSERT OR REPLACE INTO users VALUES
		('user_002', 'john_doe', 'john.doe@company.com', 'John Doe', 'John', 'Doe',
		 'http://company.com/profiles/john', 'johndoe', 'U123456', 'ML Platform', 
		 'Engineering', 'ML Engineer', 'full-time', NULL, NULL, 1, 
		 strftime('%s', 'now'), strftime('%s', 'now'), NULL),
		('user_003', 'ml_team', 'ml-team@company.com', 'ML Team', 'ML', 'Team',
		 '', '', 'U789012', 'ML Platform', 'Engineering', 'Team Account', 'team',
		 NULL, NULL, 1, strftime('%s', 'now'), strftime('%s', 'now'), NULL);
	
	-- Sample Dashboards
	INSERT OR REPLACE INTO dashboards VALUES
		('dash_001', 'recommendation_performance', 
		 'Recommendation Engine Performance',
		 'http://dashboards.company.com/recommendation_performance',
		 'Real-time monitoring of recommendation engine metrics including CTR, conversion rate, and model accuracy',
		 'Data Science', 'http://dashboards.company.com/datascience', 
		 'ML Platform', 'default', 'ml_team', 'ml_team,john_doe',
		 'ML,Recommendations,Performance', 'Production,Real-time', 
		 0, NULL, strftime('%s', 'now'), strftime('%s', 'now'), strftime('%s', 'now'),
		 'operational', '5 minutes', NULL),
		('dash_002', 'user_engagement_analytics',
		 'User Engagement Analytics',
		 'http://dashboards.company.com/user_engagement',
		 'Track user behavior patterns, session duration, and feature adoption',
		 'Product Analytics', 'http://dashboards.company.com/product',
		 'Analytics Platform', 'default', 'john_doe', 'john_doe',
		 'Users,Engagement,Analytics', 'Production', 
		 0, NULL, strftime('%s', 'now'), strftime('%s', 'now'), strftime('%s', 'now'),
		 'analytical', '1 hour', NULL);
	
	-- Sample Tables
	INSERT OR REPLACE INTO datacatalog VALUES
		('rec_engine_001',
		 'Recommendation Engine.Product Recommendation Scores',
		 'Recommendation Engine',
		 'Real-time product recommendation scores using collaborative filtering and content-based algorithms. Powers personalized product suggestions across web, mobile, and email channels.',
		 'Data Science', 'ml_team', 'ML Team',
		 'ML,Real-time,Recommendations,Production',
		 'Production,Verified,ML Model',
		 'high', 'active', 'production', 'v2.3.1',
		 '[{"name":"user_id","type":"varchar","description":"Unique user identifier","sample_value":"USR_12345","is_nullable":false,"is_primary_key":true},{"name":"product_id","type":"varchar","description":"Product identifier","sample_value":"PROD_67890","is_nullable":false,"is_primary_key":true},{"name":"score","type":"float","description":"Recommendation confidence score (0.0-1.0)","sample_value":"0.87","is_nullable":false},{"name":"model_version","type":"varchar","description":"ML model version used","sample_value":"v2.3.1","is_nullable":false},{"name":"timestamp","type":"timestamp","description":"Time when recommendation was generated","sample_value":"2024-12-01T19:57:00Z","is_nullable":false}]',
		 '[{"key":"Data Science://optimusdb.User Behavior/User Click Events","level":1},{"key":"Data Science://optimusdb.Product Catalog/Product Attributes","level":1}]',
		 '[{"key":"Data Science://optimusdb.Analytics/Recommendation Performance Metrics","level":1},{"key":"dashboard://recommendation_performance","level":1}]',
		 'ml_team,john_doe', '{"primary":"ml_team","contributors":["john_doe"]}',
		 0.95, strftime('%s', 'now'), 'Internal',
		 1250000, strftime('%s', 'now'), 125000000, 1024000000,
		 '{"row_count":125000000,"size_mb":976,"avg_score":0.72}',
		 'fact', 'real-time', 'Continuous streaming',
		 's3://ml-data/recommendations/', 'date', 'user_id,product_id',
		 'https://wiki.company.com/ml/recommendations',
		 'https://wiki.company.com/ml/recommendations',
		 '#ml-platform', NULL, NULL, NULL,
		 '{"type":"streaming","latency":"<100ms"}',
		 '{"latency":"<100ms","availability":"99.9%"}',
		 'High-throughput real-time scoring',
		 'CREATE TABLE recommendations AS SELECT * FROM ml_model_output;',
		 'Collaborative filtering + Content-based hybrid model',
		 strftime('%s', 'now') - 7776000,
		 strftime('%s', 'now'), NULL),
	
		('user_clicks_001',
		 'User Click Events',
		 'User Behavior',
		 'Raw clickstream data capturing all user interactions across web and mobile platforms.',
		 'Data Science', 'john_doe', 'John Doe',
		 'Events,User Behavior,Clickstream,Raw',
		 'Production,Real-time',
		 'medium', 'active', 'production', 'v1.0.0',
		 '[{"name":"event_id","type":"varchar","description":"Unique event identifier","sample_value":"EVT_789012","is_nullable":false,"is_primary_key":true},{"name":"user_id","type":"varchar","description":"User identifier","sample_value":"USR_12345","is_nullable":false},{"name":"event_type","type":"varchar","description":"Type of event","sample_value":"click","is_nullable":false},{"name":"timestamp","type":"timestamp","description":"Event timestamp","sample_value":"2024-12-01T19:57:00Z","is_nullable":false}]',
		 '[]',
		 '[{"key":"Data Science://optimusdb.Recommendation Engine/Recommendation Engine.Product Recommendation Scores","level":1}]',
		 'john_doe', '{"primary":"john_doe"}',
		 0.88, strftime('%s', 'now'), 'PII',
		 5500000, strftime('%s', 'now'), 500000000, 2048000000,
		 '{"row_count":500000000,"size_mb":1953}',
		 'fact', 'real-time', 'Continuous streaming',
		 's3://events/clickstream/', 'date', 'user_id,timestamp',
		 'https://wiki.company.com/analytics/clickstream',
		 'https://wiki.company.com/analytics/clickstream',
		 '#data-platform', NULL, NULL, NULL,
		 '{"type":"streaming","latency":"<1s"}',
		 '{"latency":"<5s","availability":"99.95%"}',
		 'High-volume event stream', NULL,
		 'Direct stream from application events',
		 strftime('%s', 'now') - 15552000,
		 strftime('%s', 'now'), NULL),
	
		('product_cat_001',
		 'Product Attributes',
		 'Product Catalog',
		 'Master product catalog with detailed attributes, categories, pricing, and inventory information.',
		 'Data Science', 'test_user_id', 'Test User',
		 'Products,Catalog,Reference,Master Data',
		 'Production,Verified',
		 'high', 'active', 'production', 'v3.1.0',
		 '[{"name":"product_id","type":"varchar","description":"Unique product identifier","sample_value":"PROD_67890","is_nullable":false,"is_primary_key":true},{"name":"product_name","type":"varchar","description":"Product display name","sample_value":"Wireless Headphones","is_nullable":false},{"name":"category","type":"varchar","description":"Product category","sample_value":"Electronics","is_nullable":false},{"name":"price","type":"float","description":"Current price in USD","sample_value":"79.99","is_nullable":false},{"name":"inventory_count","type":"int","description":"Available inventory","sample_value":"150","is_nullable":false}]',
		 '[]',
		 '[{"key":"Data Science://optimusdb.Recommendation Engine/Recommendation Engine.Product Recommendation Scores","level":1}]',
		 'test_user_id,ml_team', '{"primary":"test_user_id"}',
		 0.98, strftime('%s', 'now'), NULL,
		 150, strftime('%s', 'now'), 45000, 10240000,
		 '{"row_count":45000,"size_mb":9}',
		 'dimension', 'daily', '0 2 * * *',
		 's3://master-data/products/', NULL, 'product_id',
		 'https://wiki.company.com/catalog/products',
		 'https://wiki.company.com/catalog/products',
		 '#product-team', NULL, NULL, NULL,
		 '{"type":"batch","schedule":"daily 2am UTC"}',
		 '{"freshness":"<24h","completeness":"99%"}',
		 'Reference data for product information', NULL,
		 'Synchronized from ProductDB',
		 strftime('%s', 'now') - 31536000,
		 strftime('%s', 'now'), NULL);
	
	-- Table-Dashboard Relations
	INSERT OR REPLACE INTO table_dashboard_relations VALUES
		('tdr_001', 'Data Science://optimusdb.Recommendation_Engine/Recommendation_Engine.Product_Recommendation_Scores', 'recommendation_performance', strftime('%s', 'now')),
		('tdr_002', 'Data Science://optimusdb.User_Behavior/User_Click_Events', 'user_engagement_analytics', strftime('%s', 'now'));
	
	-- User-Resource Relations
	INSERT OR REPLACE INTO user_resource_relations VALUES
		('urr_001', 'rec_engine_001', 'test_user_id', 'follow', 'table', strftime('%s', 'now')),
		('urr_002', 'rec_engine_001', 'john_doe', 'follow', 'table', strftime('%s', 'now')),
		('urr_003', 'user_clicks_001', 'test_user_id', 'follow', 'table', strftime('%s', 'now'));
	`

	_, err := kb.DB.Exec(stmt)
	if err != nil {
		logger.Error("failed to insert sample data: %w", err)
		return fmt.Errorf("failed to insert sample data: %w", err)
	}
	logger.Info("Sample data inserted successfully.")
	//GlobalLoggerDB.AddToOptimusLog("INFO", "Sample data inserted successfully.", runtime.GOOS)
	return nil
}

// Create Insert Function for This Table
func (kb *KnowledgeBaseSQLite) InsertTOSCAMetadata(
	templateID string,
	description string,
	nodeCount int,
	filename string,
	filesizeBytes int64,
	contentSHA256 string,
	ipfsPath string,
	uploader string,
	sourcePod string,
	sourceIP string,
) error {
	query := `
	INSERT INTO toscametadata (
		template_id, description, node_templates_count, created_at,
		filename, filesize_bytes, content_sha256, ipfs_path, uploader, source_pod, source_ip
	) VALUES (?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(template_id) DO UPDATE SET
		description=excluded.description,
		node_templates_count=excluded.node_templates_count,
		created_at=excluded.created_at,
		filename=excluded.filename,
		filesize_bytes=excluded.filesize_bytes,
		content_sha256=excluded.content_sha256,
		ipfs_path=excluded.ipfs_path,
		uploader=excluded.uploader,
		source_pod=excluded.source_pod,
		source_ip=excluded.source_ip;
	`

	_, err := kb.DB.Exec(query,
		templateID, description, nodeCount,
		filename, filesizeBytes, contentSHA256, ipfsPath,
		uploader, sourcePod, sourceIP,
	)
	if err == nil {
		//GlobalLoggerDB.AddToOptimusLog("INFO",
		//	fmt.Sprintf("Inserted/updated record for TOSCAMetadata table: template_id=%s, filename=%s", templateID, filename),
		//	runtime.GOOS)
		logger.Info("Inserted/updated record for TOSCAMetadata table: template_id=%s, filename=%s", templateID, filename)
	} else {
		logger.Error("Failed to insert/update TOSCAMetadata: %v", err)
		//GlobalLoggerDB.AddToOptimusLog("ERROR",
		//	fmt.Sprintf("Failed to insert/update TOSCAMetadata: %v", err),
		//	runtime.GOOS)
	}
	return err
}

// AddToOptimusLog inserts a log entry into the optimusLogger table
func (kb *LoggerSQLite) AddToOptimusLog(level, message, source string) error {
	now := time.Now()
	timestamp := now.Format(time.RFC3339)
	date := now.Format("2006-01-02")
	hour := now.Format("15")

	// Automatically capture source if not provided
	if source == "" {
		if _, file, line, ok := runtime.Caller(1); ok {
			source = fmt.Sprintf("%s:%d", filepath.Base(file), line)
		} else {
			source = "unknown"
		}
	}

	stmt, err := kb.theLog.Prepare(`INSERT INTO optimusLogger(timestamp, date, hour, level, message, source)
                                VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(timestamp, date, hour, level, message, source)
	return err
}

// ///// :Log examples
// OptimusLog("INFO", "[METADATA] ✅ Background enricher started")
// OptimusLog("ERROR", "[METADATA] ❌ Failed to process metadata")
// OptimusLog logs a message to both stdout and database with automatic source detection
func OptimusLog(level, message string) {
	logger.Info(message)
	if GlobalLoggerDB != nil {
		var source string
		if _, file, line, ok := runtime.Caller(1); ok {
			source = fmt.Sprintf("%s:%d", filepath.Base(file), line)
		} else {
			source = runtime.GOOS
		}
		_ = GlobalLoggerDB.AddToOptimusLog(level, message, source)
	}
}

// Get Logs per Hr
func (kb *LoggerSQLite) GetLogsForHour(date, hour string) ([]map[string]string, error) {
	rows, err := kb.theLog.Query(`SELECT timestamp, level, message, source FROM optimusLogger
                           WHERE date = ? AND hour = ? ORDER BY timestamp DESC`, date, hour)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []map[string]string
	for rows.Next() {
		var timestamp, level, message, source string
		if err := rows.Scan(&timestamp, &level, &message, &source); err != nil {
			continue
		}
		logs = append(logs, map[string]string{
			"timestamp": timestamp,
			"level":     level,
			"message":   message,
			"source":    source,
		})
	}
	return logs, nil
}

// sqlDML executes SQL statements and returns results if it's a SELECT query.
func (kb *KnowledgeBaseSQLite) SqlDML(stmt string, logChan chan Log) (interface{}, error) {
	if kb == nil {
		return nil, errors.New("ERROR: KB obj in SQL DML is nil")
	}
	if kb.DB == nil {
		return nil, errors.New("ERROR: kb.DB obj in SQL DML is nil")
	}

	// Check if the query is a SELECT statement
	if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(stmt)), "SELECT") {
		// Execute SELECT query and fetch results
		rows, err := kb.DB.Query(stmt)
		if err != nil {
			//logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("ERROR: Problem executing SELECT statement: %v", err)}
			logger.Error("[ERROR] Problem executing SELECT statement: %v", err)
			return nil, err
		}
		defer rows.Close()

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		// Prepare a slice to store results
		var results []map[string]interface{}

		for rows.Next() {
			// Create a slice of interface{} to store each row's column values
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range columns {
				valuePtrs[i] = &values[i]
			}

			// Scan row into the slice
			if err := rows.Scan(valuePtrs...); err != nil {
				return nil, err
			}

			// Create a map to store column name -> value
			rowMap := make(map[string]interface{})
			for i, col := range columns {
				rowMap[col] = values[i]
			}
			results = append(results, rowMap)
		}

		// Return the fetched data
		return results, nil
	}

	// If it's an INSERT, UPDATE, DELETE statement
	result, err := kb.DB.Exec(stmt)
	if err != nil {
		//logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("ERROR: Problem executing DML statement: %v", err)}
		logger.Error("[ERROR] Problem executing DML statement: %v", err)
		return nil, err
	}

	// Get affected rows count
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	logger.Info("SQL statement executed successfully, affected rows: %d", rowsAffected)
	//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("SQL statement executed successfully, affected rows: %d", rowsAffected), runtime.GOOS)
	// Return success response
	return fmt.Sprintf("SQL statement executed successfully, affected rows: %d", rowsAffected), nil
}

// Close closes the database connection
func (kb *KnowledgeBaseSQLite) Close() {
	if kb.DB != nil {
		err := kb.DB.Close()
		if err != nil {
			logger.Error("[ERROR] Problem closing SQLite database:", err)
			//GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Problem closing SQLite database: %v", err), runtime.GOOS)
		} else {
			logger.Info("[INFO] SQLite database connection closed successfully.")
			//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("SQLite database connection closed successfully."), runtime.GOOS)
		}
	}
}

func (db *KnowledgeBaseDB) AddDiscoveredPeer(peerID string) {
	db.peersMutex.Lock()
	defer db.peersMutex.Unlock()
	if db.discoveredPeers == nil {
		db.discoveredPeers = make(map[string]bool)
	}
	db.discoveredPeers[peerID] = true
}

func (db *KnowledgeBaseDB) GetDiscoveredPeers() []string {
	db.peersMutex.Lock()
	defer db.peersMutex.Unlock()
	var peers []string
	for p := range db.discoveredPeers {
		peers = append(peers, p)
	}
	return peers
}

type Event struct {
	Version   string      `json:"version"`   // e.g., "v1"
	Type      string      `json:"type"`      // "upload","vote","election","heartbeat","log","startup"
	Timestamp time.Time   `json:"timestamp"` // UTC
	NodeID    string      `json:"node_id"`   // libp2p host id
	Payload   interface{} `json:"payload"`   // arbitrary
}

func (db *KnowledgeBaseDB) publishEvent(ev Event) {
	if db == nil || db.MQEMS == nil {
		return
	}
	if ev.Version == "" {
		ev.Version = "v1"
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	if ev.NodeID == "" {
		ev.NodeID = db.HostID
	}

	b, err := json.Marshal(ev)
	if err != nil {
		//GlobalLoggerDB.AddToOptimusLog("ERROR", "MQ publish marshal failed: "+err.Error(), "mq")
		logger.Error("[ERROR] MQ publish marshal failed: %v", err)
		return
	}

	// Use the default topic set when you created the client (cfg.Topic)
	_ = db.MQEMS.PublishJSON("", b)
}

// createEMSEventsTable ensures the `ems_events` table exists.
func (kb *LoggerSQLite) createEMSEventsTable() error {
	table := `
	CREATE TABLE IF NOT EXISTS ems_events (
		id            INTEGER PRIMARY KEY AUTOINCREMENT,
		received_at   TEXT,          -- UTC RFC3339
		node_id       TEXT,          -- libp2p host id
		client_id     TEXT,          -- MQ_CLIENT_ID (or fallback)
		topic         TEXT,          -- destination topic
		action        TEXT,          -- parsed from payload
		resource      TEXT,          -- parsed from payload
		params_json   TEXT,          -- marshaled params (if parsed)
		raw_json      TEXT           -- original message body
	);
	CREATE INDEX IF NOT EXISTS idx_ems_events_time ON ems_events(received_at);
	CREATE INDEX IF NOT EXISTS idx_ems_events_act_res ON ems_events(action, resource);
	`
	_, err := kb.theLog.Exec(table)
	return err
}

// Insert Helper
func (kb *LoggerSQLite) InsertEMSEvent(
	receivedAt time.Time,
	nodeID, clientID, topic, action, resource, paramsJSON, rawJSON string,
) error {
	const q = `
	INSERT INTO ems_events (received_at, node_id, client_id, topic, action, resource, params_json, raw_json)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?);`
	_, err := kb.theLog.Exec(q,
		receivedAt.UTC().Format(time.RFC3339),
		nodeID, clientID, topic, action, resource, paramsJSON, rawJSON,
	)
	return err
}

// Run a SELECT against the logger DB (optimuslog.db) and return []map[string]interface{}.
func (kb *LoggerSQLite) SelectAll(stmt string) ([]map[string]interface{}, error) {
	if kb == nil || kb.theLog == nil {
		return nil, errors.New("logger DB not initialized")
	}
	rows, err := kb.theLog.Query(stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var out []map[string]interface{}
	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols))
		for i, c := range cols {
			switch v := vals[i].(type) {
			case []byte:
				row[c] = string(v)
			default:
				row[c] = v
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
