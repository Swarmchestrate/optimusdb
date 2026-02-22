package datamodel

import (
	orbitdb "berty.tech/go-orbit-db"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"optimusdb/tosca"
)

//Replication Across Peers:
//Use replication features to share the metadata store across distributed nodes.

/*
Insert Metadata Entries
Use the Put method to insert metadata documents into the store.
*/

// MetadataEntry represents a metadata object
type MetadataEntry struct {
	// ─────────────────────────────────────────────────────────────────────────
	// ORIGINAL FIELDS (20) — unchanged, fully backward compatible
	// ─────────────────────────────────────────────────────────────────────────
	ID               string                   `json:"_id"`
	Author           string                   `json:"author"`
	MetadataType     string                   `json:"metadata_type"`
	Component        string                   `json:"component"`
	Behaviour        string                   `json:"behaviour"`
	Relationships    string                   `json:"relationships"`
	AssociatedID     string                   `json:"associated_id"`
	Name             string                   `json:"name"`
	Description      string                   `json:"description"`
	Tags             []string                 `json:"tags"`
	Status           string                   `json:"status"`
	CreatedBy        string                   `json:"created_by"`
	CreatedAt        time.Time                `json:"created_at"`
	UpdatedAt        time.Time                `json:"updated_at"`
	RelatedIDs       []string                 `json:"related_ids"`
	Priority         string                   `json:"priority"`
	SchedulingInfo   map[string]interface{}   `json:"scheduling_info"`
	SLAConstraints   map[string]interface{}   `json:"sla_constraints"`
	OwnershipDetails map[string]interface{}   `json:"ownership_details"`
	AuditTrail       []map[string]interface{} `json:"audit_trail"`

	// ─────────────────────────────────────────────────────────────────────────
	// NEW FIELDS — Data Classification & Domain (5)
	// ─────────────────────────────────────────────────────────────────────────

	// DataDomain indicates the knowledge domain of the resource.
	//   "renewable_energy", "weather", "grid_management", "energy_storage",
	//   "forecasting", "iot_telemetry", "cloud_orchestration", "general"
	DataDomain string `json:"data_domain,omitempty"`

	// DataClassification controls access sensitivity.
	//   "public", "consortium", "restricted", "confidential"
	DataClassification string `json:"data_classification,omitempty"`

	// GeoLocation stores geographic context as JSON string.
	//   {"lat":37.98,"lon":23.73,"region":"Attica","country":"GR"}
	GeoLocation string `json:"geo_location,omitempty"`

	// TemporalCoverage defines the time range the data spans (ISO 8601 interval).
	//   "2024-01-01T00:00:00Z/2025-12-31T23:59:59Z"
	TemporalCoverage string `json:"temporal_coverage,omitempty"`

	// DataQualityScore is a normalized quality indicator (0.0 – 1.0).
	DataQualityScore float64 `json:"data_quality_score,omitempty"`

	// ─────────────────────────────────────────────────────────────────────────
	// NEW FIELDS — Content Identity & Format (5)
	// ─────────────────────────────────────────────────────────────────────────

	// SchemaVersion tracks which version of the metadata schema created this.
	SchemaVersion string `json:"schema_version,omitempty"`

	// ContentHash is the SHA-256 hex digest of the original uploaded content.
	ContentHash string `json:"content_hash,omitempty"`

	// FileFormat describes the format of the associated resource.
	//   "yaml", "json", "parquet", "csv", "xlsx", "binary", "tosca"
	FileFormat string `json:"file_format,omitempty"`

	// FileSizeBytes stores the byte-level size of the resource.
	FileSizeBytes int64 `json:"file_size_bytes,omitempty"`

	// RecordCount stores the number of data records (rows, nodes, entries).
	RecordCount int `json:"record_count,omitempty"`

	// ─────────────────────────────────────────────────────────────────────────
	// NEW FIELDS — Lifecycle & Governance (6)
	// ─────────────────────────────────────────────────────────────────────────

	// UpdateFrequency describes how often the resource is refreshed.
	//   "real-time", "5min", "hourly", "daily", "weekly", "monthly",
	//   "quarterly", "on-demand", "static"
	UpdateFrequency string `json:"update_frequency,omitempty"`

	// RetentionPolicy specifies data retention rules: "90d", "1y", "indefinite"
	RetentionPolicy string `json:"retention_policy,omitempty"`

	// AccessControl specifies who can read/write the resource (JSON string).
	//   {"read":["team-a","team-b"],"write":["admin"]}
	AccessControl string `json:"access_control,omitempty"`

	// ComplianceTags captures regulatory/legal framework applicability.
	//   Comma-separated: "GDPR,EU-Horizon,FAIR-Data,CC-BY-4.0"
	ComplianceTags string `json:"compliance_tags,omitempty"`

	// ProvenanceChain records the lineage of transformations (JSON array).
	//   [{"step":"ingested","agent":"agent-3","ts":"..."},...]
	ProvenanceChain string `json:"provenance_chain,omitempty"`

	// ProcessingStatus indicates the current processing stage.
	//   "raw", "validated", "cleaned", "enriched", "published", "archived"
	ProcessingStatus string `json:"processing_status,omitempty"`

	// ─────────────────────────────────────────────────────────────────────────
	// NEW FIELDS — Access & Versioning (4)
	// ─────────────────────────────────────────────────────────────────────────

	// ApiEndpoint is the programmatic access path for this resource.
	ApiEndpoint string `json:"api_endpoint,omitempty"`

	// Version tracks the semantic version of the resource itself.
	Version string `json:"version,omitempty"`

	// ParentID enables versioning chains (previous version's _id).
	ParentID string `json:"parent_id,omitempty"`

	// ExpiryDate specifies when this metadata/resource becomes stale.
	ExpiryDate string `json:"expiry_date,omitempty"`

	// ─────────────────────────────────────────────────────────────────────────
	// NEW FIELDS — Contextual & Infrastructure (8)
	// ─────────────────────────────────────────────────────────────────────────

	// Language is the ISO 639-1 language code: "en", "el", "de", "fr", "multi"
	Language string `json:"language,omitempty"`

	// LicenseType is the data license identifier.
	//   "CC-BY-4.0", "CC-BY-SA-4.0", "MIT", "proprietary", "consortium-only"
	LicenseType string `json:"license_type,omitempty"`

	// ContactInfo stores the responsible person or team (JSON string).
	//   {"name":"Energy Ops","email":"ops@swarmchestrate.eu","role":"data-steward"}
	ContactInfo string `json:"contact_info,omitempty"`

	// NodeCount stores the count of TOSCA node_templates (TOSCA-specific).
	NodeCount int `json:"node_count,omitempty"`

	// IpfsCID is the IPFS Content Identifier for the stored resource.
	IpfsCID string `json:"ipfs_cid,omitempty"`

	// SourceAgent is the OptimusDB agent name that ingested the data.
	SourceAgent string `json:"source_agent,omitempty"`

	// SourcePod is the Kubernetes pod name (if deployed in k8s).
	SourcePod string `json:"source_pod,omitempty"`

	// SourceIP is the IP address of the originating node.
	SourceIP string `json:"source_ip,omitempty"`
}

// MetadataStore keeps track of metadata entries
type MetadataStore struct {
	sync.Mutex
	Entries map[string]MetadataEntry
}

// Global Metadata Storage
var Metadata = MetadataStore{
	Entries: make(map[string]MetadataEntry),
}

// AddMetadata inserts or updates a metadata entry
func (ms *MetadataStore) AddMetadata(entry MetadataEntry) {
	ms.Lock()
	defer ms.Unlock()

	entry.UpdatedAt = time.Now()
	if _, exists := ms.Entries[entry.ID]; !exists {
		entry.CreatedAt = time.Now()
	}

	ms.Entries[entry.ID] = entry
}

// GetMetadata retrieves metadata entries
func (ms *MetadataStore) GetMetadata() []MetadataEntry {
	ms.Lock()
	defer ms.Unlock()

	metadataList := make([]MetadataEntry, 0, len(ms.Entries))
	for _, entry := range ms.Entries {
		metadataList = append(metadataList, entry)
	}
	return metadataList
}

// DeleteMetadata removes an entry
func (ms *MetadataStore) DeleteMetadata(id string) {
	ms.Lock()
	defer ms.Unlock()
	delete(ms.Entries, id)
}

// =============================================================================
// CONVERT METADATA ENTRY → MAP (for OrbitDB DocumentStore.Put())
// =============================================================================
// After dropping this file in, update service.go ConvertMetadataToMap to delegate:
//
//   func ConvertMetadataToMap(entry datamodel.MetadataEntry) map[string]interface{} {
//       return datamodel.ConvertMetadataToMap(entry)
//   }

func ConvertMetadataToMap(entry MetadataEntry) map[string]interface{} {
	metadataMap := make(map[string]interface{})

	// ── Original fields (always present) ────────────────────────────────────
	metadataMap["_id"] = entry.ID
	metadataMap["author"] = entry.Author
	metadataMap["metadata_type"] = entry.MetadataType
	metadataMap["component"] = entry.Component
	metadataMap["behaviour"] = entry.Behaviour
	metadataMap["relationships"] = entry.Relationships
	metadataMap["associated_id"] = entry.AssociatedID
	metadataMap["name"] = entry.Name
	metadataMap["description"] = entry.Description
	metadataMap["tags"] = entry.Tags
	metadataMap["status"] = entry.Status
	metadataMap["created_by"] = entry.CreatedBy
	metadataMap["created_at"] = entry.CreatedAt.Format(time.RFC3339)
	metadataMap["updated_at"] = entry.UpdatedAt.Format(time.RFC3339)
	metadataMap["related_ids"] = entry.RelatedIDs
	metadataMap["priority"] = entry.Priority
	metadataMap["scheduling_info"] = entry.SchedulingInfo
	metadataMap["sla_constraints"] = entry.SLAConstraints
	metadataMap["ownership_details"] = entry.OwnershipDetails
	metadataMap["audit_trail"] = entry.AuditTrail

	// ── New fields (only included when set — keeps old documents clean) ──────
	if entry.DataDomain != "" {
		metadataMap["data_domain"] = entry.DataDomain
	}
	if entry.DataClassification != "" {
		metadataMap["data_classification"] = entry.DataClassification
	}
	if entry.GeoLocation != "" {
		metadataMap["geo_location"] = entry.GeoLocation
	}
	if entry.TemporalCoverage != "" {
		metadataMap["temporal_coverage"] = entry.TemporalCoverage
	}
	if entry.DataQualityScore > 0 {
		metadataMap["data_quality_score"] = entry.DataQualityScore
	}
	if entry.SchemaVersion != "" {
		metadataMap["schema_version"] = entry.SchemaVersion
	}
	if entry.ContentHash != "" {
		metadataMap["content_hash"] = entry.ContentHash
	}
	if entry.FileFormat != "" {
		metadataMap["file_format"] = entry.FileFormat
	}
	if entry.FileSizeBytes > 0 {
		metadataMap["file_size_bytes"] = entry.FileSizeBytes
	}
	if entry.RecordCount > 0 {
		metadataMap["record_count"] = entry.RecordCount
	}
	if entry.UpdateFrequency != "" {
		metadataMap["update_frequency"] = entry.UpdateFrequency
	}
	if entry.RetentionPolicy != "" {
		metadataMap["retention_policy"] = entry.RetentionPolicy
	}
	if entry.AccessControl != "" {
		metadataMap["access_control"] = entry.AccessControl
	}
	if entry.ComplianceTags != "" {
		metadataMap["compliance_tags"] = entry.ComplianceTags
	}
	if entry.ProvenanceChain != "" {
		metadataMap["provenance_chain"] = entry.ProvenanceChain
	}
	if entry.ProcessingStatus != "" {
		metadataMap["processing_status"] = entry.ProcessingStatus
	}
	if entry.ApiEndpoint != "" {
		metadataMap["api_endpoint"] = entry.ApiEndpoint
	}
	if entry.Version != "" {
		metadataMap["version"] = entry.Version
	}
	if entry.ParentID != "" {
		metadataMap["parent_id"] = entry.ParentID
	}
	if entry.ExpiryDate != "" {
		metadataMap["expiry_date"] = entry.ExpiryDate
	}
	if entry.Language != "" {
		metadataMap["language"] = entry.Language
	}
	if entry.LicenseType != "" {
		metadataMap["license_type"] = entry.LicenseType
	}
	if entry.ContactInfo != "" {
		metadataMap["contact_info"] = entry.ContactInfo
	}
	if entry.NodeCount > 0 {
		metadataMap["node_count"] = entry.NodeCount
	}
	if entry.IpfsCID != "" {
		metadataMap["ipfs_cid"] = entry.IpfsCID
	}
	if entry.SourceAgent != "" {
		metadataMap["source_agent"] = entry.SourceAgent
	}
	if entry.SourcePod != "" {
		metadataMap["source_pod"] = entry.SourcePod
	}
	if entry.SourceIP != "" {
		metadataMap["source_ip"] = entry.SourceIP
	}

	return metadataMap
}

/*
func AddMetadata(store orbitdb.DocumentStore, ctx context.Context) {
	metadata := map[string]interface{}{
		"_id":           "workflow-1",
		"metadata_type": "Workflow",
		"associated_id": "workflow-1",
		"name":          "Backup Task",
		"description":   "A scheduled workflow to back up database resources daily.",
		"tags":          []string{"backup", "critical"},
		"status":        "Active",
		"created_by":    "admin-user",
		"created_at":    time.Now().Format(time.RFC3339),
		"updated_at":    time.Now().Format(time.RFC3339),
		"related_ids":   []string{"resource-1", "resource-2"},
		"priority":      "High",
		"scheduling_info": map[string]interface{}{
			"cron_expression": "0 0 * * *",
			"time_zone":       "UTC",
		},
		"sla_constraints": map[string]interface{}{
			"latency_ms":      500,
			"throughput_mbps": 100,
		},
		"ownership_details": map[string]interface{}{
			"owner":        "Team A",
			"organization": "Company XYZ",
		},
		"audit_trail": []map[string]string{
			{
				"timestamp": time.Now().Format(time.RFC3339),
				"user":      "admin-user",
				"action":    "Created entry",
			},
		},
	}

	_, err := store.Put(ctx, metadata)
	if err != nil {
		log.Fatalf("Failed to insert metadata: %v", err)
	}

	fmt.Println("Metadata added successfully")
}
*/
/*
Delete Metadata
Use the Delete method to remove a metadata entry.
*/
func deleteMetadata(store orbitdb.DocumentStore, ctx context.Context, id string) {
	//err := store.Delete(ctx, id)
	_, err := store.Delete(ctx, id)
	if err != nil {
		log.Fatalf("Failed to delete metadata: %v", err)
	}

	fmt.Println("Metadata deleted successfully")
}

// Generate a random UUID
func generateUUID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("Error generating UUID:", err)
		return ""
	}
	return hex.EncodeToString(b)
}

func GenerateMetadataFromResource(entry map[string]interface{}) MetadataEntry {
	metadataID := entry["_id"].(string) // Ensure `_id` is properly assigned

	// Convert resource_tags safely
	tags := convertToStringSlice(entry["resource_tags"])

	metadata := MetadataEntry{
		ID:               metadataID,
		Author:           "System", // Make sure this is set
		MetadataType:     "Resource",
		Component:        fmt.Sprintf("%s-%s", entry["resource_type"], entry["resource_def"]),
		Behaviour:        "Auto-Generated",
		Relationships:    fmt.Sprintf("Related to %s", entry["resource_grpname"]),
		AssociatedID:     metadataID, // Ensure this matches the original record
		Name:             fmt.Sprintf("Metadata for %s", entry["resource_name"]),
		Description:      fmt.Sprintf("Metadata auto-generated for resource: %s", entry["resource_name"]),
		Tags:             tags, // Ensure safe conversion
		Status:           "Active",
		CreatedBy:        "System",
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		RelatedIDs:       []string{metadataID},
		Priority:         "Medium",
		SchedulingInfo:   map[string]interface{}{"schedule": "On-Demand"},
		SLAConstraints:   map[string]interface{}{"latency": "10ms"},
		OwnershipDetails: map[string]interface{}{"owner": "DefaultOwner"},
		AuditTrail: []map[string]interface{}{
			{"action": "created", "timestamp": time.Now()},
		},

		// ── New fields (sensible defaults for resource metadata) ─────────
		DataDomain:         "general",
		DataClassification: "consortium",
		SchemaVersion:      "2.0.0",
		ProcessingStatus:   "raw",
		UpdateFrequency:    "on-demand",
		RetentionPolicy:    "indefinite",
		Language:           "en",
		SourceAgent:        os.Getenv("AGENT_NAME"),
		SourcePod:          os.Getenv("POD_NAME"),
	}

	fmt.Printf("DEBUG: Generated Metadata: %+v\n", metadata) // Debug print
	return metadata
}

// Helper Function to Parse String Arrays
func parseStringArray(input interface{}) []string {
	if arr, ok := input.([]string); ok {
		return arr
	}
	if arr, ok := input.([]interface{}); ok {
		result := make([]string, len(arr))
		for i, v := range arr {
			if str, ok := v.(string); ok {
				result[i] = str
			}
		}
		return result
	}
	return []string{} // Return empty if conversion fails
}

func convertToStringSlice(input interface{}) []string {
	if input == nil {
		return []string{}
	}

	switch v := input.(type) {
	case []string:
		return v
	case []interface{}:
		var result []string
		for _, val := range v {
			if str, ok := val.(string); ok {
				result = append(result, str)
			}
		}
		return result
	default:
		return []string{}
	}
}

// GenerateMetadataFromSQL creates metadata from a SQL DML statement.
func GenerateMetadataFromSQL(sql string) MetadataEntry {
	now := time.Now().UTC()
	id := fmt.Sprintf("meta-%x", sha256.Sum256([]byte(sql+now.String())))

	tableName := extractTableFromSQL(sql)

	provenanceEntry := map[string]interface{}{
		"step":      "sql_insert",
		"agent":     os.Getenv("AGENT_NAME"),
		"timestamp": now.Format(time.RFC3339),
		"method":    "sqldml",
	}
	provenanceJSON, _ := json.Marshal([]interface{}{provenanceEntry})

	return MetadataEntry{
		ID:           id,
		MetadataType: "sql_insert",
		Component:    "SQLite",
		Behaviour:    "DataInsertion",
		AssociatedID: tableName,
		Name:         "SQL Metadata: " + tableName,
		Description:  "Auto-generated metadata for SQL insert into " + tableName,
		CreatedAt:    now,
		UpdatedAt:    now,
		CreatedBy:    "system",
		Tags:         []string{"SQLite", "AutoGenerated", tableName},
		Status:       "active",
		Priority:     "medium",
		AuditTrail: []map[string]interface{}{
			{"action": "auto_generated", "timestamp": now.Format(time.RFC3339)},
		},

		// ── New fields ───────────────────────────────────────────────────
		DataDomain:         "general",
		DataClassification: "consortium",
		SchemaVersion:      "2.0.0",
		FileFormat:         "sql",
		ProcessingStatus:   "raw",
		UpdateFrequency:    "on-demand",
		RetentionPolicy:    "90d",
		ProvenanceChain:    string(provenanceJSON),
		Language:           "en",
		SourceAgent:        os.Getenv("AGENT_NAME"),
		SourcePod:          os.Getenv("POD_NAME"),
	}
}

// =============================================================================
// GENERATE METADATA FROM TOSCA UPLOAD
// =============================================================================
// Called by uploadTOSCAHandler after the template has been stored.
// Extracts as much information as possible from the parsed TOSCA document,
// the uploaded file bytes, and the runtime environment.
//
// Parameters:
//   - templateID: SHA-based ID from tosca.ComputeTemplateID()
//   - filename:   original uploaded filename
//   - fileBytes:  raw YAML bytes (for content hash + size)
//   - toscaDoc:   parsed TOSCA as map[string]interface{} (from ParseTOSCAToFullJSON)
//   - ipfsPath:   IPFS CID path (e.g. "/ipfs/Qm...")
//   - uploader:   who uploaded (from X-User header or agent name)
//   - sourcePod:  Kubernetes pod name
//   - sourceIP:   originating IP address
//   - storeName:  target OrbitDB store name (e.g. "dsswres")
//   - agentName:  OptimusDB agent name (e.g. "Agent-1")

func GenerateMetadataFromTOSCA(
	templateID string,
	filename string,
	fileBytes []byte,
	toscaDoc map[string]interface{},
	ipfsPath string,
	uploader string,
	sourcePod string,
	sourceIP string,
	storeName string,
	agentName string,
) MetadataEntry {

	now := time.Now().UTC()

	// ── Content hash ────────────────────────────────────────────────────────
	hash := sha256.Sum256(fileBytes)
	contentHash := hex.EncodeToString(hash[:])

	// ── Extract TOSCA-specific data ─────────────────────────────────────────
	description := tosca.ExtractDescription(toscaDoc)
	nodeCount := tosca.CountNodeTemplatesFromJSON(toscaDoc)

	templateName := tosca.ExtractMetadataField(toscaDoc, "template_name")
	if templateName == "" {
		templateName = filename
	}
	templateAuthor := tosca.ExtractMetadataField(toscaDoc, "template_author")
	templateVersion := tosca.ExtractMetadataField(toscaDoc, "template_version")

	toscaVersion := ""
	if v, ok := toscaDoc["tosca_definitions_version"].(string); ok {
		toscaVersion = v
	}

	// ── Auto-detect tags from TOSCA content ─────────────────────────────────
	tags := []string{"tosca", "uploaded", storeName}
	if toscaVersion != "" {
		tags = append(tags, toscaVersion)
	}

	// Extract node template types as short tags
	nodeTypes := extractNodeTypes(toscaDoc)
	for _, nt := range nodeTypes {
		parts := strings.Split(nt, ".")
		tags = append(tags, strings.ToLower(parts[len(parts)-1]))
	}

	// ── Auto-detect data domain from keywords ───────────────────────────────
	dataDomain := inferDomainFromTOSCA(description, nodeTypes, tags)

	// ── Build relationships from node template names ────────────────────────
	nodeNames := extractNodeNames(toscaDoc)
	relationships := ""
	if len(nodeNames) > 0 {
		rels := make([]string, 0, len(nodeNames))
		for _, name := range nodeNames {
			rels = append(rels, "contains:"+name)
		}
		relationships = strings.Join(rels, ",")
	}

	// ── Build provenance chain ──────────────────────────────────────────────
	provenanceEntry := map[string]interface{}{
		"step":      "ingested",
		"agent":     agentName,
		"pod":       sourcePod,
		"ip":        sourceIP,
		"uploader":  uploader,
		"timestamp": now.Format(time.RFC3339),
		"method":    "tosca_upload",
		"store":     storeName,
	}
	provenanceJSON, _ := json.Marshal([]interface{}{provenanceEntry})

	// ── Build contact info from template author ─────────────────────────────
	contactInfo := ""
	if templateAuthor != "" {
		ci := map[string]string{"name": templateAuthor, "role": "template_author"}
		ciJSON, _ := json.Marshal(ci)
		contactInfo = string(ciJSON)
	}

	// ── Build API endpoint ──────────────────────────────────────────────────
	apiEndpoint := fmt.Sprintf("/swarmkb/command (dstype=%s, _id=%s)", storeName, templateID)

	// ── Build ownership details ─────────────────────────────────────────────
	ownership := map[string]interface{}{
		"project": "Swarmchestrate",
		"grant":   "EU Horizon Europe 101135012",
		"agent":   agentName,
	}
	if templateAuthor != "" {
		ownership["author"] = templateAuthor
	}

	// ── Build geo location (from TOSCA metadata if present) ─────────────────
	geoLocation := tosca.ExtractMetadataField(toscaDoc, "geo_location")
	if geoLocation == "" {
		geoLocation = tosca.ExtractMetadataField(toscaDoc, "location")
	}

	// ── Metadata ID ─────────────────────────────────────────────────────────
	metaIDHash := sha256.Sum256([]byte("meta-tosca-" + templateID + now.String()))
	metaID := "meta-tosca-" + hex.EncodeToString(metaIDHash[:8])

	// ── Assemble the complete entry ─────────────────────────────────────────
	return MetadataEntry{
		// ── Original fields ──────────────────────────────────────────────
		ID:            metaID,
		Author:        templateAuthor,
		MetadataType:  "tosca_resource",
		Component:     "tosca-template",
		Behaviour:     "infrastructure-definition",
		Relationships: relationships,
		AssociatedID:  templateID,
		Name:          templateName,
		Description:   description,
		Tags:          tags,
		Status:        "active",
		CreatedBy:     uploader,
		CreatedAt:     now,
		UpdatedAt:     now,
		RelatedIDs:    nodeNames,
		Priority:      "medium",
		SchedulingInfo: map[string]interface{}{
			"upload_time": now.Format(time.RFC3339),
			"indexing":    "immediate",
		},
		SLAConstraints: map[string]interface{}{
			"availability": "best-effort",
			"replication":  "crdt-automatic",
		},
		OwnershipDetails: ownership,
		AuditTrail: []map[string]interface{}{
			{
				"action":    "metadata_auto_generated",
				"agent":     agentName,
				"timestamp": now.Format(time.RFC3339),
				"trigger":   "tosca_upload",
				"details":   fmt.Sprintf("Auto-generated from TOSCA upload: %s", filename),
			},
		},

		// ── New fields ───────────────────────────────────────────────────
		DataDomain:         dataDomain,
		DataClassification: "consortium",
		GeoLocation:        geoLocation,
		TemporalCoverage:   now.Format(time.RFC3339) + "/open",
		DataQualityScore:   1.0, // freshly uploaded, validated YAML
		SchemaVersion:      "2.0.0",
		ContentHash:        contentHash,
		FileFormat:         "tosca",
		FileSizeBytes:      int64(len(fileBytes)),
		RecordCount:        nodeCount,
		UpdateFrequency:    "on-demand",
		RetentionPolicy:    "indefinite",
		AccessControl:      `{"read":["consortium"],"write":["admin","operator"]}`,
		ComplianceTags:     "EU-Horizon,FAIR-Data",
		ProvenanceChain:    string(provenanceJSON),
		ProcessingStatus:   "published",
		ApiEndpoint:        apiEndpoint,
		Version:            templateVersion,
		ParentID:           "",
		ExpiryDate:         "",
		Language:           "en",
		LicenseType:        "consortium-only",
		ContactInfo:        contactInfo,
		NodeCount:          nodeCount,
		IpfsCID:            ipfsPath,
		SourceAgent:        agentName,
		SourcePod:          sourcePod,
		SourceIP:           sourceIP,
	}
}

// =============================================================================
// SQLITE SCHEMA — Extended metadata_catalog (48 columns)
// =============================================================================
// Usage in app.go — replace the body of CreateMetadataCatalogTable():
//
//   func (kb *KnowledgeBaseSQLite) CreateMetadataCatalogTable() error {
//       _, err := kb.DB.Exec(datamodel.ExtendedMetadataCatalogSQL())
//       if err != nil { return err }
//       logger.Info("[INFO] Table `metadata_catalog` (48 columns) created or already exists.")
//       return nil
//   }

func ExtendedMetadataCatalogSQL() string {
	return `
        CREATE TABLE IF NOT EXISTS metadata_catalog (
            -- ═══════════════════════════════════════════════════════════════
            -- ORIGINAL COLUMNS (20)
            -- ═══════════════════════════════════════════════════════════════
            id TEXT PRIMARY KEY,
            author TEXT,
            metadata_type TEXT,
            component TEXT,
            behaviour TEXT,
            relationships TEXT,
            associated_id TEXT,
            name TEXT,
            description TEXT,
            tags TEXT,
            status TEXT,
            created_by TEXT,
            created_at TEXT,
            updated_at TEXT,
            related_ids TEXT,
            priority TEXT,
            scheduling_info TEXT,
            sla_constraints TEXT,
            ownership_details TEXT,
            audit_trail TEXT,

            -- ═══════════════════════════════════════════════════════════════
            -- NEW: Data Classification & Domain (5)
            -- ═══════════════════════════════════════════════════════════════
            data_domain TEXT DEFAULT 'general',
            data_classification TEXT DEFAULT 'consortium',
            geo_location TEXT,
            temporal_coverage TEXT,
            data_quality_score REAL DEFAULT 0.0,

            -- ═══════════════════════════════════════════════════════════════
            -- NEW: Content Identity & Format (5)
            -- ═══════════════════════════════════════════════════════════════
            schema_version TEXT DEFAULT '2.0.0',
            content_hash TEXT,
            file_format TEXT,
            file_size_bytes INTEGER DEFAULT 0,
            record_count INTEGER DEFAULT 0,

            -- ═══════════════════════════════════════════════════════════════
            -- NEW: Lifecycle & Governance (6)
            -- ═══════════════════════════════════════════════════════════════
            update_frequency TEXT DEFAULT 'on-demand',
            retention_policy TEXT DEFAULT 'indefinite',
            access_control TEXT,
            compliance_tags TEXT,
            provenance_chain TEXT,
            processing_status TEXT DEFAULT 'raw',

            -- ═══════════════════════════════════════════════════════════════
            -- NEW: Access & Versioning (4)
            -- ═══════════════════════════════════════════════════════════════
            api_endpoint TEXT,
            version TEXT,
            parent_id TEXT,
            expiry_date TEXT,

            -- ═══════════════════════════════════════════════════════════════
            -- NEW: Contextual & Infrastructure (8)
            -- ═══════════════════════════════════════════════════════════════
            language TEXT DEFAULT 'en',
            license_type TEXT,
            contact_info TEXT,
            node_count INTEGER DEFAULT 0,
            ipfs_cid TEXT,
            source_agent TEXT,
            source_pod TEXT,
            source_ip TEXT
        );

        -- ═══════════════════════════════════════════════════════════════════
        -- INDEXES — Original
        -- ═══════════════════════════════════════════════════════════════════
        CREATE INDEX IF NOT EXISTS idx_metadata_type
            ON metadata_catalog(metadata_type);
        CREATE INDEX IF NOT EXISTS idx_metadata_created_at
            ON metadata_catalog(created_at);
        CREATE INDEX IF NOT EXISTS idx_metadata_associated_id
            ON metadata_catalog(associated_id);
        CREATE INDEX IF NOT EXISTS idx_metadata_status
            ON metadata_catalog(status);

        -- ═══════════════════════════════════════════════════════════════════
        -- INDEXES — New
        -- ═══════════════════════════════════════════════════════════════════
        CREATE INDEX IF NOT EXISTS idx_metadata_data_domain
            ON metadata_catalog(data_domain);
        CREATE INDEX IF NOT EXISTS idx_metadata_data_classification
            ON metadata_catalog(data_classification);
        CREATE INDEX IF NOT EXISTS idx_metadata_processing_status
            ON metadata_catalog(processing_status);
        CREATE INDEX IF NOT EXISTS idx_metadata_content_hash
            ON metadata_catalog(content_hash);
        CREATE INDEX IF NOT EXISTS idx_metadata_file_format
            ON metadata_catalog(file_format);
        CREATE INDEX IF NOT EXISTS idx_metadata_source_agent
            ON metadata_catalog(source_agent);
        CREATE INDEX IF NOT EXISTS idx_metadata_license_type
            ON metadata_catalog(license_type);
        CREATE INDEX IF NOT EXISTS idx_metadata_compliance_tags
            ON metadata_catalog(compliance_tags);
        CREATE INDEX IF NOT EXISTS idx_metadata_version
            ON metadata_catalog(version);
        CREATE INDEX IF NOT EXISTS idx_metadata_parent_id
            ON metadata_catalog(parent_id);
    `
}

// =============================================================================
// SQLITE MIGRATION — ALTER TABLE for existing databases
// =============================================================================
// Returns ALTER TABLE statements to add new columns.  Safe to call repeatedly;
// "duplicate column name" errors should be ignored by the caller.
//
// Usage in app.go:
//   func (kb *KnowledgeBaseSQLite) MigrateMetadataColumns() {
//       for _, stmt := range datamodel.MigrateMetadataCatalogSQL() {
//           _, err := kb.DB.Exec(stmt)
//           if err != nil && !strings.Contains(err.Error(), "duplicate column") {
//               logger.Warn("Migration: %v — %s", err, stmt)
//           }
//       }
//   }

func MigrateMetadataCatalogSQL() []string {
	cols := []struct{ Name, Def string }{
		{"author", "TEXT"},
		{"data_domain", "TEXT DEFAULT 'general'"},
		{"data_classification", "TEXT DEFAULT 'consortium'"},
		{"geo_location", "TEXT"},
		{"temporal_coverage", "TEXT"},
		{"data_quality_score", "REAL DEFAULT 0.0"},
		{"schema_version", "TEXT DEFAULT '2.0.0'"},
		{"content_hash", "TEXT"},
		{"file_format", "TEXT"},
		{"file_size_bytes", "INTEGER DEFAULT 0"},
		{"record_count", "INTEGER DEFAULT 0"},
		{"update_frequency", "TEXT DEFAULT 'on-demand'"},
		{"retention_policy", "TEXT DEFAULT 'indefinite'"},
		{"access_control", "TEXT"},
		{"compliance_tags", "TEXT"},
		{"provenance_chain", "TEXT"},
		{"processing_status", "TEXT DEFAULT 'raw'"},
		{"api_endpoint", "TEXT"},
		{"version", "TEXT"},
		{"parent_id", "TEXT"},
		{"expiry_date", "TEXT"},
		{"language", "TEXT DEFAULT 'en'"},
		{"license_type", "TEXT"},
		{"contact_info", "TEXT"},
		{"node_count", "INTEGER DEFAULT 0"},
		{"ipfs_cid", "TEXT"},
		{"source_agent", "TEXT"},
		{"source_pod", "TEXT"},
		{"source_ip", "TEXT"},
	}
	stmts := make([]string, 0, len(cols))
	for _, c := range cols {
		stmts = append(stmts,
			fmt.Sprintf("ALTER TABLE metadata_catalog ADD COLUMN %s %s;", c.Name, c.Def))
	}
	return stmts
}

// =============================================================================
// INTERNAL HELPERS
// =============================================================================

// extractNodeTypes returns the TOSCA type of every node template
func extractNodeTypes(toscaDoc map[string]interface{}) []string {
	types := []string{}
	if tt, ok := toscaDoc["topology_template"].(map[string]interface{}); ok {
		if nts, ok := tt["node_templates"].(map[string]interface{}); ok {
			for _, node := range nts {
				if nm, ok := node.(map[string]interface{}); ok {
					if t, ok := nm["type"].(string); ok {
						types = append(types, t)
					}
				}
			}
		}
	}
	return types
}

// extractNodeNames returns the keys of all node templates
func extractNodeNames(toscaDoc map[string]interface{}) []string {
	names := []string{}
	if tt, ok := toscaDoc["topology_template"].(map[string]interface{}); ok {
		if nts, ok := tt["node_templates"].(map[string]interface{}); ok {
			for name := range nts {
				names = append(names, name)
			}
		}
	}
	return names
}

// inferDomainFromTOSCA classifies the template's domain by scanning
// the description, node types, and tags for domain-specific keywords.
func inferDomainFromTOSCA(description string, nodeTypes []string, tags []string) string {
	combined := strings.ToLower(description + " " + strings.Join(nodeTypes, " ") + " " + strings.Join(tags, " "))

	domainKeywords := map[string][]string{
		"renewable_energy":    {"solar", "wind", "photovoltaic", "turbine", "inverter", "panel", "renewable", "energy"},
		"grid_management":     {"grid", "substation", "transformer", "transmission", "distribution", "balancing"},
		"energy_storage":      {"battery", "storage", "lithium", "charge", "discharge", "capacitor"},
		"weather":             {"weather", "meteorological", "forecast", "temperature", "humidity", "irradiance"},
		"iot_telemetry":       {"sensor", "telemetry", "mqtt", "iot", "monitoring", "scada"},
		"cloud_orchestration": {"kubernetes", "docker", "container", "orchestration", "deployment", "vm", "cloud"},
		"forecasting":         {"forecast", "prediction", "model", "ml", "machine_learning", "ai"},
	}

	bestDomain := "general"
	bestScore := 0
	for domain, keywords := range domainKeywords {
		score := 0
		for _, kw := range keywords {
			if strings.Contains(combined, kw) {
				score++
			}
		}
		if score > bestScore {
			bestScore = score
			bestDomain = domain
		}
	}
	return bestDomain
}

// extractTableFromSQL attempts to extract the table name from a SQL statement.
func extractTableFromSQL(sql string) string {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	if strings.HasPrefix(upper, "INSERT") {
		idx := strings.Index(upper, "INTO")
		if idx >= 0 {
			rest := strings.TrimSpace(sql[idx+4:])
			parts := strings.Fields(rest)
			if len(parts) > 0 {
				return strings.Trim(parts[0], "`\"'")
			}
		}
	}
	if strings.HasPrefix(upper, "UPDATE") {
		parts := strings.Fields(sql)
		if len(parts) > 1 {
			return strings.Trim(parts[1], "`\"'")
		}
	}
	if strings.HasPrefix(upper, "SELECT") {
		idx := strings.Index(upper, "FROM")
		if idx >= 0 {
			rest := strings.TrimSpace(sql[idx+4:])
			parts := strings.Fields(rest)
			if len(parts) > 0 {
				return strings.Trim(parts[0], "`\"'")
			}
		}
	}
	return "unknown"
}

// safeJSON marshals any value to a JSON string; returns "{}" on nil/error.
func safeJSON(v interface{}) string {
	if v == nil {
		return "{}"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
