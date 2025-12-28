package app

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"optimusdb/tosca"
	"strings"
)

// =============================================================================
// METADATA EXTRACTOR - Extracts metadata from any document type
// =============================================================================

type MetadataExtractor struct {
	KB *KnowledgeBaseDB
}

// DocumentMetadata represents extracted metadata from any document
type DocumentMetadata struct {
	ID              string                 // Unique identifier
	Name            string                 // Human-readable name
	MetadataType    string                 // Requirements/Capacity/Template/etc
	Component       string                 // Document type/category
	Description     string                 // Description text
	Tags            []string               // Searchable tags
	Statistics      map[string]interface{} // Custom stats (JSON)
	TableURI        string                 // optimusdb://default.Type/Name
	RawDocument     map[string]interface{} // Original document
	DetectedRefs    []string               // References to other documents
	ContentType     string                 // tosca/json/sql/etc
	SourceDatastore string                 // Which OrbitDB store
}

// ExtractMetadata extracts metadata from a document based on its type
func (me *MetadataExtractor) ExtractMetadata(
	doc map[string]interface{},
	dstype string,
) (*DocumentMetadata, error) {

	// Detect content type and delegate to appropriate extractor
	if isTOSCA(doc) {
		return me.extractTOSCAMetadata(doc, dstype)
	} else if isSQL(doc) {
		return me.extractSQLMetadata(doc, dstype)
	} else {
		return me.extractGenericMetadata(doc, dstype)
	}
}

// =============================================================================
// TOSCA-SPECIFIC EXTRACTION
// =============================================================================

func (me *MetadataExtractor) extractTOSCAMetadata(
	doc map[string]interface{},
	dstype string,
) (*DocumentMetadata, error) {

	meta := &DocumentMetadata{
		RawDocument:     doc,
		SourceDatastore: dstype,
		ContentType:     "tosca",
		Tags:            []string{"tosca"},
		Statistics:      make(map[string]interface{}),
	}

	// Extract template name from metadata section
	templateName := "unknown"
	if metadata, ok := doc["metadata"].(map[string]interface{}); ok {
		if name, ok := metadata["template_name"].(string); ok {
			templateName = name
		}
	}
	meta.Name = sanitizeName(templateName)

	// Extract document_type to determine metadata_type
	docType := extractNestedString(doc, "metadata.document_type")
	if docType == "" {
		docType = "tosca_template"
	}
	meta.Component = docType

	// Map document_type to metadata_type
	meta.MetadataType = mapTOSCATypeToMetadataType(docType)

	// Build table URI
	meta.TableURI = fmt.Sprintf("optimusdb://default.%s/%s",
		meta.MetadataType, meta.Name)

	// Generate unique ID
	meta.ID = generateDocumentID(doc)

	// Extract description
	if desc, ok := doc["description"].(string); ok {
		meta.Description = desc
	} else if metadata, ok := doc["metadata"].(map[string]interface{}); ok {
		if desc, ok := metadata["description"].(string); ok {
			meta.Description = desc
		}
	}
	if meta.Description == "" {
		meta.Description = fmt.Sprintf("TOSCA template: %s", meta.Name)
	}

	// Extract node templates count
	nodeCount := tosca.CountNodeTemplatesFromJSON(doc)
	meta.Statistics["node_count"] = nodeCount

	// Extract node types
	nodeTypes := tosca.GetAllNodeTypes(doc)
	meta.Statistics["node_types"] = nodeTypes

	// Extract groups/policies
	if groups, exists := tosca.GetGroups(doc); exists {
		meta.Statistics["group_count"] = len(groups)
	}
	if policies, exists := tosca.GetPolicies(doc); exists {
		meta.Statistics["policy_count"] = len(policies)
	}

	// Extract TOSCA version
	if version, ok := doc["tosca_definitions_version"].(string); ok {
		meta.Statistics["tosca_version"] = version
	}

	// Build tags
	meta.Tags = append(meta.Tags, docType, meta.MetadataType)
	if filename, ok := doc["_filename"].(string); ok {
		meta.Tags = append(meta.Tags, filename)
		meta.Statistics["filename"] = filename
	}

	// Detect references to other templates
	meta.DetectedRefs = me.detectTOSCAReferences(doc)

	return meta, nil
}

// detectTOSCAReferences finds references to other TOSCA templates
func (me *MetadataExtractor) detectTOSCAReferences(doc map[string]interface{}) []string {
	refs := []string{}

	// Extract from requirements
	if topology, ok := doc["topology_template"].(map[string]interface{}); ok {
		if nodes, ok := topology["node_templates"].(map[string]interface{}); ok {
			for _, nodeData := range nodes {
				if node, ok := nodeData.(map[string]interface{}); ok {
					if reqs, ok := node["requirements"].([]interface{}); ok {
						for _, req := range reqs {
							if reqMap, ok := req.(map[string]interface{}); ok {
								for _, val := range reqMap {
									if reqDef, ok := val.(map[string]interface{}); ok {
										if nodeName, ok := reqDef["node"].(string); ok {
											refs = append(refs, nodeName)
										}
									} else if nodeStr, ok := val.(string); ok {
										refs = append(refs, nodeStr)
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return refs
}

// =============================================================================
// GENERIC JSON EXTRACTION
// =============================================================================

func (me *MetadataExtractor) extractGenericMetadata(
	doc map[string]interface{},
	dstype string,
) (*DocumentMetadata, error) {

	meta := &DocumentMetadata{
		RawDocument:     doc,
		SourceDatastore: dstype,
		ContentType:     "json",
		Tags:            []string{"generic"},
		Statistics:      make(map[string]interface{}),
	}

	// Try to extract common fields
	meta.ID = extractID(doc)
	meta.Name = extractName(doc)
	meta.Description = extractDescription(doc)
	meta.MetadataType = "Document"
	meta.Component = extractType(doc)

	// Build table URI
	meta.TableURI = fmt.Sprintf("optimusdb://default.%s/%s",
		meta.MetadataType, meta.Name)

	// Count fields
	meta.Statistics["field_count"] = countFields(doc)
	meta.Statistics["datastore"] = dstype

	// Detect references
	meta.DetectedRefs = detectJSONReferences(doc)

	// Build tags from keys
	for key := range doc {
		if !strings.HasPrefix(key, "_") && len(meta.Tags) < 10 {
			meta.Tags = append(meta.Tags, key)
		}
	}

	return meta, nil
}

// =============================================================================
// SQL EXTRACTION
// =============================================================================

func (me *MetadataExtractor) extractSQLMetadata(
	doc map[string]interface{},
	dstype string,
) (*DocumentMetadata, error) {

	meta := &DocumentMetadata{
		RawDocument:     doc,
		SourceDatastore: dstype,
		ContentType:     "sql",
		Tags:            []string{"sql", "table"},
		Statistics:      make(map[string]interface{}),
	}

	// Extract table name
	if tableName, ok := doc["table_name"].(string); ok {
		meta.Name = tableName
	} else {
		meta.Name = "unknown_table"
	}

	meta.MetadataType = "Table"
	meta.Component = "sql_table"
	meta.Description = fmt.Sprintf("SQL table: %s", meta.Name)

	// Build URI
	meta.TableURI = fmt.Sprintf("optimusdb://default.Table/%s", meta.Name)

	// Extract schema info
	if columns, ok := doc["columns"].([]interface{}); ok {
		meta.Statistics["column_count"] = len(columns)
		meta.Statistics["columns"] = columns
	}

	// Extract row count
	if rowCount, ok := doc["row_count"].(int); ok {
		meta.Statistics["row_count"] = rowCount
	}

	meta.ID = generateDocumentID(doc)
	meta.DetectedRefs = []string{} // SQL tables don't have doc refs typically

	return meta, nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func isTOSCA(doc map[string]interface{}) bool {
	// Check for TOSCA-specific fields
	_, hasTopology := doc["topology_template"]
	_, hasTOSCAVersion := doc["tosca_definitions_version"]
	_, hasMetadata := doc["metadata"]

	return hasTopology || hasTOSCAVersion || hasMetadata
}

func isSQL(doc map[string]interface{}) bool {
	_, hasTable := doc["table_name"]
	_, hasColumns := doc["columns"]
	return hasTable && hasColumns
}

func sanitizeName(name string) string {
	// Replace spaces and special chars with underscores
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	return name
}

func extractNestedString(obj map[string]interface{}, path string) string {
	parts := strings.Split(path, ".")
	var current interface{} = obj

	for _, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			current = m[part]
		} else {
			return ""
		}
	}

	if str, ok := current.(string); ok {
		return str
	}
	return ""
}

func mapTOSCATypeToMetadataType(docType string) string {
	mapping := map[string]string{
		"application_requirements":        "Requirements",
		"capacity_description":            "Capacity",
		"opentofu_tosca_template":         "Infrastructure",
		"application_deployment_template": "Application",
		"deployment_release_plan":         "Deployment",
		"tosca_template":                  "Template",
	}

	if metaType, ok := mapping[docType]; ok {
		return metaType
	}
	return "Template"
}

func generateDocumentID(doc map[string]interface{}) string {
	// Try existing _id first
	if id, ok := doc["_id"].(string); ok && id != "" {
		return id
	}

	// Generate from content hash
	data, _ := json.Marshal(doc)
	hash := sha256.Sum256(data)
	return fmt.Sprintf("doc_%x", hash[:8])
}

func extractID(doc map[string]interface{}) string {
	// Check common ID fields
	fields := []string{"_id", "id", "ID", "identifier", "key"}
	for _, field := range fields {
		if val, ok := doc[field].(string); ok && val != "" {
			return val
		}
	}
	return generateDocumentID(doc)
}

func extractName(doc map[string]interface{}) string {
	// Check common name fields
	fields := []string{"name", "Name", "title", "Title", "_id"}
	for _, field := range fields {
		if val, ok := doc[field].(string); ok && val != "" {
			return sanitizeName(val)
		}
	}
	return "unknown"
}

func extractDescription(doc map[string]interface{}) string {
	fields := []string{"description", "Description", "desc", "summary"}
	for _, field := range fields {
		if val, ok := doc[field].(string); ok && val != "" {
			return val
		}
	}
	return "No description"
}

func extractType(doc map[string]interface{}) string {
	fields := []string{"type", "Type", "_type", "document_type"}
	for _, field := range fields {
		if val, ok := doc[field].(string); ok && val != "" {
			return val
		}
	}
	return "document"
}

func countFields(doc map[string]interface{}) int {
	count := 0
	for key := range doc {
		if !strings.HasPrefix(key, "_") {
			count++
		}
	}
	return count
}

func detectJSONReferences(doc map[string]interface{}) []string {
	refs := []string{}

	// Look for common reference patterns
	refFields := []string{"ref", "reference", "references", "depends_on",
		"parent", "child", "related_to"}

	for _, field := range refFields {
		if val, ok := doc[field].(string); ok && val != "" {
			refs = append(refs, val)
		} else if arr, ok := doc[field].([]interface{}); ok {
			for _, item := range arr {
				if str, ok := item.(string); ok {
					refs = append(refs, str)
				}
			}
		}
	}

	return refs
}
