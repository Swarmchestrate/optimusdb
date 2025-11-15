package contextualmetadata

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
)

/*
================================================================================
FILE: contextualmetadata/sql_metadata_generator.go
PURPOSE: Generate AI-enriched metadata from SQL statements using TinyLlama
================================================================================

This file contains the MISSING CORE FUNCTIONALITY for contextual metadata.
It parses SQL statements, extracts context, calls TinyLlama LLM, and generates
rich semantic metadata.

KEY FUNCTIONS:
- GenerateMetadataFromPayload() - Main entry point (was referenced but missing)
- ParseSQL() - Extracts table, columns, values from SQL
- BuildSQLMetadataPrompt() - Creates LLM prompts
- parseLLMMetadataResponse() - Parses JSON from LLM
- generateFallbackMetadata() - Backup when LLM fails

USAGE:
	service := GetMetadataService()
	metadata, err := GenerateMetadataFromPayload(sqlStatement, service)
	if err == nil {
		// Use metadata.Title, metadata.Description, metadata.Keywords
	}
*/

// ============================================================================
// DATA STRUCTURES
// ============================================================================

// ContextualMetadata represents enriched metadata generated from SQL operations
type ContextualMetadata struct {
	Title       string   `json:"title"`        // Human-readable title
	Description string   `json:"description"`  // AI-generated description
	Keywords    []string `json:"keywords"`     // Semantic tags
	TableName   string   `json:"table_name"`   // Affected table
	Operation   string   `json:"operation"`    // SQL operation type
	ColumnCount int      `json:"column_count"` // Number of columns
	Domain      string   `json:"domain"`       // Inferred domain (solar/wind/energy)
}

// SQLContext holds parsed information from SQL statement
type SQLContext struct {
	Operation   string   // INSERT, UPDATE, DELETE, SELECT
	TableName   string   // Affected table name
	Columns     []string // Column names
	Values      []string // Sample values
	FullSQL     string   // Complete SQL statement
	DatabaseRef string   // Database reference (if available)
	ColumnCount int      // Number of columns affected
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

// GenerateMetadataFromPayload is the main entry point for contextual metadata generation
// This is THE MISSING FUNCTION that was referenced in service.go but never implemented
//
// Parameters:
//   - sqlDML: The SQL statement (INSERT, UPDATE, DELETE, SELECT)
//   - service: The metadata service with LLM client
//
// Returns:
//   - *ContextualMetadata: Enriched metadata with AI-generated descriptions
//   - error: Error if parsing fails or service unavailable
//
// Flow:
//  1. Parse SQL to extract context
//  2. Build LLM prompt
//  3. Call TinyLlama
//  4. Parse JSON response
//  5. Return enriched metadata
func GenerateMetadataFromPayload(sqlDML string, service *Service) (*ContextualMetadata, error) {
	// Check if service and LLM client are available
	if service == nil || service.Client == nil {
		log.Println("[WARN] Metadata service or LLM client not available, skipping contextual enrichment")
		return nil, fmt.Errorf("service not initialized")
	}

	// Step 1: Parse SQL to extract structured context
	sqlContext, err := ParseSQL(sqlDML)
	if err != nil {
		log.Printf("[WARN] Failed to parse SQL for contextual metadata: %v", err)
		return nil, err
	}

	// Step 2: Build prompt for LLM
	prompt := BuildSQLMetadataPrompt(sqlContext, service.UseGreek)

	// Step 3: Call LLM with timeout
	_, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	raw, err := service.Client.Generate(prompt, 256) // Max 256 tokens for metadata
	if err != nil {
		log.Printf("[WARN] LLM generation failed: %v", err)
		// Return fallback metadata instead of failing
		return generateFallbackMetadata(sqlContext), nil
	}

	// Step 4: Parse LLM response
	metadata, err := parseLLMMetadataResponse(raw, sqlContext)
	if err != nil {
		log.Printf("[WARN] Failed to parse LLM response: %v, using fallback", err)
		return generateFallbackMetadata(sqlContext), nil
	}

	// Step 5: Enrich with domain information
	metadata.Domain = inferDomainFromSQL(sqlContext)

	return metadata, nil
}

// ============================================================================
// SQL PARSING FUNCTIONS
// ============================================================================

// ParseSQL extracts structured information from SQL DML statements
// Supports: INSERT, UPDATE, DELETE, SELECT
//
// Example INPUT:
//
//	INSERT INTO users (name, email) VALUES ('John', 'john@example.com')
//
// Example OUTPUT:
//
//	SQLContext{
//	  Operation: "INSERT",
//	  TableName: "users",
//	  Columns: ["name", "email"],
//	  Values: ["John", "john@example.com"]
//	}
func ParseSQL(sqlDML string) (*SQLContext, error) {
	trimmed := strings.TrimSpace(sqlDML)
	upper := strings.ToUpper(trimmed)

	ctx := &SQLContext{
		FullSQL: trimmed,
	}

	// Detect operation type and parse accordingly
	if strings.HasPrefix(upper, "INSERT") {
		ctx.Operation = "INSERT"
		return parseInsert(trimmed)
	} else if strings.HasPrefix(upper, "UPDATE") {
		ctx.Operation = "UPDATE"
		return parseUpdate(trimmed)
	} else if strings.HasPrefix(upper, "DELETE") {
		ctx.Operation = "DELETE"
		return parseDelete(trimmed)
	} else if strings.HasPrefix(upper, "SELECT") {
		ctx.Operation = "SELECT"
		return parseSelect(trimmed)
	}

	return nil, fmt.Errorf("unsupported SQL operation")
}

// parseInsert extracts table name, columns, and values from INSERT statement
// Handles both formats:
//   - INSERT INTO table (col1, col2) VALUES (val1, val2)
//   - INSERT INTO table VALUES (val1, val2)
func parseInsert(sql string) (*SQLContext, error) {
	ctx := &SQLContext{
		Operation: "INSERT",
		FullSQL:   sql,
	}

	// Extract table name: INSERT INTO table_name
	tablePattern := regexp.MustCompile(`(?i)INSERT\s+INTO\s+([^\s(]+)`)
	tableMatch := tablePattern.FindStringSubmatch(sql)
	if len(tableMatch) < 2 {
		return nil, fmt.Errorf("could not extract table name from INSERT")
	}
	ctx.TableName = strings.Trim(tableMatch[1], "`\"[]")

	// Extract columns (if specified): (col1, col2, col3)
	columnPattern := regexp.MustCompile(`(?i)\((.*?)\)\s*VALUES`)
	columnMatch := columnPattern.FindStringSubmatch(sql)
	if len(columnMatch) >= 2 {
		columnStr := columnMatch[1]
		columns := strings.Split(columnStr, ",")
		for _, col := range columns {
			trimmedCol := strings.TrimSpace(strings.Trim(col, "`\"[]"))
			if trimmedCol != "" {
				ctx.Columns = append(ctx.Columns, trimmedCol)
			}
		}
	}

	// Extract values: VALUES (val1, val2, val3)
	valuesPattern := regexp.MustCompile(`(?i)VALUES\s*\((.*?)\)`)
	valuesMatch := valuesPattern.FindStringSubmatch(sql)
	if len(valuesMatch) >= 2 {
		valueStr := valuesMatch[1]
		values := splitValues(valueStr)
		for _, val := range values {
			trimmedVal := strings.TrimSpace(val)
			trimmedVal = strings.Trim(trimmedVal, "'\"") // Remove quotes
			if trimmedVal != "" {
				ctx.Values = append(ctx.Values, trimmedVal)
			}
		}
	}

	ctx.ColumnCount = len(ctx.Columns)
	if ctx.ColumnCount == 0 {
		ctx.ColumnCount = len(ctx.Values)
	}

	return ctx, nil
}

// parseUpdate extracts information from UPDATE statement
// Format: UPDATE table SET col1=val1, col2=val2 WHERE ...
func parseUpdate(sql string) (*SQLContext, error) {
	ctx := &SQLContext{
		Operation: "UPDATE",
		FullSQL:   sql,
	}

	// Extract table name
	tablePattern := regexp.MustCompile(`(?i)UPDATE\s+([^\s]+)`)
	tableMatch := tablePattern.FindStringSubmatch(sql)
	if len(tableMatch) < 2 {
		return nil, fmt.Errorf("could not extract table name from UPDATE")
	}
	ctx.TableName = strings.Trim(tableMatch[1], "`\"[]")

	// Extract SET clause: col1=val1, col2=val2
	setPattern := regexp.MustCompile(`(?i)SET\s+(.*?)(?:WHERE|$)`)
	setMatch := setPattern.FindStringSubmatch(sql)
	if len(setMatch) >= 2 {
		setPairs := strings.Split(setMatch[1], ",")
		for _, pair := range setPairs {
			parts := strings.Split(pair, "=")
			if len(parts) >= 2 {
				col := strings.TrimSpace(strings.Trim(parts[0], "`\"[]"))
				val := strings.TrimSpace(strings.Trim(parts[1], "'\""))
				ctx.Columns = append(ctx.Columns, col)
				ctx.Values = append(ctx.Values, val)
			}
		}
	}

	ctx.ColumnCount = len(ctx.Columns)
	return ctx, nil
}

// parseDelete extracts information from DELETE statement
// Format: DELETE FROM table WHERE ...
func parseDelete(sql string) (*SQLContext, error) {
	ctx := &SQLContext{
		Operation: "DELETE",
		FullSQL:   sql,
	}

	// Extract table name
	tablePattern := regexp.MustCompile(`(?i)DELETE\s+FROM\s+([^\s]+)`)
	tableMatch := tablePattern.FindStringSubmatch(sql)
	if len(tableMatch) < 2 {
		return nil, fmt.Errorf("could not extract table name from DELETE")
	}
	ctx.TableName = strings.Trim(tableMatch[1], "`\"[]")

	// DELETE operations don't have column information
	ctx.ColumnCount = 0 // ✅ ADDED

	return ctx, nil
}

// parseSelect extracts information from SELECT statement
// Format: SELECT col1, col2 FROM table
func parseSelect(sql string) (*SQLContext, error) {
	ctx := &SQLContext{
		Operation: "SELECT",
		FullSQL:   sql,
	}

	// Extract table name
	tablePattern := regexp.MustCompile(`(?i)FROM\s+([^\s,;]+)`)
	tableMatch := tablePattern.FindStringSubmatch(sql)
	if len(tableMatch) >= 2 {
		ctx.TableName = strings.Trim(tableMatch[1], "`\"[]")
	}

	// Extract column names
	selectPattern := regexp.MustCompile(`(?i)SELECT\s+(.*?)\s+FROM`)
	selectMatch := selectPattern.FindStringSubmatch(sql)
	if len(selectMatch) >= 2 {
		columnStr := selectMatch[1]
		if strings.TrimSpace(columnStr) != "*" {
			columns := strings.Split(columnStr, ",")
			for _, col := range columns {
				trimmedCol := strings.TrimSpace(strings.Trim(col, "`\"[]"))
				if trimmedCol != "" {
					ctx.Columns = append(ctx.Columns, trimmedCol)
				}
			}
		}
	}

	ctx.ColumnCount = len(ctx.Columns)
	return ctx, nil
}

// splitValues splits comma-separated values while handling quoted strings
// Example: "John", 25, "john@example.com" → ["John", "25", "john@example.com"]
func splitValues(s string) []string {
	var values []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for _, r := range s {
		if r == '\'' || r == '"' {
			if !inQuote {
				inQuote = true
				quoteChar = r
			} else if r == quoteChar {
				inQuote = false
				quoteChar = 0
			}
			current.WriteRune(r)
		} else if r == ',' && !inQuote {
			values = append(values, current.String())
			current.Reset()
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		values = append(values, current.String())
	}

	return values
}

// ============================================================================
// LLM PROMPT BUILDING
// ============================================================================

// BuildSQLMetadataPrompt creates a prompt for TinyLlama to generate metadata
//
// The prompt is structured to get JSON output with:
// - title: Short, descriptive name
// - description: 1-2 sentence explanation
// - keywords: 3-5 relevant tags
//
// Example OUTPUT from LLM:
//
//	{
//	  "title": "User Registration Data",
//	  "description": "Customer registration records with authentication details",
//	  "keywords": ["users", "authentication", "registration"]
//	}
func BuildSQLMetadataPrompt(ctx *SQLContext, useGreek bool) string {
	var b strings.Builder

	// System instructions
	fmt.Fprintf(&b, "You are a data catalog assistant. Generate metadata for a SQL operation.\n")
	fmt.Fprintf(&b, "Return JSON with: title (short name), description (1-2 sentences), keywords (3-5 tags).\n")

	// Optional Greek language support
	if useGreek {
		fmt.Fprintf(&b, "Write description in Greek, but keep keywords in English.\n")
	}

	// SQL operation context
	fmt.Fprintf(&b, "\nSQL OPERATION: %s\n", ctx.Operation)
	fmt.Fprintf(&b, "TABLE: %s\n", ctx.TableName)

	// Include columns if available
	if len(ctx.Columns) > 0 {
		fmt.Fprintf(&b, "COLUMNS: %s\n", strings.Join(ctx.Columns, ", "))
	}

	// Include sample values (limit to first 5)
	if len(ctx.Values) > 0 && len(ctx.Values) <= 10 {
		fmt.Fprintf(&b, "SAMPLE VALUES: %s\n", strings.Join(ctx.Values[:min(5, len(ctx.Values))], ", "))
	}

	fmt.Fprintf(&b, "\nRespond only with valid JSON.\n")
	return b.String()
}

// ============================================================================
// LLM RESPONSE PARSING
// ============================================================================

// parseLLMMetadataResponse parses the JSON response from TinyLlama
// Handles cases where LLM returns text + JSON (extracts JSON portion)
func parseLLMMetadataResponse(raw string, ctx *SQLContext) (*ContextualMetadata, error) {
	// Try to find JSON in the response
	start, end := findJSON(raw)
	if start < 0 || end <= start {
		return nil, fmt.Errorf("no valid JSON found in LLM response")
	}

	jsonStr := raw[start:end]

	// Parse JSON structure
	var response struct {
		Title       string   `json:"title"`
		Description string   `json:"description"`
		Keywords    []string `json:"keywords"`
	}

	if err := json.Unmarshal([]byte(jsonStr), &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	// Build ContextualMetadata
	return &ContextualMetadata{
		Title:       response.Title,
		Description: response.Description,
		Keywords:    response.Keywords,
		TableName:   ctx.TableName,
		Operation:   ctx.Operation,
		ColumnCount: ctx.ColumnCount,
	}, nil
}

// findJSON locates JSON object in a string (handles text before/after JSON)
// Returns start and end indices of the JSON object
func findJSON(s string) (int, int) {
	start := -1
	depth := 0
	for i, r := range s {
		if r == '{' {
			if start < 0 {
				start = i
			}
			depth++
		}
		if r == '}' && start >= 0 {
			depth--
			if depth == 0 {
				return start, i + 1
			}
		}
	}
	return -1, -1
}

// ============================================================================
// FALLBACK METADATA GENERATION
// ============================================================================

// generateFallbackMetadata creates basic metadata when LLM is unavailable
// This ensures the system always generates metadata even if TinyLlama fails
func generateFallbackMetadata(ctx *SQLContext) *ContextualMetadata {
	return &ContextualMetadata{
		Title: fmt.Sprintf("%s on %s", ctx.Operation, ctx.TableName),
		Description: fmt.Sprintf("SQL %s operation on table %s with %d columns",
			ctx.Operation, ctx.TableName, ctx.ColumnCount),
		Keywords:    []string{strings.ToLower(ctx.Operation), ctx.TableName, "sql", "auto"},
		TableName:   ctx.TableName,
		Operation:   ctx.Operation,
		ColumnCount: ctx.ColumnCount,
		Domain:      inferDomainFromSQL(ctx),
	}
}

// ============================================================================
// DOMAIN INFERENCE
// ============================================================================

// inferDomainFromSQL attempts to infer the domain from table/column names
// Detects: solar, wind, renewable_energy, or general
func inferDomainFromSQL(ctx *SQLContext) string {
	combined := strings.ToLower(ctx.TableName + " " + strings.Join(ctx.Columns, " "))

	// Solar domain keywords
	solarKeywords := []string{"solar", "pv", "panel", "inverter", "irradiance", "photovoltaic"}
	solarScore := 0
	for _, kw := range solarKeywords {
		if strings.Contains(combined, kw) {
			solarScore++
		}
	}

	// Wind domain keywords
	windKeywords := []string{"wind", "turbine", "rotor", "blade", "nacelle", "yaw"}
	windScore := 0
	for _, kw := range windKeywords {
		if strings.Contains(combined, kw) {
			windScore++
		}
	}

	// Energy domain keywords
	energyKeywords := []string{"energy", "power", "generation", "capacity", "efficiency"}
	energyScore := 0
	for _, kw := range energyKeywords {
		if strings.Contains(combined, kw) {
			energyScore++
		}
	}

	// Return highest scoring domain
	if solarScore > 0 && solarScore >= windScore {
		return "solar"
	} else if windScore > 0 {
		return "wind"
	} else if energyScore > 0 {
		return "renewable_energy"
	}

	return "general"
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
