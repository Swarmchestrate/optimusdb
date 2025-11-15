package contextualmetadata

import (
	"database/sql"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ColumnProfile holds inferred semantics & stats.
type ColumnProfile struct {
	Name          string
	SampleValues  []string
	InferredType  string // int, float, bool, date, datetime, string, json, categorical
	NullRatio     float64
	Cardinality   int
	ExampleValues []string
	Min, Max      *string // if numeric/date summarized as strings
	AvgLength     float64
	Entropy       float64
	IsIdentifier  bool
	IsTimestamp   bool
	IsGeo         bool
	IsCodeLike    bool // SKU, ID-like patterns
}

type DatasetProfile struct {
	DB       string
	Table    string
	RowCount int
	Profiles []ColumnProfile
}

// ProfileTable analyzes a table and generates comprehensive column profiles
func ProfileTable(dbName, table string, maxRows int) (*DatasetProfile, error) {
	// Open the SQLite database
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Get column names
	columns, err := getColumnNames(db, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Get row count
	rowCount, err := getRowCount(db, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get row count: %w", err)
	}

	// Sample rows for analysis
	sampleRows, err := sampleTableData(db, table, columns, maxRows)
	if err != nil {
		return nil, fmt.Errorf("failed to sample data: %w", err)
	}

	// Profile each column
	profiles := make([]ColumnProfile, len(columns))
	for i, colName := range columns {
		profiles[i] = profileColumn(colName, sampleRows, i, rowCount)
	}

	return &DatasetProfile{
		DB:       dbName,
		Table:    table,
		RowCount: rowCount,
		Profiles: profiles,
	}, nil
}

// getColumnNames retrieves column names from table
func getColumnNames(db *sql.DB, table string) ([]string, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", table)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var dfltValue sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, nil
}

// getRowCount counts total rows in table
func getRowCount(db *sql.DB, table string) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	var count int
	err := db.QueryRow(query).Scan(&count)
	return count, err
}

// sampleTableData retrieves sample rows for profiling
func sampleTableData(db *sql.DB, table string, columns []string, maxRows int) ([][]string, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", table, maxRows)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data [][]string
	for rows.Next() {
		// Create slice for column values
		values := make([]sql.NullString, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Convert to string slice
		row := make([]string, len(columns))
		for i, v := range values {
			if v.Valid {
				row[i] = v.String
			} else {
				row[i] = ""
			}
		}
		data = append(data, row)
	}

	return data, nil
}

// profileColumn analyzes a single column
func profileColumn(name string, data [][]string, colIndex int, totalRows int) ColumnProfile {
	profile := ColumnProfile{
		Name:         name,
		SampleValues: []string{},
	}

	values := extractColumnValues(data, colIndex)

	// Basic statistics
	profile.NullRatio = calculateNullRatio(values)
	profile.Cardinality = calculateCardinality(values)
	profile.AvgLength = calculateAvgLength(values)
	profile.Entropy = calculateEntropy(values)

	// Type inference
	profile.InferredType = inferType(values)

	// Get example values (first 5 non-null distinct)
	profile.ExampleValues = getExampleValues(values, 5)

	// Heuristic flags
	profile.IsIdentifier = isIdentifierColumn(name, values, profile.Cardinality, totalRows)
	profile.IsTimestamp = isTimestampColumn(name, values)
	profile.IsGeo = isGeoColumn(name, values)
	profile.IsCodeLike = isCodeLikeColumn(values)

	// Min/Max for numeric columns
	if profile.InferredType == "int" || profile.InferredType == "float" {
		min, max := getNumericMinMax(values)
		profile.Min = &min
		profile.Max = &max
	}

	return profile
}

// extractColumnValues extracts all values for a specific column
func extractColumnValues(data [][]string, colIndex int) []string {
	values := make([]string, len(data))
	for i, row := range data {
		if colIndex < len(row) {
			values[i] = row[colIndex]
		}
	}
	return values
}

// calculateNullRatio calculates percentage of null/empty values
func calculateNullRatio(values []string) float64 {
	if len(values) == 0 {
		return 0
	}
	nullCount := 0
	for _, v := range values {
		if v == "" {
			nullCount++
		}
	}
	return float64(nullCount) / float64(len(values))
}

// calculateCardinality counts distinct non-null values
func calculateCardinality(values []string) int {
	seen := make(map[string]bool)
	for _, v := range values {
		if v != "" {
			seen[v] = true
		}
	}
	return len(seen)
}

// calculateAvgLength calculates average string length
func calculateAvgLength(values []string) float64 {
	if len(values) == 0 {
		return 0
	}
	totalLen := 0
	for _, v := range values {
		totalLen += len(v)
	}
	return float64(totalLen) / float64(len(values))
}

// calculateEntropy calculates Shannon entropy
func calculateEntropy(values []string) float64 {
	if len(values) == 0 {
		return 0
	}

	freq := make(map[string]int)
	for _, v := range values {
		if v != "" {
			freq[v]++
		}
	}

	entropy := 0.0
	total := float64(len(values))
	for _, count := range freq {
		p := float64(count) / total
		if p > 0 {
			entropy -= p * math.Log2(p)
		}
	}
	return entropy
}

// inferType infers the data type of the column
func inferType(values []string) string {
	if len(values) == 0 {
		return "string"
	}

	// Count successful parses
	intCount := 0
	floatCount := 0
	boolCount := 0
	dateCount := 0
	jsonCount := 0

	for _, v := range values {
		if v == "" {
			continue
		}

		// Try integer
		if _, err := strconv.ParseInt(v, 10, 64); err == nil {
			intCount++
			continue
		}

		// Try float
		if _, err := strconv.ParseFloat(v, 64); err == nil {
			floatCount++
			continue
		}

		// Try bool
		vLower := strings.ToLower(strings.TrimSpace(v))
		if vLower == "true" || vLower == "false" || vLower == "t" || vLower == "f" ||
			vLower == "yes" || vLower == "no" || vLower == "1" || vLower == "0" {
			boolCount++
			continue
		}

		// Try date/datetime
		if isDateFormat(v) {
			dateCount++
			continue
		}

		// Try JSON
		if (strings.HasPrefix(v, "{") && strings.HasSuffix(v, "}")) ||
			(strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]")) {
			jsonCount++
		}
	}

	// Determine type based on majority
	total := len(values)
	threshold := int(float64(total) * 0.8) // 80% threshold

	if intCount >= threshold {
		return "int"
	}
	if floatCount >= threshold {
		return "float"
	}
	if boolCount >= threshold {
		return "bool"
	}
	if dateCount >= threshold {
		if containsTime(values) {
			return "datetime"
		}
		return "date"
	}
	if jsonCount >= threshold {
		return "json"
	}

	// Check for categorical (low cardinality)
	cardinality := calculateCardinality(values)
	if cardinality <= 20 && cardinality < len(values)/2 {
		return "categorical"
	}

	return "string"
}

// isDateFormat checks if string matches common date formats
func isDateFormat(s string) bool {
	datePatterns := []string{
		`^\d{4}-\d{2}-\d{2}$`,                  // 2024-01-15
		`^\d{2}/\d{2}/\d{4}$`,                  // 01/15/2024
		`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`, // ISO 8601
		`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`, // 2024-01-15 14:30:00
	}

	for _, pattern := range datePatterns {
		if matched, _ := regexp.MatchString(pattern, s); matched {
			return true
		}
	}

	// Try parsing with time.Parse
	formats := []string{
		time.RFC3339,
		"2006-01-02",
		"01/02/2006",
		"2006-01-02 15:04:05",
	}

	for _, format := range formats {
		if _, err := time.Parse(format, s); err == nil {
			return true
		}
	}

	return false
}

// containsTime checks if any value contains time component
func containsTime(values []string) bool {
	for _, v := range values {
		if strings.Contains(v, ":") {
			return true
		}
	}
	return false
}

// getExampleValues returns first N distinct non-null values
func getExampleValues(values []string, n int) []string {
	seen := make(map[string]bool)
	examples := []string{}

	for _, v := range values {
		if v != "" && !seen[v] && len(examples) < n {
			seen[v] = true
			examples = append(examples, v)
		}
	}

	return examples
}

// isIdentifierColumn checks if column appears to be an identifier
func isIdentifierColumn(name string, values []string, cardinality, totalRows int) bool {
	// Check name patterns
	nameLower := strings.ToLower(name)
	if strings.Contains(nameLower, "id") ||
		strings.Contains(nameLower, "key") ||
		strings.Contains(nameLower, "uuid") ||
		strings.Contains(nameLower, "guid") {
		return true
	}

	// Check if high cardinality (unique or near-unique)
	if cardinality >= int(float64(totalRows)*0.95) {
		return true
	}

	return false
}

// isTimestampColumn checks if column appears to be a timestamp
func isTimestampColumn(name string, values []string) bool {
	nameLower := strings.ToLower(name)
	timestampNames := []string{"timestamp", "created_at", "updated_at", "time", "datetime", "date"}

	for _, ts := range timestampNames {
		if strings.Contains(nameLower, ts) {
			return true
		}
	}

	// Check if values look like timestamps
	dateCount := 0
	for _, v := range values {
		if v != "" && isDateFormat(v) {
			dateCount++
		}
	}

	return dateCount >= len(values)/2
}

// isGeoColumn checks if column contains geographic data
func isGeoColumn(name string, values []string) bool {
	nameLower := strings.ToLower(name)
	geoNames := []string{"lat", "lon", "latitude", "longitude", "coord", "location", "geo"}

	for _, geo := range geoNames {
		if strings.Contains(nameLower, geo) {
			return true
		}
	}

	// Check for coordinate-like values
	coordPattern := regexp.MustCompile(`^-?\d+\.\d+$`)
	coordCount := 0
	for _, v := range values {
		if v != "" && coordPattern.MatchString(v) {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				if f >= -180 && f <= 180 { // Valid coordinate range
					coordCount++
				}
			}
		}
	}

	return coordCount >= len(values)/2
}

// isCodeLikeColumn checks if column contains codes/SKUs
func isCodeLikeColumn(values []string) bool {
	// Look for patterns like: ABC-123, SKU12345, etc.
	codePattern := regexp.MustCompile(`^[A-Z0-9]{2,}-?[A-Z0-9]+$`)
	codeCount := 0

	for _, v := range values {
		if v != "" && codePattern.MatchString(v) {
			codeCount++
		}
	}

	return codeCount >= len(values)/2
}

// getNumericMinMax finds min and max for numeric columns
func getNumericMinMax(values []string) (string, string) {
	var min, max float64
	first := true

	for _, v := range values {
		if v == "" {
			continue
		}

		if f, err := strconv.ParseFloat(v, 64); err == nil {
			if first {
				min = f
				max = f
				first = false
			} else {
				if f < min {
					min = f
				}
				if f > max {
					max = f
				}
			}
		}
	}

	if first {
		return "", ""
	}

	return fmt.Sprintf("%.2f", min), fmt.Sprintf("%.2f", max)
}
