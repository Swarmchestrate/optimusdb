# TinyLlama Pipeline - Complete Implementation Guide

## 📋 Table of Contents
1. [Current State Analysis](#current-state-analysis)
2. [Architecture Overview](#architecture-overview)
3. [Complete Implementation](#complete-implementation)
4. [Integration Steps](#integration-steps)
5. [Testing & Validation](#testing-validation)
6. [Deployment Guide](#deployment-guide)
7. [Performance Optimization](#performance-optimization)

---

## 1. Current State Analysis

### ✅ What You Have (Good Foundation)
```
contextualmetadata/
├── enricher.go         ✅ Good structure, needs profiler integration
├── profiler.go         ⚠️  Skeleton only - needs full implementation
├── prompts.go          ✅ Well-designed prompt builder
├── tinyllama_http.go   ⚠️  Basic HTTP client - needs enhancement
└── tinyllama_local.go  ⚠️  Basic local client - needs testing
```

### ❌ What's Missing (Critical Gaps)

1. **ProfileTable() is not implemented** - Returns error "not wired yet"
2. **No integration with file uploads** - Metadata generation not triggered
3. **No domain vocabulary system** - Paper describes renewable energy vocabularies
4. **No automated enrichment pipeline** - Manual API call required
5. **No result caching** - Every request hits LLM
6. **No error recovery** - Fails completely if LLM unavailable
7. **TinyLlama client is commented out in http.go** - Not actually used!

---

## 2. Architecture Overview

### Paper Requirements (CENTERIS 2025, Section 4, Page 5)

> "The process generates a fully automated contextual metadata generation, allowing for real-time semantic enrichment right at the edge without any user input"

### Complete Pipeline Flow

```
┌─────────────────┐
│  File Upload    │
└────────┬────────┘
│
▼
┌─────────────────────────────────┐
│ 1. Dataset Profiling            │
│    - Schema extraction          │
│    - Type inference             │
│    - Statistical analysis       │
│    - Sample data extraction     │
└────────┬────────────────────────┘
│
▼
┌─────────────────────────────────┐
│ 2. Prompt Building              │
│    - Profile → Structured prompt│
│    - Domain context injection   │
│    - Format specification       │
└────────┬────────────────────────┘
│
▼
┌─────────────────────────────────┐
│ 3. TinyLlama Inference          │
│    - HTTP or Local inference    │
│    - JSON response parsing      │
│    - Fallback handling          │
└────────┬────────────────────────┘
│
▼
┌─────────────────────────────────┐
│ 4. Vocabulary Enrichment        │
│    - Domain-specific keywords   │
│    - Renewable energy terms     │
│    - Merge with LLM output      │
└────────┬────────────────────────┘
│
▼
┌─────────────────────────────────┐
│ 5. Multi-Layer Storage          │
│    - IPFS: Store enriched data  │
│    - OrbitDB: Replicate metadata│
│    - SQLite: Index for queries  │
└────────┬────────────────────────┘
│
▼
┌─────────────────────────────────┐
│ 6. Federation Broadcast         │
│    - GossipSub notification     │
│    - Peer synchronization       │
└─────────────────────────────────┘
```

---

## 3. Complete Implementation

### Step 1: Complete the Profiler

**File: `contextualmetadata/profiler.go`** (Replace entirely)

```go
package contextualmetadata

import (
"database/sql"
"fmt"
"math"
"optimusdb/sql/storage"
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
`^\d{4}-\d{2}-\d{2}$`,                          // 2024-01-15
`^\d{2}/\d{2}/\d{4}$`,                          // 01/15/2024
`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`,        // ISO 8601
`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,        // 2024-01-15 14:30:00
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
```

### Step 2: Enhanced TinyLlama HTTP Client with Retry Logic

**File: `contextualmetadata/tinyllama_http.go`** (Replace entirely)

```go
//go:build !tinyllama_local

package contextualmetadata

import (
"bytes"
"context"
"encoding/json"
"fmt"
"io"
"net/http"
"os"
"time"
)

type LLMRequest struct {
Prompt      string  `json:"prompt"`
MaxTokens   int     `json:"max_tokens"`
Temperature float64 `json:"temperature"`
Stop        []string `json:"stop,omitempty"`
}

type LLMResponse struct {
Text    string  `json:"text"`
Tokens  int     `json:"tokens,omitempty"`
Latency float64 `json:"latency_ms,omitempty"`
}

type HTTPClient struct {
Endpoint    string
Timeout     time.Duration
MaxRetries  int
RetryDelay  time.Duration
client      *http.Client
}

func NewTinyLlamaHTTP() (*HTTPClient, error) {
ep := os.Getenv("TINYLLAMA_ENDPOINT")
if ep == "" {
ep = "http://localhost:8080/v1/completions" // Default
}

return &HTTPClient{
Endpoint:   ep,
Timeout:    60 * time.Second,
MaxRetries: 3,
RetryDelay: 2 * time.Second,
client: &http.Client{
Timeout: 60 * time.Second,
},
}, nil
}

func (c *HTTPClient) Generate(prompt string, maxTokens int) (string, error) {
return c.GenerateWithContext(context.Background(), prompt, maxTokens)
}

func (c *HTTPClient) GenerateWithContext(ctx context.Context, prompt string, maxTokens int) (string, error) {
req := LLMRequest{
Prompt:      prompt,
MaxTokens:   maxTokens,
Temperature: 0.2,
Stop:        []string{"\n\n", "###"},
}

var lastErr error
for attempt := 0; attempt <= c.MaxRetries; attempt++ {
if attempt > 0 {
select {
case <-time.After(c.RetryDelay):
case <-ctx.Done():
return "", ctx.Err()
}
}

resp, err := c.doRequest(ctx, req)
if err != nil {
lastErr = err
continue
}

return resp.Text, nil
}

return "", fmt.Errorf("failed after %d attempts: %w", c.MaxRetries+1, lastErr)
}

func (c *HTTPClient) doRequest(ctx context.Context, req LLMRequest) (*LLMResponse, error) {
body, err := json.Marshal(req)
if err != nil {
return nil, fmt.Errorf("marshal request: %w", err)
}

httpReq, err := http.NewRequestWithContext(ctx, "POST", c.Endpoint, bytes.NewReader(body))
if err != nil {
return nil, fmt.Errorf("create request: %w", err)
}

httpReq.Header.Set("Content-Type", "application/json")
httpReq.Header.Set("Accept", "application/json")

start := time.Now()
resp, err := c.client.Do(httpReq)
if err != nil {
return nil, fmt.Errorf("http request: %w", err)
}
defer resp.Body.Close()

if resp.StatusCode != http.StatusOK {
bodyBytes, _ := io.ReadAll(resp.Body)
return nil, fmt.Errorf("bad status %d: %s", resp.StatusCode, string(bodyBytes))
}

var llmResp LLMResponse
if err := json.NewDecoder(resp.Body).Decode(&llmResp); err != nil {
return nil, fmt.Errorf("decode response: %w", err)
}

llmResp.Latency = float64(time.Since(start).Milliseconds())
return &llmResp, nil
}

// HealthCheck verifies TinyLlama service is reachable
func (c *HTTPClient) HealthCheck(ctx context.Context) error {
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()

testPrompt := "Test"
_, err := c.GenerateWithContext(ctx, testPrompt, 10)
return err
}
```

### Step 3: Domain Vocabulary System

**New File: `contextualmetadata/vocabularies.go`**

```go
package contextualmetadata

import (
"strings"
)

// DomainVocabulary holds domain-specific terminology
type DomainVocabulary struct {
Domain   string
Keywords []string
Synonyms map[string][]string
}

// GetRenewableEnergyVocabulary returns renewable energy domain vocabulary
func GetRenewableEnergyVocabulary() *DomainVocabulary {
return &DomainVocabulary{
Domain: "renewable_energy",
Keywords: []string{
// General
"renewable_energy", "clean_energy", "green_energy", "power_generation",
"energy_production", "capacity", "efficiency", "availability",

// Solar
"photovoltaic", "PV", "solar_panel", "solar_array", "inverter",
"irradiance", "GHI", "DNI", "DHI", "solar_radiation",
"sun_elevation", "azimuth", "tracking", "fixed_tilt",
"DC_power", "AC_power", "MPPT", "string_voltage",

// Wind
"wind_turbine", "wind_farm", "nacelle", "rotor", "blade",
"wind_speed", "wind_direction", "yaw", "pitch",
"cut_in_speed", "rated_speed", "cut_out_speed",
"power_curve", "capacity_factor",

// Grid
"grid_connection", "grid_frequency", "voltage", "current",
"active_power", "reactive_power", "power_factor",
"grid_synchronization", "frequency_regulation",

// Environmental
"temperature", "humidity", "pressure", "precipitation",
"cloud_cover", "weather_conditions",

// Operational
"availability", "downtime", "maintenance", "fault",
"alarm", "warning", "status", "state",
"SCADA", "monitoring", "telemetry", "sensor",

// Performance
"performance_ratio", "yield", "losses", "degradation",
"soiling", "shading", "curtailment",
},
Synonyms: map[string][]string{
"power":       {"energy", "electricity", "generation"},
"turbine":     {"generator", "wind_generator"},
"solar_panel": {"PV_module", "photovoltaic_panel"},
"inverter":    {"converter", "DC_AC_converter"},
"temperature": {"temp", "thermal"},
},
}
}

// GetSolarVocabulary returns solar-specific vocabulary
func GetSolarVocabulary() *DomainVocabulary {
return &DomainVocabulary{
Domain: "solar",
Keywords: []string{
"PV_module", "solar_cell", "crystalline_silicon", "thin_film",
"module_temperature", "cell_temperature", "NOCT",
"short_circuit_current", "open_circuit_voltage",
"fill_factor", "efficiency", "degradation_rate",
"bifacial", "monofacial", "tracking_system",
"single_axis", "dual_axis", "fixed_tilt",
},
}
}

// GetWindVocabulary returns wind-specific vocabulary
func GetWindVocabulary() *DomainVocabulary {
return &DomainVocabulary{
Domain: "wind",
Keywords: []string{
"wind_class", "turbulence_intensity", "wind_shear",
"hub_height", "rotor_diameter", "swept_area",
"tip_speed_ratio", "blade_pitch_angle", "yaw_error",
"gearbox", "generator", "transformer",
"offshore", "onshore", "capacity_factor",
},
}
}

// EnrichWithVocabulary enhances metadata with domain vocabulary
func EnrichWithVocabulary(metadata map[string]interface{}, domain string) map[string]interface{} {
var vocab *DomainVocabulary

domainLower := strings.ToLower(domain)
switch {
case strings.Contains(domainLower, "solar"):
vocab = GetSolarVocabulary()
case strings.Contains(domainLower, "wind"):
vocab = GetWindVocabulary()
default:
vocab = GetRenewableEnergyVocabulary()
}

// Add domain keywords if not present
if tags, ok := metadata["tags"].([]string); ok {
enrichedTags := enrichTags(tags, vocab)
metadata["tags"] = enrichedTags
}

// Add domain field
metadata["domain"] = vocab.Domain
metadata["domain_keywords"] = vocab.Keywords

return metadata
}

// enrichTags adds relevant domain keywords to tags
func enrichTags(tags []string, vocab *DomainVocabulary) []string {
tagSet := make(map[string]bool)
for _, tag := range tags {
tagSet[strings.ToLower(tag)] = true
}

// Add relevant domain keywords that match existing tags
for _, keyword := range vocab.Keywords {
keywordLower := strings.ToLower(keyword)

// Check if keyword or its synonyms appear in tags
if tagSet[keywordLower] {
continue
}

// Check synonyms
for originalTerm, synonyms := range vocab.Synonyms {
if tagSet[strings.ToLower(originalTerm)] {
for _, synonym := range synonyms {
if strings.ToLower(synonym) == keywordLower {
tags = append(tags, keyword)
break
}
}
}
}
}

return tags
}

// InferDomain attempts to infer domain from column names and values
func InferDomain(profile *DatasetProfile) string {
solarScore := 0
windScore := 0

solarTerms := []string{"solar", "pv", "irradiance", "inverter", "panel"}
windTerms := []string{"wind", "turbine", "rotor", "blade", "nacelle"}

// Check column names
for _, col := range profile.Profiles {
nameLower := strings.ToLower(col.Name)

for _, term := range solarTerms {
if strings.Contains(nameLower, term) {
solarScore++
}
}

for _, term := range windTerms {
if strings.Contains(nameLower, term) {
windScore++
}
}
}

if solarScore > windScore {
return "solar"
} else if windScore > solarScore {
return "wind"
}

return "renewable_energy"
}
```

### Step 4: Enhanced Enricher with Full Pipeline

**File: `contextualmetadata/enricher.go`** (Significant enhancements)

Add to existing file:

```go
// EnrichDatasetWithProfile enriches using existing profile
func (s *Service) EnrichDatasetWithProfile(ctx context.Context, profile *DatasetProfile) (map[string]any, error) {
// Infer domain from profile
domain := InferDomain(profile)

// Build prompt
prompt := BuildPrompt(EnrichmentRequest{
DB:       profile.DB,
Table:    profile.Table,
Profile:  profile,
UseGreek: s.UseGreek,
})

// Call LLM with fallback
var raw string
var err error

if s.Client != nil {
raw, err = s.Client.Generate(prompt, 512)
if err != nil {
// Fallback to basic metadata
return s.generateBasicMetadata(profile, domain), nil
}
} else {
// No LLM client, use basic metadata
return s.generateBasicMetadata(profile, domain), nil
}

// Parse LLM response
var out EnrichmentOutput
if err := json.Unmarshal([]byte(raw), &out); err != nil {
// Try to extract JSON
start, end := findJSON(raw)
if start >= 0 && end > start {
if err2 := json.Unmarshal([]byte(raw[start:end]), &out); err2 != nil {
// Fallback to basic
return s.generateBasicMetadata(profile, domain), nil
}
} else {
// Fallback to basic
return s.generateBasicMetadata(profile, domain), nil
}
}

// Build enriched metadata
entry := map[string]any{
"metadata_type": "dataset_context",
"name":          profile.Table,
"description":   out.Description,
"tags":          out.Tags,
"columns":       out.Columns,
"domain":        domain,
"row_count":     profile.RowCount,
"column_count":  len(profile.Profiles),
"status":        "generated",
"created_by":    "tinyllama-enricher",
"created_at":    time.Now().UTC(),
}

// Enrich with domain vocabulary
entry = EnrichWithVocabulary(entry, domain)

return entry, nil
}

// generateBasicMetadata creates basic metadata without LLM
func (s *Service) generateBasicMetadata(profile *DatasetProfile, domain string) map[string]any {
// Generate basic description
description := fmt.Sprintf("Dataset %s.%s with %d rows and %d columns.",
profile.DB, profile.Table, profile.RowCount, len(profile.Profiles))

// Extract basic tags from column names
tags := []string{domain}
for _, col := range profile.Profiles {
// Add column type as tag
if col.InferredType != "string" {
tags = append(tags, col.InferredType)
}
// Add identifier columns
if col.IsIdentifier {
tags = append(tags, "identifier")
}
if col.IsTimestamp {
tags = append(tags, "temporal")
}
}

entry := map[string]any{
"metadata_type": "dataset_context",
"name":          profile.Table,
"description":   description,
"tags":          tags,
"domain":        domain,
"row_count":     profile.RowCount,
"column_count":  len(profile.Profiles),
"status":        "basic",
"created_by":    "basic-profiler",
"created_at":    time.Now().UTC(),
}

return EnrichWithVocabulary(entry, domain)
}
```

---

## 4. Integration Steps

### Step 4.1: File Upload Integration

**Modify File: `api/http.go`**

Find the file upload handler and add:

```go
// HandleFileUpload with automated metadata generation
func HandleFileUpload(w http.ResponseWriter, r *http.Request) {
// ... existing file upload logic ...

// After successful file upload, trigger metadata generation
go generateMetadataAsync(filePath, fileName, fileType)

// Return success response immediately
w.WriteHeader(http.StatusOK)
json.NewEncoder(w).Encode(map[string]string{
"status":  "uploaded",
"message": "File uploaded successfully, metadata generation in progress",
"file":    fileName,
})
}

// generateMetadataAsync generates metadata in background
func generateMetadataAsync(filePath, fileName, fileType string) {
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()

log.Printf("Starting metadata generation for: %s", fileName)

// Initialize enrichment service
var svc contextualmetadata.Service

// Try to initialize TinyLlama client
if c, err := contextualmetadata.NewTinyLlamaHTTP(); err == nil {
// Health check
if err := c.HealthCheck(ctx); err == nil {
svc.Client = c
log.Println("TinyLlama HTTP client initialized")
} else {
log.Printf("TinyLlama health check failed: %v, using basic profiling", err)
}
} else {
log.Printf("TinyLlama client init failed: %v, using basic profiling", err)
}

// Profile the dataset
profile, err := contextualmetadata.ProfileTable(filePath, fileName, 200)
if err != nil {
log.Printf("ERROR: Profiling failed for %s: %v", fileName, err)
return
}

// Enrich with TinyLlama or basic profiling
enriched, err := svc.EnrichDatasetWithProfile(ctx, profile)
if err != nil {
log.Printf("ERROR: Enrichment failed for %s: %v", fileName, err)
return
}

// Store metadata across all layers
if err := storeEnrichedMetadata(ctx, enriched); err != nil {
log.Printf("ERROR: Failed to store metadata: %v", err)
return
}

log.Printf("SUCCESS: Metadata generated and stored for: %s", fileName)
}
```

### Step 4.2: Enable TinyLlama Endpoint

**Modify File: `api/http.go`**

Uncomment and fix the enrich endpoint:

```go
// Handler for POST /metadata/enrich
func enrichHandler(kb *app.KnowledgeBaseDB) http.HandlerFunc {
return func(w http.ResponseWriter, r *http.Request) {
var req enrichReq
if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
http.Error(w, "invalid JSON", http.StatusBadRequest)
return
}
if req.MaxRows <= 0 {
req.MaxRows = 200
}

// Initialize service with TinyLlama client
var svc contextualmetadata.Service
svc.UseGreek = req.Greek

// Try HTTP client first, fall back to local, then basic
if c, err := contextualmetadata.NewTinyLlamaHTTP(); err == nil {
if err := c.HealthCheck(r.Context()); err == nil {
svc.Client = c
}
}

// Enrich dataset
entry, err := svc.EnrichDataset(r.Context(), kb, req.DB, req.Table, req.MaxRows)
if err != nil {
http.Error(w, err.Error(), http.StatusInternalServerError)
return
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(entry)
}
}
```

---

## 5. Testing & Validation

### Test Script 1: Unit Tests

**New File: `contextualmetadata/profiler_test.go`**

```go
package contextualmetadata

import (
"testing"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
)

func TestProfileColumn(t *testing.T) {
data := [][]string{
{"123", "456"},
{"789", "101"},
{"", "202"},
}

profile := profileColumn("test_col", data, 0, 3)

assert.Equal(t, "test_col", profile.Name)
assert.Equal(t, "int", profile.InferredType)
assert.InDelta(t, 0.33, profile.NullRatio, 0.01)
assert.Equal(t, 2, profile.Cardinality)
}

func TestInferType(t *testing.T) {
tests := []struct {
name     string
values   []string
expected string
}{
{"integers", []string{"1", "2", "3"}, "int"},
{"floats", []string{"1.5", "2.3", "3.7"}, "float"},
{"dates", []string{"2024-01-01", "2024-01-02"}, "date"},
{"strings", []string{"abc", "def", "ghi"}, "string"},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
result := inferType(tt.values)
assert.Equal(t, tt.expected, result)
})
}
}

func TestDomainInference(t *testing.T) {
profile := &DatasetProfile{
Profiles: []ColumnProfile{
{Name: "solar_irradiance"},
{Name: "panel_temperature"},
{Name: "power_output"},
},
}

domain := InferDomain(profile)
assert.Equal(t, "solar", domain)
}
```

### Test Script 2: Integration Test

**New File: `test/integration/tinyllama_test.go`**

```go
package integration

import (
"context"
"optimusdb/contextualmetadata"
"testing"
"github.com/stretchr/testify/require"
)

func TestTinyLlamaIntegration(t *testing.T) {
if testing.Short() {
t.Skip("Skipping integration test")
}

// This requires TinyLlama server running
client, err := contextualmetadata.NewTinyLlamaHTTP()
require.NoError(t, err)

// Health check
err = client.HealthCheck(context.Background())
require.NoError(t, err)

// Test inference
prompt := "Describe a solar panel dataset in JSON format"
result, err := client.Generate(prompt, 256)
require.NoError(t, err)
require.NotEmpty(t, result)
}

func TestEndToEndMetadataGeneration(t *testing.T) {
// Create test database
// ... setup code ...

// Profile dataset
profile, err := contextualmetadata.ProfileTable("test.db", "solar_data", 100)
require.NoError(t, err)
require.NotNil(t, profile)

// Enrich with TinyLlama
var svc contextualmetadata.Service
client, _ := contextualmetadata.NewTinyLlamaHTTP()
svc.Client = client

enriched, err := svc.EnrichDatasetWithProfile(context.Background(), profile)
require.NoError(t, err)
require.Contains(t, enriched, "description")
require.Contains(t, enriched, "domain")
}
```

---

## 6. Deployment Guide

### Docker Compose Configuration

**New File: `docker-compose.tinyllama.yml`**

```yaml
version: '3.8'

services:
# TinyLlama Inference Server
tinyllama:
image: ghcr.io/ggerganov/llama.cpp:server
container_name: tinyllama-server
environment:
- MODEL=/models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf
- HOST=0.0.0.0
- PORT=8080
- CTX_SIZE=2048
volumes:
- ./models:/models:ro
ports:
- "8080:8080"
command: >
--server
--host 0.0.0.0
--port 8080
--model /models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf
--ctx-size 2048
--n-gpu-layers 32
deploy:
resources:
reservations:
devices:
- driver: nvidia
count: 1
capabilities: [gpu]

# OptimusDB Agent with TinyLlama Integration
optimusdb-agent:
build:
context: .
dockerfile: Dockerfile
container_name: optimusdb-agent
environment:
- NODE_ID=1
- HTTP_PORT=18001
- TINYLLAMA_ENDPOINT=http://tinyllama:8080/completion
- ENABLE_AUTO_METADATA=true
depends_on:
- tinyllama
volumes:
- ./data:/data
ports:
- "18001:18001"
networks:
- optimusdb-net

networks:
optimusdb-net:
driver: bridge
```

### Model Download Script

**New File: `scripts/download_tinyllama.sh`**

```bash
#!/bin/bash

# Download TinyLlama model
mkdir -p models
cd models

echo "Downloading TinyLlama 1.1B Chat model (Q4 quantized)..."
wget -nc https://huggingface.co/TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF/resolve/main/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf

echo "Model downloaded successfully!"
echo "Size: $(du -h tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf)"
```

### Startup Script

**New File: `scripts/start_with_tinyllama.sh`**

```bash
#!/bin/bash

set -e

echo "🚀 Starting OptimusDB with TinyLlama Pipeline..."

# Download model if not present
if [ ! -f "models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf" ]; then
echo "📥 Downloading TinyLlama model..."
bash scripts/download_tinyllama.sh
fi

# Start services
echo "🔧 Starting services..."
docker-compose -f docker-compose.tinyllama.yml up -d

# Wait for TinyLlama to be ready
echo "⏳ Waiting for TinyLlama to start..."
sleep 10

# Health check
echo "🏥 Health check..."
curl -f http://localhost:8080/health || {
echo "❌ TinyLlama health check failed"
exit 1
}

echo "✅ OptimusDB with TinyLlama is running!"
echo "📊 TinyLlama endpoint: http://localhost:8080"
echo "🔍 OptimusDB API: http://localhost:18001"
```

---

## 7. Performance Optimization

### Caching Strategy

**New File: `contextualmetadata/cache.go`**

```go
package contextualmetadata

import (
"crypto/sha256"
"encoding/hex"
"sync"
"time"
)

type CacheEntry struct {
Result    map[string]any
Timestamp time.Time
}

type MetadataCache struct {
mu      sync.RWMutex
entries map[string]*CacheEntry
ttl     time.Duration
}

func NewMetadataCache(ttl time.Duration) *MetadataCache {
cache := &MetadataCache{
entries: make(map[string]*CacheEntry),
ttl:     ttl,
}

// Start cleanup goroutine
go cache.cleanup()

return cache
}

func (c *MetadataCache) Get(db, table string) (map[string]any, bool) {
c.mu.RLock()
defer c.mu.RUnlock()

key := c.makeKey(db, table)
entry, exists := c.entries[key]

if !exists {
return nil, false
}

// Check if expired
if time.Since(entry.Timestamp) > c.ttl {
return nil, false
}

return entry.Result, true
}

func (c *MetadataCache) Set(db, table string, result map[string]any) {
c.mu.Lock()
defer c.mu.Unlock()

key := c.makeKey(db, table)
c.entries[key] = &CacheEntry{
Result:    result,
Timestamp: time.Now(),
}
}

func (c *MetadataCache) makeKey(db, table string) string {
h := sha256.Sum256([]byte(db + "/" + table))
return hex.EncodeToString(h[:])
}

func (c *MetadataCache) cleanup() {
ticker := time.NewTicker(10 * time.Minute)
defer ticker.Stop()

for range ticker.C {
c.mu.Lock()
for key, entry := range c.entries {
if time.Since(entry.Timestamp) > c.ttl {
delete(c.entries, key)
}
}
c.mu.Unlock()
}
}
```

### Batch Processing

**Add to enricher.go:**

```go
// EnrichMultipleDatasets processes multiple datasets in batch
func (s *Service) EnrichMultipleDatasets(ctx context.Context, datasets []DatasetInfo) []EnrichmentResult {
results := make([]EnrichmentResult, len(datasets))
var wg sync.WaitGroup

// Process in parallel with worker pool
workerCount := 3 // Limit concurrent LLM calls
sem := make(chan struct{}, workerCount)

for i, ds := range datasets {
wg.Add(1)
go func(idx int, dataset DatasetInfo) {
defer wg.Done()

sem <- struct{}{}        // Acquire
defer func() { <-sem }() // Release

profile, err := ProfileTable(dataset.DB, dataset.Table, 200)
if err != nil {
results[idx] = EnrichmentResult{Error: err}
return
}

enriched, err := s.EnrichDatasetWithProfile(ctx, profile)
results[idx] = EnrichmentResult{
Metadata: enriched,
Error:    err,
}
}(i, ds)
}

wg.Wait()
return results
}

type DatasetInfo struct {
DB    string
Table string
}

type EnrichmentResult struct {
Metadata map[string]any
Error    error
}
```

---

## 8. Monitoring & Logging

### Metrics Collection

**New File: `contextualmetadata/metrics.go`**

```go
package contextualmetadata

import (
"sync"
"time"
)

type EnrichmentMetrics struct {
mu                 sync.RWMutex
TotalEnrichments   int64
SuccessfulLLM      int64
FailedLLM          int64
FallbackToBasic    int64
AvgProfileTime     time.Duration
AvgLLMLatency      time.Duration
AvgTotalTime       time.Duration
}

var globalMetrics = &EnrichmentMetrics{}

func (m *EnrichmentMetrics) RecordEnrichment(profileTime, llmTime, totalTime time.Duration, llmSuccess bool) {
m.mu.Lock()
defer m.mu.Unlock()

m.TotalEnrichments++
if llmSuccess {
m.SuccessfulLLM++
} else {
m.FallbackToBasic++
}

// Update averages (simple moving average)
m.AvgProfileTime = (m.AvgProfileTime*time.Duration(m.TotalEnrichments-1) + profileTime) / time.Duration(m.TotalEnrichments)
m.AvgLLMLatency = (m.AvgLLMLatency*time.Duration(m.TotalEnrichments-1) + llmTime) / time.Duration(m.TotalEnrichments)
m.AvgTotalTime = (m.AvgTotalTime*time.Duration(m.TotalEnrichments-1) + totalTime) / time.Duration(m.TotalEnrichments)
}

func GetMetrics() EnrichmentMetrics {
globalMetrics.mu.RLock()
defer globalMetrics.mu.RUnlock()
return *globalMetrics
}
```

---

## 9. Usage Examples

### Example 1: Manual API Call

```bash
# Enrich a specific dataset
curl -X POST http://localhost:18001/metadata/enrich \
-H "Content-Type: application/json" \
-d '{
"db": "solar_farm_db",
"table": "telemetry_2024",
"maxRows": 200,
"greek": false
}'
```

### Example 2: Automated File Upload

```bash
# Upload CSV file - metadata generated automatically
curl -X POST http://localhost:18001/upload \
-F "file=@solar_data.csv"

# Response:
# {
#   "status": "uploaded",
#   "message": "File uploaded successfully, metadata generation in progress",
#   "file": "solar_data.csv"
# }
```

### Example 3: Query Generated Metadata

```bash
# Query metadata
curl http://localhost:18001/query \
-H "Content-Type: application/json" \
-d '{
"sql": "SELECT * FROM metadata WHERE domain = '\''solar'\''"
}'
```

---

## 10. Troubleshooting Guide

### Issue 1: TinyLlama Not Responding

**Symptoms:** Metadata generation falls back to basic profiling

**Solutions:**
```bash
# Check TinyLlama health
curl http://localhost:8080/health

# View TinyLlama logs
docker logs tinyllama-server

# Restart TinyLlama
docker-compose -f docker-compose.tinyllama.yml restart tinyllama
```

### Issue 2: ProfileTable Fails

**Symptoms:** "ProfileTable not wired yet" error

**Solution:** Ensure you've replaced the profiler.go file with the complete implementation provided above.

### Issue 3: Memory Issues with Large Datasets

**Solution:** Adjust maxRows parameter:
```go
// In profiler call, limit sample size
profile, err := ProfileTable(dbName, table, 100) // Reduced from 200
```

---


