// Copyright Contributors to the OptimusDB project.
// SPDX-License-Identifier: Apache-2.0

package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"optimusdb/logger"
)

// ============================================================================
// ADAPTER INTERFACE
// ============================================================================

// NLQueryAdapter interface for the NL Query engine
type NLQueryAdapter interface {
	TranslateQuery(ctx context.Context, prompt string, dstype string) (*NLQueryResult, error)
	ExecuteQuery(ctx context.Context, prompt string, dstype string) (*NLQueryResult, error)
	GetSchema(ctx context.Context, dstype string) (*SchemaInfo, error)
	GetAvailableDatasets() []DatasetInfo
}

// ============================================================================
// FUNCTION TYPES FOR DEPENDENCY INJECTION
// ============================================================================

// QueryFunc is the function signature for executing queries against KB
type QueryFunc func(ctx context.Context, dstype string, criteria []map[string]interface{}) ([]map[string]interface{}, error)

// SchemaFunc is the function signature for getting schema
type SchemaFunc func(dstype string) (*SchemaInfo, error)

// ============================================================================
// ADAPTER CONFIGURATION
// ============================================================================

// AdapterConfig configuration for the KnowledgeBase adapter
type AdapterConfig struct {
	TinyllamaURL string
	QueryFunc    QueryFunc
	SchemaFunc   SchemaFunc
	Datasets     []DatasetInfo
	Timeout      time.Duration
	SchemaTTL    time.Duration
}

// DefaultAdapterConfig returns default configuration
func DefaultAdapterConfig() AdapterConfig {
	return AdapterConfig{
		TinyllamaURL: "http://localhost:11434/api/chat",
		Datasets: []DatasetInfo{
			{Type: "dsswres", Name: "Solar & Wind Resources", Description: "Renewable energy asset metadata"},
			{Type: "dsswresaloc", Name: "Resource Allocations", Description: "Resource allocation data"},
			{Type: "kbmetadata", Name: "Knowledge Base Metadata", Description: "Catalog metadata"},
			{Type: "kbdata", Name: "Knowledge Base Data", Description: "General documents"},
		},
		Timeout:   30 * time.Second,
		SchemaTTL: 5 * time.Minute,
	}
}

// ============================================================================
// KNOWLEDGE BASE ADAPTER
// ============================================================================

// KnowledgeBaseAdapter connects directly to OptimusDB's knowledge base
type KnowledgeBaseAdapter struct {
	tinyllamaURL  string
	queryFunc     QueryFunc
	schemaFunc    SchemaFunc
	datasets      []DatasetInfo
	client        *http.Client
	schemaCache   map[string]*SchemaInfo
	schemaCacheMu sync.RWMutex
	schemaTTL     time.Duration
}

// NewKnowledgeBaseAdapter creates a new KB adapter
func NewKnowledgeBaseAdapter(config AdapterConfig) *KnowledgeBaseAdapter {
	return &KnowledgeBaseAdapter{
		tinyllamaURL: config.TinyllamaURL,
		queryFunc:    config.QueryFunc,
		schemaFunc:   config.SchemaFunc,
		datasets:     config.Datasets,
		client: &http.Client{
			Timeout: config.Timeout,
		},
		schemaCache: make(map[string]*SchemaInfo),
		schemaTTL:   config.SchemaTTL,
	}
}

// TranslateQuery translates natural language to query without executing
func (a *KnowledgeBaseAdapter) TranslateQuery(ctx context.Context, prompt string, dstype string) (*NLQueryResult, error) {
	logger.Info("[CHAT-ADAPTER] Translating query: %s (dstype: %s)", truncateString(prompt, 50), dstype)

	cmd, cmdType, criteria, err := a.translateWithTinyLlama(ctx, prompt, dstype)
	if err != nil {
		logger.Warn("[CHAT-ADAPTER] TinyLlama translation failed: %v, using fallback", err)
		cmd, cmdType, criteria = a.fallbackTranslation(prompt, dstype)
	}

	return &NLQueryResult{
		OriginalPrompt: prompt,
		TranslatedCmd:  cmd,
		CommandType:    cmdType,
		Parameters:     map[string]interface{}{"criteria": criteria},
		ResultCount:    0,
	}, nil
}

// ExecuteQuery translates and executes the query
func (a *KnowledgeBaseAdapter) ExecuteQuery(ctx context.Context, prompt string, dstype string) (*NLQueryResult, error) {
	logger.Info("[CHAT-ADAPTER] Executing query: %s (dstype: %s)", truncateString(prompt, 50), dstype)

	result, err := a.TranslateQuery(ctx, prompt, dstype)
	if err != nil {
		return nil, err
	}

	if a.queryFunc != nil {
		start := time.Now()

		var criteria []map[string]interface{}
		if c, ok := result.Parameters["criteria"].([]map[string]interface{}); ok {
			criteria = c
		}

		results, err := a.queryFunc(ctx, dstype, criteria)
		if err != nil {
			result.Error = err.Error()
			logger.Warn("[CHAT-ADAPTER] Query execution failed: %v", err)
			return result, nil
		}

		result.Results = results
		result.ResultCount = len(results)
		result.ExecutionTime = time.Since(start)

		logger.Info("[CHAT-ADAPTER] Query returned %d results in %v", result.ResultCount, result.ExecutionTime)
	}

	return result, nil
}

// GetSchema returns schema information for a dataset type
func (a *KnowledgeBaseAdapter) GetSchema(ctx context.Context, dstype string) (*SchemaInfo, error) {
	a.schemaCacheMu.RLock()
	if cached, ok := a.schemaCache[dstype]; ok {
		if time.Since(cached.LastUpdated) < a.schemaTTL {
			a.schemaCacheMu.RUnlock()
			return cached, nil
		}
	}
	a.schemaCacheMu.RUnlock()

	if a.schemaFunc != nil {
		schema, err := a.schemaFunc(dstype)
		if err != nil {
			return nil, err
		}

		a.schemaCacheMu.Lock()
		a.schemaCache[dstype] = schema
		a.schemaCacheMu.Unlock()

		return schema, nil
	}

	return a.getDefaultSchema(dstype), nil
}

// GetAvailableDatasets returns list of available dataset types
func (a *KnowledgeBaseAdapter) GetAvailableDatasets() []DatasetInfo {
	return a.datasets
}

// ============================================================================
// TINYLLAMA INTEGRATION
// ============================================================================

func (a *KnowledgeBaseAdapter) translateWithTinyLlama(ctx context.Context, prompt string, dstype string) (string, string, []map[string]interface{}, error) {
	systemPrompt := buildTranslationPrompt(dstype)

	reqBody := map[string]interface{}{
		"model": "tinyllama",
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": prompt},
		},
		"temperature": 0.1,
		"max_tokens":  256,
		"stream":      false,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", "", nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.tinyllamaURL, strings.NewReader(string(jsonBody)))
	if err != nil {
		return "", "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return "", "", nil, fmt.Errorf("TinyLlama request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", "", nil, fmt.Errorf("TinyLlama returned status %d", resp.StatusCode)
	}

	var llmResp struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&llmResp); err != nil {
		return "", "", nil, err
	}

	cmd, cmdType, criteria := parseTranslationResponse(llmResp.Message.Content)
	return cmd, cmdType, criteria, nil
}

func buildTranslationPrompt(dstype string) string {
	return fmt.Sprintf(`You are a query translator for OptimusDB data catalog.
Translate natural language to OptimusDB query criteria.

Dataset type: %s

Response format (JSON only, no explanation):
{"command": "get|query", "criteria": [{"field": "...", "operator": "==|>|<|contains", "value": "..."}]}

Examples:
"Show all solar assets" -> {"command": "get", "criteria": [{"field": "type", "operator": "==", "value": "solar"}]}
"Find capacity > 1000" -> {"command": "query", "criteria": [{"field": "capacity", "operator": ">", "value": 1000}]}
"Show wind turbines in Greece" -> {"command": "query", "criteria": [{"field": "type", "operator": "==", "value": "wind"}, {"field": "location", "operator": "contains", "value": "Greece"}]}
"List all assets" -> {"command": "get", "criteria": []}

Respond with JSON only.`, dstype)
}

func parseTranslationResponse(response string) (string, string, []map[string]interface{}) {
	response = strings.TrimSpace(response)

	if strings.Contains(response, "```") {
		start := strings.Index(response, "{")
		end := strings.LastIndex(response, "}")
		if start >= 0 && end > start {
			response = response[start : end+1]
		}
	}

	var parsed struct {
		Command  string                   `json:"command"`
		Criteria []map[string]interface{} `json:"criteria"`
	}

	if err := json.Unmarshal([]byte(response), &parsed); err != nil {
		return "get", "crudget", nil
	}

	cmdType := "crudget"
	if parsed.Command == "query" {
		cmdType = "crudquery"
	}

	return parsed.Command, cmdType, parsed.Criteria
}

func (a *KnowledgeBaseAdapter) fallbackTranslation(prompt string, dstype string) (string, string, []map[string]interface{}) {
	promptLower := strings.ToLower(prompt)
	criteria := []map[string]interface{}{}

	if strings.Contains(promptLower, "solar") {
		criteria = append(criteria, map[string]interface{}{
			"field":    "type",
			"operator": "==",
			"value":    "solar",
		})
	}
	if strings.Contains(promptLower, "wind") {
		criteria = append(criteria, map[string]interface{}{
			"field":    "type",
			"operator": "==",
			"value":    "wind",
		})
	}

	locations := []string{"greece", "thessaloniki", "athens", "crete", "patras"}
	for _, loc := range locations {
		if strings.Contains(promptLower, loc) {
			criteria = append(criteria, map[string]interface{}{
				"field":    "location",
				"operator": "contains",
				"value":    loc,
			})
			break
		}
	}

	cmdType := "crudget"
	if strings.Contains(promptLower, ">") || strings.Contains(promptLower, "<") ||
		strings.Contains(promptLower, "greater") || strings.Contains(promptLower, "less") {
		cmdType = "crudquery"
	}

	return cmdType, cmdType, criteria
}

func (a *KnowledgeBaseAdapter) getDefaultSchema(dstype string) *SchemaInfo {
	schemas := map[string]*SchemaInfo{
		"dsswres": {
			DatasetType: "dsswres",
			Tables: []TableInfo{
				{
					Name:        "assets",
					Description: "Renewable energy assets",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "name", Type: "string", Required: true},
						{Name: "type", Type: "string", Required: true},
						{Name: "location", Type: "string"},
						{Name: "capacity", Type: "number"},
						{Name: "status", Type: "string"},
						{Name: "owner", Type: "string"},
						{Name: "installed_date", Type: "date"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
		"dsswresaloc": {
			DatasetType: "dsswresaloc",
			Tables: []TableInfo{
				{
					Name:        "allocations",
					Description: "Resource allocations",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "resource_id", Type: "string", Required: true},
						{Name: "allocated_to", Type: "string"},
						{Name: "start_time", Type: "datetime"},
						{Name: "end_time", Type: "datetime"},
						{Name: "priority", Type: "number"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
		"kbmetadata": {
			DatasetType: "kbmetadata",
			Tables: []TableInfo{
				{
					Name:        "metadata",
					Description: "Catalog metadata entries",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "table_name", Type: "string"},
						{Name: "column_name", Type: "string"},
						{Name: "data_type", Type: "string"},
						{Name: "description", Type: "string"},
						{Name: "owner", Type: "string"},
						{Name: "tags", Type: "array"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
	}

	if schema, ok := schemas[dstype]; ok {
		return schema
	}

	return &SchemaInfo{
		DatasetType: dstype,
		Tables: []TableInfo{
			{
				Name:        "documents",
				Description: "Document store",
				Fields: []FieldInfo{
					{Name: "_id", Type: "string", Required: true},
					{Name: "data", Type: "object"},
				},
			},
		},
		LastUpdated: time.Now(),
	}
}
