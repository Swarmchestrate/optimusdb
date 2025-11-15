package contextualmetadata

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"optimusdb/app"
)

type MetadataHandler struct {
	Service *Service
	KB      *app.KnowledgeBaseDB
	Cache   *MetadataCache
}

// EnrichDatasetRequest holds the request for dataset enrichment
type EnrichDatasetRequest struct {
	Database string `json:"database"`
	Table    string `json:"table"`
	MaxRows  int    `json:"max_rows"`
}

// EnrichDataset handles POST /api/v1/metadata/enrich
func (h *MetadataHandler) EnrichDataset(w http.ResponseWriter, r *http.Request) {
	var req EnrichDatasetRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Database == "" || req.Table == "" {
		http.Error(w, "database and table are required", http.StatusBadRequest)
		return
	}

	if req.MaxRows == 0 {
		req.MaxRows = 200
	}

	// Check cache first
	if cached, found := h.Cache.Get(req.Database, req.Table); found {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":    "cached",
			"metadata":  cached,
			"timestamp": time.Now().UTC(),
		})
		return
	}

	// Enrich dataset
	startTime := time.Now()
	metadata, err := h.Service.EnrichDataset(context.Background(), h.KB, req.Database, req.Table, req.MaxRows)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	elapsed := time.Since(startTime)

	// Cache result
	h.Cache.Set(req.Database, req.Table, metadata)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":         "success",
		"metadata":       metadata,
		"elapsed_ms":     elapsed.Milliseconds(),
		"timestamp":      time.Now().UTC(),
		"cache_duration": "24h",
	})
}

// BatchEnrichRequest holds the request for batch enrichment
type BatchEnrichRequest struct {
	Datasets []struct {
		Database string `json:"database"`
		Table    string `json:"table"`
	} `json:"datasets"`
}

// EnrichBatch handles POST /api/v1/metadata/enrich-batch
func (h *MetadataHandler) EnrichBatch(w http.ResponseWriter, r *http.Request) {
	var req BatchEnrichRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Datasets) == 0 {
		http.Error(w, "no datasets specified", http.StatusBadRequest)
		return
	}

	datasets := make([]DatasetInfo, 0, len(req.Datasets))
	for _, ds := range req.Datasets {
		// Skip cached entries
		if _, found := h.Cache.Get(ds.Database, ds.Table); found {
			continue
		}
		datasets = append(datasets, DatasetInfo{
			DB:    ds.Database,
			Table: ds.Table,
		})
	}

	startTime := time.Now()
	results := h.Service.EnrichMultipleDatasets(context.Background(), datasets)
	elapsed := time.Since(startTime)

	// Cache successful results
	for i, result := range results {
		if result.Error == nil {
			h.Cache.Set(datasets[i].DB, datasets[i].Table, result.Metadata)
		}
	}

	// Convert to response format
	response := make([]map[string]any, len(results))
	successCount := 0
	for i, result := range results {
		if result.Error == nil {
			response[i] = map[string]any{
				"database": datasets[i].DB,
				"table":    datasets[i].Table,
				"status":   "success",
				"metadata": result.Metadata,
			}
			successCount++
		} else {
			response[i] = map[string]any{
				"database": datasets[i].DB,
				"table":    datasets[i].Table,
				"status":   "error",
				"error":    result.Error.Error(),
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":         "completed",
		"total":          len(results),
		"successful":     successCount,
		"failed":         len(results) - successCount,
		"elapsed_ms":     elapsed.Milliseconds(),
		"results":        response,
		"timestamp":      time.Now().UTC(),
		"cache_duration": "24h",
	})
}

// GetMetrics handles GET /api/v1/metadata/metrics
func (h *MetadataHandler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := GetMetrics()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":    "success",
		"metrics":   metrics,
		"timestamp": time.Now().UTC(),
	})
}

// ProfileDataset handles GET /api/v1/metadata/profile
func (h *MetadataHandler) ProfileDataset(w http.ResponseWriter, r *http.Request) {
	db := r.URL.Query().Get("db")
	table := r.URL.Query().Get("table")
	maxRowsStr := r.URL.Query().Get("max_rows")

	if db == "" || table == "" {
		http.Error(w, "db and table query parameters are required", http.StatusBadRequest)
		return
	}

	maxRows := 200
	if maxRowsStr != "" {
		if parsed, err := strconv.Atoi(maxRowsStr); err == nil && parsed > 0 {
			maxRows = parsed
		}
	}

	startTime := time.Now()
	profile, err := ProfileTable(db, table, maxRows)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	elapsed := time.Since(startTime)

	// Infer domain
	domain := InferDomain(profile)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":     "success",
		"profile":    profile,
		"domain":     domain,
		"elapsed_ms": elapsed.Milliseconds(),
		"timestamp":  time.Now().UTC(),
	})
}

// HealthCheck handles GET /api/v1/metadata/health
func (h *MetadataHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	// Prevent panics from crashing the server
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("[ERROR] Health check panic: %v", rec)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"status":     "error",
				"llm_status": "error",
				"llm_error":  fmt.Sprintf("panic: %v", rec),
				"timestamp":  time.Now().UTC(),
			})
		}
	}()

	llmStatus := "unavailable"
	var llmError string

	// Safely check if LLM client is available
	if h.Service != nil && h.Service.Client != nil {
		// Use type switch for safer type assertion
		switch client := h.Service.Client.(type) {
		case *HTTPClient:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := client.HealthCheck(ctx); err != nil {
				llmStatus = "error"
				llmError = err.Error()
			} else {
				llmStatus = "healthy"
			}
		default:
			// Client exists but is not HTTPClient type (e.g., LocalClient)
			llmStatus = "available"
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":     "healthy",
		"llm_status": llmStatus,
		"llm_error":  llmError,
		"timestamp":  time.Now().UTC(),
	})
}

// ClearCache handles DELETE /api/v1/metadata/cache
func (h *MetadataHandler) ClearCache(w http.ResponseWriter, r *http.Request) {
	// Note: You'll need to add a Clear() method to MetadataCache
	// For now, just reinitialize
	h.Cache = NewMetadataCache(24 * time.Hour)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":    "success",
		"message":   "cache cleared",
		"timestamp": time.Now().UTC(),
	})
}
