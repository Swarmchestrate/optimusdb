package contextualmetadata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"optimusdb/app"
)

type EnrichmentOutput struct {
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	Columns     []struct {
		Name  string `json:"name"`
		Role  string `json:"role"`
		Notes string `json:"notes"`
	} `json:"columns"`
}

type Saver interface {
	SaveMetadata(ctx context.Context, kb *app.KnowledgeBaseDB, entry map[string]any) error
}

// Default saver → OrbitDB KBMetadata store
type OrbitDBSaver struct{}

func (s OrbitDBSaver) SaveMetadata(ctx context.Context, kb *app.KnowledgeBaseDB, entry map[string]any) error {
	dbMetaDocStore := *kb.KBMetadata
	metadataRecordsAsInterface := make([]interface{}, len(entry))
	_, err := dbMetaDocStore.PutAll(ctx, metadataRecordsAsInterface)

	return err
}

type Service struct {
	UseGreek bool
	Client   interface {
		Generate(string, int) (string, error)
	}
	Saver Saver
}

func (s *Service) EnrichDataset(ctx context.Context, kb *app.KnowledgeBaseDB, dbName, table string, maxRows int) (map[string]any, error) {
	profile, err := ProfileTable(dbName, table, maxRows)
	if err != nil {
		return nil, fmt.Errorf("profiling failed: %w", err)
	}

	prompt := BuildPrompt(EnrichmentRequest{DB: dbName, Table: table, Profile: profile, UseGreek: s.UseGreek})
	raw, err := s.Client.Generate(prompt, 512)
	if err != nil {
		return nil, fmt.Errorf("llm generate failed: %w", err)
	}

	var out EnrichmentOutput
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		// Be forgiving: small models can spill text — try to locate JSON braces quickly.
		start, end := findJSON(raw)
		if start >= 0 && end > start {
			if err2 := json.Unmarshal([]byte(raw[start:end]), &out); err2 != nil {
				return nil, fmt.Errorf("failed to parse llm JSON: %v; raw: %.200s", err2, raw)
			}
		} else {
			return nil, fmt.Errorf("no JSON in llm output: %.200s", raw)
		}
	}

	// Build KBMetadata entry
	h := sha256.Sum256([]byte(dbName + "/" + table + time.Now().UTC().String()))
	id := "meta-" + hex.EncodeToString(h[:])
	entry := map[string]any{
		"_id":           id,
		"metadata_type": "dataset_context",
		"associated_id": fmt.Sprintf("%s/%s", dbName, table),
		"name":          table,
		"description":   out.Description,
		"tags":          out.Tags,
		"status":        "generated",
		"created_by":    "contextual-enricher",
		"created_at":    time.Now().UTC(),
		"updated_at":    time.Now().UTC(),
	}

	if s.Saver == nil {
		s.Saver = OrbitDBSaver{}
	}
	if err := s.Saver.SaveMetadata(ctx, kb, entry); err != nil {
		return nil, fmt.Errorf("save to KBMetadata failed: %w", err)
	}
	return entry, nil
}

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
