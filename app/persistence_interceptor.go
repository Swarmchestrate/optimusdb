package app

import (
	"fmt"
)

// =============================================================================
// PERSISTENCE INTERCEPTOR - Hooks into all data write operations
// =============================================================================

type PersistenceInterceptor struct {
	extractor *MetadataExtractor
	lineage   *LineageManager
	enabled   bool
}

// NewPersistenceInterceptor creates a new interceptor
func NewPersistenceInterceptor(kb *KnowledgeBaseDB) *PersistenceInterceptor {
	return &PersistenceInterceptor{
		extractor: &MetadataExtractor{KB: kb},
		lineage:   &LineageManager{KB: kb, SQLite: GlobalKBSQLite},
		enabled:   true,
	}
}

// OnDocumentPut is called when a document is inserted/updated
func (pi *PersistenceInterceptor) OnDocumentPut(
	doc map[string]interface{},
	dstype string,
) error {

	if !pi.enabled {
		return nil
	}

	// Extract metadata
	meta, err := pi.extractor.ExtractMetadata(doc, dstype)
	if err != nil {
		return fmt.Errorf("metadata extraction failed: %w", err)
	}

	// Populate datacatalog
	if err := pi.lineage.PopulateDatacatalog(meta); err != nil {
		return fmt.Errorf("datacatalog population failed: %w", err)
	}

	// Create lineage relationships
	if err := pi.lineage.CreateLineageRelationships(meta); err != nil {
		return fmt.Errorf("lineage creation failed: %w", err)
	}

	return nil
}

// OnDocumentUpdate is called when a document is updated
func (pi *PersistenceInterceptor) OnDocumentUpdate(
	doc map[string]interface{},
	dstype string,
) error {

	// Same as Put - update metadata and lineage
	return pi.OnDocumentPut(doc, dstype)
}

// OnDocumentDelete is called when a document is deleted
func (pi *PersistenceInterceptor) OnDocumentDelete(
	doc map[string]interface{},
	dstype string,
) error {

	if !pi.enabled {
		return nil
	}

	// Extract table URI
	meta, err := pi.extractor.ExtractMetadata(doc, dstype)
	if err != nil {
		return err
	}

	// Clean up lineage
	return pi.lineage.RemoveLineageForDeletedDocument(meta.TableURI)
}

// Enable enables the interceptor
func (pi *PersistenceInterceptor) Enable() {
	pi.enabled = true
}

// Disable disables the interceptor
func (pi *PersistenceInterceptor) Disable() {
	pi.enabled = false
}
