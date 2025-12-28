package app

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// =============================================================================
// LINEAGE MANAGER - Manages lineage relationships in datacatalog
// =============================================================================

type LineageManager struct {
	KB     *KnowledgeBaseDB
	SQLite *KnowledgeBaseSQLite
}

// LineageRelationship represents a relationship between two entities
type LineageRelationship struct {
	SourceURI    string
	TargetURI    string
	RelationType string  // "depends_on", "provides_to", "contains", etc
	Strength     float64 // Confidence score 0-1
	DetectedFrom string  // "requirements", "references", "inferred"
}

// PopulateDatacatalog creates or updates a datacatalog entry
func (lm *LineageManager) PopulateDatacatalog(meta *DocumentMetadata) error {
	if GlobalKBSQLite == nil {
		return fmt.Errorf("GlobalKBSQLite not initialized")
	}

	// Prepare statistics JSON
	statsJSON, err := json.Marshal(meta.Statistics)
	if err != nil {
		return fmt.Errorf("failed to marshal statistics: %w", err)
	}

	// Build tags string
	tagsStr := strings.Join(meta.Tags, ",")

	// Initialize empty lineage arrays
	upstreamJSON := "[]"
	downstreamJSON := "[]"

	// Check if entry already exists
	exists, err := lm.entryExists(meta.TableURI)
	if err != nil {
		return err
	}

	if exists {
		// UPDATE existing entry
		query := `
			UPDATE datacatalog 
			SET 
				name = ?,
				metadata_type = ?,
				component = ?,
				description = ?,
				tags = ?,
				statistics = ?,
				updated_timestamp = ?
			WHERE _id = ?
		`
		_, err = GlobalKBSQLite.DB.Exec(query,
			meta.Name,
			meta.MetadataType,
			meta.Component,
			meta.Description,
			tagsStr,
			string(statsJSON),
			time.Now().Unix(),
			meta.TableURI,
		)
	} else {
		// INSERT new entry
		query := `
			INSERT INTO datacatalog (
				_id, name, metadata_type, component, description,
				tags, statistics, generation_code,
				lineage_upstream, lineage_downstream,
				created_timestamp, updated_timestamp
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`
		_, err = GlobalKBSQLite.DB.Exec(query,
			meta.TableURI,
			meta.Name,
			meta.MetadataType,
			meta.Component,
			meta.Description,
			tagsStr,
			string(statsJSON),
			"auto_extract", // generation_code
			upstreamJSON,
			downstreamJSON,
			time.Now().Unix(),
			time.Now().Unix(),
		)
	}

	return err
}

// CreateLineageRelationships creates lineage edges based on detected references
func (lm *LineageManager) CreateLineageRelationships(
	meta *DocumentMetadata,
) error {

	if len(meta.DetectedRefs) == 0 {
		return nil // No relationships to create
	}

	relationships := []LineageRelationship{}

	// Convert detected references to lineage relationships
	for _, ref := range meta.DetectedRefs {
		// Resolve reference to a table URI
		targetURI, err := lm.resolveReferenceToURI(ref, meta.SourceDatastore)
		if err != nil {
			continue // Skip unresolvable references
		}

		relationships = append(relationships, LineageRelationship{
			SourceURI:    meta.TableURI,
			TargetURI:    targetURI,
			RelationType: "depends_on",
			Strength:     1.0,
			DetectedFrom: "references",
		})
	}

	// Update lineage in datacatalog
	return lm.updateLineageEdges(meta.TableURI, relationships)
}

// resolveReferenceToURI converts a reference (node name, ID, etc) to a table URI
func (lm *LineageManager) resolveReferenceToURI(ref string, dstype string) (string, error) {
	// Query datacatalog to find matching entry
	query := `
		SELECT _id FROM datacatalog 
		WHERE name = ? OR _id LIKE ? 
		LIMIT 1
	`

	var uri string
	err := GlobalKBSQLite.DB.QueryRow(query, ref, "%"+ref+"%").Scan(&uri)
	if err != nil {
		// Not found in datacatalog - create placeholder URI
		return fmt.Sprintf("optimusdb://default.Unknown/%s", sanitizeName(ref)), nil
	}

	return uri, nil
}

// updateLineageEdges updates upstream/downstream lineage arrays
func (lm *LineageManager) updateLineageEdges(
	sourceURI string,
	relationships []LineageRelationship,
) error {

	if len(relationships) == 0 {
		return nil
	}

	// Build upstream array (things this depends on)
	upstream := []string{}
	for _, rel := range relationships {
		if rel.RelationType == "depends_on" {
			upstream = append(upstream, rel.TargetURI)
		}
	}

	// Update source's upstream
	if len(upstream) > 0 {
		upstreamJSON, _ := json.Marshal(upstream)
		query := `
			UPDATE datacatalog 
			SET lineage_upstream = ?,
				updated_timestamp = ?
			WHERE _id = ?
		`
		_, err := GlobalKBSQLite.DB.Exec(query,
			string(upstreamJSON),
			time.Now().Unix(),
			sourceURI)
		if err != nil {
			return err
		}
	}

	// Update targets' downstream (add sourceURI to their downstream)
	for _, targetURI := range upstream {
		err := lm.addToDownstream(targetURI, sourceURI)
		if err != nil {
			// Log but don't fail
			fmt.Printf("Warning: failed to update downstream for %s: %v\n",
				targetURI, err)
		}
	}

	return nil
}

// addToDownstream adds a URI to the downstream array of another entry
func (lm *LineageManager) addToDownstream(targetURI, newDownstream string) error {
	// Get current downstream
	var downstreamJSON string
	query := `SELECT lineage_downstream FROM datacatalog WHERE _id = ?`
	err := GlobalKBSQLite.DB.QueryRow(query, targetURI).Scan(&downstreamJSON)

	if err == sql.ErrNoRows {
		// Entry doesn't exist yet - skip
		return nil
	}
	if err != nil {
		return err
	}

	// Parse current downstream
	var downstream []string
	if downstreamJSON != "" && downstreamJSON != "[]" {
		json.Unmarshal([]byte(downstreamJSON), &downstream)
	}

	// Add if not already present
	found := false
	for _, uri := range downstream {
		if uri == newDownstream {
			found = true
			break
		}
	}

	if !found {
		downstream = append(downstream, newDownstream)
		newDownstreamJSON, _ := json.Marshal(downstream)

		query = `
			UPDATE datacatalog 
			SET lineage_downstream = ?,
				updated_timestamp = ?
			WHERE _id = ?
		`
		_, err = GlobalKBSQLite.DB.Exec(query,
			string(newDownstreamJSON),
			time.Now().Unix(),
			targetURI)
		return err
	}

	return nil
}

// entryExists checks if a datacatalog entry already exists
func (lm *LineageManager) entryExists(tableURI string) (bool, error) {
	var count int
	query := `SELECT COUNT(*) FROM datacatalog WHERE _id = ?`
	err := GlobalKBSQLite.DB.QueryRow(query, tableURI).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// RemoveLineageForDeletedDocument cleans up lineage when a document is deleted
func (lm *LineageManager) RemoveLineageForDeletedDocument(tableURI string) error {
	// Get upstream and downstream before deletion
	var upstreamJSON, downstreamJSON string
	query := `SELECT lineage_upstream, lineage_downstream FROM datacatalog WHERE _id = ?`
	err := GlobalKBSQLite.DB.QueryRow(query, tableURI).Scan(&upstreamJSON, &downstreamJSON)

	if err != nil {
		return err // Entry not found or error
	}

	// Parse arrays
	var upstream, downstream []string
	json.Unmarshal([]byte(upstreamJSON), &upstream)
	json.Unmarshal([]byte(downstreamJSON), &downstream)

	// Remove this URI from all upstream's downstream arrays
	for _, upstreamURI := range upstream {
		lm.removeFromDownstream(upstreamURI, tableURI)
	}

	// Remove this URI from all downstream's upstream arrays
	for _, downstreamURI := range downstream {
		lm.removeFromUpstream(downstreamURI, tableURI)
	}

	// Delete the entry
	query = `DELETE FROM datacatalog WHERE _id = ?`
	_, err = GlobalKBSQLite.DB.Exec(query, tableURI)

	return err
}

func (lm *LineageManager) removeFromDownstream(targetURI, uriToRemove string) error {
	var downstreamJSON string
	query := `SELECT lineage_downstream FROM datacatalog WHERE _id = ?`
	err := GlobalKBSQLite.DB.QueryRow(query, targetURI).Scan(&downstreamJSON)
	if err != nil {
		return err
	}

	var downstream []string
	json.Unmarshal([]byte(downstreamJSON), &downstream)

	// Filter out the URI
	filtered := []string{}
	for _, uri := range downstream {
		if uri != uriToRemove {
			filtered = append(filtered, uri)
		}
	}

	newJSON, _ := json.Marshal(filtered)
	query = `UPDATE datacatalog SET lineage_downstream = ?, updated_timestamp = ? WHERE _id = ?`
	_, err = GlobalKBSQLite.DB.Exec(query, string(newJSON), time.Now().Unix(), targetURI)
	return err
}

func (lm *LineageManager) removeFromUpstream(targetURI, uriToRemove string) error {
	var upstreamJSON string
	query := `SELECT lineage_upstream FROM datacatalog WHERE _id = ?`
	err := GlobalKBSQLite.DB.QueryRow(query, targetURI).Scan(&upstreamJSON)
	if err != nil {
		return err
	}

	var upstream []string
	json.Unmarshal([]byte(upstreamJSON), &upstream)

	// Filter out the URI
	filtered := []string{}
	for _, uri := range upstream {
		if uri != uriToRemove {
			filtered = append(filtered, uri)
		}
	}

	newJSON, _ := json.Marshal(filtered)
	query = `UPDATE datacatalog SET lineage_upstream = ?, updated_timestamp = ? WHERE _id = ?`
	_, err = GlobalKBSQLite.DB.Exec(query, string(newJSON), time.Now().Unix(), targetURI)
	return err
}
