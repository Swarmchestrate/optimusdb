package tosca

// =============================================================================
// UNIT TESTS — tosca/toscaparser_test.go
// =============================================================================
// Tests for the template ID generation functions, specifically covering the
// SAT deduplication fix (KB v1 integration, filename == swarmID).
//
// Run with:   go test ./tosca/... -v
// Run single: go test ./tosca/... -v -run TestComputeTemplateID
// =============================================================================

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

var (
	sampleContent    = []byte("tosca_definitions_version: tosca_simple_yaml_1_3\ndescription: Sample SAT")
	duplicateContent = []byte("tosca_definitions_version: tosca_simple_yaml_1_3\ndescription: Sample SAT") // identical to sampleContent
	differentContent = []byte("tosca_definitions_version: tosca_simple_yaml_1_3\ndescription: Different SAT")
)

// ---------------------------------------------------------------------------
// ComputeTemplateID (original, content-only hash)
// ---------------------------------------------------------------------------

// TestComputeTemplateID_Deterministic verifies that the same content always
// produces the same ID (required for cache lookups / re-upload detection).
func TestComputeTemplateID_Deterministic(t *testing.T) {
	id1 := ComputeTemplateID(sampleContent)
	id2 := ComputeTemplateID(sampleContent)
	assert.Equal(t, id1, id2, "same content must always produce the same ID")
}

// TestComputeTemplateID_DifferentContentProducesDifferentID is a basic
// sanity check that the hash distinguishes genuinely different files.
func TestComputeTemplateID_DifferentContentProducesDifferentID(t *testing.T) {
	id1 := ComputeTemplateID(sampleContent)
	id2 := ComputeTemplateID(differentContent)
	assert.NotEqual(t, id1, id2, "different content must produce different IDs")
}

// TestComputeTemplateID_FormatIsHex verifies the returned string is a
// lowercase hex string of the expected length (8 bytes = 16 hex chars).
func TestComputeTemplateID_FormatIsHex(t *testing.T) {
	id := ComputeTemplateID(sampleContent)
	assert.Len(t, id, 16, "ID must be 16 hex characters (8 bytes)")
	assert.Regexp(t, "^[0-9a-f]+$", id, "ID must be lowercase hex")
}

// ---------------------------------------------------------------------------
// THE CORE BUG REGRESSION TEST
// ---------------------------------------------------------------------------

// TestComputeTemplateID_SameContentSameID reproduces the original bug:
// two files with identical bytes produced the same _id, causing OrbitDB to
// overwrite the first document when the second was Put().
//
// This test documents the KNOWN LIMITATION of ComputeTemplateID and confirms
// that it is the root cause. It should PASS (i.e. the IDs ARE equal),
// serving as a permanent regression marker that reminds future developers
// WHY ComputeTemplateIDWithSeed exists.
func TestComputeTemplateID_SameContentSameID_KnownLimitation(t *testing.T) {
	idA := ComputeTemplateID(sampleContent)
	idB := ComputeTemplateID(duplicateContent)

	// This EQUALITY is the bug. If it ever becomes NOT equal something
	// has changed in the hash function — flag it immediately.
	assert.Equal(t, idA, idB,
		"[KNOWN LIMITATION] ComputeTemplateID is content-only: two files with identical "+
			"content will collide in OrbitDB. Use ComputeTemplateIDWithSeed instead.")
}

// ---------------------------------------------------------------------------
// ComputeTemplateIDWithSeed (the fix)
// ---------------------------------------------------------------------------

// TestComputeTemplateIDWithSeed_SameContentDifferentSeedsProduceDifferentIDs
// is the PRIMARY regression test for the fix.
//
// Scenario: the RA uploads two SAT files with identical YAML content but
// different filenames (swarmIDs). Before the fix both would map to the same
// OrbitDB _id and the second upload would silently overwrite the first.
func TestComputeTemplateIDWithSeed_SameContentDifferentSeedsProduceDifferentIDs(t *testing.T) {
	idA := ComputeTemplateIDWithSeed("swarm-alpha.yaml", sampleContent)
	idB := ComputeTemplateIDWithSeed("swarm-beta.yaml", sampleContent)

	assert.NotEqual(t, idA, idB,
		"same content with different filenames (swarmIDs) must produce different IDs "+
			"to prevent OrbitDB _id collisions and silent overwrites")
}

// TestComputeTemplateIDWithSeed_Deterministic verifies the seeded variant is
// also deterministic (same seed + content → same ID on every call).
func TestComputeTemplateIDWithSeed_Deterministic(t *testing.T) {
	id1 := ComputeTemplateIDWithSeed("swarm-alpha.yaml", sampleContent)
	id2 := ComputeTemplateIDWithSeed("swarm-alpha.yaml", sampleContent)
	assert.Equal(t, id1, id2, "seeded ID must be deterministic for the same seed + content")
}

// TestComputeTemplateIDWithSeed_DifferentContentDifferentSeed checks that
// both dimensions (seed and content) contribute to uniqueness.
func TestComputeTemplateIDWithSeed_DifferentContentDifferentSeed(t *testing.T) {
	cases := []struct {
		name     string
		seedA    string
		contentA []byte
		seedB    string
		contentB []byte
	}{
		{
			name:  "same seed, different content",
			seedA: "file.yaml", contentA: sampleContent,
			seedB: "file.yaml", contentB: differentContent,
		},
		{
			name:  "different seed, different content",
			seedA: "alpha.yaml", contentA: sampleContent,
			seedB: "beta.yaml", contentB: differentContent,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idA := ComputeTemplateIDWithSeed(tc.seedA, tc.contentA)
			idB := ComputeTemplateIDWithSeed(tc.seedB, tc.contentB)
			assert.NotEqual(t, idA, idB, "IDs must differ: %s", tc.name)
		})
	}
}

// TestComputeTemplateIDWithSeed_DiffersFromContentOnlyHash ensures the seeded
// ID is distinct from the pure content hash, so the two functions cannot be
// accidentally swapped in a multi-store environment.
func TestComputeTemplateIDWithSeed_DiffersFromContentOnlyHash(t *testing.T) {
	unseeded := ComputeTemplateID(sampleContent)
	seeded := ComputeTemplateIDWithSeed("swarm-alpha.yaml", sampleContent)
	assert.NotEqual(t, unseeded, seeded,
		"seeded ID must differ from the plain content hash")
}

// TestComputeTemplateIDWithSeed_FormatIsHex verifies the seeded variant
// returns the same 16-character lowercase hex format.
func TestComputeTemplateIDWithSeed_FormatIsHex(t *testing.T) {
	id := ComputeTemplateIDWithSeed("swarm-alpha.yaml", sampleContent)
	assert.Len(t, id, 16, "seeded ID must be 16 hex characters (8 bytes)")
	assert.Regexp(t, "^[0-9a-f]+$", id, "seeded ID must be lowercase hex")
}

// TestComputeTemplateIDWithSeed_EmptySeedFallsBackToContentHash documents
// that an empty seed produces a result different from ComputeTemplateID.
// (The seed separator ":" is always included, so "" + ":" + content ≠ content.)
// This guards against accidentally passing an empty swarmID.
func TestComputeTemplateIDWithSeed_EmptySeedBehavior(t *testing.T) {
	seededEmpty := ComputeTemplateIDWithSeed("", sampleContent)
	unseeded := ComputeTemplateID(sampleContent)
	assert.NotEqual(t, seededEmpty, unseeded,
		"empty-seed result must differ from content-only hash due to the ':' separator; "+
			"callers must not pass an empty seed — use ComputeTemplateID directly for legacy paths")
}

// ---------------------------------------------------------------------------
// Table-driven: multi-file upload simulation (the exact KB v1 scenario)
// ---------------------------------------------------------------------------

// TestKBv1_MultipleFilesWithSameContent simulates the RA uploading several
// SAT files that happen to share the same YAML body (e.g. default templates
// deployed across different swarms). Verifies every swarmID gets a unique _id.
func TestKBv1_MultipleFilesWithSameContent(t *testing.T) {
	sharedContent := []byte("tosca_definitions_version: tosca_simple_yaml_1_3\ndescription: Default SAT template")

	swarmIDs := []string{
		"swarm-eu-west-1.yaml",
		"swarm-eu-west-2.yaml",
		"swarm-eu-central-1.yaml",
		"swarm-ap-southeast-1.yaml",
		"swarm-us-east-1.yaml",
	}

	generated := make(map[string]string) // id → swarmID (for collision detection)

	for _, swarmID := range swarmIDs {
		id := ComputeTemplateIDWithSeed(swarmID, sharedContent)
		if existing, collision := generated[id]; collision {
			t.Errorf("COLLISION: swarmID %q and %q produced the same _id %q — "+
				"OrbitDB would overwrite the first document", swarmID, existing, id)
		}
		generated[id] = swarmID
	}

	assert.Len(t, generated, len(swarmIDs),
		"every swarmID must produce a unique _id even when all content is identical")
}
