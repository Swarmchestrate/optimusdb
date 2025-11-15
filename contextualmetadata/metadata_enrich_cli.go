package contextualmetadata

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	// Command-line flags
	db := flag.String("db", "", "Database file path (required)")
	table := flag.String("table", "", "Table name (required)")
	maxRows := flag.Int("rows", 200, "Maximum rows to sample for profiling")
	endpoint := flag.String("endpoint", "", "TinyLlama endpoint (default from env)")
	profileOnly := flag.Bool("profile-only", false, "Only profile, don't enrich")
	useGreek := flag.Bool("greek", false, "Generate description in Greek")
	verbose := flag.Bool("v", false, "Verbose output")

	flag.Parse()

	// Validate required flags
	if *db == "" || *table == "" {
		fmt.Println("Error: Both -db and -table are required")
		flag.Usage()
		os.Exit(1)
	}

	// Set log level
	if !*verbose {
		log.SetOutput(os.Stderr)
	}

	// Print header
	printHeader()

	// Step 1: Profile the dataset
	fmt.Println("📊 Step 1: Profiling dataset...")
	fmt.Printf("   Database: %s\n", *db)
	fmt.Printf("   Table: %s\n", *table)
	fmt.Printf("   Max rows: %d\n\n", *maxRows)

	profileStart := time.Now()
	profile, err := ProfileTable(*db, *table, *maxRows)
	if err != nil {
		fmt.Printf("❌ Profiling failed: %v\n", err)
		os.Exit(1)
	}
	profileDuration := time.Since(profileStart)

	fmt.Printf("✅ Profiling completed in %v\n\n", profileDuration)

	// Display profile summary
	displayProfileSummary(profile)

	// If profile-only mode, exit here
	if *profileOnly {
		fmt.Println("\n✨ Profile-only mode: Done!")
		os.Exit(0)
	}

	// Step 2: Initialize TinyLlama client
	fmt.Println("\n🤖 Step 2: Connecting to TinyLlama...")

	if *endpoint != "" {
		os.Setenv("TINYLLAMA_ENDPOINT", *endpoint)
	}

	client, err := NewTinyLlamaHTTP()
	if err != nil {
		fmt.Printf("⚠️  Failed to create TinyLlama client: %v\n", err)
		fmt.Println("ℹ️  Will use basic metadata generation\n")
		client = nil
	} else {
		// Health check
		healthCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		fmt.Printf("   Testing connection to: %s\n", client.Endpoint)
		if err := client.HealthCheck(healthCtx); err != nil {
			fmt.Printf("⚠️  Health check failed: %v\n", err)
			fmt.Println("ℹ️  Will use basic metadata generation\n")
			client = nil
		} else {
			fmt.Println("✅ TinyLlama is healthy\n")
		}
	}

	// Step 3: Enrich metadata
	fmt.Println("✨ Step 3: Generating enriched metadata...")

	service := &Service{
		UseGreek: *useGreek,
		Client:   client,
	}

	enrichStart := time.Now()
	metadata, err := service.EnrichDatasetWithProfile(context.Background(), profile)
	if err != nil {
		fmt.Printf("❌ Enrichment failed: %v\n", err)
		os.Exit(1)
	}
	enrichDuration := time.Since(enrichStart)

	fmt.Printf("✅ Enrichment completed in %v\n\n", enrichDuration)

	// Step 4: Display results
	displayResults(metadata, profileDuration, enrichDuration)

	// Save to file
	saveResults(metadata, *db, *table)

	fmt.Println("\n🎉 All done!")
}

func printHeader() {
	header := `
╔═══════════════════════════════════════════════════════════╗
║         OptimusDB Metadata Enrichment Tool                ║
║         Powered by TinyLlama 1.1B                          ║
╚═══════════════════════════════════════════════════════════╝
`
	fmt.Println(header)
}

func displayProfileSummary(profile *DatasetProfile) {
	fmt.Println("📈 Profile Summary:")
	fmt.Println(strings.Repeat("─", 60))
	fmt.Printf("  Rows: %d\n", profile.RowCount)
	fmt.Printf("  Columns: %d\n\n", len(profile.Profiles))

	// Infer domain
	domain := InferDomain(profile)
	fmt.Printf("  Inferred Domain: %s\n\n", domain)

	// Column breakdown
	fmt.Println("  Column Types:")
	typeCount := make(map[string]int)
	for _, col := range profile.Profiles {
		typeCount[col.InferredType]++
	}
	for typ, count := range typeCount {
		fmt.Printf("    - %s: %d\n", typ, count)
	}

	// Special columns
	identifiers := 0
	timestamps := 0
	geo := 0

	for _, col := range profile.Profiles {
		if col.IsIdentifier {
			identifiers++
		}
		if col.IsTimestamp {
			timestamps++
		}
		if col.IsGeo {
			geo++
		}
	}

	if identifiers > 0 || timestamps > 0 || geo > 0 {
		fmt.Println("\n  Special Columns:")
		if identifiers > 0 {
			fmt.Printf("    - Identifiers: %d\n", identifiers)
		}
		if timestamps > 0 {
			fmt.Printf("    - Timestamps: %d\n", timestamps)
		}
		if geo > 0 {
			fmt.Printf("    - Geographic: %d\n", geo)
		}
	}

	fmt.Println()
}

func displayResults(metadata map[string]any, profileTime, enrichTime time.Duration) {
	fmt.Println("📝 Enriched Metadata:")
	fmt.Println(strings.Repeat("═", 60))

	// Description
	if desc, ok := metadata["description"].(string); ok {
		fmt.Println("\n📖 Description:")
		fmt.Println(strings.Repeat("─", 60))
		wrapped := wrapText(desc, 58)
		for _, line := range wrapped {
			fmt.Printf("  %s\n", line)
		}
	}

	// Tags
	if tags, ok := metadata["tags"].([]string); ok {
		fmt.Println("\n🏷️  Tags:")
		fmt.Println(strings.Repeat("─", 60))
		fmt.Printf("  %s\n", strings.Join(tags, ", "))
	}

	// Domain
	if domain, ok := metadata["domain"].(string); ok {
		fmt.Printf("\n🌐 Domain: %s\n", domain)
	}

	// Statistics
	fmt.Println("\n⏱️  Performance:")
	fmt.Println(strings.Repeat("─", 60))
	fmt.Printf("  Profile time: %v\n", profileTime)
	fmt.Printf("  Enrich time: %v\n", enrichTime)
	fmt.Printf("  Total time: %v\n", profileTime+enrichTime)

	// Status
	if status, ok := metadata["status"].(string); ok {
		fmt.Printf("\n📊 Status: %s\n", status)
	}
}

func saveResults(metadata map[string]any, db, table string) {
	filename := fmt.Sprintf("metadata_%s_%s_%d.json",
		sanitizeFilename(db),
		sanitizeFilename(table),
		time.Now().Unix())

	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("\n⚠️  Could not save results to file: %v\n", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metadata); err != nil {
		fmt.Printf("\n⚠️  Error writing JSON: %v\n", err)
		return
	}

	fmt.Printf("\n💾 Results saved to: %s\n", filename)
}

func wrapText(text string, width int) []string {
	words := strings.Fields(text)
	var lines []string
	var currentLine strings.Builder

	for _, word := range words {
		if currentLine.Len()+len(word)+1 > width {
			if currentLine.Len() > 0 {
				lines = append(lines, currentLine.String())
				currentLine.Reset()
			}
		}
		if currentLine.Len() > 0 {
			currentLine.WriteString(" ")
		}
		currentLine.WriteString(word)
	}

	if currentLine.Len() > 0 {
		lines = append(lines, currentLine.String())
	}

	return lines
}

func sanitizeFilename(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, ".db", "")
	return s
}
