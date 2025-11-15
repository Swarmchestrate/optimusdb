package tester

import (
	"fmt"
	"log"

	"optimusdb/datamodel"
)

func main() {
	// Load schemas from the file
	filePath := "kbmetadata.json"

	schemasData, err := datamodel.LoadSchemas(filePath)
	//schemas, err := loadSchemas(filePath)
	if err != nil {
		log.Fatalf("Error loading schemas: %v", err)
	}

	// Generate and print the struct for KBmetadata
	kbmetadataSchema, exists := schemasData["KBmetadata"]
	if !exists {
		log.Fatalf("Schema for KBmetadata not found")
	}

	structDef := datamodel.GenerateStruct(kbmetadataSchema)
	fmt.Println("Generated Struct:\n", structDef)
}
