package datamodel

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

// Field represents a single field in the schema
type Field struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

// Schema represents the structure of a table
type Schema struct {
	Fields []Field `json:"fields"`
}

// Schemas represents the collection of all schemas
type Schemas map[string]Schema

func LoadSchemas(filePath string) (Schemas, error) {
	// Open the schema file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open schema file: %v", err)
	}
	defer file.Close()

	// Read and parse the file
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %v", err)
	}

	var schemas Schemas
	if err := json.Unmarshal(data, &schemas); err != nil {
		return nil, fmt.Errorf("failed to parse schema file: %v", err)
	}

	return schemas, nil
}

func GenerateStruct(schema Schema) string {
	structDef := "type Metadata struct {\n"
	for _, field := range schema.Fields {
		goType := mapToGoType(field.Type)
		structDef += fmt.Sprintf("    %s %s `json:\"%s\"` // %s\n",
			capitalize(field.Name), goType, field.Name, field.Description)
	}
	structDef += "}"
	return structDef
}

func mapToGoType(schemaType string) string {
	switch schemaType {
	case "string":
		return "string"
	case "timestamp":
		return "time.Time"
	case "array[string]":
		return "[]string"
	case "array[object]":
		return "[]map[string]interface{}"
	case "object":
		return "map[string]interface{}"
	default:
		return "interface{}"
	}
}

func capitalize(name string) string {
	return string(name[0]-32) + name[1:]
}
