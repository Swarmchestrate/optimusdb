// =============================================================================
// TOSCA QUERY HELPERS - tosca/query_helpers.go
// =============================================================================
// This file provides reusable query functions for TOSCA templates in OrbitDB
// =============================================================================

package tosca

import (
	"context"
	"fmt"
	"strings"

	iface "berty.tech/go-orbit-db"
)

// =============================================================================
// QUERY BUILDER
// =============================================================================

// QueryBuilder provides a fluent interface for building TOSCA queries
type QueryBuilder struct {
	filters []func(map[string]interface{}) bool
}

// NewQueryBuilder creates a new query builder
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{filters: make([]func(map[string]interface{}) bool, 0)}
}

// WithNodeType adds a node type filter
func (qb *QueryBuilder) WithNodeType(nodeType string) *QueryBuilder {
	qb.filters = append(qb.filters, func(doc map[string]interface{}) bool {
		return HasNodeType(doc, nodeType)
	})
	return qb
}

// WithNodeName adds a node name filter
func (qb *QueryBuilder) WithNodeName(nodeName string) *QueryBuilder {
	qb.filters = append(qb.filters, func(doc map[string]interface{}) bool {
		return HasNodeTemplate(doc, nodeName)
	})
	return qb
}

// WithProperty adds a property filter for any node
func (qb *QueryBuilder) WithProperty(propertyName string, propertyValue interface{}) *QueryBuilder {
	qb.filters = append(qb.filters, func(doc map[string]interface{}) bool {
		if topologyTemplate, ok := doc["topology_template"].(map[string]interface{}); ok {
			if nodeTemplates, ok := topologyTemplate["node_templates"].(map[string]interface{}); ok {
				for _, nodeData := range nodeTemplates {
					if node, ok := nodeData.(map[string]interface{}); ok {
						if properties, ok := node["properties"].(map[string]interface{}); ok {
							if value, exists := properties[propertyName]; exists {
								return fmt.Sprintf("%v", value) == fmt.Sprintf("%v", propertyValue)
							}
						}
					}
				}
			}
		}
		return false
	})
	return qb
}

// WithNodeProperty adds a property filter for a specific node
func (qb *QueryBuilder) WithNodeProperty(nodeName, propertyName string, propertyValue interface{}) *QueryBuilder {
	qb.filters = append(qb.filters, func(doc map[string]interface{}) bool {
		value, exists := GetNodeProperty(doc, nodeName, propertyName)
		return exists && fmt.Sprintf("%v", value) == fmt.Sprintf("%v", propertyValue)
	})
	return qb
}

// WithPort adds a port filter
func (qb *QueryBuilder) WithPort(port string) *QueryBuilder {
	qb.filters = append(qb.filters, func(doc map[string]interface{}) bool {
		nodes := FindPortMapping(doc, port)
		return len(nodes) > 0
	})
	return qb
}

// WithEnvironmentVariable adds an environment variable filter
func (qb *QueryBuilder) WithEnvironmentVariable(varName string) *QueryBuilder {
	qb.filters = append(qb.filters, func(doc map[string]interface{}) bool {
		nodes := FindEnvironmentVariable(doc, varName)
		return len(nodes) > 0
	})
	return qb
}

// WithMetadata adds a metadata filter
func (qb *QueryBuilder) WithMetadata(fieldName, fieldValue string) *QueryBuilder {
	qb.filters = append(qb.filters, func(doc map[string]interface{}) bool {
		value := ExtractMetadataField(doc, fieldName)
		return value == fieldValue
	})
	return qb
}

// WithTOSCAVersion adds a TOSCA version filter
func (qb *QueryBuilder) WithTOSCAVersion(version string) *QueryBuilder {
	qb.filters = append(qb.filters, func(doc map[string]interface{}) bool {
		if v, ok := doc["tosca_definitions_version"].(string); ok {
			return v == version
		}
		return false
	})
	return qb
}

// WithMinNodeCount adds a minimum node count filter
func (qb *QueryBuilder) WithMinNodeCount(minCount int) *QueryBuilder {
	qb.filters = append(qb.filters, func(doc map[string]interface{}) bool {
		return CountNodeTemplatesFromJSON(doc) >= minCount
	})
	return qb
}

// WithMaxNodeCount adds a maximum node count filter
func (qb *QueryBuilder) WithMaxNodeCount(maxCount int) *QueryBuilder {
	qb.filters = append(qb.filters, func(doc map[string]interface{}) bool {
		return CountNodeTemplatesFromJSON(doc) <= maxCount
	})
	return qb
}

// Build creates the final query filter function
func (qb *QueryBuilder) Build() func(interface{}) (bool, error) {
	return func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		// Check if it's a TOSCA document
		if _, hasTosca := record["topology_template"]; !hasTosca {
			return false, nil
		}

		// All filters must pass (AND logic)
		for _, filter := range qb.filters {
			if !filter(record) {
				return false, nil
			}
		}

		return true, nil
	}
}

// =============================================================================
// CONVENIENCE QUERY FUNCTIONS
// =============================================================================

// FindAllTOSCATemplates returns all TOSCA templates from the store
func FindAllTOSCATemplates(dbDocStore iface.DocumentStore) ([]map[string]interface{}, error) {
	ctx := context.Background()

	results, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		// Check if it's a TOSCA document
		_, hasTosca := record["topology_template"]
		return hasTosca, nil
	})

	if err != nil {
		return nil, err
	}

	return convertResults(results), nil
}

// FindTemplatesByNodeType finds all templates containing a specific node type
func FindTemplatesByNodeType(dbDocStore iface.DocumentStore, nodeType string) ([]map[string]interface{}, error) {
	ctx := context.Background()

	results, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		return HasNodeType(record, nodeType), nil
	})

	if err != nil {
		return nil, err
	}

	return convertResults(results), nil
}

// FindTemplatesByImage finds all templates with a specific Docker image
func FindTemplatesByImage(dbDocStore iface.DocumentStore, imageName string) ([]map[string]interface{}, error) {
	ctx := context.Background()

	results, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		topologyTemplate, ok := record["topology_template"].(map[string]interface{})
		if !ok {
			return false, nil
		}

		nodeTemplates, ok := topologyTemplate["node_templates"].(map[string]interface{})
		if !ok {
			return false, nil
		}

		for _, nodeData := range nodeTemplates {
			node, ok := nodeData.(map[string]interface{})
			if !ok {
				continue
			}

			properties, ok := node["properties"].(map[string]interface{})
			if !ok {
				continue
			}

			image, ok := properties["image"].(string)
			if ok && strings.Contains(image, imageName) {
				return true, nil
			}
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return convertResults(results), nil
}

// FindTemplatesByPort finds all templates exposing a specific port
func FindTemplatesByPort(dbDocStore iface.DocumentStore, port string) ([]map[string]interface{}, error) {
	ctx := context.Background()

	results, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		nodes := FindPortMapping(record, port)
		return len(nodes) > 0, nil
	})

	if err != nil {
		return nil, err
	}

	return convertResults(results), nil
}

// FindTemplatesByEnvVar finds all templates with a specific environment variable
func FindTemplatesByEnvVar(dbDocStore iface.DocumentStore, varName string) ([]map[string]interface{}, error) {
	ctx := context.Background()

	results, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		nodes := FindEnvironmentVariable(record, varName)
		return len(nodes) > 0, nil
	})

	if err != nil {
		return nil, err
	}

	return convertResults(results), nil
}

// FindTemplatesByNodeCount finds templates with a specific number of nodes
func FindTemplatesByNodeCount(dbDocStore iface.DocumentStore, minNodes, maxNodes int) ([]map[string]interface{}, error) {
	ctx := context.Background()

	results, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		nodeCount := CountNodeTemplatesFromJSON(record)
		return nodeCount >= minNodes && nodeCount <= maxNodes, nil
	})

	if err != nil {
		return nil, err
	}

	return convertResults(results), nil
}

// FindTemplatesByRequirement finds templates that require a specific dependency
func FindTemplatesByRequirement(dbDocStore iface.DocumentStore, requirementType string) ([]map[string]interface{}, error) {
	ctx := context.Background()

	results, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		topologyTemplate, ok := record["topology_template"].(map[string]interface{})
		if !ok {
			return false, nil
		}

		nodeTemplates, ok := topologyTemplate["node_templates"].(map[string]interface{})
		if !ok {
			return false, nil
		}

		for _, nodeData := range nodeTemplates {
			node, ok := nodeData.(map[string]interface{})
			if !ok {
				continue
			}

			requirements, ok := node["requirements"].([]interface{})
			if !ok {
				continue
			}

			for _, req := range requirements {
				reqMap, ok := req.(map[string]interface{})
				if !ok {
					continue
				}

				// Check for requirement type
				for key, value := range reqMap {
					if strings.Contains(strings.ToLower(key), strings.ToLower(requirementType)) {
						return true, nil
					}
					if valueStr, ok := value.(string); ok {
						if strings.Contains(strings.ToLower(valueStr), strings.ToLower(requirementType)) {
							return true, nil
						}
					}
				}
			}
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return convertResults(results), nil
}

// FindTemplateByID finds a template by its _id (fast lookup)
func FindTemplateByID(dbDocStore iface.DocumentStore, templateID string) (map[string]interface{}, error) {
	ctx := context.Background()

	results, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		if id, ok := record["_id"].(string); ok {
			return id == templateID, nil
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	converted := convertResults(results)
	if len(converted) == 0 {
		return nil, fmt.Errorf("template not found: %s", templateID)
	}

	return converted[0], nil
}

// FindTemplatesByTOSCAVersion finds templates by TOSCA version
func FindTemplatesByTOSCAVersion(dbDocStore iface.DocumentStore, version string) ([]map[string]interface{}, error) {
	ctx := context.Background()

	results, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		if v, ok := record["tosca_definitions_version"].(string); ok {
			return v == version, nil
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return convertResults(results), nil
}

// =============================================================================
// ADVANCED QUERY EXAMPLES
// =============================================================================

// FindComplexTemplates finds templates matching multiple complex criteria
// Example: Docker containers with PostgreSQL dependency and HTTPS exposed
func FindComplexTemplates(dbDocStore iface.DocumentStore) ([]map[string]interface{}, error) {
	query := NewQueryBuilder().
		WithNodeType("Docker").
		WithNodeType("PostgreSQL").
		WithPort("443").
		Build()

	ctx := context.Background()
	results, err := dbDocStore.Query(ctx, query)

	if err != nil {
		return nil, err
	}

	return convertResults(results), nil
}

// FindProductionTemplates finds templates marked for production
func FindProductionTemplates(dbDocStore iface.DocumentStore) ([]map[string]interface{}, error) {
	ctx := context.Background()

	results, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		// Check metadata for environment=production
		if metadata, ok := record["metadata"].(map[string]interface{}); ok {
			if env, ok := metadata["environment"].(string); ok {
				return strings.ToLower(env) == "production", nil
			}
		}

		// Check if any node has production-related properties
		if topologyTemplate, ok := record["topology_template"].(map[string]interface{}); ok {
			if nodeTemplates, ok := topologyTemplate["node_templates"].(map[string]interface{}); ok {
				for _, nodeData := range nodeTemplates {
					if node, ok := nodeData.(map[string]interface{}); ok {
						if properties, ok := node["properties"].(map[string]interface{}); ok {
							if env, ok := properties["environment"].(map[string]interface{}); ok {
								if nodeEnv, ok := env["NODE_ENV"].(string); ok {
									return strings.ToLower(nodeEnv) == "production", nil
								}
							}
						}
					}
				}
			}
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return convertResults(results), nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// convertResults converts OrbitDB query results to []map[string]interface{}
func convertResults(results []interface{}) []map[string]interface{} {
	converted := make([]map[string]interface{}, 0, len(results))
	for _, doc := range results {
		if docMap, ok := doc.(map[string]interface{}); ok {
			converted = append(converted, docMap)
		}
	}
	return converted
}

// CountResults returns the count of matching documents without retrieving them
func CountResults(dbDocStore iface.DocumentStore, filterFunc func(interface{}) (bool, error)) (int, error) {
	ctx := context.Background()

	results, err := dbDocStore.Query(ctx, filterFunc)
	if err != nil {
		return 0, err
	}

	return len(results), nil
}
