// =============================================================================
// COMPLETE TOSCA PARSER - tosca/toscaparser.go
// =============================================================================
// This file provides comprehensive TOSCA template parsing and manipulation
// Supports both legacy struct-based parsing and full JSON structure preservation
// =============================================================================

package tosca

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// =============================================================================
// LEGACY STRUCTURES (Backward Compatibility)
// =============================================================================

// TOSCATemplate represents a basic TOSCA template structure
type TOSCATemplate struct {
	ToscaDefinitionsVersion string                 `yaml:"tosca_definitions_version"`
	Description             string                 `yaml:"description"`
	Metadata                map[string]interface{} `yaml:"metadata"`
	TopologyTemplate        TopologyTemplate       `yaml:"topology_template"`
}

// TopologyTemplate represents the topology_template section
type TopologyTemplate struct {
	NodeTemplates map[string]NodeTemplate `yaml:"node_templates"`
}

// NodeTemplate represents a node template
type NodeTemplate struct {
	Type       string                 `yaml:"type"`
	Properties map[string]interface{} `yaml:"properties"`
}

// =============================================================================
// LEGACY PARSING FUNCTIONS
// =============================================================================

// ParseTOSCA parses TOSCA YAML into a structured object (legacy approach)
func ParseTOSCA(yamlContent []byte) (*TOSCATemplate, error) {
	var template TOSCATemplate
	err := yaml.Unmarshal(yamlContent, &template)
	if err != nil {
		return nil, err
	}
	return &template, nil
}

// CountNodeTemplates counts node templates in a parsed TOSCA template (legacy)
func CountNodeTemplates(template *TOSCATemplate) int {
	if template == nil || template.TopologyTemplate.NodeTemplates == nil {
		return 0
	}
	return len(template.TopologyTemplate.NodeTemplates)
}

// =============================================================================
// MODERN FULL STRUCTURE SUPPORT
// =============================================================================

// ParseTOSCAToFullJSON parses TOSCA YAML to complete queryable JSON structure
// This preserves the entire TOSCA structure as a map for queryability in OrbitDB
func ParseTOSCAToFullJSON(yamlContent []byte) (map[string]interface{}, error) {
	var toscaStructure map[string]interface{}

	// Parse YAML to map with full structure preservation
	err := yaml.Unmarshal(yamlContent, &toscaStructure)
	if err != nil {
		return nil, fmt.Errorf("failed to parse TOSCA YAML: %w", err)
	}

	return toscaStructure, nil
}

// CountNodeTemplatesFromJSON counts node templates from parsed JSON structure
// This works with the full JSON structure instead of the TOSCATemplate struct
func CountNodeTemplatesFromJSON(toscaDoc map[string]interface{}) int {
	// Navigate to topology_template.node_templates
	if topologyTemplate, ok := toscaDoc["topology_template"].(map[string]interface{}); ok {
		if nodeTemplates, ok := topologyTemplate["node_templates"].(map[string]interface{}); ok {
			return len(nodeTemplates)
		}
	}

	// Fallback: check if there's a direct node_templates key (some TOSCA variants)
	if nodeTemplates, ok := toscaDoc["node_templates"].(map[string]interface{}); ok {
		return len(nodeTemplates)
	}

	return 0
}

// =============================================================================
// ID GENERATION
// =============================================================================

// ComputeTemplateID generates a unique ID for a TOSCA template based on content hash.
// NOTE: Two files with identical content will produce the same ID regardless of filename.
// Use ComputeTemplateIDWithSeed when the filename (e.g. swarmID) must distinguish them.
func ComputeTemplateID(yamlContent []byte) string {
	hash := sha256.Sum256(yamlContent)
	return fmt.Sprintf("%x", hash[:8]) // Use first 8 bytes for shorter ID
}

// ComputeTemplateIDWithSeed generates a unique ID by hashing both the seed (e.g. filename /
// swarmID) and the file content together. This guarantees that two files with identical
// content but different seeds (filenames) produce different IDs, preventing OrbitDB
// DocumentStore overwrites caused by _id collisions.
//
// Use this in v1 KB/RA integration where filename == swarmID.
// The pure content hash is preserved in the content_hash metadata field for dedup auditing.
func ComputeTemplateIDWithSeed(seed string, yamlContent []byte) string {
	combined := append([]byte(seed+":"), yamlContent...)
	hash := sha256.Sum256(combined)
	return fmt.Sprintf("%x", hash[:8])
}

// =============================================================================
// FIELD EXTRACTION HELPERS
// =============================================================================

// ExtractMetadataField extracts a specific metadata field from parsed TOSCA
func ExtractMetadataField(toscaDoc map[string]interface{}, fieldName string) string {
	if metadata, ok := toscaDoc["metadata"].(map[string]interface{}); ok {
		if value, ok := metadata[fieldName].(string); ok {
			return value
		}
	}
	return ""
}

// ExtractDescription extracts the description from TOSCA document
func ExtractDescription(toscaDoc map[string]interface{}) string {
	if desc, ok := toscaDoc["description"].(string); ok {
		return desc
	}

	// Fallback to metadata description
	if metadata, ok := toscaDoc["metadata"].(map[string]interface{}); ok {
		if desc, ok := metadata["description"].(string); ok {
			return desc
		}
		if templateName, ok := metadata["template_name"].(string); ok {
			return templateName
		}
	}

	return "No description"
}

// ExtractNodeTemplate extracts a specific node template by name
func ExtractNodeTemplate(toscaDoc map[string]interface{}, nodeName string) (map[string]interface{}, bool) {
	if topologyTemplate, ok := toscaDoc["topology_template"].(map[string]interface{}); ok {
		if nodeTemplates, ok := topologyTemplate["node_templates"].(map[string]interface{}); ok {
			if nodeTemplate, ok := nodeTemplates[nodeName].(map[string]interface{}); ok {
				return nodeTemplate, true
			}
		}
	}
	return nil, false
}

// GetNodeProperty gets a property value from a node template
func GetNodeProperty(toscaDoc map[string]interface{}, nodeName, propertyName string) (interface{}, bool) {
	if nodeTemplate, exists := ExtractNodeTemplate(toscaDoc, nodeName); exists {
		if properties, ok := nodeTemplate["properties"].(map[string]interface{}); ok {
			if value, ok := properties[propertyName]; ok {
				return value, true
			}
		}
	}
	return nil, false
}

// GetNestedProperty gets a nested property using dot notation
// Example: GetNestedProperty(nodeTemplate, "properties.environment.DATABASE.host")
func GetNestedProperty(obj map[string]interface{}, path string) (interface{}, bool) {
	parts := strings.Split(path, ".")

	var current interface{} = obj
	for _, part := range parts {
		if currentMap, ok := current.(map[string]interface{}); ok {
			if next, exists := currentMap[part]; exists {
				current = next
			} else {
				return nil, false
			}
		} else {
			return nil, false
		}
	}

	return current, true
}

// GetAllNodeNames returns a list of all node template names
func GetAllNodeNames(toscaDoc map[string]interface{}) []string {
	names := []string{}

	if topologyTemplate, ok := toscaDoc["topology_template"].(map[string]interface{}); ok {
		if nodeTemplates, ok := topologyTemplate["node_templates"].(map[string]interface{}); ok {
			for name := range nodeTemplates {
				names = append(names, name)
			}
		}
	}

	return names
}

// GetAllNodeTypes returns a list of all unique node types in the template
func GetAllNodeTypes(toscaDoc map[string]interface{}) []string {
	typesMap := make(map[string]bool)
	types := []string{}

	if topologyTemplate, ok := toscaDoc["topology_template"].(map[string]interface{}); ok {
		if nodeTemplates, ok := topologyTemplate["node_templates"].(map[string]interface{}); ok {
			for _, nodeData := range nodeTemplates {
				if node, ok := nodeData.(map[string]interface{}); ok {
					if nodeType, ok := node["type"].(string); ok {
						if !typesMap[nodeType] {
							typesMap[nodeType] = true
							types = append(types, nodeType)
						}
					}
				}
			}
		}
	}

	return types
}

// =============================================================================
// VALIDATION HELPERS
// =============================================================================

// ValidateTOSCAStructure performs basic validation on parsed TOSCA
func ValidateTOSCAStructure(toscaDoc map[string]interface{}) error {
	// Check for required fields
	if _, ok := toscaDoc["tosca_definitions_version"]; !ok {
		return fmt.Errorf("missing required field: tosca_definitions_version")
	}

	// Check for topology_template
	if _, ok := toscaDoc["topology_template"]; !ok {
		return fmt.Errorf("missing topology_template section")
	}

	return nil
}

// HasNodeTemplate checks if a specific node template exists
func HasNodeTemplate(toscaDoc map[string]interface{}, nodeName string) bool {
	_, exists := ExtractNodeTemplate(toscaDoc, nodeName)
	return exists
}

// HasNodeType checks if any node has the specified type
func HasNodeType(toscaDoc map[string]interface{}, nodeType string) bool {
	if topologyTemplate, ok := toscaDoc["topology_template"].(map[string]interface{}); ok {
		if nodeTemplates, ok := topologyTemplate["node_templates"].(map[string]interface{}); ok {
			for _, nodeData := range nodeTemplates {
				if node, ok := nodeData.(map[string]interface{}); ok {
					if nt, ok := node["type"].(string); ok {
						if strings.Contains(nt, nodeType) {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

// =============================================================================
// REQUIREMENT/CAPABILITY HELPERS
// =============================================================================

// GetNodeRequirements extracts requirements for a specific node
func GetNodeRequirements(toscaDoc map[string]interface{}, nodeName string) ([]map[string]interface{}, bool) {
	if nodeTemplate, exists := ExtractNodeTemplate(toscaDoc, nodeName); exists {
		if requirements, ok := nodeTemplate["requirements"].([]interface{}); ok {
			result := make([]map[string]interface{}, 0, len(requirements))
			for _, req := range requirements {
				if reqMap, ok := req.(map[string]interface{}); ok {
					result = append(result, reqMap)
				}
			}
			return result, true
		}
	}
	return nil, false
}

// GetNodeCapabilities extracts capabilities for a specific node
func GetNodeCapabilities(toscaDoc map[string]interface{}, nodeName string) (map[string]interface{}, bool) {
	if nodeTemplate, exists := ExtractNodeTemplate(toscaDoc, nodeName); exists {
		if capabilities, ok := nodeTemplate["capabilities"].(map[string]interface{}); ok {
			return capabilities, true
		}
	}
	return nil, false
}

// HasRequirement checks if a node has a specific requirement type
func HasRequirement(toscaDoc map[string]interface{}, nodeName, requirementType string) bool {
	if requirements, exists := GetNodeRequirements(toscaDoc, nodeName); exists {
		for _, req := range requirements {
			for key := range req {
				if strings.Contains(strings.ToLower(key), strings.ToLower(requirementType)) {
					return true
				}
			}
		}
	}
	return false
}

// =============================================================================
// POLICY AND GROUP HELPERS
// =============================================================================

// GetPolicies extracts all policies from TOSCA template
func GetPolicies(toscaDoc map[string]interface{}) ([]map[string]interface{}, bool) {
	if topologyTemplate, ok := toscaDoc["topology_template"].(map[string]interface{}); ok {
		if policies, ok := topologyTemplate["policies"].([]interface{}); ok {
			result := make([]map[string]interface{}, 0, len(policies))
			for _, policy := range policies {
				if policyMap, ok := policy.(map[string]interface{}); ok {
					result = append(result, policyMap)
				}
			}
			return result, true
		}
	}
	return nil, false
}

// GetGroups extracts all groups from TOSCA template
func GetGroups(toscaDoc map[string]interface{}) (map[string]interface{}, bool) {
	if topologyTemplate, ok := toscaDoc["topology_template"].(map[string]interface{}); ok {
		if groups, ok := topologyTemplate["groups"].(map[string]interface{}); ok {
			return groups, true
		}
	}
	return nil, false
}

// =============================================================================
// ARRAY SEARCH HELPERS
// =============================================================================

// FindPortMapping searches for a specific port in any node's properties
func FindPortMapping(toscaDoc map[string]interface{}, port string) []string {
	nodesWithPort := []string{}

	if topologyTemplate, ok := toscaDoc["topology_template"].(map[string]interface{}); ok {
		if nodeTemplates, ok := topologyTemplate["node_templates"].(map[string]interface{}); ok {
			for nodeName, nodeData := range nodeTemplates {
				if node, ok := nodeData.(map[string]interface{}); ok {
					if properties, ok := node["properties"].(map[string]interface{}); ok {
						// Check ports array
						if ports, ok := properties["ports"].([]interface{}); ok {
							for _, p := range ports {
								if portStr, ok := p.(string); ok {
									if strings.Contains(portStr, port) {
										nodesWithPort = append(nodesWithPort, nodeName)
										break
									}
								}
							}
						}
						// Check single port property
						if portInt, ok := properties["port"].(int); ok {
							if fmt.Sprintf("%d", portInt) == port {
								nodesWithPort = append(nodesWithPort, nodeName)
							}
						}
					}
				}
			}
		}
	}

	return nodesWithPort
}

// FindEnvironmentVariable searches for nodes with a specific environment variable
func FindEnvironmentVariable(toscaDoc map[string]interface{}, varName string) []string {
	nodesWithVar := []string{}

	if topologyTemplate, ok := toscaDoc["topology_template"].(map[string]interface{}); ok {
		if nodeTemplates, ok := topologyTemplate["node_templates"].(map[string]interface{}); ok {
			for nodeName, nodeData := range nodeTemplates {
				if node, ok := nodeData.(map[string]interface{}); ok {
					if properties, ok := node["properties"].(map[string]interface{}); ok {
						if environment, ok := properties["environment"].(map[string]interface{}); ok {
							if _, exists := environment[varName]; exists {
								nodesWithVar = append(nodesWithVar, nodeName)
							}
						}
					}
				}
			}
		}
	}

	return nodesWithVar
}

// =============================================================================
// STATISTICS HELPERS
// =============================================================================

// GetTOSCAStatistics returns comprehensive statistics about the TOSCA template
func GetTOSCAStatistics(toscaDoc map[string]interface{}) map[string]interface{} {
	stats := make(map[string]interface{})

	stats["node_count"] = CountNodeTemplatesFromJSON(toscaDoc)
	stats["node_types"] = GetAllNodeTypes(toscaDoc)
	stats["node_type_count"] = len(GetAllNodeTypes(toscaDoc))

	if policies, exists := GetPolicies(toscaDoc); exists {
		stats["policy_count"] = len(policies)
	} else {
		stats["policy_count"] = 0
	}

	if groups, exists := GetGroups(toscaDoc); exists {
		stats["group_count"] = len(groups)
	} else {
		stats["group_count"] = 0
	}

	stats["tosca_version"] = ExtractMetadataField(toscaDoc, "tosca_definitions_version")
	if stats["tosca_version"] == "" {
		if version, ok := toscaDoc["tosca_definitions_version"].(string); ok {
			stats["tosca_version"] = version
		}
	}

	return stats
}
