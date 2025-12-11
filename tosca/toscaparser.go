package tosca

import (
	"crypto/sha256"
	"fmt"
	"gopkg.in/yaml.v3"
)

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

// ComputeTemplateID generates a unique ID for a TOSCA template
func ComputeTemplateID(yamlContent []byte) string {
	hash := sha256.Sum256(yamlContent)
	return fmt.Sprintf("%x", hash[:8]) // Use first 8 bytes for shorter ID
}

// =============================================================================
// NEW FUNCTIONS for Full Structure Support
// =============================================================================

// ParseTOSCAToFullJSON parses TOSCA YAML to complete queryable JSON structure
// This preserves the entire TOSCA structure as a map for queryability
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
// OPTIONAL: Helper Functions for TOSCA JSON Manipulation
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

// ValidateTOSCAStructure performs basic validation on parsed TOSCA
func ValidateTOSCAStructure(toscaDoc map[string]interface{}) error {
	// Check for required fields
	if _, ok := toscaDoc["tosca_definitions_version"]; !ok {
		return fmt.Errorf("missing required field: tosca_definitions_version")
	}

	return nil
}
