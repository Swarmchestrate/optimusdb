package FIles

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Generic structures
type OptimusDBRequest struct {
	Method    Method                   `json:"method"`
	Args      []string                 `json:"args"`
	DSType    string                   `json:"dstype"`
	SQLSelect string                   `json:"sqlselect"`
	Criteria  []map[string]interface{} `json:"criteria"`
}

type Method struct {
	ArgCnt int    `json:"argcnt"`
	Cmd    string `json:"cmd"`
}

// Flattener for nested maps
func flattenMap(prefix string, data map[string]interface{}, result map[string]interface{}, maxDepth int, currentDepth int) {
	if currentDepth >= maxDepth {
		// Store deeply nested content as-is
		if prefix != "" {
			result[prefix] = data
		}
		return
	}

	for key, value := range data {
		newKey := key
		if prefix != "" {
			newKey = prefix + "." + key
		}

		switch v := value.(type) {
		case map[string]interface{}:
			flattenMap(newKey, v, result, maxDepth, currentDepth+1)
		case []interface{}:
			// Store arrays as-is, but try to extract searchable info
			result[newKey] = v
			// Also create a flattened version for simple string arrays
			if isSimpleArray(v) {
				result[newKey+"_list"] = v
			}
		default:
			result[newKey] = v
		}
	}
}

func isSimpleArray(arr []interface{}) bool {
	for _, item := range arr {
		switch item.(type) {
		case string, int, float64, bool:
			continue
		default:
			return false
		}
	}
	return true
}

// Extract searchable top-level fields
func extractTopLevelFields(data map[string]interface{}) map[string]interface{} {
	extracted := make(map[string]interface{})

	// Extract metadata if present
	if metadata, ok := data["metadata"].(map[string]interface{}); ok {
		for k, v := range metadata {
			extracted["metadata_"+k] = v
		}
	}

	// Extract TOSCA version
	if version, ok := data["tosca_definitions_version"].(string); ok {
		extracted["tosca_version"] = version
	}

	// Extract description
	if desc, ok := data["description"].(string); ok {
		extracted["description"] = strings.TrimSpace(desc)
	}

	// Extract imports
	if imports, ok := data["imports"].([]interface{}); ok {
		importsList := []string{}
		for _, imp := range imports {
			if impMap, ok := imp.(map[string]interface{}); ok {
				for _, v := range impMap {
					if str, ok := v.(string); ok {
						importsList = append(importsList, str)
					}
				}
			} else if impStr, ok := imp.(string); ok {
				importsList = append(importsList, impStr)
			}
		}
		if len(importsList) > 0 {
			extracted["imports"] = importsList
		}
	}

	return extracted
}

// Extract node types from topology_template
func extractNodeTypes(topology map[string]interface{}) []string {
	nodeTypes := make(map[string]bool)

	if nodeTemplates, ok := topology["node_templates"].(map[string]interface{}); ok {
		for _, node := range nodeTemplates {
			if nodeMap, ok := node.(map[string]interface{}); ok {
				if nodeType, ok := nodeMap["type"].(string); ok {
					nodeTypes[nodeType] = true
				}
			}
		}
	}

	result := []string{}
	for t := range nodeTypes {
		result = append(result, t)
	}
	return result
}

// Extract policy types
func extractPolicyTypes(topology map[string]interface{}) []string {
	policyTypes := make(map[string]bool)

	if policies, ok := topology["policies"].([]interface{}); ok {
		for _, policy := range policies {
			if policyMap, ok := policy.(map[string]interface{}); ok {
				for _, policyData := range policyMap {
					if policyDataMap, ok := policyData.(map[string]interface{}); ok {
						if policyType, ok := policyDataMap["type"].(string); ok {
							policyTypes[policyType] = true
						}
					}
				}
			}
		}
	}

	result := []string{}
	for t := range policyTypes {
		result = append(result, t)
	}
	return result
}

// Extract groups
func extractGroups(topology map[string]interface{}) []string {
	groups := []string{}

	if groupsMap, ok := topology["groups"].(map[string]interface{}); ok {
		for groupName := range groupsMap {
			groups = append(groups, groupName)
		}
	}

	return groups
}

// Extract workflows
func extractWorkflows(topology map[string]interface{}) []string {
	workflows := []string{}

	if workflowsMap, ok := topology["workflows"].(map[string]interface{}); ok {
		for workflowName := range workflowsMap {
			workflows = append(workflows, workflowName)
		}
	}

	return workflows
}

// Infer document type from metadata and content
func inferDocumentType(data map[string]interface{}) string {
	// Check metadata.kb_datastore
	if metadata, ok := data["metadata"].(map[string]interface{}); ok {
		if datastore, ok := metadata["kb_datastore"].(string); ok {
			switch datastore {
			case "ADT":
				return "application_description"
			case "Capacity_Descriptions":
				return "capacity_description"
			case "OpenTofu_TOSCA_Templates":
				return "opentofu_tosca_template"
			case "Deployment_Release_Plans":
				return "deployment_release_plan"
			default:
				return "tosca_template"
			}
		}
	}

	// Check for specific sections
	if topology, ok := data["topology_template"].(map[string]interface{}); ok {
		if _, hasWorkflows := topology["workflows"]; hasWorkflows {
			return "deployment_release_plan"
		}
		if _, hasOpenTofu := data["opentofu_config"]; hasOpenTofu {
			return "opentofu_tosca_template"
		}
		if nodeTemplates, ok := topology["node_templates"].(map[string]interface{}); ok {
			// Check if it's capacity description (has physical compute nodes)
			for _, node := range nodeTemplates {
				if nodeMap, ok := node.(map[string]interface{}); ok {
					if nodeType, ok := nodeMap["type"].(string); ok {
						if strings.Contains(nodeType, "Compute.Physical") {
							return "capacity_description"
						}
						if strings.Contains(nodeType, "Requirements") {
							return "application_requirements"
						}
					}
				}
			}
		}
	}

	return "tosca_template"
}

// Convert TOSCA to OptimusDB format
func convertToscaToOptimusDB(toscaFile string, optimusdbURL string) error {
	// Read TOSCA YAML file
	yamlData, err := ioutil.ReadFile(toscaFile)
	if err != nil {
		return fmt.Errorf("failed to read TOSCA file: %v", err)
	}

	// Parse YAML into generic map
	var toscaData map[string]interface{}
	err = yaml.Unmarshal(yamlData, &toscaData)
	if err != nil {
		return fmt.Errorf("failed to parse TOSCA YAML: %v", err)
	}

	// Create document
	document := make(map[string]interface{})

	// Generate document ID
	templateName := "unknown"
	templateVersion := "1.0.0"
	if metadata, ok := toscaData["metadata"].(map[string]interface{}); ok {
		if name, ok := metadata["template_name"].(string); ok {
			templateName = name
		}
		if version, ok := metadata["template_version"].(string); ok {
			templateVersion = version
		}
	}
	docID := fmt.Sprintf("tosca_%s_v%s", sanitizeID(templateName), sanitizeID(templateVersion))
	document["_id"] = docID

	// Infer document type
	docType := inferDocumentType(toscaData)
	document["document_type"] = docType

	// Extract top-level searchable fields
	topLevel := extractTopLevelFields(toscaData)
	for k, v := range topLevel {
		document[k] = v
	}

	// Flatten topology_template
	if topology, ok := toscaData["topology_template"].(map[string]interface{}); ok {
		flattened := make(map[string]interface{})
		flattenMap("topology", topology, flattened, 3, 0)

		// Store flattened topology
		for k, v := range flattened {
			document[k] = v
		}

		// Extract searchable lists
		document["node_types"] = extractNodeTypes(topology)
		document["policy_types"] = extractPolicyTypes(topology)
		document["groups"] = extractGroups(topology)
		document["workflows"] = extractWorkflows(topology)

		// Store full topology as-is for complete access
		document["topology_template_full"] = topology
	}

	// Handle special sections based on document type
	if swarmStatus, ok := toscaData["swarm_status"].(map[string]interface{}); ok {
		document["swarm_status"] = swarmStatus
	}

	if openTofuConfig, ok := toscaData["opentofu_config"].(map[string]interface{}); ok {
		document["opentofu_config"] = openTofuConfig
	}

	if openTofuVars, ok := toscaData["opentofu_variables"].(map[string]interface{}); ok {
		document["opentofu_variables"] = openTofuVars
	}

	if openTofuOutputs, ok := toscaData["opentofu_outputs"].(map[string]interface{}); ok {
		document["opentofu_outputs"] = openTofuOutputs
	}

	if capacityMatching, ok := toscaData["capacity_matching"].(map[string]interface{}); ok {
		document["capacity_matching"] = capacityMatching
	}

	if resourceAllocation, ok := toscaData["resource_allocation"].(map[string]interface{}); ok {
		document["resource_allocation"] = resourceAllocation
	}

	// Store original YAML for complete reference
	document["original_yaml"] = string(yamlData)

	// Add timestamp fields
	document["created_at"] = extractCreationTimestamp(toscaData)
	document["updated_at"] = "auto-calculate"
	document["indexed_at"] = getCurrentTimestamp()

	// Add agent info if available
	if metadata, ok := toscaData["metadata"].(map[string]interface{}); ok {
		if author, ok := metadata["template_author"].(string); ok {
			document["kbagent"] = sanitizeID(author)
		}
		if orchestrator, ok := metadata["orchestrator"].(string); ok {
			document["kbagent"] = sanitizeID(orchestrator)
		}
	}
	document["kbagent_type"] = "TOSCA"

	// Build OptimusDB request
	request := OptimusDBRequest{
		Method: Method{
			ArgCnt: 10000,
			Cmd:    "crudput",
		},
		Args:      []string{docID, docType},
		DSType:    "kbdata",
		SQLSelect: "",
		Criteria:  []map[string]interface{}{document},
	}

	// Send to OptimusDB
	return sendToOptimusDB(request, optimusdbURL)
}

func extractCreationTimestamp(data map[string]interface{}) string {
	if metadata, ok := data["metadata"].(map[string]interface{}); ok {
		if ts, ok := metadata["creation_timestamp"].(string); ok {
			return ts
		}
		if ts, ok := metadata["submission_timestamp"].(string); ok {
			return ts
		}
		if ts, ok := metadata["last_updated"].(string); ok {
			return ts
		}
	}
	return getCurrentTimestamp()
}

func getCurrentTimestamp() string {
	return fmt.Sprintf("%s", "2025-11-05T10:30:00Z") // Use actual time in production
}

func sanitizeID(s string) string {
	// Replace non-alphanumeric with underscores and convert to lowercase
	reg := regexp.MustCompile("[^a-zA-Z0-9]+")
	return strings.ToLower(reg.ReplaceAllString(s, "_"))
}

func sendToOptimusDB(request OptimusDBRequest, url string) error {
	jsonData, err := json.MarshalIndent(request, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	fmt.Println("Sending to OptimusDB:")
	fmt.Println(string(jsonData))

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	fmt.Printf("\nResponse Status: %s\n", resp.Status)
	fmt.Printf("Response Body: %s\n", string(body))

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return fmt.Errorf("OptimusDB returned error status: %d", resp.StatusCode)
	}

	fmt.Println("\n✓ Successfully sent TOSCA template to OptimusDB!")
	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: tosca_client <tosca_file.yaml> <optimusdb_url>")
		fmt.Println("Example: tosca_client webapp.yaml http://localhost:3000/api/data")
		os.Exit(1)
	}

	toscaFile := os.Args[1]
	optimusdbURL := os.Args[2]

	err := convertToscaToOptimusDB(toscaFile, optimusdbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
