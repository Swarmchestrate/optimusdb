#!/usr/bin/env python3
# tosca_client.py

import sys
import json
import yaml
import requests
import re
from datetime import datetime
from typing import Dict, List, Any, Optional
from collections import defaultdict

def sanitize_id(s: str) -> str:
    """Convert string to valid ID format"""
    return re.sub(r'[^a-zA-Z0-9]+', '_', s.lower())

def flatten_dict(data: Dict, prefix: str = '', result: Dict = None, max_depth: int = 3, current_depth: int = 0) -> Dict:
    """Recursively flatten nested dictionaries"""
    if result is None:
        result = {}

    if current_depth >= max_depth:
        # Store deeply nested content as-is
        if prefix:
            result[prefix] = data
        return result

    for key, value in data.items():
        new_key = f"{prefix}.{key}" if prefix else key

        if isinstance(value, dict):
            flatten_dict(value, new_key, result, max_depth, current_depth + 1)
        elif isinstance(value, list):
            # Store arrays as-is
            result[new_key] = value
            # Create flattened version for simple arrays
            if is_simple_array(value):
                result[f"{new_key}_list"] = value
        else:
            result[new_key] = value

    return result

def is_simple_array(arr: List) -> bool:
    """Check if array contains only simple types"""
    return all(isinstance(item, (str, int, float, bool, type(None))) for item in arr)

def extract_top_level_fields(data: Dict) -> Dict:
    """Extract searchable top-level fields"""
    extracted = {}

    # Extract metadata
    if 'metadata' in data and isinstance(data['metadata'], dict):
        for k, v in data['metadata'].items():
            extracted[f'metadata_{k}'] = v

    # Extract TOSCA version
    if 'tosca_definitions_version' in data:
        extracted['tosca_version'] = data['tosca_definitions_version']

    # Extract description
    if 'description' in data:
        extracted['description'] = str(data['description']).strip()

    # Extract imports
    if 'imports' in data and isinstance(data['imports'], list):
        imports_list = []
        for imp in data['imports']:
            if isinstance(imp, dict):
                imports_list.extend(str(v) for v in imp.values())
            elif isinstance(imp, str):
                imports_list.append(imp)
        if imports_list:
            extracted['imports'] = imports_list

    return extracted

def extract_node_types(topology: Dict) -> List[str]:
    """Extract unique node types from topology_template"""
    node_types = set()

    if 'node_templates' in topology and isinstance(topology['node_templates'], dict):
        for node_data in topology['node_templates'].values():
            if isinstance(node_data, dict) and 'type' in node_data:
                node_types.add(node_data['type'])

    return list(node_types)

def extract_policy_types(topology: Dict) -> List[str]:
    """Extract unique policy types from topology_template"""
    policy_types = set()

    if 'policies' in topology and isinstance(topology['policies'], list):
        for policy in topology['policies']:
            if isinstance(policy, dict):
                for policy_data in policy.values():
                    if isinstance(policy_data, dict) and 'type' in policy_data:
                        policy_types.add(policy_data['type'])

    return list(policy_types)

def extract_groups(topology: Dict) -> List[str]:
    """Extract group names from topology_template"""
    if 'groups' in topology and isinstance(topology['groups'], dict):
        return list(topology['groups'].keys())
    return []

def extract_workflows(topology: Dict) -> List[str]:
    """Extract workflow names from topology_template"""
    if 'workflows' in topology and isinstance(topology['workflows'], dict):
        return list(topology['workflows'].keys())
    return []

def infer_document_type(data: Dict) -> str:
    """Infer document type from metadata and content"""
    # Check metadata.kb_datastore
    if 'metadata' in data and isinstance(data['metadata'], dict):
        datastore = data['metadata'].get('kb_datastore', '')

        type_mapping = {
            'ADT': 'application_description',
            'Capacity_Descriptions': 'capacity_description',
            'OpenTofu_TOSCA_Templates': 'opentofu_tosca_template',
            'Deployment_Release_Plans': 'deployment_release_plan'
        }

        if datastore in type_mapping:
            return type_mapping[datastore]

    # Check for specific sections
    if 'topology_template' in data:
        topology = data['topology_template']

        # Has workflows? Likely deployment plan
        if 'workflows' in topology:
            return 'deployment_release_plan'

        # Has OpenTofu config? OpenTofu template
        if 'opentofu_config' in data:
            return 'opentofu_tosca_template'

        # Check node types
        if 'node_templates' in topology:
            for node_data in topology['node_templates'].values():
                if isinstance(node_data, dict) and 'type' in node_data:
                    node_type = node_data['type']

                    if 'Compute.Physical' in node_type:
                        return 'capacity_description'
                    if 'Requirements' in node_type:
                        return 'application_requirements'

    return 'tosca_template'

def extract_creation_timestamp(data: Dict) -> str:
    """Extract creation timestamp from metadata"""
    if 'metadata' in data and isinstance(data['metadata'], dict):
        metadata = data['metadata']
        for key in ['creation_timestamp', 'submission_timestamp', 'last_updated']:
            if key in metadata:
                return str(metadata[key])

    return datetime.utcnow().isoformat() + 'Z'

def convert_tosca_to_optimusdb(tosca_file: str) -> Dict:
    """Convert TOSCA YAML to OptimusDB format (generic)"""

    # Read and parse TOSCA YAML
    with open(tosca_file, 'r') as f:
        tosca_data = yaml.safe_load(f)
        yaml_content = f.read()

    # Create document
    document = {}

    # Generate document ID
    template_name = 'unknown'
    template_version = '1.0.0'

    if 'metadata' in tosca_data and isinstance(tosca_data['metadata'], dict):
        template_name = tosca_data['metadata'].get('template_name', 'unknown')
        template_version = tosca_data['metadata'].get('template_version', '1.0.0')

    doc_id = f"tosca_{sanitize_id(template_name)}_v{sanitize_id(template_version)}"
    document['_id'] = doc_id

    # Infer document type
    doc_type = infer_document_type(tosca_data)
    document['document_type'] = doc_type

    # Extract top-level searchable fields
    top_level = extract_top_level_fields(tosca_data)
    document.update(top_level)

    # Flatten topology_template
    if 'topology_template' in tosca_data:
        topology = tosca_data['topology_template']
        flattened = flatten_dict(topology, prefix='topology', max_depth=3)
        document.update(flattened)

        # Extract searchable lists
        document['node_types'] = extract_node_types(topology)
        document['policy_types'] = extract_policy_types(topology)
        document['groups'] = extract_groups(topology)
        document['workflows'] = extract_workflows(topology)

        # Store full topology for complete access
        document['topology_template_full'] = topology

    # Handle special sections based on document type
    special_sections = [
        'swarm_status',
        'opentofu_config',
        'opentofu_variables',
        'opentofu_outputs',
        'capacity_matching',
        'resource_allocation'
    ]

    for section in special_sections:
        if section in tosca_data:
            document[section] = tosca_data[section]

    # Store original YAML for complete reference
    with open(tosca_file, 'r') as f:
        document['original_yaml'] = f.read()

    # Add timestamp fields
    document['created_at'] = extract_creation_timestamp(tosca_data)
    document['updated_at'] = 'auto-calculate'
    document['indexed_at'] = datetime.utcnow().isoformat() + 'Z'

    # Add agent info if available
    if 'metadata' in tosca_data and isinstance(tosca_data['metadata'], dict):
        metadata = tosca_data['metadata']
        if 'template_author' in metadata:
            document['kbagent'] = sanitize_id(metadata['template_author'])
        elif 'orchestrator' in metadata:
            document['kbagent'] = sanitize_id(metadata['orchestrator'])

    document['kbagent_type'] = 'TOSCA'

    return document

def send_to_optimusdb(document: Dict, url: str) -> bool:
    """Send document to OptimusDB"""

    request = {
        "method": {
            "argcnt": 10000,
            "cmd": "crudput"
        },
        "args": [document["_id"], document["document_type"]],
        "dstype": "kbdata",
        "sqlselect": "",
        "criteria": [document]
    }

    print("Sending to OptimusDB:")
    print(json.dumps(request, indent=2))

    try:
        response = requests.post(url, json=request, headers={"Content-Type": "application/json"})

        print(f"\nResponse Status: {response.status_code}")
        print(f"Response Body: {response.text}")

        if response.status_code in [200, 201]:
            print("\n✓ Successfully sent TOSCA template to OptimusDB!")
            return True
        else:
            print(f"\n✗ OptimusDB returned error status: {response.status_code}")
            return False

    except Exception as e:
        print(f"\n✗ Error sending request: {e}")
        return False

def main():
    if len(sys.argv) < 3:
        print("Usage: python tosca_client.py <tosca_file.yaml> <optimusdb_url>")
        print("Example: python tosca_client.py webapp.yaml http://localhost:3000/api/data")
        sys.exit(1)

    tosca_file = sys.argv[1]
    optimusdb_url = sys.argv[2]

    try:
        document = convert_tosca_to_optimusdb(tosca_file)
        send_to_optimusdb(document, optimusdb_url)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()