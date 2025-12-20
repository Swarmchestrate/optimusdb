#!/usr/bin/env python3
"""
OptimusDB TOSCA Upload Script (Python)
======================================
Uploads TOSCA YAML files to OptimusDB with base64 encoding
and persists template IDs to a JSON file.

Project: OptimusDB - EU Horizon Europe Grant 101135012

Usage:
    python upload_tosca_files_complete.py [base_url] [files_directory]

Example:
    python upload_tosca_files_complete.py http://localhost:18001 ./tosca_samples
"""

import sys
import os
import json
import base64
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

# Color codes for terminal output
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    CYAN = '\033[0;36m'
    GRAY = '\033[0;90m'
    NC = '\033[0m'  # No Color
    BOLD = '\033[1m'

@dataclass
class UploadResult:
    """Result of a TOSCA file upload"""
    filename: str
    description: str
    template_id: str
    queryable: bool
    storage_location: str
    uploaded_at: str

@dataclass
class UploadSession:
    """Summary of upload session"""
    timestamp: str
    base_url: str
    total_files: int
    uploaded: int
    failed: int

class ToscaUploader:
    """Main class for uploading TOSCA files to OptimusDB"""

    # TOSCA files configuration
    TOSCA_FILES = {
        "webapp_adt.yaml": "WebApp Microservices Application",
        "capacity_profile.yaml": "Edge Cluster Capacity Profile",
        "opentofu_hybrid.yaml": "Hybrid Infrastructure with OpenTofu",
        "deployment_plan.yaml": "Deployment Plan with Workflows",
        "app_requirements.yaml": "ML Training Application Requirements"
    }

    def __init__(self, base_url: str = "http://localhost:18001",
                 files_dir: str = "."):
        self.base_url = base_url
        self.files_dir = Path(files_dir)
        self.output_file = "uploaded_tosca_templates.json"
        self.log_file = f"upload_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        self.uploaded_count = 0
        self.failed_count = 0
        self.upload_results: List[UploadResult] = []

    def log(self, message: str) -> None:
        """Write message to log file"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] {message}\n")

    def print_header(self, text: str) -> None:
        """Print section header"""
        print()
        print(f"{Colors.CYAN}{'═' * 63}{Colors.NC}")
        print(f"{Colors.CYAN}{text}{Colors.NC}")
        print(f"{Colors.CYAN}{'═' * 63}{Colors.NC}")
        print()

    def print_success(self, message: str) -> None:
        """Print success message"""
        print(f"{Colors.GREEN}✅ {message}{Colors.NC}")
        self.log(f"SUCCESS: {message}")

    def print_error(self, message: str) -> None:
        """Print error message"""
        print(f"{Colors.RED}❌ {message}{Colors.NC}")
        self.log(f"ERROR: {message}")

    def print_warning(self, message: str) -> None:
        """Print warning message"""
        print(f"{Colors.YELLOW}⚠️  {message}{Colors.NC}")
        self.log(f"WARNING: {message}")

    def print_info(self, message: str) -> None:
        """Print info message"""
        print(f"{Colors.CYAN}ℹ️  {message}{Colors.NC}")

    def print_detail(self, message: str) -> None:
        """Print detail message"""
        print(f"{Colors.GRAY}   {message}{Colors.NC}")

    def check_dependencies(self) -> bool:
        """Check if required dependencies are installed"""
        self.print_info("Checking dependencies...")

        try:
            import requests
            self.print_success("All dependencies available")
            return True
        except ImportError:
            self.print_error("Missing required dependency: requests")
            print("\nPlease install requests:")
            print("  pip install requests")
            return False

    def test_connectivity(self) -> bool:
        """Test connection to OptimusDB API"""
        self.print_info(f"Testing connection to {self.base_url}...")

        try:
            response = requests.get(
                f"{self.base_url}/health",
                timeout=5
            )
            self.print_success("API is reachable")
            return True
        except requests.exceptions.RequestException:
            self.print_warning("Health endpoint not responding (this may be normal)")
            self.print_info("Attempting to continue anyway...")
            return True

    def convert_to_base64(self, filepath: Path) -> Optional[str]:
        """Convert file to base64 encoding"""
        try:
            with open(filepath, 'rb') as f:
                content = f.read()
                return base64.b64encode(content).decode('utf-8')
        except Exception as e:
            self.print_error(f"Failed to convert file to base64: {e}")
            return None

    def upload_tosca_file(self, filename: str, description: str) -> Optional[UploadResult]:
        """Upload a single TOSCA file"""
        print()
        self.print_info(f"Processing: {description}")
        self.print_detail(f"File: {filename}")

        filepath = self.files_dir / filename

        # Check file exists
        if not filepath.exists():
            self.print_error(f"File not found: {filepath}")
            return None

        # Get file size
        size_kb = filepath.stat().st_size / 1024
        self.print_detail(f"Size: {size_kb:.2f} KB")

        # Convert to base64
        self.print_detail("Converting to base64...")
        base64_content = self.convert_to_base64(filepath)

        if not base64_content:
            return None

        # Prepare request payload
        payload = {
            "file": base64_content,
            "filename": filename,
            "store_full_structure": True
        }

        # Upload to OptimusDB
        self.print_detail(f"Uploading to {self.base_url}/swarmkb/upload...")

        try:
            response = requests.post(
                f"{self.base_url}/swarmkb/upload",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=60
            )

            # Check HTTP status
            if response.status_code != 200:
                self.print_error(f"Upload failed with HTTP {response.status_code}")
                self.print_detail(f"Response: {response.text}")
                return None

            # Parse response
            data = response.json()

            if data.get('status') != 200:
                error_msg = data.get('message', 'Unknown error')
                self.print_error(f"Upload failed: {error_msg}")
                return None

            # Extract response data
            response_data = data.get('data', {})
            template_id = response_data.get('template_id')
            queryable = response_data.get('queryable', False)
            storage_location = response_data.get('storage_location', '')

            if not template_id:
                self.print_error("No template ID returned in response")
                self.print_detail(f"Response: {data}")
                return None

            # Success!
            self.print_success("Upload successful")
            self.print_detail(f"Template ID: {template_id}")
            self.print_detail(f"Queryable: {queryable}")
            self.print_detail(f"Storage: {storage_location}")

            # Create result object
            return UploadResult(
                filename=filename,
                description=description,
                template_id=template_id,
                queryable=queryable,
                storage_location=storage_location,
                uploaded_at=datetime.now(timezone.utc).isoformat()
            )

        except requests.exceptions.RequestException as e:
            self.print_error(f"Upload failed: {e}")
            return None
        except json.JSONDecodeError as e:
            self.print_error(f"Failed to parse response JSON: {e}")
            return None

    def save_results(self) -> None:
        """Save upload results to JSON file"""
        output_data = {
            "upload_session": asdict(UploadSession(
                timestamp=datetime.now(timezone.utc).isoformat(),
                base_url=self.base_url,
                total_files=len(self.TOSCA_FILES),
                uploaded=self.uploaded_count,
                failed=self.failed_count
            )),
            "templates": [asdict(result) for result in self.upload_results]
        }

        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        self.print_success(f"Results saved to: {self.output_file}")

    def verify_uploads(self) -> bool:
        """Verify uploads by querying the database"""
        self.print_info("Verifying uploads...")

        query_payload = {
            "method": {"cmd": "crudget", "argcnt": 1},
            "dstype": "dsswres",
            "criteria": []
        }

        try:
            response = requests.post(
                f"{self.base_url}/swarmkb/command",
                json=query_payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )

            data = response.json()
            total_count = len(data.get('data', []))

            if total_count > 0:
                self.print_success(f"Verified: {total_count} total templates in database")

                # Count TOSCA templates
                tosca_count = sum(
                    1 for item in data.get('data', [])
                    if 'tosca_definitions_version' in item
                )
                self.print_detail(f"TOSCA templates: {tosca_count}")

                return True
            else:
                self.print_warning("Could not verify uploads (query returned no results)")
                return False

        except Exception as e:
            self.print_warning(f"Could not verify uploads: {e}")
            return False

    def run(self) -> int:
        """Main execution method"""
        self.print_header("OptimusDB TOSCA Upload Script")

        print("Configuration:")
        print(f"  Base URL: {self.base_url}")
        print(f"  Files Directory: {self.files_dir}")
        print(f"  Output File: {self.output_file}")
        print(f"  Log File: {self.log_file}")
        print()

        # Check dependencies
        if not self.check_dependencies():
            return 1

        # Test connectivity
        self.test_connectivity()

        # Process each file
        self.print_header("Uploading TOSCA Files")

        for filename, description in self.TOSCA_FILES.items():
            result = self.upload_tosca_file(filename, description)

            if result:
                self.upload_results.append(result)
                self.uploaded_count += 1
            else:
                self.failed_count += 1

            # Brief pause between uploads
            import time
            time.sleep(1)

        # Summary
        print()
        self.print_header("Upload Summary")

        total_files = len(self.TOSCA_FILES)
        print(f"Total Files:     {total_files}")
        print(f"{Colors.GREEN}Uploaded:        {self.uploaded_count}{Colors.NC}")
        print(f"{Colors.RED}Failed:          {self.failed_count}{Colors.NC}")
        print()

        # Save results if any succeeded
        if self.uploaded_count > 0:
            self.save_results()

            print()
            self.print_info(f"Template IDs saved to {self.output_file}")
            print()

            # Show uploaded templates
            print("Uploaded Templates:")
            for result in self.upload_results:
                print(f"{Colors.GREEN}  ✓ {result.description}{Colors.NC}")
                print(f"{Colors.GRAY}    ID: {result.template_id}{Colors.NC}")

            # Verify uploads
            print()
            self.verify_uploads()

        # Final status
        print()
        if self.failed_count == 0:
            self.print_header("✅ All Uploads Successful!")
            return 0
        elif self.uploaded_count > 0:
            self.print_header("⚠️  Partial Success - Some Uploads Failed")
            return 1
        else:
            self.print_header("❌ All Uploads Failed")
            return 1

def main():
    """Main entry point"""
    # Parse command line arguments
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:18001"
    files_dir = sys.argv[2] if len(sys.argv) > 2 else "."

    # Create uploader and run
    uploader = ToscaUploader(base_url, files_dir)
    exit_code = uploader.run()

    sys.exit(exit_code)

if __name__ == "__main__":
    main()