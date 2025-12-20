#!/usr/bin/env python3
"""
OptimusDB End-to-End Testing Script (Python)
=============================================
Uploads TOSCA files and executes comprehensive test scenarios
with expected vs actual result comparison.

Project: OptimusDB - EU Horizon Europe Grant 101135012

Usage:
    python e2e_test_complete.py [base_url] [files_directory]

Example:
    python e2e_test_complete.py http://localhost:18001 ./tosca_samples
"""

import sys
import os
import json
import base64
import requests
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict

# Color codes for terminal output
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    CYAN = '\033[0;36m'
    BLUE = '\033[0;34m'
    MAGENTA = '\033[0;35m'
    GRAY = '\033[0;90m'
    BOLD = '\033[1m'
    NC = '\033[0m'

@dataclass
class TestResult:
    """Result of a single test"""
    scenario: str
    description: str
    expected: str
    command: str
    passed: bool
    actual_result: str
    execution_time: float
    timestamp: str

@dataclass
class TestSession:
    """Summary of test session"""
    timestamp: str
    base_url: str
    total_tests: int
    passed: int
    failed: int
    duration: float

class E2ETestRunner:
    """End-to-end test runner for OptimusDB"""

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
        self.test_results: List[TestResult] = []
        self.template_ids: List[str] = []
        self.test_start_time = time.time()

        # Report files
        self.report_file = f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self.json_report = f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    def print_banner(self, text: str, char: str = "═") -> None:
        """Print a banner"""
        width = 80
        print()
        print(f"{Colors.CYAN}{char * width}{Colors.NC}")
        print(f"{Colors.CYAN}{Colors.BOLD}{text.center(width)}{Colors.NC}")
        print(f"{Colors.CYAN}{char * width}{Colors.NC}")
        print()

    def print_section(self, text: str) -> None:
        """Print section header"""
        print()
        print(f"{Colors.BLUE}{Colors.BOLD}{'─' * 80}{Colors.NC}")
        print(f"{Colors.BLUE}{Colors.BOLD}{text}{Colors.NC}")
        print(f"{Colors.BLUE}{Colors.BOLD}{'─' * 80}{Colors.NC}")
        print()

    def print_test_header(self, scenario: str, description: str) -> None:
        """Print test header"""
        print()
        print(f"{Colors.MAGENTA}{Colors.BOLD}TEST SCENARIO: {scenario}{Colors.NC}")
        print(f"{Colors.CYAN}Description: {description}{Colors.NC}")

    def print_expected(self, expected: str) -> None:
        """Print expected outcome"""
        print(f"{Colors.YELLOW}Expected: {expected}{Colors.NC}")

    def print_command(self, command: str) -> None:
        """Print command being executed"""
        print(f"{Colors.GRAY}Command: {command}{Colors.NC}")

    def print_result(self, passed: bool, message: str) -> None:
        """Print test result"""
        if passed:
            print(f"{Colors.GREEN}✅ PASS: {message}{Colors.NC}")
        else:
            print(f"{Colors.RED}❌ FAIL: {message}{Colors.NC}")

    def record_test(self, scenario: str, description: str, expected: str,
                   command: str, passed: bool, actual: str, exec_time: float) -> None:
        """Record test result"""
        result = TestResult(
            scenario=scenario,
            description=description,
            expected=expected,
            command=command,
            passed=passed,
            actual_result=actual,
            execution_time=exec_time,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
        self.test_results.append(result)

    def upload_tosca_file(self, filename: str, description: str) -> Optional[str]:
        """Upload a single TOSCA file and return template ID"""
        filepath = self.files_dir / filename

        if not filepath.exists():
            return None

        # Convert to base64
        with open(filepath, 'rb') as f:
            content = f.read()
            base64_content = base64.b64encode(content).decode('utf-8')

        # Prepare request
        payload = {
            "file": base64_content,
            "filename": filename,
            "store_full_structure": True
        }

        try:
            response = requests.post(
                f"{self.base_url}/swarmkb/upload",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=60
            )

            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 200:
                    return data.get('data', {}).get('template_id')
        except Exception:
            pass

        return None

    def upload_all_tosca_files(self) -> bool:
        """Upload all TOSCA files"""
        self.print_section("PHASE 1: Upload TOSCA Files")

        success_count = 0

        for filename, description in self.TOSCA_FILES.items():
            print(f"Uploading: {description}...", end=" ")

            template_id = self.upload_tosca_file(filename, description)

            if template_id:
                self.template_ids.append(template_id)
                id_preview = template_id[:20] + "..." if len(template_id) > 20 else template_id
                print(f"{Colors.GREEN}✅ Success{Colors.NC} (ID: {id_preview})")
                success_count += 1
            else:
                print(f"{Colors.RED}❌ Failed{Colors.NC}")

        print()
        print(f"Upload Summary: {success_count}/{len(self.TOSCA_FILES)} successful")

        return success_count == len(self.TOSCA_FILES)

    def test_get_all_templates(self) -> None:
        """Test: Get all TOSCA templates"""
        scenario = "Get All TOSCA Templates"
        description = "Retrieve all templates from dsswres"
        expected = f"Returns array with {len(self.TOSCA_FILES)}+ templates"

        payload = {
            "method": {"cmd": "crudget", "argcnt": 1},
            "dstype": "dsswres",
            "criteria": []
        }
        command = f"POST /swarmkb/command with criteria: []"

        self.print_test_header(scenario, description)
        self.print_expected(expected)
        self.print_command(command)

        start_time = time.time()

        try:
            response = requests.post(
                f"{self.base_url}/swarmkb/command",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )

            exec_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                count = len(data.get('data', []))

                passed = count >= len(self.TOSCA_FILES)
                actual = f"Returned {count} templates"

                self.print_result(passed, actual)
                self.record_test(scenario, description, expected, command, passed, actual, exec_time)
            else:
                actual = f"HTTP {response.status_code}: {response.text}"
                self.print_result(False, actual)
                self.record_test(scenario, description, expected, command, False, actual, exec_time)

        except Exception as e:
            exec_time = time.time() - start_time
            actual = f"Exception: {str(e)}"
            self.print_result(False, actual)
            self.record_test(scenario, description, expected, command, False, actual, exec_time)

    def test_find_by_template_id(self) -> None:
        """Test: Find template by ID"""
        if not self.template_ids:
            print("Skipping: No template IDs available")
            return

        scenario = "Find Template by ID"
        description = "Retrieve specific template using its ID"
        expected = "Returns exactly 1 template with matching ID"

        test_id = self.template_ids[0]
        payload = {
            "method": {"cmd": "crudget", "argcnt": 1},
            "dstype": "dsswres",
            "criteria": [{"_id": test_id}]
        }
        id_preview = test_id[:20] + "..." if len(test_id) > 20 else test_id
        command = f"POST /swarmkb/command with criteria: [{{'_id': '{id_preview}'}}]"

        self.print_test_header(scenario, description)
        self.print_expected(expected)
        self.print_command(command)

        start_time = time.time()

        try:
            response = requests.post(
                f"{self.base_url}/swarmkb/command",
                json=payload,
                timeout=30
            )

            exec_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                results = data.get('data', [])
                count = len(results)

                passed = (count == 1 and results[0].get('_id') == test_id)
                actual = f"Returned {count} template(s), ID match: {passed}"

                self.print_result(passed, actual)
                self.record_test(scenario, description, expected, command, passed, actual, exec_time)
            else:
                actual = f"HTTP {response.status_code}"
                self.print_result(False, actual)
                self.record_test(scenario, description, expected, command, False, actual, exec_time)

        except Exception as e:
            exec_time = time.time() - start_time
            actual = f"Exception: {str(e)}"
            self.print_result(False, actual)
            self.record_test(scenario, description, expected, command, False, actual, exec_time)

    def test_find_by_tosca_version(self) -> None:
        """Test: Find templates by TOSCA version"""
        scenario = "Find by TOSCA Version"
        description = "Find all templates using tosca_simple_yaml_1_3"
        expected = f"Returns {len(self.TOSCA_FILES)} templates"

        payload = {
            "method": {"cmd": "crudget", "argcnt": 1},
            "dstype": "dsswres",
            "criteria": [{"tosca_definitions_version": "tosca_simple_yaml_1_3"}]
        }
        command = "POST /swarmkb/command with criteria: [{'tosca_definitions_version': 'tosca_simple_yaml_1_3'}]"

        self.print_test_header(scenario, description)
        self.print_expected(expected)
        self.print_command(command)

        start_time = time.time()

        try:
            response = requests.post(
                f"{self.base_url}/swarmkb/command",
                json=payload,
                timeout=30
            )

            exec_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                count = len(data.get('data', []))

                passed = count >= len(self.TOSCA_FILES)
                actual = f"Returned {count} templates"

                self.print_result(passed, actual)
                self.record_test(scenario, description, expected, command, passed, actual, exec_time)
            else:
                actual = f"HTTP {response.status_code}"
                self.print_result(False, actual)
                self.record_test(scenario, description, expected, command, False, actual, exec_time)

        except Exception as e:
            exec_time = time.time() - start_time
            actual = f"Exception: {str(e)}"
            self.print_result(False, actual)
            self.record_test(scenario, description, expected, command, False, actual, exec_time)

    def test_crud_insert(self) -> Optional[str]:
        """Test: CRUD INSERT operation"""
        scenario = "CRUD - INSERT"
        description = "Insert a test renewable energy resource document"
        expected = "Document inserted successfully with confirmation message"

        test_id = f"test_solar_farm_{int(time.time())}"
        payload = {
            "method": {"cmd": "crudput", "argcnt": 1},
            "dstype": "dsswres",
            "criteria": [{
                "_id": test_id,
                "name": "Athens Solar Farm Test",
                "type": "solar",
                "capacity_mw": 500,
                "location": {
                    "country": "Greece",
                    "region": "Attica",
                    "coordinates": {"lat": 37.9838, "lon": 23.7275}
                },
                "status": "operational",
                "commissioned_date": "2024-06-15"
            }]
        }
        command = f"POST /swarmkb/command with crudput, _id: {test_id}"

        self.print_test_header(scenario, description)
        self.print_expected(expected)
        self.print_command(command)

        start_time = time.time()

        try:
            response = requests.post(
                f"{self.base_url}/swarmkb/command",
                json=payload,
                timeout=30
            )

            exec_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                message = data.get('data', '')

                passed = 'inserted' in message.lower() or 'success' in message.lower()
                actual = f"Response: {message}"

                self.print_result(passed, actual)
                self.record_test(scenario, description, expected, command, passed, actual, exec_time)

                return test_id if passed else None
            else:
                actual = f"HTTP {response.status_code}"
                self.print_result(False, actual)
                self.record_test(scenario, description, expected, command, False, actual, exec_time)
                return None

        except Exception as e:
            exec_time = time.time() - start_time
            actual = f"Exception: {str(e)}"
            self.print_result(False, actual)
            self.record_test(scenario, description, expected, command, False, actual, exec_time)
            return None

    def test_crud_query(self, test_id: str) -> bool:
        """Test: CRUD QUERY operation"""
        scenario = "CRUD - QUERY"
        description = "Query the test document we just inserted"
        expected = "Returns exactly 1 document with matching _id"

        payload = {
            "method": {"cmd": "crudget", "argcnt": 1},
            "dstype": "dsswres",
            "criteria": [{"_id": test_id}]
        }
        command = f"POST /swarmkb/command with crudget, _id: {test_id}"

        self.print_test_header(scenario, description)
        self.print_expected(expected)
        self.print_command(command)

        start_time = time.time()

        try:
            response = requests.post(
                f"{self.base_url}/swarmkb/command",
                json=payload,
                timeout=30
            )

            exec_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                results = data.get('data', [])
                count = len(results)

                passed = (count == 1 and results[0].get('_id') == test_id)
                actual = f"Returned {count} document(s), _id match: {passed}"

                self.print_result(passed, actual)
                self.record_test(scenario, description, expected, command, passed, actual, exec_time)

                return passed
            else:
                actual = f"HTTP {response.status_code}"
                self.print_result(False, actual)
                self.record_test(scenario, description, expected, command, False, actual, exec_time)
                return False

        except Exception as e:
            exec_time = time.time() - start_time
            actual = f"Exception: {str(e)}"
            self.print_result(False, actual)
            self.record_test(scenario, description, expected, command, False, actual, exec_time)
            return False

    def test_crud_update(self, test_id: str) -> bool:
        """Test: CRUD UPDATE operation"""
        scenario = "CRUD - UPDATE"
        description = "Update test document with new values"
        expected = "Document updated successfully, _id preserved"

        payload = {
            "method": {"cmd": "crudupdate", "argcnt": 1},
            "dstype": "dsswres",
            "criteria": [{"_id": test_id}],
            "UpdateData": [{
                "status": "maintenance",
                "maintenance_reason": "Scheduled panel cleaning",
                "capacity_mw": 550
            }]
        }
        command = f"POST /swarmkb/command with crudupdate, _id: {test_id}"

        self.print_test_header(scenario, description)
        self.print_expected(expected)
        self.print_command(command)

        start_time = time.time()

        try:
            response = requests.post(
                f"{self.base_url}/swarmkb/command",
                json=payload,
                timeout=30
            )

            exec_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                message = data.get('data', '')

                passed = 'updated' in message.lower() or 'success' in message.lower()
                actual = f"Response: {message}"

                self.print_result(passed, actual)
                self.record_test(scenario, description, expected, command, passed, actual, exec_time)

                return passed
            else:
                actual = f"HTTP {response.status_code}"
                self.print_result(False, actual)
                self.record_test(scenario, description, expected, command, False, actual, exec_time)
                return False

        except Exception as e:
            exec_time = time.time() - start_time
            actual = f"Exception: {str(e)}"
            self.print_result(False, actual)
            self.record_test(scenario, description, expected, command, False, actual, exec_time)
            return False

    def test_crud_verify_update(self, test_id: str) -> bool:
        """Test: CRUD VERIFY UPDATE - Critical test for _id preservation"""
        scenario = "CRUD - VERIFY UPDATE (CRITICAL)"
        description = "Verify update applied correctly and _id was preserved"
        expected = "_id preserved, status='maintenance', capacity_mw=550, has _updated_at"

        payload = {
            "method": {"cmd": "crudget", "argcnt": 1},
            "dstype": "dsswres",
            "criteria": [{"_id": test_id}]
        }
        command = f"POST /swarmkb/command with crudget, verify _id preserved"

        self.print_test_header(scenario, description)
        self.print_expected(expected)
        self.print_command(command)

        start_time = time.time()

        try:
            response = requests.post(
                f"{self.base_url}/swarmkb/command",
                json=payload,
                timeout=30
            )

            exec_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                results = data.get('data', [])

                if len(results) == 1:
                    doc = results[0]

                    id_preserved = doc.get('_id') == test_id
                    status_updated = doc.get('status') == 'maintenance'
                    capacity_updated = doc.get('capacity_mw') == 550
                    has_timestamp = '_updated_at' in doc

                    passed = id_preserved and status_updated and capacity_updated and has_timestamp

                    actual = (f"_id preserved: {id_preserved}, "
                            f"status: {doc.get('status')}, "
                            f"capacity: {doc.get('capacity_mw')}, "
                            f"has _updated_at: {has_timestamp}")

                    self.print_result(passed, actual)

                    if passed:
                        print(f"{Colors.GREEN}   🎉 CRITICAL TEST PASSED - UPDATE fix working correctly!{Colors.NC}")
                    else:
                        print(f"{Colors.RED}   ⚠️  CRITICAL TEST FAILED - UPDATE may have issues!{Colors.NC}")

                    self.record_test(scenario, description, expected, command, passed, actual, exec_time)
                    return passed
                else:
                    actual = f"Expected 1 document, got {len(results)}"
                    self.print_result(False, actual)
                    self.record_test(scenario, description, expected, command, False, actual, exec_time)
                    return False
            else:
                actual = f"HTTP {response.status_code}"
                self.print_result(False, actual)
                self.record_test(scenario, description, expected, command, False, actual, exec_time)
                return False

        except Exception as e:
            exec_time = time.time() - start_time
            actual = f"Exception: {str(e)}"
            self.print_result(False, actual)
            self.record_test(scenario, description, expected, command, False, actual, exec_time)
            return False

    def test_crud_delete(self, test_id: str) -> bool:
        """Test: CRUD DELETE operation"""
        scenario = "CRUD - DELETE"
        description = "Delete the test document"
        expected = "Document deleted successfully"

        payload = {
            "method": {"cmd": "cruddelete", "argcnt": 1},
            "dstype": "dsswres",
            "criteria": [{"_id": test_id}]
        }
        command = f"POST /swarmkb/command with cruddelete, _id: {test_id}"

        self.print_test_header(scenario, description)
        self.print_expected(expected)
        self.print_command(command)

        start_time = time.time()

        try:
            response = requests.post(
                f"{self.base_url}/swarmkb/command",
                json=payload,
                timeout=30
            )

            exec_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                message = data.get('data', '')

                passed = 'deleted' in message.lower() or 'success' in message.lower()
                actual = f"Response: {message}"

                self.print_result(passed, actual)
                self.record_test(scenario, description, expected, command, passed, actual, exec_time)

                return passed
            else:
                actual = f"HTTP {response.status_code}"
                self.print_result(False, actual)
                self.record_test(scenario, description, expected, command, False, actual, exec_time)
                return False

        except Exception as e:
            exec_time = time.time() - start_time
            actual = f"Exception: {str(e)}"
            self.print_result(False, actual)
            self.record_test(scenario, description, expected, command, False, actual, exec_time)
            return False

    def test_crud_verify_delete(self, test_id: str) -> bool:
        """Test: CRUD VERIFY DELETE"""
        scenario = "CRUD - VERIFY DELETE"
        description = "Verify document was deleted"
        expected = "Query returns empty array (0 results)"

        payload = {
            "method": {"cmd": "crudget", "argcnt": 1},
            "dstype": "dsswres",
            "criteria": [{"_id": test_id}]
        }
        command = f"POST /swarmkb/command with crudget, should return empty"

        self.print_test_header(scenario, description)
        self.print_expected(expected)
        self.print_command(command)

        start_time = time.time()

        try:
            response = requests.post(
                f"{self.base_url}/swarmkb/command",
                json=payload,
                timeout=30
            )

            exec_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                count = len(data.get('data', []))

                passed = count == 0
                actual = f"Returned {count} document(s)"

                self.print_result(passed, actual)
                self.record_test(scenario, description, expected, command, passed, actual, exec_time)

                return passed
            else:
                actual = f"HTTP {response.status_code}"
                self.print_result(False, actual)
                self.record_test(scenario, description, expected, command, False, actual, exec_time)
                return False

        except Exception as e:
            exec_time = time.time() - start_time
            actual = f"Exception: {str(e)}"
            self.print_result(False, actual)
            self.record_test(scenario, description, expected, command, False, actual, exec_time)
            return False

    def run_crud_tests(self) -> None:
        """Run complete CRUD test sequence"""
        self.print_section("PHASE 3: CRUD Operations Testing")

        # INSERT
        test_id = self.test_crud_insert()

        if not test_id:
            print(f"{Colors.RED}CRUD tests aborted - INSERT failed{Colors.NC}")
            return

        time.sleep(1)  # Brief pause

        # QUERY
        if not self.test_crud_query(test_id):
            print(f"{Colors.YELLOW}Warning: QUERY failed but continuing...{Colors.NC}")

        time.sleep(1)

        # UPDATE
        if not self.test_crud_update(test_id):
            print(f"{Colors.YELLOW}Warning: UPDATE failed but continuing...{Colors.NC}")

        time.sleep(1)

        # VERIFY UPDATE (CRITICAL TEST)
        self.test_crud_verify_update(test_id)

        time.sleep(1)

        # DELETE
        if not self.test_crud_delete(test_id):
            print(f"{Colors.YELLOW}Warning: DELETE failed but continuing...{Colors.NC}")

        time.sleep(1)

        # VERIFY DELETE
        self.test_crud_verify_delete(test_id)

    def generate_report(self) -> None:
        """Generate test report"""
        self.print_section("PHASE 4: Test Report Generation")

        total_duration = time.time() - self.test_start_time
        passed = sum(1 for r in self.test_results if r.passed)
        failed = len(self.test_results) - passed

        # Console report
        print(f"\n{Colors.BOLD}{'═' * 80}{Colors.NC}")
        print(f"{Colors.BOLD}{Colors.CYAN}TEST EXECUTION SUMMARY{Colors.NC}")
        print(f"{Colors.BOLD}{'═' * 80}{Colors.NC}\n")

        print(f"Total Tests:     {len(self.test_results)}")
        print(f"{Colors.GREEN}Passed:          {passed}{Colors.NC}")
        print(f"{Colors.RED}Failed:          {failed}{Colors.NC}")
        print(f"Duration:        {total_duration:.2f}s")
        print(f"Success Rate:    {(passed/len(self.test_results)*100) if self.test_results else 0:.1f}%\n")

        # Detailed results
        print(f"{Colors.BOLD}DETAILED RESULTS:{Colors.NC}\n")

        for i, result in enumerate(self.test_results, 1):
            status = f"{Colors.GREEN}✅ PASS{Colors.NC}" if result.passed else f"{Colors.RED}❌ FAIL{Colors.NC}"
            print(f"{i}. {status} - {result.scenario}")
            print(f"   Expected: {result.expected}")
            print(f"   Actual:   {result.actual_result}")
            print(f"   Time:     {result.execution_time:.3f}s")
            print()

        # Save text report
        with open(self.report_file, 'w', encoding='utf-8') as f:
            f.write("OptimusDB End-to-End Test Report\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Test Session: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Base URL: {self.base_url}\n")
            f.write(f"Total Tests: {len(self.test_results)}\n")
            f.write(f"Passed: {passed}\n")
            f.write(f"Failed: {failed}\n")
            f.write(f"Duration: {total_duration:.2f}s\n")
            f.write(f"Success Rate: {(passed/len(self.test_results)*100) if self.test_results else 0:.1f}%\n\n")

            f.write("Detailed Results:\n")
            f.write("-" * 80 + "\n\n")

            for i, result in enumerate(self.test_results, 1):
                f.write(f"{i}. {'PASS' if result.passed else 'FAIL'} - {result.scenario}\n")
                f.write(f"   Description: {result.description}\n")
                f.write(f"   Expected: {result.expected}\n")
                f.write(f"   Actual: {result.actual_result}\n")
                f.write(f"   Command: {result.command}\n")
                f.write(f"   Execution Time: {result.execution_time:.3f}s\n")
                f.write(f"   Timestamp: {result.timestamp}\n\n")

        print(f"{Colors.GREEN}✅ Text report saved to: {self.report_file}{Colors.NC}")

        # Save JSON report
        report_data = {
            "session": {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "base_url": self.base_url,
                "total_tests": len(self.test_results),
                "passed": passed,
                "failed": failed,
                "duration": total_duration,
                "success_rate": (passed/len(self.test_results)*100) if self.test_results else 0
            },
            "test_results": [asdict(r) for r in self.test_results]
        }

        with open(self.json_report, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2)

        print(f"{Colors.GREEN}✅ JSON report saved to: {self.json_report}{Colors.NC}")

        # Final status
        print()
        if failed == 0:
            print(f"{Colors.GREEN}{Colors.BOLD}🎉 ALL TESTS PASSED! 🎉{Colors.NC}")
        else:
            print(f"{Colors.YELLOW}{Colors.BOLD}⚠️  {failed} TEST(S) FAILED{Colors.NC}")

    def run(self) -> int:
        """Execute complete test suite"""
        self.print_banner("OptimusDB End-to-End Test Suite", "═")

        print(f"{Colors.BOLD}Configuration:{Colors.NC}")
        print(f"  Base URL: {self.base_url}")
        print(f"  Files Directory: {self.files_dir}")
        print(f"  Test Report: {self.report_file}")
        print()

        # Phase 1: Upload TOSCA files
        if not self.upload_all_tosca_files():
            print(f"{Colors.RED}Upload phase failed - aborting tests{Colors.NC}")
            return 1

        time.sleep(2)  # Wait for replication

        # Phase 2: Simple Query Tests
        self.print_section("PHASE 2: Simple Query Tests")

        self.test_get_all_templates()
        time.sleep(1)

        self.test_find_by_template_id()
        time.sleep(1)

        self.test_find_by_tosca_version()
        time.sleep(1)

        # Phase 3: CRUD Tests
        self.run_crud_tests()

        # Phase 4: Generate Report
        self.generate_report()

        # Return exit code
        failed = sum(1 for r in self.test_results if not r.passed)
        return 0 if failed == 0 else 1

def main():
    """Main entry point"""
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:18001"
    files_dir = sys.argv[2] if len(sys.argv) > 2 else "."

    runner = E2ETestRunner(base_url, files_dir)
    exit_code = runner.run()

    sys.exit(exit_code)

if __name__ == "__main__":
    main()