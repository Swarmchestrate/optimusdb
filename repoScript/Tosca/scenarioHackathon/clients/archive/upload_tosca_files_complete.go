package main

/*
OptimusDB TOSCA Upload Script (Go)
===================================
Uploads TOSCA YAML files to OptimusDB with base64 encoding
and persists template IDs to a JSON file.

Project: OptimusDB - EU Horizon Europe Grant 101135012

Usage:
	go run upload_tosca_files_complete.go [base_url] [files_directory]

Example:
	go run upload_tosca_files_complete.go http://localhost:18001 ./tosca_samples

Build:
	go build -o upload_tosca upload_tosca_files_complete.go

Dependencies:
	Standard library only - no external dependencies required!
*/

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Color codes for terminal output
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorCyan   = "\033[36m"
	ColorGray   = "\033[90m"
)

// ToscaFile represents a TOSCA file to upload
type ToscaFile struct {
	Filename    string
	Description string
}

// UploadRequest represents the request payload for uploading a TOSCA file
type UploadRequest struct {
	File               string `json:"file"`
	Filename           string `json:"filename"`
	StoreFullStructure bool   `json:"store_full_structure"`
}

// UploadResponseData represents the data field in upload response
type UploadResponseData struct {
	TemplateID      string `json:"template_id"`
	Queryable       bool   `json:"queryable"`
	StorageLocation string `json:"storage_location"`
	Filename        string `json:"filename"`
}

// UploadResponse represents the response from upload endpoint
type UploadResponse struct {
	Status  int                `json:"status"`
	Message string             `json:"message"`
	Data    UploadResponseData `json:"data"`
}

// UploadResult represents the result of uploading a file
type UploadResult struct {
	Filename        string `json:"filename"`
	Description     string `json:"description"`
	TemplateID      string `json:"template_id"`
	Queryable       bool   `json:"queryable"`
	StorageLocation string `json:"storage_location"`
	UploadedAt      string `json:"uploaded_at"`
}

// UploadSession represents the summary of the upload session
type UploadSession struct {
	Timestamp  string `json:"timestamp"`
	BaseURL    string `json:"base_url"`
	TotalFiles int    `json:"total_files"`
	Uploaded   int    `json:"uploaded"`
	Failed     int    `json:"failed"`
}

// OutputData represents the final JSON output
type OutputData struct {
	Session   UploadSession  `json:"upload_session"`
	Templates []UploadResult `json:"templates"`
}

// QueryRequest represents a query request
type QueryRequest struct {
	Method   map[string]interface{} `json:"method"`
	DsType   string                 `json:"dstype"`
	Criteria []interface{}          `json:"criteria"`
}

// QueryResponse represents a query response
type QueryResponse struct {
	Status int                      `json:"status"`
	Data   []map[string]interface{} `json:"data"`
}

// ToscaUploader manages the upload process
type ToscaUploader struct {
	BaseURL       string
	FilesDir      string
	OutputFile    string
	LogFile       string
	UploadedCount int
	FailedCount   int
	Results       []UploadResult
	LogWriter     *os.File
}

// TOSCA files configuration
var toscaFiles = []ToscaFile{
	{"webapp_adt.yaml", "WebApp Microservices Application"},
	{"capacity_profile.yaml", "Edge Cluster Capacity Profile"},
	{"opentofu_hybrid.yaml", "Hybrid Infrastructure with OpenTofu"},
	{"deployment_plan.yaml", "Deployment Plan with Workflows"},
	{"app_requirements.yaml", "ML Training Application Requirements"},
}

// NewToscaUploader creates a new uploader instance
func NewToscaUploader(baseURL, filesDir string) *ToscaUploader {
	timestamp := time.Now().Format("20060102_150405")
	logFile := fmt.Sprintf("upload_log_%s.txt", timestamp)

	logWriter, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Warning: Could not open log file: %v\n", err)
	}

	return &ToscaUploader{
		BaseURL:    baseURL,
		FilesDir:   filesDir,
		OutputFile: "uploaded_tosca_templates.json",
		LogFile:    logFile,
		Results:    make([]UploadResult, 0),
		LogWriter:  logWriter,
	}
}

// Close closes the log file
func (u *ToscaUploader) Close() {
	if u.LogWriter != nil {
		u.LogWriter.Close()
	}
}

// Log writes a message to the log file
func (u *ToscaUploader) Log(message string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logMessage := fmt.Sprintf("[%s] %s\n", timestamp, message)
	if u.LogWriter != nil {
		u.LogWriter.WriteString(logMessage)
	}
}

// Print functions with colors
func (u *ToscaUploader) PrintHeader(text string) {
	fmt.Println()
	fmt.Printf("%s%s%s\n", ColorCyan, "═══════════════════════════════════════════════════════════", ColorReset)
	fmt.Printf("%s%s%s\n", ColorCyan, text, ColorReset)
	fmt.Printf("%s%s%s\n", ColorCyan, "═══════════════════════════════════════════════════════════", ColorReset)
	fmt.Println()
}

func (u *ToscaUploader) PrintSuccess(message string) {
	fmt.Printf("%s✅ %s%s\n", ColorGreen, message, ColorReset)
	u.Log("SUCCESS: " + message)
}

func (u *ToscaUploader) PrintError(message string) {
	fmt.Printf("%s❌ %s%s\n", ColorRed, message, ColorReset)
	u.Log("ERROR: " + message)
}

func (u *ToscaUploader) PrintWarning(message string) {
	fmt.Printf("%s⚠️  %s%s\n", ColorYellow, message, ColorReset)
	u.Log("WARNING: " + message)
}

func (u *ToscaUploader) PrintInfo(message string) {
	fmt.Printf("%sℹ️  %s%s\n", ColorCyan, message, ColorReset)
}

func (u *ToscaUploader) PrintDetail(message string) {
	fmt.Printf("%s   %s%s\n", ColorGray, message, ColorReset)
}

// TestConnectivity tests connection to the API
func (u *ToscaUploader) TestConnectivity() bool {
	u.PrintInfo(fmt.Sprintf("Testing connection to %s...", u.BaseURL))

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(u.BaseURL + "/health")

	if err == nil && resp.StatusCode == 200 {
		u.PrintSuccess("API is reachable")
		resp.Body.Close()
		return true
	}

	u.PrintWarning("Health endpoint not responding (this may be normal)")
	u.PrintInfo("Attempting to continue anyway...")
	return true
}

// ConvertToBase64 converts a file to base64
func (u *ToscaUploader) ConvertToBase64(filepath string) (string, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return "", err
	}

	encoded := base64.StdEncoding.EncodeToString(data)
	return encoded, nil
}

// UploadToscaFile uploads a single TOSCA file
func (u *ToscaUploader) UploadToscaFile(file ToscaFile) *UploadResult {
	fmt.Println()
	u.PrintInfo(fmt.Sprintf("Processing: %s", file.Description))
	u.PrintDetail(fmt.Sprintf("File: %s", file.Filename))

	filepath := filepath.Join(u.FilesDir, file.Filename)

	// Check file exists
	fileInfo, err := os.Stat(filepath)
	if err != nil {
		u.PrintError(fmt.Sprintf("File not found: %s", filepath))
		return nil
	}

	// Get file size
	sizeKB := float64(fileInfo.Size()) / 1024.0
	u.PrintDetail(fmt.Sprintf("Size: %.2f KB", sizeKB))

	// Convert to base64
	u.PrintDetail("Converting to base64...")
	base64Content, err := u.ConvertToBase64(filepath)
	if err != nil {
		u.PrintError(fmt.Sprintf("Failed to convert to base64: %v", err))
		return nil
	}

	// Prepare request
	uploadReq := UploadRequest{
		File:               base64Content,
		Filename:           file.Filename,
		StoreFullStructure: true,
	}

	jsonData, err := json.Marshal(uploadReq)
	if err != nil {
		u.PrintError(fmt.Sprintf("Failed to marshal JSON: %v", err))
		return nil
	}

	// Upload to OptimusDB
	u.PrintDetail(fmt.Sprintf("Uploading to %s/swarmkb/upload...", u.BaseURL))

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Post(
		u.BaseURL+"/swarmkb/upload",
		"application/json",
		bytes.NewBuffer(jsonData),
	)

	if err != nil {
		u.PrintError(fmt.Sprintf("Upload failed: %v", err))
		return nil
	}
	defer resp.Body.Close()

	// Check HTTP status
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		u.PrintError(fmt.Sprintf("Upload failed with HTTP %d", resp.StatusCode))
		u.PrintDetail(fmt.Sprintf("Response: %s", string(body)))
		return nil
	}

	// Parse response
	var uploadResp UploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		u.PrintError(fmt.Sprintf("Failed to parse response: %v", err))
		return nil
	}

	if uploadResp.Status != 200 {
		u.PrintError(fmt.Sprintf("Upload failed: %s", uploadResp.Message))
		return nil
	}

	if uploadResp.Data.TemplateID == "" {
		u.PrintError("No template ID returned in response")
		return nil
	}

	// Success!
	u.PrintSuccess("Upload successful")
	u.PrintDetail(fmt.Sprintf("Template ID: %s", uploadResp.Data.TemplateID))
	u.PrintDetail(fmt.Sprintf("Queryable: %t", uploadResp.Data.Queryable))
	u.PrintDetail(fmt.Sprintf("Storage: %s", uploadResp.Data.StorageLocation))

	// Create result
	return &UploadResult{
		Filename:        file.Filename,
		Description:     file.Description,
		TemplateID:      uploadResp.Data.TemplateID,
		Queryable:       uploadResp.Data.Queryable,
		StorageLocation: uploadResp.Data.StorageLocation,
		UploadedAt:      time.Now().UTC().Format(time.RFC3339),
	}
}

// SaveResults saves results to JSON file
func (u *ToscaUploader) SaveResults() error {
	outputData := OutputData{
		Session: UploadSession{
			Timestamp:  time.Now().UTC().Format(time.RFC3339),
			BaseURL:    u.BaseURL,
			TotalFiles: len(toscaFiles),
			Uploaded:   u.UploadedCount,
			Failed:     u.FailedCount,
		},
		Templates: u.Results,
	}

	jsonData, err := json.MarshalIndent(outputData, "", "  ")
	if err != nil {
		return err
	}

	err = os.WriteFile(u.OutputFile, jsonData, 0644)
	if err != nil {
		return err
	}

	u.PrintSuccess(fmt.Sprintf("Results saved to: %s", u.OutputFile))
	return nil
}

// VerifyUploads queries the database to verify uploads
func (u *ToscaUploader) VerifyUploads() bool {
	u.PrintInfo("Verifying uploads...")

	queryReq := QueryRequest{
		Method: map[string]interface{}{
			"cmd":    "crudget",
			"argcnt": 1,
		},
		DsType:   "dsswres",
		Criteria: make([]interface{}, 0),
	}

	jsonData, err := json.Marshal(queryReq)
	if err != nil {
		u.PrintWarning(fmt.Sprintf("Could not verify uploads: %v", err))
		return false
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(
		u.BaseURL+"/swarmkb/command",
		"application/json",
		bytes.NewBuffer(jsonData),
	)

	if err != nil {
		u.PrintWarning(fmt.Sprintf("Could not verify uploads: %v", err))
		return false
	}
	defer resp.Body.Close()

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		u.PrintWarning(fmt.Sprintf("Could not parse query response: %v", err))
		return false
	}

	totalCount := len(queryResp.Data)

	if totalCount > 0 {
		u.PrintSuccess(fmt.Sprintf("Verified: %d total templates in database", totalCount))

		// Count TOSCA templates
		toscaCount := 0
		for _, item := range queryResp.Data {
			if _, ok := item["tosca_definitions_version"]; ok {
				toscaCount++
			}
		}

		u.PrintDetail(fmt.Sprintf("TOSCA templates: %d", toscaCount))
		return true
	}

	u.PrintWarning("Could not verify uploads (query returned no results)")
	return false
}

// Run executes the upload process
func (u *ToscaUploader) Run() int {
	u.PrintHeader("OptimusDB TOSCA Upload Script")

	fmt.Println("Configuration:")
	fmt.Printf("  Base URL: %s\n", u.BaseURL)
	fmt.Printf("  Files Directory: %s\n", u.FilesDir)
	fmt.Printf("  Output File: %s\n", u.OutputFile)
	fmt.Printf("  Log File: %s\n", u.LogFile)
	fmt.Println()

	// Test connectivity
	u.TestConnectivity()

	// Process each file
	u.PrintHeader("Uploading TOSCA Files")

	for _, file := range toscaFiles {
		result := u.UploadToscaFile(file)

		if result != nil {
			u.Results = append(u.Results, *result)
			u.UploadedCount++
		} else {
			u.FailedCount++
		}

		// Brief pause between uploads
		time.Sleep(1 * time.Second)
	}

	// Summary
	fmt.Println()
	u.PrintHeader("Upload Summary")

	totalFiles := len(toscaFiles)
	fmt.Printf("Total Files:     %d\n", totalFiles)
	fmt.Printf("%sUploaded:        %d%s\n", ColorGreen, u.UploadedCount, ColorReset)
	fmt.Printf("%sFailed:          %d%s\n", ColorRed, u.FailedCount, ColorReset)
	fmt.Println()

	// Save results if any succeeded
	if u.UploadedCount > 0 {
		if err := u.SaveResults(); err != nil {
			u.PrintError(fmt.Sprintf("Failed to save results: %v", err))
		}

		fmt.Println()
		u.PrintInfo(fmt.Sprintf("Template IDs saved to %s", u.OutputFile))
		fmt.Println()

		// Show uploaded templates
		fmt.Println("Uploaded Templates:")
		for _, result := range u.Results {
			fmt.Printf("%s  ✓ %s%s\n", ColorGreen, result.Description, ColorReset)
			fmt.Printf("%s    ID: %s%s\n", ColorGray, result.TemplateID, ColorReset)
		}

		// Verify uploads
		fmt.Println()
		u.VerifyUploads()
	}

	// Final status
	fmt.Println()
	if u.FailedCount == 0 {
		u.PrintHeader("✅ All Uploads Successful!")
		return 0
	} else if u.UploadedCount > 0 {
		u.PrintHeader("⚠️  Partial Success - Some Uploads Failed")
		return 1
	} else {
		u.PrintHeader("❌ All Uploads Failed")
		return 1
	}
}

func main() {
	// Parse command line arguments
	baseURL := "http://localhost:18001"
	filesDir := "."

	if len(os.Args) > 1 {
		baseURL = os.Args[1]
	}
	if len(os.Args) > 2 {
		filesDir = os.Args[2]
	}

	// Create uploader
	uploader := NewToscaUploader(baseURL, filesDir)
	defer uploader.Close()

	// Run and exit with appropriate code
	exitCode := uploader.Run()
	os.Exit(exitCode)
}
