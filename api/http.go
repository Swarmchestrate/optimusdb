package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"io/ioutil"
	"net"
	"net/http"
	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/contextualmetadata"
	"optimusdb/credentials"
	"optimusdb/election"
	"optimusdb/logger"
	"optimusdb/tosca"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PeerTracker stores discovered peers
type PeerTracker struct {
	sync.Mutex
	Peers map[peer.ID]peer.AddrInfo
}

// Global peer tracker
var peerTracker = &PeerTracker{Peers: make(map[peer.ID]peer.AddrInfo)}

type enrichReq struct {
	DB      string `json:"db"`
	Table   string `json:"table"`
	MaxRows int    `json:"maxRows"`
	Greek   bool   `json:"greek"`
}

// TrackPeer adds a new peer to the list
func TrackPeer(pi peer.AddrInfo) {
	peerTracker.Lock()
	defer peerTracker.Unlock()
	peerTracker.Peers[pi.ID] = pi
}

// GetPeers returns all discovered peers
func GetPeers() []peer.AddrInfo {
	peerTracker.Lock()
	defer peerTracker.Unlock()
	peers := make([]peer.AddrInfo, 0, len(peerTracker.Peers))
	for _, info := range peerTracker.Peers {
		peers = append(peers, info)
	}
	return peers
}

// peersHandler returns a JSON list of known peers
func peersHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		peers := GetPeers()

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(peers)
		if err != nil {
			http.Error(w, "Failed to encode peers", http.StatusInternalServerError)
			return
		}
	}
}

// LogsHandler handles GET /<context>/log?date=YYYY-MM-DD&hour=HH
func LogsHandler(kb *app.LoggerSQLite) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		date := r.URL.Query().Get("date")
		hour := r.URL.Query().Get("hour")

		if date == "" || hour == "" {
			http.Error(w, "Missing 'date' or 'hour' query parameter", http.StatusBadRequest)
			return
		}

		logs, err := kb.GetLogsForHour(date, hour)
		if err != nil {
			http.Error(w, "Failed to fetch logs: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(logs)
	}
}

// uploadTOSCAHandler handles TOSCA template uploads with optional full structure storage
func uploadTOSCAHandler(optimusdb *app.KnowledgeBaseDB) http.HandlerFunc {
	type UploadRequest struct {
		File               string `json:"file"`
		Filename           string `json:"filename,omitempty"`
		StoreFullStructure bool   `json:"store_full_structure,omitempty"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST is allowed")
			return
		}

		var req UploadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.File == "" {
			sendErrorResponse(w, http.StatusBadRequest, "Invalid JSON payload")
			return
		}

		// 1) Base64 decode
		decoded, err := base64.StdEncoding.DecodeString(req.File)
		if err != nil {
			sendErrorResponse(w, http.StatusBadRequest, "Base64 decoding failed")
			return
		}

		ctx := r.Context()
		templateID := tosca.ComputeTemplateID(decoded)
		filename := req.Filename
		if filename == "" {
			filename = "unknown"
		}

		// 2) Determine storage strategy based on request parameter
		if req.StoreFullStructure {
			// ============================================================
			// NEW APPROACH: Store full parsed structure for queryability
			// ============================================================

			// Parse TOSCA YAML to complete JSON structure
			toscaDoc, err := tosca.ParseTOSCAToFullJSON(decoded)
			if err != nil {
				sendErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("TOSCA parse error: %v", err))
				return
			}

			// Add system fields for tracking and lineage
			toscaDoc["_id"] = templateID
			toscaDoc["_original_yaml"] = string(decoded)
			toscaDoc["_imported_at"] = time.Now().UTC().Format(time.RFC3339)
			toscaDoc["_filename"] = filename
			toscaDoc["_storage_type"] = "full_structure"

			// Add lineage metadata
			uploader := r.Header.Get("X-User")
			if uploader == "" {
				uploader = app.GetAgentName()
			}
			sourcePod := os.Getenv("POD_NAME")
			sourceIP, _ := getLocalIPAddress()

			toscaDoc["_lineage"] = map[string]interface{}{
				"uploader":   uploader,
				"source_pod": sourcePod,
				"source_ip":  sourceIP,
			}

			// Store in dsswres for full queryability
			if optimusdb.DsSWres == nil {
				sendErrorResponse(w, http.StatusInternalServerError, "Data store (dsswres) not initialized")
				return
			}

			if _, err := (*optimusdb.DsSWres).Put(ctx, toscaDoc); err != nil {
				sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to persist full structure: %v", err))
				return
			}

			// Trigger automatic metadata extraction and lineage tracking
			if optimusdb.Interceptor != nil {
				if err := optimusdb.Interceptor.OnDocumentPut(toscaDoc, "dsswres"); err != nil {
					logger.Warn("Metadata extraction failed for TOSCA upload %s: %v", templateID, err)
				} else {
					logger.Lineage("TOSCA document %s indexed with automatic lineage tracking", templateID)
				}
			}

			// Also index in SQLite for fast lookups
			if app.GlobalKBSQLite != nil {
				nodeCount := tosca.CountNodeTemplatesFromJSON(toscaDoc)
				description := extractDescription(toscaDoc)

				// Add to IPFS
				var ipfsPath string
				if optimusdb.Orbit != nil {
					coreAPI := (*optimusdb.Orbit).IPFS()
					nd := files.NewBytesFile(decoded)
					p, err := coreAPI.Unixfs().Add(ctx, nd)
					if err == nil {
						ipfsPath = p.String()
					}
				}

				filesize := int64(len(decoded))
				sum := sha256.Sum256(decoded)
				sha := fmt.Sprintf("%x", sum[:])

				app.GlobalKBSQLite.InsertTOSCAMetadata(
					templateID, description, nodeCount, filename,
					filesize, sha, ipfsPath, uploader, sourcePod, sourceIP,
				)
			}

			// Extract sample queryable fields for response
			queryableFields := extractQueryableFieldPaths(toscaDoc, "", 50)

			logger.Info("TOSCA uploaded with full structure: %s (filename: %s, size: %d bytes)",
				templateID, filename, len(decoded))

			sendSuccessResponse(w, map[string]interface{}{
				"message":          "TOSCA uploaded with full queryable structure",
				"template_id":      templateID,
				"storage_type":     "full_structure",
				"storage_location": "dsswres",
				"filename":         filename,
				"filesize":         len(decoded),
				"queryable":        true,
				"sample_fields":    queryableFields,
				"query_info": map[string]interface{}{
					"datastore": "dsswres",
					"query_example": fmt.Sprintf(
						`{"method":{"cmd":"query"},"dstype":"dsswres","criteria":[{"field":"_id","operator":"==","value":"%s"}]}`,
						templateID,
					),
				},
			})

		} else {
			// ============================================================
			// LEGACY APPROACH: Store minimal metadata + YAML blob
			// ============================================================

			tmpl, err := tosca.ParseTOSCA(decoded)
			if err != nil {
				sendErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("TOSCA parse error: %v", err))
				return
			}

			nodeCount := tosca.CountNodeTemplates(tmpl)
			description := tmpl.Description

			// Store in DsTOSCA_Imported (legacy store)
			if optimusdb.DsTOSCA_Imported == nil {
				sendErrorResponse(w, http.StatusInternalServerError, "TOSCA store not initialized")
				return
			}

			doc := map[string]interface{}{
				"_id":         templateID,
				"type":        "tosca_template",
				"description": description,
				"nodeCount":   nodeCount,
				"yaml":        string(decoded),
				"createdAt":   time.Now().UTC().Format(time.RFC3339),
			}

			if _, err := (*optimusdb.DsTOSCA_Imported).Put(ctx, doc); err != nil {
				sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to persist to OrbitDB: %v", err))
				return
			}

			// Trigger automatic metadata extraction for legacy TOSCA uploads
			if optimusdb.Interceptor != nil {
				if err := optimusdb.Interceptor.OnDocumentPut(doc, "tosca_imported"); err != nil {
					logger.Warn("Metadata extraction failed for legacy TOSCA upload %s: %v", templateID, err)
				} else {
					logger.Lineage("Legacy TOSCA document %s indexed", templateID)
				}
			}

			// Index in SQLite
			if app.GlobalKBSQLite != nil {
				var ipfsPath string
				if optimusdb.Orbit != nil {
					coreAPI := (*optimusdb.Orbit).IPFS()
					nd := files.NewBytesFile(decoded)
					p, err := coreAPI.Unixfs().Add(ctx, nd)
					if err == nil {
						ipfsPath = p.String()
					}
				}

				filesize := int64(len(decoded))
				sum := sha256.Sum256(decoded)
				sha := fmt.Sprintf("%x", sum[:])

				uploader := r.Header.Get("X-User")
				if uploader == "" {
					uploader = app.GetAgentName()
				}
				sourcePod := os.Getenv("POD_NAME")
				sourceIP, _ := getLocalIPAddress()

				app.GlobalKBSQLite.InsertTOSCAMetadata(
					templateID, description, nodeCount, filename,
					filesize, sha, ipfsPath, uploader, sourcePod, sourceIP,
				)
			}

			logger.Info("TOSCA uploaded (legacy mode): %s (filename: %s, nodes: %d)",
				templateID, filename, nodeCount)

			sendSuccessResponse(w, map[string]interface{}{
				"message":          "TOSCA uploaded (legacy mode)",
				"template_id":      templateID,
				"storage_type":     "yaml_blob",
				"storage_location": "tosca_imported",
				"node_count":       nodeCount,
				"filename":         filename,
				"filesize":         len(decoded),
				"queryable":        false,
				"note":             "Set store_full_structure:true for queryable fields",
			})
		}
	}
}

// extractDescription extracts description from parsed TOSCA JSON
func extractDescription(toscaDoc map[string]interface{}) string {
	if desc, ok := toscaDoc["description"].(string); ok {
		return desc
	}
	if metadata, ok := toscaDoc["metadata"].(map[string]interface{}); ok {
		if desc, ok := metadata["template_name"].(string); ok {
			return desc
		}
	}
	return "No description"
}

// extractQueryableFieldPaths extracts sample queryable field paths from TOSCA structure
func extractQueryableFieldPaths(doc map[string]interface{}, prefix string, limit int) []string {
	fields := []string{}
	count := 0

	var extract func(string, interface{})
	extract = func(path string, obj interface{}) {
		if count >= limit {
			return
		}

		switch v := obj.(type) {
		case map[string]interface{}:
			for key, val := range v {
				newPath := key
				if path != "" {
					newPath = path + "." + key
				}
				// Skip internal fields except _id
				if !strings.HasPrefix(key, "_") || key == "_id" {
					fields = append(fields, newPath)
					count++
					if count < limit {
						extract(newPath, val)
					}
				}
			}
		case []interface{}:
			// Show array notation
			fields = append(fields, path+"[]")
			count++
		}
	}

	extract(prefix, doc)
	return fields
}

// benchmarksHandler gathers all benchmark data from known peers
func benchmarksHandler(optimusdb *app.KnowledgeBaseDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		o := *optimusdb.Orbit
		ctx := context.Background()
		cinfo, err := o.IPFS().Swarm().Peers(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		client := &http.Client{}
		var benchmarks []app.Benchmark
		for _, c := range cinfo {
			ma := c.Address()
			ip, err := extractIPFromMultiaddr(ma)
			if err != nil {
				logger.Warn("Failed to extract IP from multiaddr %s: %v", ma, err)
				continue
			}

			bm, err := getBenchmark(client, ip)
			if err != nil {
				logger.Warn("Failed to get benchmark from peer %s: %v", ip, err)
				continue
			}

			benchmarks = append(benchmarks, bm)
		}
		benchmarks = append(benchmarks, *optimusdb.Benchmark)

		// convert data to json
		jsonData, err := json.Marshal(benchmarks)
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		// send response
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	}
}

// getBenchmark retrieves benchmark data from a peer
func getBenchmark(client *http.Client, peerIP string) (app.Benchmark, error) {
	var bm app.Benchmark

	bmReq := app.Request{Method: app.BENCHMARK, Args: []string{}}
	jsonData, err := json.Marshal(bmReq)
	if err != nil {
		return bm, err
	}

	// send get benchmark request
	cmdPath := "http://" + peerIP + ":" + *config.FlagHTTPPort + "/" + *config.FlagContext + "/command"
	req, err := http.NewRequest("POST", cmdPath, bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("There is an error in the request: %v\n", err)
		logger.Error("There is an error in the request: %v with error: %v", req, err)
		return bm, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return bm, err
	}
	defer resp.Body.Close()

	// read response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("There is an error in the read response body: %v\n", err)
		logger.Error("There is an error in the read response body: %v with error: %v", body, err)
		return bm, err
	}

	// unmarshal response
	err = json.Unmarshal(body, &bm)
	if err != nil {
		fmt.Printf("There is an error in the unmarshal response response body: %v\n", err)
		logger.Error("There is an error in the unmarshal response response body: %v with error: %v", body, err)
		return bm, err
	}

	return bm, nil
}

func extractIPFromMultiaddr(maddr multiaddr.Multiaddr) (string, error) {
	re := regexp.MustCompile(`/ip4/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})`)
	match := re.FindStringSubmatch(maddr.String())

	if len(match) >= 2 {
		return match[1], nil
	}

	return "", fmt.Errorf("No ip found in ma " + maddr.String())
}

// commandHandler handles HTTP requests and routes them to the service layer
func commandHandler(reqChan chan<- app.Request, resChan <-chan interface{}) http.HandlerFunc {

	type HTTPRequest struct {
		Method          app.Method               `json:"method"`
		Args            []string                 `json:"args"`
		File            string                   `json:"file"`
		DSType          string                   `json:"dstype"`
		Criteria        []map[string]interface{} `json:"criteria"`
		UpdateData      []map[string]interface{} `json:"UpdateData"`
		Graph_traversal []map[string]interface{} `json:"graph_Traversal"`
		SQLDML          string                   `json:"sqldml"`
	}

	logger.Debug("Command handler initialized")

	return func(w http.ResponseWriter, r *http.Request) {
		logger.Debug("Processing command request from %s", r.RemoteAddr)

		if r.Method != "POST" {
			sendErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}

		var req HTTPRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			sendErrorResponse(w, http.StatusBadRequest, "Invalid JSON payload")
			return
		}

		serviceReq := app.Request{
			Method:          req.Method,
			Args:            req.Args,
			DSType:          req.DSType,
			Criteria:        req.Criteria,
			UpdateData:      req.UpdateData,
			SQLDML:          req.SQLDML,
			Graph_traversal: req.Graph_traversal,
		}

		if serviceReq.Method == app.POST {
			decoded, err := base64.StdEncoding.DecodeString(req.File)
			if err != nil {
				sendErrorResponse(w, http.StatusBadRequest, "Error decoding Base64")
				return
			}
			serviceReq.Args = append(serviceReq.Args, string(decoded))
		}

		reqChan <- serviceReq // send request to processing
		res := <-resChan      // wait for response

		_, err = json.Marshal(res)
		if err != nil {
			sendErrorResponse(w, http.StatusBadRequest, "Internal Server Error, parsing the service Request json Marshal")
			return
		}

		if result, ok := res.(map[string]interface{}); ok && result["error"] != nil {
			sendErrorResponse(w, http.StatusBadRequest, "Error processing request")
		} else {
			sendSuccessResponse(w, res)
		}
	}
}

// sendErrorResponse sends an error response
func sendErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  http.Error,
		"message": message,
	})
}

// sendSuccessResponse sends a success response
func sendSuccessResponse(w http.ResponseWriter, data interface{}) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": http.StatusOK,
		"data":   data,
	})
}

// agentStatusHandler returns comprehensive agent status including role and peer health
func agentStatusHandler(optimusdb *app.KnowledgeBaseDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			sendErrorResponse(w, http.StatusMethodNotAllowed, "Only GET is allowed")
			return
		}

		ctx := context.Background()

		// Get current node info
		h := optimusdb.Node.PeerHost
		selfPeerID := h.ID().String()

		// Get self addresses
		selfAddrs := make([]string, 0)
		for _, addr := range h.Addrs() {
			selfAddrs = append(selfAddrs, addr.String())
		}

		// Get election status
		role, currentLeader, currentTerm, leadershipCount := election.GetNodeStatus()

		// Determine if this node is the coordinator
		isCoordinator := (role == "Coordinator")
		isCurrentLeader := (role == "Coordinator" && selfPeerID == currentLeader)

		// Get self reputation/health metrics
		selfReputation, _ := election.GetPeerReputation(selfPeerID)
		var selfHealthScore float64 = 0
		if selfReputation != nil {
			selfHealthScore = election.CalculateHealthScore(*selfReputation)
		}

		// Get all peers reputation
		allReputations, err := election.GetAllPeersReputation()
		if err != nil {
			logger.Error("Failed to get peer reputations: %v", err)
			allReputations = []election.NodeReputation{}
		}

		// Create a map for quick reputation lookup
		reputationMap := make(map[string]*election.NodeReputation)
		for i := range allReputations {
			reputationMap[allReputations[i].NodeID] = &allReputations[i]
		}

		// Get connected peers from IPFS
		coreAPI := (*optimusdb.Orbit).IPFS()
		connInfo, _ := coreAPI.Swarm().Peers(ctx)
		connectedPeerIDs := make(map[string]bool)
		for _, ci := range connInfo {
			connectedPeerIDs[ci.ID().String()] = true
		}

		// Get discovered peers
		discoveredPeers := optimusdb.GetDiscoveredPeers()

		// Build peer list with roles and health
		peersList := make([]map[string]interface{}, 0)

		// Use connected peers as source
		for peerIDStr := range connectedPeerIDs {
			// Skip self
			if peerIDStr == selfPeerID {
				continue
			}

			// Try to get reputation data
			rep, hasReputation := reputationMap[peerIDStr]

			// Determine peer role
			peerRole := "Follower"
			isLeader := false
			if peerIDStr == currentLeader {
				peerRole = "Coordinator"
				isLeader = true
			}

			var peerInfo map[string]interface{}

			if hasReputation {
				healthScore := election.CalculateHealthScore(*rep)

				var healthStatus string
				if healthScore >= 80 {
					healthStatus = "Excellent"
				} else if healthScore >= 60 {
					healthStatus = "Good"
				} else if healthScore >= 40 {
					healthStatus = "Fair"
				} else if healthScore >= 20 {
					healthStatus = "Poor"
				} else {
					healthStatus = "Critical"
				}

				peerInfo = map[string]interface{}{
					"peer_id":   peerIDStr,
					"role":      peerRole,
					"is_leader": isLeader,
					"connected": true,
					"health": map[string]interface{}{
						"score":        fmt.Sprintf("%.2f", healthScore),
						"status":       healthStatus,
						"cpu_usage":    fmt.Sprintf("%.2f%%", rep.UserCPU+rep.SystemCPU),
						"cpu_idle":     fmt.Sprintf("%.2f%%", rep.IdleCPU),
						"memory_used":  fmt.Sprintf("%.2f MB", rep.MemoryAvailable),
						"memory_total": fmt.Sprintf("%.2f MB", rep.MemoryAllocationTotal),
						"memory_sys":   fmt.Sprintf("%.2f MB", rep.MemorySystem),
						"disk_read":    fmt.Sprintf("%.2f MB/s", rep.AvgReadMBs),
						"disk_write":   fmt.Sprintf("%.2f MB/s", rep.AvgWriteMBs),
						"latency":      fmt.Sprintf("%.2f ms", rep.Latency),
						"uptime":       fmt.Sprintf("%.2f", rep.Uptime),
					},
					"metrics": map[string]interface{}{
						"leadership_count": rep.LeadershipCount,
						"geography_score":  rep.GeographyScore,
					},
				}
			} else {
				peerInfo = map[string]interface{}{
					"peer_id":   peerIDStr,
					"role":      peerRole,
					"is_leader": isLeader,
					"connected": true,
					"health": map[string]interface{}{
						"score":        "50.00",
						"status":       "Connected",
						"cpu_usage":    "N/A",
						"cpu_idle":     "N/A",
						"memory_used":  "N/A",
						"memory_total": "N/A",
						"memory_sys":   "N/A",
						"disk_read":    "N/A",
						"disk_write":   "N/A",
						"latency":      "10.00 ms",
						"uptime":       "N/A",
					},
					"metrics": map[string]interface{}{
						"leadership_count": 0,
						"geography_score":  0,
					},
				}
			}

			peersList = append(peersList, peerInfo)
		}

		// Get latest election info
		_, lastElectionTerm, lastElectionTime, _ := election.GetLatestElectionInfo()

		// Build self health info
		var selfHealth map[string]interface{}
		if selfReputation != nil {
			healthStatus := "Unknown"
			if selfHealthScore >= 80 {
				healthStatus = "Excellent"
			} else if selfHealthScore >= 60 {
				healthStatus = "Good"
			} else if selfHealthScore >= 40 {
				healthStatus = "Fair"
			} else if selfHealthScore >= 20 {
				healthStatus = "Poor"
			} else {
				healthStatus = "Critical"
			}

			selfHealth = map[string]interface{}{
				"score":        fmt.Sprintf("%.2f", selfHealthScore),
				"status":       healthStatus,
				"cpu_usage":    fmt.Sprintf("%.2f%%", selfReputation.UserCPU+selfReputation.SystemCPU),
				"cpu_idle":     fmt.Sprintf("%.2f%%", selfReputation.IdleCPU),
				"memory_used":  fmt.Sprintf("%.2f MB", selfReputation.MemoryAvailable),
				"memory_total": fmt.Sprintf("%.2f MB", selfReputation.MemoryAllocationTotal),
				"memory_sys":   fmt.Sprintf("%.2f MB", selfReputation.MemorySystem),
				"disk_read":    fmt.Sprintf("%.2f MB/s", selfReputation.AvgReadMBs),
				"disk_write":   fmt.Sprintf("%.2f MB/s", selfReputation.AvgWriteMBs),
				"latency":      fmt.Sprintf("%.2f ms", selfReputation.Latency),
				"uptime":       fmt.Sprintf("%.2f", selfReputation.Uptime),
			}
		} else {
			selfHealth = map[string]interface{}{
				"score":  "N/A",
				"status": "Initializing",
			}
		}

		// Count coordinators and followers
		coordCount := 0
		followerCount := 0
		for _, peer := range peersList {
			if peer["role"] == "Coordinator" {
				coordCount++
			} else {
				followerCount++
			}
		}
		// Add self to counts
		if role == "Coordinator" {
			coordCount++
		} else {
			followerCount++
		}

		// Build complete response
		response := map[string]interface{}{
			"status": "success",
			"agent": map[string]interface{}{
				"peer_id":           selfPeerID,
				"addresses":         selfAddrs,
				"role":              role,
				"is_coordinator":    isCoordinator,
				"is_current_leader": isCurrentLeader,
				"health":            selfHealth,
				"metrics": map[string]interface{}{
					"leadership_count": leadershipCount,
				},
			},
			"election": map[string]interface{}{
				"current_leader":     currentLeader,
				"current_term":       currentTerm,
				"last_election_time": lastElectionTime,
				"last_election_term": lastElectionTerm,
			},
			"cluster": map[string]interface{}{
				"total_peers":      len(peersList) + 1,
				"connected_peers":  len(connectedPeerIDs) - 1,
				"discovered_peers": len(discoveredPeers),
				"coordinators":     coordCount,
				"followers":        followerCount,
			},
			"peers": peersList,
			"configuration": map[string]interface{}{
				"context":   *config.FlagContext,
				"http_port": *config.FlagHTTPPort,
			},
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		}

		sendJSONResponse(w, response)
	}
}

// ServeHTTP initializes and starts the HTTP server
func ServeHTTP(optimusdb *app.KnowledgeBaseDB, theLog *app.LoggerSQLite, reqChan chan app.Request,
	resChan chan interface{}, logChan chan app.Log) {

	server := http.NewServeMux()

	// middleware to handle CORS headers and preflight requests
	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := r.RemoteAddr
			logChan <- app.Log{app.Info, "Received HTTP request from " + ip}
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "*")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	// Register command handler
	server.Handle("/"+*config.FlagContext+"/command", mw(commandHandler(reqChan, resChan)))

	// Agent status endpoint
	server.Handle("/"+*config.FlagContext+"/agent/status", mw(agentStatusHandler(optimusdb)))

	// TOSCA upload endpoint
	server.Handle("/"+*config.FlagContext+"/upload", mw(uploadTOSCAHandler(optimusdb)))

	// Peers endpoint
	server.Handle("/"+*config.FlagContext+"/peers", mw(peersHandler()))

	// EMS endpoints
	server.Handle("/"+*config.FlagContext+"/ems",
		mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sendSuccessResponse(w, map[string]string{
				"hint": "Try /" + *config.FlagContext + "/ems/logs and /" + *config.FlagContext + "/ems/events",
			})
		})))

	// Logging endpoint
	server.Handle("/"+*config.FlagContext+"/log", mw(LogsHandler(theLog)))

	// EMS logs endpoint with filters
	server.Handle("/"+*config.FlagContext+"/ems/logs",
		mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if app.GlobalLoggerDB == nil {
				sendErrorResponse(w, http.StatusServiceUnavailable, "logger DB not ready")
				return
			}
			q := r.URL.Query()

			// limit (safe clamp)
			limit := 50
			if s := q.Get("limit"); s != "" {
				if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 1000 {
					limit = n
				}
			}
			// level (whitelist)
			level := strings.ToUpper(strings.TrimSpace(q.Get("level")))
			if level != "INFO" && level != "WARN" && level != "ERROR" && level != "DEBUG" {
				level = ""
			}
			// since_min (relative time window)
			sinceMin := 0
			if s := q.Get("since_min"); s != "" {
				if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 24*60 {
					sinceMin = n
				}
			}

			// Build WHERE
			where := `source = 'ems'`
			if level != "" {
				where += fmt.Sprintf(` AND level = '%s'`, level)
			}
			if sinceMin > 0 {
				where += fmt.Sprintf(` AND timestamp >= datetime('now','-%d minutes')`, sinceMin)
			}

			sql := fmt.Sprintf(`
			SELECT id, timestamp, level, source, message
			FROM optimusLogger
			WHERE %s
			ORDER BY id DESC
			LIMIT %d;`, where, limit)

			rows, err := app.GlobalLoggerDB.SelectAll(sql)
			if err != nil {
				sendErrorResponse(w, http.StatusInternalServerError, "query failed: "+err.Error())
				return
			}
			sendJSONResponse(w, map[string]interface{}{"records": rows})
		})))

	// EMS events endpoint
	server.Handle("/"+*config.FlagContext+"/ems/events",
		mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if app.GlobalLoggerDB == nil {
				sendErrorResponse(w, http.StatusServiceUnavailable, "logger DB not ready")
				return
			}
			q := r.URL.Query()
			limit := 50
			if s := q.Get("limit"); s != "" {
				if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 1000 {
					limit = n
				}
			}
			sinceMin := 0
			if s := q.Get("since_min"); s != "" {
				if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 24*60 {
					sinceMin = n
				}
			}

			where := `1=1`
			if sinceMin > 0 {
				where += fmt.Sprintf(` AND received_at >= datetime('now','-%d minutes')`, sinceMin)
			}

			sql := fmt.Sprintf(`
			SELECT id, received_at, client_id, topic, action, resource,
			       substr(params_json,1,240) AS params,
			       substr(raw_json,1,240)    AS raw
			FROM ems_events
			WHERE %s
			ORDER BY id DESC
			LIMIT %d;`, where, limit)

			rows, err := app.GlobalLoggerDB.SelectAll(sql)
			if err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "no such table") {
					sendErrorResponse(w, http.StatusNotFound, "ems_events table not found (enable EMS persistence or redeploy with events table)")
					return
				}
				sendErrorResponse(w, http.StatusInternalServerError, "query failed: "+err.Error())
				return
			}
			sendJSONResponse(w, map[string]interface{}{"records": rows})
		})))

	// EMS SQL endpoint
	server.Handle("/"+*config.FlagContext+"/ems/sql",
		mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if app.GlobalLoggerDB == nil {
				sendErrorResponse(w, http.StatusServiceUnavailable, "logger DB not ready")
				return
			}
			var sql string
			if r.Method == http.MethodGet {
				sql = r.URL.Query().Get("q")
			} else if r.Method == http.MethodPost {
				var body struct {
					SQL string `json:"sql"`
				}
				_ = json.NewDecoder(r.Body).Decode(&body)
				sql = body.SQL
			} else {
				sendErrorResponse(w, http.StatusMethodNotAllowed, "use GET or POST")
				return
			}
			sql = strings.TrimSpace(sql)
			if sql == "" {
				sendErrorResponse(w, http.StatusBadRequest, "missing SQL")
				return
			}
			rows, err := app.GlobalLoggerDB.SelectAll(sql)
			if err != nil {
				sendErrorResponse(w, http.StatusBadRequest, "query failed: "+err.Error())
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"records": rows})
		})))

	// Register inventory endpoint
	server.Handle("/"+*config.FlagContext+"/agent/inventory",
		mw(AgentInventoryHandler(optimusdb, app.GlobalKBSQLite, theLog)))

	// DID Endpoints
	credentials.SetupCredentialsEndpoints(server, mw, *config.FlagContext, optimusdb, theLog)

	// Add metadata routes
	metadataRouter := mux.NewRouter()
	RegisterMetadataRoutes(metadataRouter, optimusdb)
	server.Handle("/api/", mw(metadataRouter))

	// Register benchmarks handler
	if *config.FlagBenchmark {
		server.Handle("/"+*config.FlagContext+"/benchmarks", mw(benchmarksHandler(optimusdb)))
	}

	// Get the local IP address
	ip, err := getLocalIPAddress()
	if err != nil {
		logger.Warn("Failed to determine local IP address: %v", err)
		ip = "unknown"
	}

	logger.Info("Starting HTTP Server on IP %s and port %s", ip, *config.FlagHTTPPort)
	logChan <- app.Log{
		Type: app.Info,
		Data: fmt.Sprintf("Starting HTTP Server on IP %s and port %s", ip, *config.FlagHTTPPort),
	}

	http.ListenAndServe(":"+*config.FlagHTTPPort, server)
}

// GetLocalIPAddress retrieves the first non-loopback IPv4 address
func getLocalIPAddress() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, iface := range interfaces {
		// Skip down or loopback interfaces
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			// Check if the address is IPv4
			if ipNet, ok := addr.(*net.IPNet); ok && ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}

	return "", nil
}

// sendJSONResponse writes a 200 JSON body
func sendJSONResponse(w http.ResponseWriter, payload interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(payload)
}

func EnrichHandler(kb *app.KnowledgeBaseDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req enrichReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if req.MaxRows <= 0 {
			req.MaxRows = 200
		}

		var svc contextualmetadata.Service
		svc.UseGreek = req.Greek

		entry, err := svc.EnrichDataset(r.Context(), kb, req.DB, req.Table, req.MaxRows)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(entry)
	}
}

// RegisterMetadataRoutes registers metadata enrichment endpoints
func RegisterMetadataRoutes(router *mux.Router, kb *app.KnowledgeBaseDB) {
	if kb.MetadataService == nil || kb.MetadataCache == nil {
		logger.Info("Metadata service not initialized, skipping metadata routes")
		return
	}

	metadataHandler := &contextualmetadata.MetadataHandler{
		Service: kb.MetadataService.(*contextualmetadata.Service),
		KB:      kb,
		Cache:   kb.MetadataCache.(*contextualmetadata.MetadataCache),
	}

	chatHandler := &contextualmetadata.ChatHandler{
		KB:      kb,
		Service: kb.MetadataService.(*contextualmetadata.Service),
	}

	// Create API v1 subrouter
	apiV1 := router.PathPrefix("/api/v1").Subrouter()

	// Register metadata endpoints
	apiV1.HandleFunc("/metadata/enrich", metadataHandler.EnrichDataset).Methods("POST")
	apiV1.HandleFunc("/metadata/enrich-batch", metadataHandler.EnrichBatch).Methods("POST")
	apiV1.HandleFunc("/metadata/profile", metadataHandler.ProfileDataset).Methods("GET")
	apiV1.HandleFunc("/metadata/metrics", metadataHandler.GetMetrics).Methods("GET")
	apiV1.HandleFunc("/metadata/health", metadataHandler.HealthCheck).Methods("GET")
	apiV1.HandleFunc("/metadata/cache", metadataHandler.ClearCache).Methods("DELETE")
	apiV1.HandleFunc("/chat", chatHandler.HandleChat).Methods("POST")

	logger.Info("Metadata enrichment endpoints registered at /api/v1/metadata")
	logger.Info("Chat endpoint registered at /api/v1/chat")
}
