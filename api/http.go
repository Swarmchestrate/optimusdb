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
	"log"
	"net"
	"net/http"
	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/contextualmetadata"
	"optimusdb/credentials"
	"optimusdb/election"
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

/*
// logsHandler handles GET /<context>/log?date=YYYY-MM-DD&hour=HH
*/
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

/*
*  dedicated HTTP handler for TOSCA uploads
 */
func uploadTOSCAHandler(optimusdb *app.KnowledgeBaseDB) http.HandlerFunc {
	type UploadRequest struct {
		File     string `json:"file"`               // Base64-encoded TOSCA YAML
		Filename string `json:"filename,omitempty"` // optional
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

		// 2) Parse TOSCA
		tmpl, err := tosca.ParseTOSCA(decoded)
		if err != nil {
			sendErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("TOSCA parse error: %v", err))
			return
		}

		// 3) Compute IDs / counts
		templateID := tosca.ComputeTemplateID(decoded)
		nodeCount := tosca.CountNodeTemplates(tmpl)
		description := tmpl.Description

		// 4) Persist ORIGINAL YAML to OrbitDB (e.g., DsTOSCA_Imported)
		if optimusdb.DsTOSCA_Imported == nil {
			sendErrorResponse(w, http.StatusInternalServerError, "TOSCA store not initialized")
			return
		}
		ctx := r.Context()
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

		// 5) Index lightweight metadata into SQLite
		if app.GlobalKBSQLite == nil {
			sendErrorResponse(w, http.StatusInternalServerError, "SQLite not initialized")
			return
		}

		// (B) Also add the raw YAML blob to IPFS to get a stable content path (ipfsPath)
		var ipfsPath string
		if optimusdb.Orbit != nil {
			coreAPI := (*optimusdb.Orbit).IPFS()
			nd := files.NewBytesFile(decoded)
			p, err := coreAPI.Unixfs().Add(ctx, nd)
			if err == nil {
				ipfsPath = p.String() // e.g., /ipfs/<CID>
			} // if it fails, we'll just leave ipfsPath empty
		}

		filename := req.Filename
		if filename == "" {
			filename = "unknown"
		}
		filesize := int64(len(decoded))

		// compute sha256
		sum := sha256.Sum256(decoded)
		sha := fmt.Sprintf("%x", sum[:])

		uploader := r.Header.Get("X-User") // optional: caller can set this
		if uploader == "" {
			uploader = app.GetAgentName() // fallback to your agent name
		}
		sourcePod := os.Getenv("POD_NAME") // set by Kubernetes downward API in your manifest
		sourceIP, _ := getLocalIPAddress() // your helper already exists

		// (D) Insert into SQLite with contextual metadata generation
		if err := app.GlobalKBSQLite.InsertTOSCAMetadata(
			templateID,
			description,
			nodeCount,
			filename,
			filesize,
			sha,
			ipfsPath,
			uploader,
			sourcePod,
			sourceIP,
		); err != nil {
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to index metadata: %v", err))
			return
		}

		// 6) Respond
		sendSuccessResponse(w, map[string]interface{}{
			"message":     "TOSCA uploaded successfully",
			"template_id": templateID,
			"node_count":  nodeCount,
			"filename":    filename,
			"filesize":    filesize,
			"sha256":      sha,
		})
	}
}

/*
func uploadTOSCAHandler(optimusdb *app.KnowledgeBaseDB) http.HandlerFunc {
	type UploadRequest struct {
		File     string `json:"file"`               // Base64-encoded TOSCA YAML
		Filename string `json:"filename,omitempty"` // optional
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

		// 2) Parse TOSCA
		tmpl, err := tosca.ParseTOSCA(decoded)
		if err != nil {
			sendErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("TOSCA parse error: %v", err))
			return
		}

		// 3) Compute IDs / counts
		templateID := tosca.ComputeTemplateID(decoded)
		nodeCount := tosca.CountNodeTemplates(tmpl)
		description := tmpl.Description

		// 4) Persist ORIGINAL YAML to OrbitDB (e.g., DsTOSCA_Imported)
		if optimusdb.DsTOSCA_Imported == nil {
			sendErrorResponse(w, http.StatusInternalServerError, "TOSCA store not initialized")
			return
		}
		ctx := r.Context()
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

		// 5) Index lightweight metadata into SQLite
		if app.GlobalKBSQLite == nil {
			sendErrorResponse(w, http.StatusInternalServerError, "SQLite not initialized")
			return
		}

		// (B) Also add the raw YAML blob to IPFS to get a stable content path (ipfsPath)
		var ipfsPath string
		if optimusdb.Orbit != nil {
			coreAPI := (*optimusdb.Orbit).IPFS()
			nd := files.NewBytesFile(decoded)
			p, err := coreAPI.Unixfs().Add(ctx, nd)
			if err == nil {
				ipfsPath = p.String() // e.g., /ipfs/<CID>
			} // if it fails, we'll just leave ipfsPath empty
		}

		filename := req.Filename
		if filename == "" {
			filename = "unknown"
		}
		filesize := int64(len(decoded))

		// compute sha256
		sum := sha256.Sum256(decoded)
		sha := fmt.Sprintf("%x", sum[:])

		uploader := r.Header.Get("X-User") // optional: caller can set this
		if uploader == "" {
			uploader = app.GetAgentName() // fallback to your agent name
		}
		sourcePod := os.Getenv("POD_NAME") // set by Kubernetes downward API in your manifest
		sourceIP, _ := getLocalIPAddress() // your helper already exists

		// (D) Insert into SQLite with the new signature
		if err := app.GlobalKBSQLite.InsertTOSCAMetadata(
			templateID,
			description,
			nodeCount,
			filename,
			filesize,
			sha,
			ipfsPath,
			uploader,
			sourcePod,
			sourceIP,
		); err != nil {
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to index metadata: %v", err))
			return
		}

		// 6) Respond
		sendSuccessResponse(w, map[string]interface{}{
			"message":     "TOSCA uploaded successfully",
			"template_id": templateID,
			"node_count":  nodeCount,
			"filename":    filename,
			"filesize":    filesize,
			"sha256":      sha,
		})
	}
}

*/

/*
func uploadTOSCAHandler(optimusdb *app.KnowledgeBaseDB) http.HandlerFunc {
	type UploadRequest struct {
		File string `json:"file"` // Base64-encoded TOSCA YAML
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST is allowed")
			return
		}

		var req UploadRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil || req.File == "" {
			sendErrorResponse(w, http.StatusBadRequest, "Invalid JSON payload")
			return
		}

		// Decode base64
		//decoded, err := base64.StdEncoding.DecodeString(req.File)
		_, err = base64.StdEncoding.DecodeString(req.File)
		if err != nil {
			sendErrorResponse(w, http.StatusBadRequest, "Base64 decoding failed")
			return
		}

		// Process TOSCA (parser + OrbitDB + SQLite)
		//err = HandleTOSCAUpload(decoded, optimusdb)
		//err = tosca.HandleTOSCAUpload(decoded, optimusdb)
		//if err != nil {
		//	sendErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("TOSCA upload failed: %v", err))
		//	return
		//}

		sendSuccessResponse(w, map[string]string{"message": "TOSCA uploaded successfully"})
	}
}
*/

// gathers all benchmark data from known peers
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
				// TODO : log the error using logChan
				fmt.Print(err)
				continue
			}

			bm, err := getBenchmark(client, ip)
			if err != nil {
				// TODO : log the error ?
				fmt.Print(err)
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

/*
*
 */
func getBenchmark(client *http.Client, peerIP string) (app.Benchmark, error) {
	var bm app.Benchmark

	bmReq := app.Request{Method: app.BENCHMARK, Args: []string{}}
	jsonData, err := json.Marshal(bmReq)
	if err != nil {
		return bm, err
	}

	// send get benchmark request
	//cmdPath := "http://" + peerIP + ":8089/optimusdb/command"
	cmdPath := "http://" + peerIP + ":" + *config.FlagHTTPPort + "/" + *config.FlagContext + "/command"
	req, err := http.NewRequest("POST", cmdPath, bytes.NewBuffer(jsonData))
	if err != nil {
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
		return bm, err
	}

	// unmarshal response
	err = json.Unmarshal(body, &bm)
	if err != nil {
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

/*
*
This is the context handler for the REQ/RSP of the Http payload
*/
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

	fmt.Println("[DEBUG] Inside command Handler formation")

	return func(w http.ResponseWriter, r *http.Request) {

		fmt.Println("[DEBUG] Inside command Handler ~ Response Writer")
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

/*
*	sendErrorResponse
 */
func sendErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		//"status":  "error",
		"status":  http.Error,
		"message": message,
	})
}

/*
*	sendSuccessResponse
 */
func sendSuccessResponse(w http.ResponseWriter, data interface{}) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		//"status": "success",
		"status": http.StatusOK,
		"data":   data,
	})
}

// agentStatusHandler returns comprehensive agent status including role and peer health
// Add this function to api/http.go (before ServeHTTP function)
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
		isCoordinator := *config.FlagCoordinator
		isCurrentLeader := (role == "Leader" || role == "leader")

		// Get self reputation/health metrics
		selfReputation, _ := election.GetPeerReputation(selfPeerID)
		var selfHealthScore float64 = 0
		if selfReputation != nil {
			selfHealthScore = election.CalculateHealthScore(*selfReputation)
		}

		// Get all peers reputation
		allReputations, err := election.GetAllPeersReputation()
		if err != nil {
			log.Printf("[ERROR] Failed to get peer reputations: %v", err)
			allReputations = []election.NodeReputation{}
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

		for _, rep := range allReputations {
			// Skip self
			if rep.NodeID == selfPeerID {
				continue
			}

			// Determine peer role
			peerRole := "Follower"
			if rep.NodeID == currentLeader {
				peerRole = "Coordinator"
			}

			// Calculate health score
			healthScore := election.CalculateHealthScore(rep)

			// Determine connection status
			isConnected := connectedPeerIDs[rep.NodeID]

			// Health status based on score
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

			peerInfo := map[string]interface{}{
				"peer_id":   rep.NodeID,
				"role":      peerRole,
				"is_leader": rep.NodeID == currentLeader,
				"connected": isConnected,
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

			peersList = append(peersList, peerInfo)
		}

		// Get latest election info
		//lastElectionLeader, lastElectionTerm, lastElectionTime, _ := election.GetLatestElectionInfo()
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
				"total_peers":      len(peersList),
				"connected_peers":  len(connectedPeerIDs) - 1, // -1 to exclude self
				"discovered_peers": len(discoveredPeers),
				"coordinators":     1, // Always 1 coordinator in the cluster
				"followers":        len(peersList),
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

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ServeHTTP
// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func ServeHTTP(optimusdb *app.KnowledgeBaseDB, theLog *app.LoggerSQLite, reqChan chan app.Request,
	resChan chan interface{}, logChan chan app.Log) {

	server := http.NewServeMux()

	// middleware to handle CORS headers and preflight requests
	mw := func(next http.Handler) http.Handler {
		/////
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := r.RemoteAddr
			logChan <- app.Log{app.Info, "Received HTTP request from " + ip}
			w.Header().Set("Access-Control-Allow-Origin", "*")
			//w.Header().Set("Access-Control-Allow-Methods", "POST")
			w.Header().Set("Access-Control-Allow-Methods", "*")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	// register command handler which allows to run commands similar to the shell
	//fmt.Println("config.FlagContext is:", *config.FlagContext)

	/**
	simple HTTP endpoint all command related to data stores
	*/
	server.Handle("/"+*config.FlagContext+"/command", mw(commandHandler(reqChan, resChan)))

	/**
	Agent status endpoint - shows coordinator/follower status and peer health
	*/
	server.Handle("/"+*config.FlagContext+"/agent/status", mw(agentStatusHandler(optimusdb)))

	/**
		/upload-tosca endpoint
		curl -X POST http://localhost:8089/optimusdb/upload-tosca \
	  -H "Content-Type: application/json" \
	  -d '{"file": "'$(base64 -w 0 mytosca.yaml)'"}'

	*/
	server.Handle("/"+*config.FlagContext+"/upload", mw(uploadTOSCAHandler(optimusdb)))

	/**
	simple HTTP endpoint to fetch the list of connected peers
	*/
	server.Handle("/"+*config.FlagContext+"/peers", mw(peersHandler()))

	/**
	simple HTTP endpoint to fetch the communication with EMS
	*/
	//server.Handle("/"+*config.FlagContext+"/ems", mw(peersHandler()))
	server.Handle("/"+*config.FlagContext+"/ems",
		mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sendSuccessResponse(w, map[string]string{
				"hint": "Try /" + *config.FlagContext + "/ems/logs and /" + *config.FlagContext + "/ems/events",
			})
		})))

	/**
	simple HTTP endpoint to fetch the communication with Logging of OptimusDB
	*/
	server.Handle("/"+*config.FlagContext+"/log", mw(LogsHandler(theLog)))

	/**
	Added as context to be retrieved
	*/
	// GET /<context>/ems/logs?limit=50&level=ERROR&since_min=60
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

	// GET /<context>/ems/events?limit=50&since_min=60
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

			// Note: ems_events lives in the logger DB
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
				// If table doesn’t exist yet, return a friendly message
				if strings.Contains(strings.ToLower(err.Error()), "no such table") {
					sendErrorResponse(w, http.StatusNotFound, "ems_events table not found (enable EMS persistence or redeploy with events table)")
					return
				}
				sendErrorResponse(w, http.StatusInternalServerError, "query failed: "+err.Error())
				return
			}
			sendJSONResponse(w, map[string]interface{}{"records": rows})
		})))

	// GET /<context>/ems/sql?q=<URL-encoded SQL>
	// or POST with {"sql": "..."} to run against the logger DB (ems_events lives here)
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

	// DID Endpoints
	credentials.SetupCredentialsEndpoints(server, mw, *config.FlagContext, optimusdb, theLog)

	// Add metadata routes

	metadataRouter := mux.NewRouter()
	RegisterMetadataRoutes(metadataRouter, optimusdb)
	// Mount the metadata router at /api/ prefix
	server.Handle("/api/", mw(metadataRouter))

	/////////
	// register benchmarks handler which is specific for this API because it's
	// used to gather all peers data
	if *config.FlagBenchmark {
		server.Handle("/"+*config.FlagContext+"/benchmarks", mw(benchmarksHandler(optimusdb)))
	}

	// start the HTTP server
	// Get the local IP address of the server
	ip, err := getLocalIPAddress()
	if err != nil {
		logChan <- app.Log{app.Info, "\nFailed to determine local IP address: " + err.Error()}
		ip = "unknown"
	}
	//logChan <- app.Log{app.Info, "Starting HTTP Server in port: " + " " + *config.FlagHTTPPort}
	//logChan <- app.Log{app.Info, fmt.Sprintf("Starting HTTP Server on IP %s and port %s", ip, *config.FlagHTTPPort)}

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

// sendJSONResponse writes a 200 JSON body.
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

		// Choose client by build tag
		var svc contextualmetadata.Service
		svc.UseGreek = req.Greek
		// HTTP mode:

		/*
			if c, err := contextualmetadata.NewTinyLlamaHTTP(); err == nil {
				svc.Client = c
			} else if c2, err2 := contextualmetadata.NewTinyLlamaLocal(); err2 == nil {
				svc.Client = c2
			} else {
				http.Error(w, "No TinyLlama client configured", http.StatusInternalServerError)
				return
			}

		*/

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
		log.Println("[API] Metadata service not initialized, skipping metadata routes")
		return
	}

	metadataHandler := &contextualmetadata.MetadataHandler{
		Service: kb.MetadataService.(*contextualmetadata.Service),
		KB:      kb,
		Cache:   kb.MetadataCache.(*contextualmetadata.MetadataCache),
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

	log.Println("[API] ✅ Metadata enrichment endpoints registered")
}
