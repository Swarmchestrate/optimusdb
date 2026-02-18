package app

import (
	"encoding/json"
	"fmt"
	"github.com/go-stomp/stomp"
	"golang.org/x/net/context"
	"optimusdb/logger"
	"optimusdb/mq"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type EMSSensorMessage struct {
	Destination    string            `json:"destination"`
	Metric         string            `json:"metric"`
	Instance       string            `json:"instance"`
	ProducerHost   string            `json:"producer_host"`
	SourceNode     string            `json:"source_node"`
	SourceEndpoint string            `json:"source_endpoint"`
	NodeID         string            `json:"node_id"`
	Cloud          string            `json:"cloud"`
	Region         string            `json:"region"`
	Zone           string            `json:"zone"`
	PublicIP       string            `json:"public_ip"`
	PrivateIP      string            `json:"private_ip"`
	Timestamp      string            `json:"timestamp"`
	MessageID      string            `json:"message_id"`
	Headers        map[string]string `json:"headers"` // all raw headers preserved
}

// parseSensorHeaders extracts structured fields from STOMP headers.
func parseSensorHeaders(destination string, headers map[string]string) EMSSensorMessage {
	return EMSSensorMessage{
		Destination:    destination,
		Metric:         headers["metric"],
		Instance:       headers["instance"],
		ProducerHost:   headers["producer-host"],
		SourceNode:     headers["source-node"],
		SourceEndpoint: headers["source-endpoint"],
		NodeID:         headers["node-id"],
		Cloud:          headers["cloud"],
		Region:         headers["region"],
		Zone:           headers["zone"],
		PublicIP:       headers["public-ip"],
		PrivateIP:      headers["private-ip"],
		Timestamp:      headers["timestamp"],
		MessageID:      headers["message-id"],
		Headers:        headers,
	}
}

// StartEMSSubscriber starts EMS service with auto-reconnect
// Updated 18.02.2026: Extract STOMP headers and handle SENSOR messages
func (db *KnowledgeBaseDB) StartEMSSubscriber(ctx context.Context) (cleanup func() error, err error) {
	host := os.Getenv("EMS_SERVICE_NAME")
	if host == "" {
		host = "ems-broker.default.svc.cluster.local"
	}
	portStr := os.Getenv("EMS_STOMP_PORT")
	if portStr == "" {
		portStr = "61610"
	}
	stompPort, _ := strconv.Atoi(portStr)

	user := os.Getenv("MQ_USER")
	if user == "" {
		user = "aaa"
	}
	pass := os.Getenv("MQ_PASS")
	if pass == "" {
		pass = "111"
	}
	clientID := os.Getenv("MQ_CLIENT_ID")
	topic := os.Getenv("EMS_TOPIC")
	if topic == "" {
		topic = "/topic/>"
	}

	cfg := mq.Config{
		Host:     host,
		Port:     stompPort,
		User:     user,
		Pass:     pass,
		ClientID: clientID,
		Topic:    topic,
	}

	service := mq.NewEMSService(cfg, 10*time.Second)

	// =========================================================================
	// CHANGED: Extract headers, use actual destination, always call handler
	// OLD:
	//   service.OnMessage(func(dest string, msg *stomp.Message) {
	//       if msg != nil && msg.Body != nil {       // ← SENSOR body is nil → skipped!
	//           _ = db.handleEMSMessage(msg.Body)    // ← headers lost
	//       }
	//   })
	// NEW:
	// =========================================================================
	service.OnMessage(func(dest string, msg *stomp.Message) {
		if msg == nil {
			return
		}

		// Extract actual destination from the message (not the subscription pattern)
		actualDest := dest
		if msg.Destination != "" {
			actualDest = msg.Destination
		}

		// Extract all STOMP headers into a flat map
		headers := extractSTOMPHeaders(msg)

		// Always call handler — even if body is nil (SENSOR messages)
		_ = db.handleEMSMessageFull(actualDest, headers, msg.Body)
	})

	service.OnConnected(func() {
		logger.Info("[INFO] EMS connected (host=%s port=%d topic=%s)", cfg.Host, cfg.Port, cfg.Topic)
	})
	service.OnDisconnected(func(err error) {
		logger.Error("[ERROR] EMS disconnected: %v", err)
	})

	db.EMSService = service
	service.Start()

	return func() error {
		service.Stop()
		db.EMSService = nil
		return nil
	}, nil
}

func extractSTOMPHeaders(msg *stomp.Message) map[string]string {
	headers := make(map[string]string)
	if msg == nil || msg.Header == nil {
		return headers
	}

	// stomp.Header has Len() and GetAt(index) methods
	for i := 0; i < msg.Header.Len(); i++ {
		key, val := msg.Header.GetAt(i)
		headers[key] = val
	}

	return headers
}

// ─────────────────────────────────────────────────────────────────────────────
// handleEMSMessageFull processes EMS messages with full header support.
//
// Three message types:
//  1. Body-based JSON   — standard OptimusDB {action, resource, params}
//  2. Header-based SENSOR — SwarmChestrate monitoring (body empty, data in headers)
//  3. Empty unknown      — logged as warning
//
// All messages are persisted to ems_events (including headers_json).
// Added 18.02.2026
// ─────────────────────────────────────────────────────────────────────────────
func (db *KnowledgeBaseDB) handleEMSMessageFull(destination string, headers map[string]string, body []byte) error {
	now := time.Now().UTC()
	clientID := os.Getenv("MQ_CLIENT_ID")

	// Marshal headers to JSON for storage
	headersJSON := ""
	if len(headers) > 0 {
		if b, err := json.Marshal(headers); err == nil {
			headersJSON = string(b)
		}
	}

	// Determine if this is a SENSOR message (header-based, empty body)
	hasBody := len(body) > 0 && len(strings.TrimSpace(string(body))) > 0
	isSensor := isSensorMessage(destination, headers)

	// =========================================================================
	// PATH A: Body-based message (existing JSON format)
	// =========================================================================
	if hasBody {
		raw := string(body)
		var m EMSMessage
		parseErr := json.Unmarshal(body, &m)

		if parseErr != nil {
			// Try Java-style normalization
			normalized := normalizeEMSMessage(raw)
			if normalized != "" {
				if err := json.Unmarshal([]byte(normalized), &m); err == nil {
					parseErr = nil
				}
			}
		}

		// Persist to ems_events (with headers_json)
		if GlobalLoggerDB != nil {
			paramsJSON := ""
			if parseErr == nil && m.Params != nil {
				if b, err := json.Marshal(m.Params); err == nil {
					paramsJSON = string(b)
				}
			}
			_ = GlobalLoggerDB.InsertEMSEvent(
				now, db.HostID, clientID, destination,
				m.Action, m.Resource, paramsJSON, raw, headersJSON,
			)

			if parseErr != nil {
				logger.Error("[ERROR] EMS recv (unmarshal failed): dest=%s body=%s",
					destination, truncate(raw, 180))
			} else {
				logger.Info("EMS recv action=%s resource=%s dest=%s",
					m.Action, m.Resource, destination)
			}
		}

		if parseErr != nil {
			return parseErr
		}
		return db.ProcessEMS(m.Action, m.Resource, m.Params)
	}

	// =========================================================================
	// PATH B: SENSOR message (header-based, empty body)
	// =========================================================================
	if isSensor {
		sensor := parseSensorHeaders(destination, headers)

		if GlobalLoggerDB != nil {
			_ = GlobalLoggerDB.InsertEMSEvent(
				now, db.HostID, clientID, destination,
				"SENSOR", sensor.Metric, "", "", headersJSON,
			)
			logger.Info("EMS SENSOR recv metric=%s instance=%s dest=%s producer=%s",
				sensor.Metric, sensor.Instance, destination, sensor.ProducerHost)
		}

		return db.ProcessEMSSensor(sensor)
	}

	// =========================================================================
	// PATH C: Empty message, not a sensor → log warning
	// =========================================================================
	if GlobalLoggerDB != nil {
		_ = GlobalLoggerDB.InsertEMSEvent(
			now, db.HostID, clientID, destination,
			"UNKNOWN", "", "", "", headersJSON,
		)
		logger.Warn("[WARN] EMS recv empty non-sensor message on dest=%s headers=%d",
			destination, len(headers))
	}

	return nil
}

// handleEMSMessage is the legacy body-only handler.
// Kept for backward compatibility with the old mq.Client callback (SubscribeJSON).
// New code uses handleEMSMessageFull via StartEMSSubscriber.
func (db *KnowledgeBaseDB) handleEMSMessage(body []byte) error {
	topic := getenvDefault("EMS_TOPIC", "/topic/>")
	return db.handleEMSMessageFull(topic, nil, body)
}

func isSensorMessage(destination string, headers map[string]string) bool {
	if strings.Contains(strings.ToUpper(destination), "SENSOR") {
		return true
	}
	if _, ok := headers["metric"]; ok {
		return true
	}
	if _, ok := headers["source-endpoint"]; ok {
		// Has source-endpoint but no body → likely a monitoring message
		return true
	}
	return false
}

// tiny helper used above
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 3 {
		return s[:n]
	}
	return s[:n-3] + "..."
}
func getenvDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func getenvIntDefault(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
func getenvBoolDefault(k string, def bool) bool {
	switch strings.ToLower(os.Getenv(k)) {
	case "1", "true", "yes", "y":
		return true
	case "0", "false", "no", "n":
		return false
	default:
		return def
	}
}
func (db *KnowledgeBaseDB) EMSSend(dest, contentType string, body []byte) error {
	if db.EMSService == nil {
		return fmt.Errorf("EMS service not initialized")
	}
	return db.EMSService.Send(dest, contentType, body)
}

func normalizeEMSMessage(s string) string {
	out := ""
	inQuotes := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' {
			inQuotes = !inQuotes
		}
		if !inQuotes && c == '=' {
			out += ":"
		} else {
			out += string(c)
		}
	}
	out = strings.ReplaceAll(out, "'", "\"")
	re := regexp.MustCompile(`([,{]\s*)([A-Za-z0-9_]+)(\s*:)`)
	out = re.ReplaceAllString(out, `$1"$2"$3`)
	return out
}
