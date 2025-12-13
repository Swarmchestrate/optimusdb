package contextualmetadata

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/logger"

	_ "github.com/mattn/go-sqlite3"
)

// ChatRequest represents the incoming chat message
type ChatRequest struct {
	Message             string                   `json:"message"`
	ConversationHistory []map[string]interface{} `json:"conversation_history"`
	Language            string                   `json:"language"`
}

// ChatResponse represents the outgoing response
type ChatResponse struct {
	Response    string                   `json:"response"`
	Datasets    []map[string]interface{} `json:"datasets"`
	Suggestions []string                 `json:"suggestions"`
}

// ChatHandler handles chat requests
type ChatHandler struct {
	KB      *app.KnowledgeBaseDB
	Service *Service
}

// getDBPath constructs the database path
// This follows the same pattern as in app/initPeer.go line 373:
// rdbmsCache := filepath.Join(cache, *config.FlagRDBMSDB+".db")
func (h *ChatHandler) getDBPath() string {
	// Get home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "."
	}

	// Construct cache directory path
	cacheDir := filepath.Join(homeDir, ".cache")

	// Build the full path following initPeer.go pattern:
	// filepath.Join(cacheDir, "optimusdb", *config.FlagRepo, "orbitdb", *config.FlagRDBMSDB+".db")
	dbPath := filepath.Join(cacheDir, "optimusdb", *config.FlagRepo, "orbitdb", *config.FlagRDBMSDB+".db")

	logger.Info("[CHAT] Using database path: %s", dbPath)
	return dbPath
}

// ParseIntent determines what the user is asking for
func (h *ChatHandler) ParseIntent(message string) (intent string, query string) {
	msgLower := strings.ToLower(message)

	// Greeting
	if regexp.MustCompile(`\b(hello|hi|hey|greetings)\b`).MatchString(msgLower) {
		return "greeting", ""
	}

	// Help
	if regexp.MustCompile(`\b(help|what can you)\b`).MatchString(msgLower) {
		return "help", ""
	}

	// Search by owner
	if strings.Contains(msgLower, "owner") || strings.Contains(msgLower, "owned by") {
		re := regexp.MustCompile(`(?:owned by|owner)\s+([a-z_\s]+?)(?:\s|$)`)
		if match := re.FindStringSubmatch(msgLower); len(match) > 1 {
			return "search_by_owner", strings.TrimSpace(match[1])
		}
	}

	// Search by tag
	if strings.Contains(msgLower, "tag") {
		re := regexp.MustCompile(`tag(?:ged)?\s+(?:with\s+)?["']?([a-z0-9_-]+)["']?`)
		if match := re.FindStringSubmatch(msgLower); len(match) > 1 {
			return "search_by_tag", match[1]
		}
	}

	// Table details
	if regexp.MustCompile(`\b(describe|what is|details|columns)\b`).MatchString(msgLower) {
		re := regexp.MustCompile(`\b([a-z_][a-z0-9_]*)\s*table\b`)
		if match := re.FindStringSubmatch(msgLower); len(match) > 1 {
			return "table_details", match[1]
		}
		return "table_details", h.extractKeywords(message)
	}

	// Search tables
	if regexp.MustCompile(`\b(show|find|search|list)\b`).MatchString(msgLower) {
		return "search_tables", h.extractKeywords(message)
	}

	return "search_tables", message
}

// extractKeywords extracts search keywords from message
func (h *ChatHandler) extractKeywords(message string) string {
	stopWords := map[string]bool{
		"show": true, "me": true, "find": true, "search": true,
		"list": true, "the": true, "a": true, "all": true,
	}

	words := strings.Fields(strings.ToLower(message))
	keywords := make([]string, 0)

	for _, word := range words {
		if !stopWords[word] && len(word) > 2 {
			keywords = append(keywords, word)
		}
	}

	if len(keywords) > 0 {
		return strings.Join(keywords, " ")
	}
	return message
}

// SearchTables searches for tables matching query
func (h *ChatHandler) SearchTables(query string) ([]map[string]interface{}, error) {
	dbPath := h.getDBPath()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database at %s: %w", dbPath, err)
	}
	defer db.Close()

	// Use the table name from config
	tableName := *config.FlagRDBMSTable1

	sqlQuery := fmt.Sprintf(`
		SELECT name, description, database, cluster, schema, tags, owner
		FROM %s
		WHERE name LIKE ? OR description LIKE ? OR tags LIKE ?
		ORDER BY last_updated_timestamp DESC
		LIMIT 20
	`, tableName)

	pattern := "%" + query + "%"
	rows, err := db.Query(sqlQuery, pattern, pattern, pattern)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	results := make([]map[string]interface{}, 0)
	for rows.Next() {
		var name, description, database, cluster, schema, tags, owner sql.NullString

		if err := rows.Scan(&name, &description, &database, &cluster, &schema, &tags, &owner); err != nil {
			continue
		}

		result := map[string]interface{}{
			"name":        name.String,
			"description": description.String,
			"database":    database.String,
			"cluster":     cluster.String,
			"schema":      schema.String,
			"tags":        tags.String,
			"owner":       owner.String,
		}
		results = append(results, result)
	}

	return results, nil
}

// GetTableDetails gets details for a specific table
func (h *ChatHandler) GetTableDetails(tableName string) (map[string]interface{}, error) {
	dbPath := h.getDBPath()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	catalogTable := *config.FlagRDBMSTable1

	sqlQuery := fmt.Sprintf(`SELECT name, description, database, cluster, schema, tags, owner 
	             FROM %s 
	             WHERE name = ? OR name LIKE ? 
	             LIMIT 1`, catalogTable)

	row := db.QueryRow(sqlQuery, tableName, "%"+tableName+"%")

	var name, description, database, cluster, schema, tags, owner sql.NullString
	err = row.Scan(&name, &description, &database, &cluster, &schema, &tags, &owner)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	result := map[string]interface{}{
		"name":        name.String,
		"description": description.String,
		"database":    database.String,
		"cluster":     cluster.String,
		"schema":      schema.String,
		"tags":        tags.String,
		"owner":       owner.String,
	}

	return result, nil
}

// SearchByOwner finds tables by owner
func (h *ChatHandler) SearchByOwner(owner string) ([]map[string]interface{}, error) {
	dbPath := h.getDBPath()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	tableName := *config.FlagRDBMSTable1

	sqlQuery := fmt.Sprintf(`
		SELECT name, description, database, owner
		FROM %s
		WHERE owner LIKE ?
		LIMIT 20
	`, tableName)

	pattern := "%" + owner + "%"
	rows, err := db.Query(sqlQuery, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]map[string]interface{}, 0)
	for rows.Next() {
		var name, description, database, owner sql.NullString

		if err := rows.Scan(&name, &description, &database, &owner); err != nil {
			continue
		}

		result := map[string]interface{}{
			"name":        name.String,
			"description": description.String,
			"database":    database.String,
			"owner":       owner.String,
		}
		results = append(results, result)
	}

	return results, nil
}

// SearchByTag finds tables by tag
func (h *ChatHandler) SearchByTag(tag string) ([]map[string]interface{}, error) {
	dbPath := h.getDBPath()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	tableName := *config.FlagRDBMSTable1

	sqlQuery := fmt.Sprintf(`
		SELECT name, description, tags, owner
		FROM %s
		WHERE tags LIKE ?
		LIMIT 20
	`, tableName)

	pattern := "%" + tag + "%"
	rows, err := db.Query(sqlQuery, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]map[string]interface{}, 0)
	for rows.Next() {
		var name, description, tags, owner sql.NullString

		if err := rows.Scan(&name, &description, &tags, &owner); err != nil {
			continue
		}

		result := map[string]interface{}{
			"name":        name.String,
			"description": description.String,
			"tags":        tags.String,
			"owner":       owner.String,
		}
		results = append(results, result)
	}

	return results, nil
}

// GenerateResponse generates natural language response
func (h *ChatHandler) GenerateResponse(intent string, data interface{}, query string) string {
	switch intent {
	case "greeting":
		return "👋 Hello! I'm your OptimusDB assistant. Ask me about tables, owners, or tags!"

	case "help":
		return `I can help you with:
• "Show me customer tables"
• "Find tables owned by data team"
• "What's in the sales_fact table?"
• "Show tables tagged with finance"

What would you like to know?`

	case "search_tables":
		datasets, ok := data.([]map[string]interface{})
		if !ok || len(datasets) == 0 {
			return fmt.Sprintf("I couldn't find any tables matching '%s'.", query)
		}

		response := fmt.Sprintf("I found **%d table(s)**:\n\n", len(datasets))
		for i, table := range datasets {
			if i >= 5 {
				break
			}
			desc := ""
			if d, ok := table["description"].(string); ok {
				desc = d
				if len(desc) > 80 {
					desc = desc[:80] + "..."
				}
			}
			if desc == "" {
				desc = "No description"
			}
			response += fmt.Sprintf("%d. **%s** - %s\n", i+1, table["name"], desc)
		}

		if len(datasets) > 5 {
			response += fmt.Sprintf("\n...and %d more.", len(datasets)-5)
		}
		return response

	case "search_by_owner":
		datasets, ok := data.([]map[string]interface{})
		if !ok || len(datasets) == 0 {
			return fmt.Sprintf("I couldn't find any tables owned by '%s'.", query)
		}

		response := fmt.Sprintf("I found **%d table(s)** owned by **%s**:\n\n", len(datasets), query)
		for i, table := range datasets {
			if i >= 8 {
				break
			}
			response += fmt.Sprintf("%d. **%s** (%s)\n", i+1, table["name"], table["database"])
		}
		return response

	case "search_by_tag":
		datasets, ok := data.([]map[string]interface{})
		if !ok || len(datasets) == 0 {
			return fmt.Sprintf("I couldn't find any tables with tag '%s'.", query)
		}

		response := fmt.Sprintf("I found **%d table(s)** tagged with **'%s'**:\n\n", len(datasets), query)
		for i, table := range datasets {
			if i >= 8 {
				break
			}
			response += fmt.Sprintf("%d. **%s**\n", i+1, table["name"])
		}
		return response

	case "table_details":
		table, ok := data.(map[string]interface{})
		if !ok || table == nil {
			return fmt.Sprintf("I couldn't find a table named '%s'.", query)
		}

		response := fmt.Sprintf("# %s\n\n", table["name"])
		if desc, ok := table["description"].(string); ok && desc != "" {
			response += fmt.Sprintf("**Description:** %s\n\n", desc)
		}
		response += fmt.Sprintf("**Location:** %s.%s\n", table["database"], table["schema"])
		response += fmt.Sprintf("**Owner:** %s\n", table["owner"])
		if tags, ok := table["tags"].(string); ok && tags != "" {
			response += fmt.Sprintf("**Tags:** %s\n", tags)
		}
		return response
	}

	return "I'm here to help! Try asking about tables, owners, or tags."
}

// GetSuggestions generates follow-up suggestions
func (h *ChatHandler) GetSuggestions(intent string) []string {
	suggestions := map[string][]string{
		"search_tables":   {"Show me details", "Who owns these?", "What tags?"},
		"table_details":   {"Show similar tables", "Find by owner", "Search by tag"},
		"help":            {"Show all tables", "Find by owner", "Search by tag"},
		"search_by_owner": {"Show details", "Search by tag"},
		"search_by_tag":   {"Show details", "Find by owner"},
		"greeting":        {"Show all tables", "Help"},
	}

	if sugg, ok := suggestions[intent]; ok {
		return sugg
	}
	return []string{"What else can you help with?"}
}

// HandleChat is the HTTP handler for chat requests
func (h *ChatHandler) HandleChat(w http.ResponseWriter, r *http.Request) {
	var req ChatRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	logger.Info("[CHAT] Received message: %s", req.Message)

	// Parse intent
	intent, query := h.ParseIntent(req.Message)
	logger.Info("[CHAT] Intent: %s, Query: %s", intent, query)

	// Execute appropriate action
	var datasets []map[string]interface{}
	var responseText string

	switch intent {
	case "search_tables":
		var err error
		datasets, err = h.SearchTables(query)
		if err != nil {
			logger.Error("[CHAT] Error searching tables: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		responseText = h.GenerateResponse(intent, datasets, query)

	case "search_by_owner":
		var err error
		datasets, err = h.SearchByOwner(query)
		if err != nil {
			logger.Error("[CHAT] Error searching by owner: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		responseText = h.GenerateResponse(intent, datasets, query)

	case "search_by_tag":
		var err error
		datasets, err = h.SearchByTag(query)
		if err != nil {
			logger.Error("[CHAT] Error searching by tag: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		responseText = h.GenerateResponse(intent, datasets, query)

	case "table_details":
		table, err := h.GetTableDetails(query)
		if err != nil {
			logger.Error("[CHAT] Error getting table details: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		responseText = h.GenerateResponse(intent, table, query)
		if table != nil {
			datasets = []map[string]interface{}{table}
		}

	case "help", "greeting":
		responseText = h.GenerateResponse(intent, nil, "")

	default:
		var err error
		datasets, err = h.SearchTables(req.Message)
		if err != nil {
			logger.Error("[CHAT] Error in default search: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		responseText = h.GenerateResponse("search_tables", datasets, req.Message)
	}

	// Generate suggestions
	suggestions := h.GetSuggestions(intent)

	// Create response
	response := ChatResponse{
		Response:    responseText,
		Datasets:    datasets,
		Suggestions: suggestions,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Error("[CHAT] Error encoding response: %v", err)
	}

	logger.Info("[CHAT] Response sent successfully")
}
