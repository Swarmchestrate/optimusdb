package api

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"optimusdb/app"
	"optimusdb/logger"
	"os"
	"strings"
)

// ShellConfig holds shell configuration
type ShellConfig struct {
	HistorySize int
	Prompt      string
	EnableColor bool
}

// CommandHistory tracks command history
type CommandHistory struct {
	commands []string
	maxSize  int
	index    int
}

// NewCommandHistory creates a new command history tracker
func NewCommandHistory(maxSize int) *CommandHistory {
	return &CommandHistory{
		commands: make([]string, 0, maxSize),
		maxSize:  maxSize,
		index:    0,
	}
}

// Add adds a command to history
func (h *CommandHistory) Add(cmd string) {
	if cmd == "" {
		return
	}

	// Avoid duplicates of the last command
	if len(h.commands) > 0 && h.commands[len(h.commands)-1] == cmd {
		return
	}

	h.commands = append(h.commands, cmd)
	if len(h.commands) > h.maxSize {
		h.commands = h.commands[1:]
	}
	h.index = len(h.commands)
}

// GetHistory returns all commands in history
func (h *CommandHistory) GetHistory() []string {
	return h.commands
}

// processReq checks whether a string list matches a method definition and forwards the
// resulting request
func processReq(cmdList []string, method app.Method,
	reqChan chan app.Request,
	resChan chan interface{},
	logChan chan app.Log) {

	if len(cmdList) != method.ArgCnt+1 {
		logger.Error("Invalid argument count for %s: expected %d, got %d", method.Cmd, method.ArgCnt, len(cmdList)-1)
		logChan <- app.Log{
			Type: app.RecoverableErr,
			Data: errors.New("double check the given args")}
		return
	}

	logger.Debug("Processing command: %s with %d arguments", method.Cmd, len(cmdList)-1)

	// send request
	reqChan <- app.Request{Method: method, Args: cmdList[1:]}

	// await response and log it
	res := <-resChan
	logChan <- app.Log{Type: app.Print, Data: res}
	logChan <- app.Log{Type: app.Print, Data: "\n"}

	logger.Debug("Command %s completed successfully", method.Cmd)
}

// displayHelp shows available commands
func displayHelp() {
	help := `
OptimusDB Interactive Shell
===========================

Available Commands:
------------------
  get <key>              - Retrieve a value by key
  post <filepath>        - Upload a file to the knowledge base
  connect <peer-addr>    - Connect to a peer
  query <query-string>   - Execute a query
  sql <sql-statement>    - Execute SQL SELECT statement
  benchmark              - Get system benchmark information
  querykbdata <args>     - Query knowledge base data
  crudget <id>           - CRUD: Get document by ID
  crudput <data>         - CRUD: Put/update document
  lineage <table-id>     - Get lineage information for a table
  metadata <db> <table>  - Get metadata for a table
  enrich <db> <table>    - Enrich table with AI-generated metadata
  peers                  - List connected peers
  status                 - Show node status
  history                - Show command history
  help                   - Show this help message
  exit/quit              - Exit the shell

Examples:
---------
  > get mykey
  > post /path/to/tosca.yaml
  > connect /ip4/192.168.1.100/tcp/4001/p2p/Qm...
  > sql SELECT * FROM datacatalog LIMIT 10
  > metadata knowledgebase datacatalog
  > enrich knowledgebase wind_turbine_data
  > lineage table_12345
  > peers
  > status

Notes:
------
  - Use Tab for command completion (if supported by terminal)
  - Use Ctrl+C to cancel current operation
  - Command history is maintained for the session
`
	fmt.Println(help)
}

// displayBanner shows the startup banner
func displayBanner() {
	banner := `
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║   ██████╗ ██████╗ ████████╗██╗███╗   ███╗██╗   ██╗███████╗
║  ██╔═══██╗██╔══██╗╚══██╔══╝██║████╗ ████║██║   ██║██╔════╝
║  ██║   ██║██████╔╝   ██║   ██║██╔████╔██║██║   ██║███████╗
║  ██║   ██║██╔═══╝    ██║   ██║██║╚██╔╝██║██║   ██║╚════██║
║  ╚██████╔╝██║        ██║   ██║██║ ╚═╝ ██║╚██████╔╝███████║
║   ╚═════╝ ╚═╝        ╚═╝   ╚═╝╚═╝     ╚═╝ ╚═════╝ ╚══════╝
║                                                           ║
║         Distributed Knowledge Base - Interactive Shell   ║
║                      Type 'help' for commands             ║
╚═══════════════════════════════════════════════════════════╝
`
	fmt.Println(banner)
}

// Shell starts listening for commands and implements the API for the user
func Shell(reqChan chan app.Request,
	resChan chan interface{},
	logChan chan app.Log) {

	logger.Info("Starting interactive shell")
	logChan <- app.Log{
		Type: app.Info,
		Data: "Starting shell"}

	// Display banner
	displayBanner()

	// Initialize command history
	history := NewCommandHistory(100)

	// Shell configuration
	config := ShellConfig{
		HistorySize: 100,
		Prompt:      "optimusdb> ",
		EnableColor: true,
	}

	reader := bufio.NewReader(os.Stdin)

	for {
		// Display prompt
		fmt.Print(config.Prompt)

		// Read command
		cmd, err := reader.ReadString('\n')
		if err != nil {
			logger.Error("Failed to read shell input: %v", err)
			logChan <- app.Log{Type: app.RecoverableErr, Data: err}
			continue
		}

		// Trim and parse command
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}

		// Add to history
		history.Add(cmd)

		// Parse command
		cmdList := strings.Split(cmd, " ")
		cmdName := strings.ToLower(cmdList[0])

		logger.Debug("Shell command received: %s", cmdName)

		switch cmdName {

		// ============================================================
		// CORE DATA OPERATIONS
		// ============================================================

		case app.GET.Cmd:
			processReq(cmdList, app.GET, reqChan, resChan, logChan)

		case app.POST.Cmd:
			if len(cmdList) != app.POST.ArgCnt+1 {
				logger.Error("Invalid POST command: expected filepath argument")
				logChan <- app.Log{
					Type: app.RecoverableErr,
					Data: errors.New("usage: post <filepath>")}
				continue
			}

			// Read file contents
			filePath := cmdList[1]
			logger.Debug("Reading file: %s", filePath)

			fileBytes, err := ioutil.ReadFile(filePath)
			if err != nil {
				logger.Error("Failed to read file %s: %v", filePath, err)
				logChan <- app.Log{Type: app.RecoverableErr, Data: err}
				continue
			}

			logger.Info("Uploading file: %s (%d bytes)", filePath, len(fileBytes))
			cmdList = []string{app.POST.Cmd, string(fileBytes)}
			processReq(cmdList, app.POST, reqChan, resChan, logChan)

		case app.CONNECT.Cmd:
			if len(cmdList) < 2 {
				logger.Error("Invalid CONNECT command: missing peer address")
				logChan <- app.Log{
					Type: app.RecoverableErr,
					Data: errors.New("usage: connect <peer-multiaddr>")}
				continue
			}
			logger.Info("Connecting to peer: %s", cmdList[1])
			processReq(cmdList, app.CONNECT, reqChan, resChan, logChan)

		case app.QUERY.Cmd:
			processReq(cmdList, app.QUERY, reqChan, resChan, logChan)

		case app.BENCHMARK.Cmd:
			logger.Info("Fetching benchmark information")
			processReq(cmdList, app.BENCHMARK, reqChan, resChan, logChan)

		case app.SQLSELECT.Cmd:
			if len(cmdList) < 2 {
				logger.Error("Invalid SQL command: missing query")
				logChan <- app.Log{
					Type: app.RecoverableErr,
					Data: errors.New("usage: sql <SELECT statement>")}
				continue
			}
			// Reconstruct SQL statement (may have spaces)
			sqlStatement := strings.Join(cmdList[1:], " ")
			logger.Query("Executing SQL: %s", sqlStatement)
			processReq([]string{app.SQLSELECT.Cmd, sqlStatement}, app.SQLSELECT, reqChan, resChan, logChan)

		case app.QUERYKBDATA.Cmd:
			processReq(cmdList, app.QUERYKBDATA, reqChan, resChan, logChan)

		// ============================================================
		// CRUD OPERATIONS
		// ============================================================

		case app.CRUDGET.Cmd:
			if len(cmdList) < 2 {
				logger.Error("Invalid CRUDGET command: missing document ID")
				logChan <- app.Log{
					Type: app.RecoverableErr,
					Data: errors.New("usage: crudget <document-id>")}
				continue
			}
			logger.Debug("CRUD GET: document_id=%s", cmdList[1])
			processReq(cmdList, app.CRUDGET, reqChan, resChan, logChan)

		case app.CRUDPUT.Cmd:
			if len(cmdList) < 2 {
				logger.Error("Invalid CRUDPUT command: missing document data")
				logChan <- app.Log{
					Type: app.RecoverableErr,
					Data: errors.New("usage: crudput <json-data>")}
				continue
			}
			// Reconstruct JSON data (may have spaces)
			jsonData := strings.Join(cmdList[1:], " ")
			logger.Debug("CRUD PUT: %d bytes", len(jsonData))
			processReq([]string{app.CRUDPUT.Cmd, jsonData}, app.CRUDPUT, reqChan, resChan, logChan)

		// ============================================================
		// LINEAGE & METADATA OPERATIONS
		// ============================================================

		case "lineage":
			if len(cmdList) < 2 {
				logger.Error("Invalid LINEAGE command: missing table ID")
				fmt.Println("Usage: lineage <table-id>")
				continue
			}
			logger.Lineage("Querying lineage for table: %s", cmdList[1])

			reqChan <- app.Request{
				Method: app.Method{Cmd: "lineage", ArgCnt: 1},
				Args:   cmdList[1:],
			}
			res := <-resChan
			logChan <- app.Log{Type: app.Print, Data: res}
			logChan <- app.Log{Type: app.Print, Data: "\n"}

		case "metadata":
			if len(cmdList) < 3 {
				logger.Error("Invalid METADATA command: missing arguments")
				fmt.Println("Usage: metadata <database> <table>")
				continue
			}
			logger.Info("Fetching metadata: db=%s, table=%s", cmdList[1], cmdList[2])

			reqChan <- app.Request{
				Method: app.Method{Cmd: "metadata", ArgCnt: 2},
				Args:   cmdList[1:],
			}
			res := <-resChan
			logChan <- app.Log{Type: app.Print, Data: res}
			logChan <- app.Log{Type: app.Print, Data: "\n"}

		case "enrich":
			if len(cmdList) < 3 {
				logger.Error("Invalid ENRICH command: missing arguments")
				fmt.Println("Usage: enrich <database> <table>")
				continue
			}
			logger.AI("Enriching table with AI metadata: db=%s, table=%s", cmdList[1], cmdList[2])

			reqChan <- app.Request{
				Method: app.Method{Cmd: "enrich", ArgCnt: 2},
				Args:   cmdList[1:],
			}
			res := <-resChan
			logChan <- app.Log{Type: app.Print, Data: res}
			logChan <- app.Log{Type: app.Print, Data: "\n"}

		// ============================================================
		// CLUSTER & NETWORK OPERATIONS
		// ============================================================

		case "peers":
			logger.Info("Listing connected peers")

			reqChan <- app.Request{
				Method: app.Method{Cmd: "peers", ArgCnt: 0},
				Args:   []string{},
			}
			res := <-resChan
			logChan <- app.Log{Type: app.Print, Data: res}
			logChan <- app.Log{Type: app.Print, Data: "\n"}

		case "status":
			logger.Info("Fetching node status")

			reqChan <- app.Request{
				Method: app.Method{Cmd: "status", ArgCnt: 0},
				Args:   []string{},
			}
			res := <-resChan
			logChan <- app.Log{Type: app.Print, Data: res}
			logChan <- app.Log{Type: app.Print, Data: "\n"}

		// ============================================================
		// UTILITY COMMANDS
		// ============================================================

		case app.HELP.Cmd, "?":
			displayHelp()

		case "history":
			fmt.Println("\nCommand History:")
			fmt.Println("================")
			for i, histCmd := range history.GetHistory() {
				fmt.Printf("%3d: %s\n", i+1, histCmd)
			}
			fmt.Println()

		case "clear", "cls":
			// Clear screen
			fmt.Print("\033[H\033[2J")
			displayBanner()

		case "exit", "quit", "q":
			logger.Info("Exiting interactive shell")
			fmt.Println("\nGoodbye! 👋")
			os.Exit(0)

		// ============================================================
		// UNKNOWN COMMAND
		// ============================================================

		default:
			logger.Warn("Unknown command: %s", cmdName)
			fmt.Printf("Unknown command: '%s'. Type 'help' for available commands.\n", cmdName)
			logChan <- app.Log{
				Type: app.RecoverableErr,
				Data: errors.New("command not supported")}
		}
	}
}

// ShellCommandExecutor executes a single command non-interactively
func ShellCommandExecutor(command string, reqChan chan app.Request, resChan chan interface{}) (interface{}, error) {
	logger.Debug("Executing non-interactive command: %s", command)

	cmd := strings.TrimSpace(command)
	if cmd == "" {
		return nil, errors.New("empty command")
	}

	cmdList := strings.Split(cmd, " ")
	cmdName := strings.ToLower(cmdList[0])

	// Create temporary log channel
	logChan := make(chan app.Log, 10)
	go func() {
		for range logChan {
			// Discard logs in non-interactive mode
		}
	}()

	// Process command
	switch cmdName {
	case app.GET.Cmd:
		if len(cmdList) != app.GET.ArgCnt+1 {
			return nil, errors.New("invalid argument count")
		}
		reqChan <- app.Request{Method: app.GET, Args: cmdList[1:]}
		return <-resChan, nil

	case app.QUERY.Cmd:
		if len(cmdList) != app.QUERY.ArgCnt+1 {
			return nil, errors.New("invalid argument count")
		}
		reqChan <- app.Request{Method: app.QUERY, Args: cmdList[1:]}
		return <-resChan, nil

	case app.SQLSELECT.Cmd:
		sqlStatement := strings.Join(cmdList[1:], " ")
		reqChan <- app.Request{Method: app.SQLSELECT, Args: []string{sqlStatement}}
		return <-resChan, nil

	default:
		return nil, fmt.Errorf("unsupported command: %s", cmdName)
	}
}

// BatchCommandExecutor executes multiple commands from a file
func BatchCommandExecutor(filepath string, reqChan chan app.Request, resChan chan interface{}) error {
	logger.Info("Executing batch commands from: %s", filepath)

	file, err := os.Open(filepath)
	if err != nil {
		logger.Error("Failed to open batch file %s: %v", filepath, err)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		logger.Debug("Executing batch command [line %d]: %s", lineNum, line)

		result, err := ShellCommandExecutor(line, reqChan, resChan)
		if err != nil {
			logger.Error("Batch command failed [line %d]: %v", lineNum, err)
			return fmt.Errorf("command failed at line %d: %w", lineNum, err)
		}

		logger.Debug("Batch command result [line %d]: %v", lineNum, result)
	}

	if err := scanner.Err(); err != nil {
		logger.Error("Error reading batch file: %v", err)
		return err
	}

	logger.Info("Batch execution completed: %d commands", lineNum)
	return nil
}
