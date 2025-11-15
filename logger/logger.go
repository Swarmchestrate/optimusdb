package logger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"optimusdb/config"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type LogLevel int

const (
	INFO LogLevel = iota
	ERROR
	DEBUG
)

var (
	mutexSync sync.Mutex //define mutex properly here
)

// Logger is a custom logger with different log levels
type Logger struct {
	level   LogLevel
	logFile *os.File
	lokiURL string
}

// NewLogger initializes a new logger instance with file & Loki support
func NewLogger(level LogLevel, logFilePath, lokiURL string) *Logger {
	logDir := filepath.Dir(logFilePath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("Failed to create logs directory: %v", err)
	}
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("[INFO] Loki usrl is: %v\n", lokiURL)
	return &Logger{level: level, logFile: logFile, lokiURL: lokiURL}
}

// Log writes a log message based on the log level
func (l *Logger) Log(level LogLevel, message string, args ...interface{}) {
	if level >= l.level {
		prefix := ""
		switch level {
		case INFO:
			prefix = "INFO"
		case ERROR:
			prefix = "ERROR"
		case DEBUG:
			prefix = "DEBUG"
		default:
			prefix = "LOG"
		}

		formattedMessage := fmt.Sprintf("[%s] %s\n", prefix, fmt.Sprintf(message, args...))

		// Ensure only one log writes at a time (avoids race conditions)
		mutexSync.Lock()
		log.Print(formattedMessage) // Logs to file
		mutexSync.Unlock()
		//log.Print(formattedMessage) // Logs to file

		// Send log to Loki
		l.sendToLoki(prefix, formattedMessage)
	}
}
func escapeLogMessage(message string) string {
	message = strings.ReplaceAll(message, "\n", " ")    // Replace newlines with space
	message = strings.ReplaceAll(message, "\t", " ")    // Replace tabs with space
	message = strings.ReplaceAll(message, "\r", " ")    // Remove carriage returns
	message = strings.ReplaceAll(message, "\\", "\\\\") // Escape backslashes
	message = strings.ReplaceAll(message, `"`, `\"`)    // Escape quotes
	return message
}

func (l *Logger) sendToLoki(level, message string) {
	if l.lokiURL == "" {
		log.Printf("[INFO] Loki URL is not set\n")
		return // Loki URL not set, skip sending logs
	} else if *config.FlagLokiIsDisabled {
		log.Printf("[INFO] Loki is disabled\n")
		return
	}

	// Escape special characters in log message to prevent Loki JSON errors
	//escapedMessage := strings.ReplaceAll(message, "\n", "\\n")
	//escapedMessage = strings.ReplaceAll(escapedMessage, "\"", "'") // Replace quotes to avoid breaking JSON

	escapedMessage := escapeLogMessage(message)

	log.Printf("[DEBUG] Sending log to Loki: %s", escapedMessage)

	// Properly structured log entry
	logEntry := map[string]interface{}{
		"streams": []map[string]interface{}{
			{
				"stream": map[string]string{
					"job":    "optimusdbLoki",
					"level":  level,
					"source": "optimusdb",
				},
				"values": [][]string{
					{time.Now().Format(time.RFC3339Nano), escapedMessage},
				},
			},
		},
	}

	jsonData, err := json.Marshal(logEntry)
	if err != nil {
		log.Printf("[ERROR] Failed to parse jsonData log for Loki: %v\n", err)
		return
	}

	// Retry mechanism
	for i := 0; i < 3; i++ {
		resp, err := http.Post(l.lokiURL, "application/json", bytes.NewBuffer(jsonData)) // Use `l.lokiURL` not `lokiURL`
		if err != nil {
			log.Printf("[WARN] Failed to send log to Loki, retrying... (%d/3): %v\n", i+1, err)
			time.Sleep(1 * time.Second) // Wait before retrying
			continue                    // Try again
		}

		// Ensure response body is always closed
		if resp.StatusCode == http.StatusOK {
			log.Printf("[INFO] Loki Data sent successfully\n")
			resp.Body.Close() // Explicitly close response body
			return            // Success, exit retry loop
		}

		log.Printf("[WARN] Loki returned non-200 status: %s, retrying...\n", resp.Status)
		resp.Body.Close() // Explicitly close before retrying

		time.Sleep(1 * time.Second) // Wait before retrying
	}

	log.Printf("[ERROR] Failed to send log to Loki after 3 attempts")
}

// CloseLogger closes the log file
func (l *Logger) CloseLogger() {
	l.logFile.Close()
}

// GlobalLogger instance accessible throughout the app
var lokiURL = os.Getenv("LOKI_URL")
var GlobalLogger = NewLogger(INFO, *config.FlagLogFilename, lokiURL)

// Info logs an info-level message
func Info(format string, args ...interface{}) {
	GlobalLogger.Log(INFO, format, args...)
}

// Error logs an error-level message
func Error(format string, args ...interface{}) {
	GlobalLogger.Log(ERROR, format, args...)
}

// Debug logs a debug-level message
func Debug(format string, args ...interface{}) {
	GlobalLogger.Log(DEBUG, format, args...)
}

// CheckAndLogError logs the error if it is not nil
// CheckAndLogError logs the error if it is not nil
func CheckAndLogError(err error, message string, args ...interface{}) {
	if err != nil {
		Error("%s: %v", fmt.Sprintf(message, args...), err)
	}
}
