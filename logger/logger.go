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
	"runtime"
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
	mutexSync sync.Mutex
)

// Logger is a custom logger with different log levels
type Logger struct {
	level   LogLevel
	logFile *os.File
	lokiURL string
	db      LoggerDBInterface // Add database interface
}

// LoggerDBInterface allows injecting the database logger
type LoggerDBInterface interface {
	AddToOptimusLog(level, message, source string) error
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
	log.Printf("[INFO] Loki url is: %v\n", lokiURL)
	return &Logger{level: level, logFile: logFile, lokiURL: lokiURL}
}

// SetDatabase sets the database logger for persistence
func (l *Logger) SetDatabase(db LoggerDBInterface) {
	l.db = db
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

		formattedMessage := fmt.Sprintf(message, args...)
		fullMessage := fmt.Sprintf("[%s] %s\n", prefix, formattedMessage)

		// Ensure only one log writes at a time (avoids race conditions)
		mutexSync.Lock()
		log.Print(fullMessage) // Logs to file
		mutexSync.Unlock()

		// Send log to Loki
		l.sendToLoki(prefix, fullMessage)

		// Persist to database
		l.persistToDatabase(prefix, formattedMessage, 3)
	}
}

// persistToDatabase saves the log entry to the database
func (l *Logger) persistToDatabase(level, message string, callerDepth int) {
	if l.db != nil {
		var source string
		if _, file, line, ok := runtime.Caller(callerDepth); ok {
			source = fmt.Sprintf("%s:%d", filepath.Base(file), line)
		} else {
			source = runtime.GOOS
		}
		_ = l.db.AddToOptimusLog(level, message, source)
	}
}

func escapeLogMessage(message string) string {
	message = strings.ReplaceAll(message, "\n", " ")
	message = strings.ReplaceAll(message, "\t", " ")
	message = strings.ReplaceAll(message, "\r", " ")
	message = strings.ReplaceAll(message, "\\", "\\\\")
	message = strings.ReplaceAll(message, `"`, `\"`)
	return message
}

func (l *Logger) sendToLoki(level, message string) {
	if l.lokiURL == "" {
		log.Printf("[INFO] Loki URL is not set\n")
		return
	} else if *config.FlagLokiIsDisabled {
		log.Printf("[INFO] Loki is disabled\n")
		return
	}

	escapedMessage := escapeLogMessage(message)
	log.Printf("[DEBUG] Sending log to Loki: %s", escapedMessage)

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

	for i := 0; i < 3; i++ {
		resp, err := http.Post(l.lokiURL, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("[WARN] Failed to send log to Loki, retrying... (%d/3): %v\n", i+1, err)
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusOK {
			log.Printf("[INFO] Loki Data sent successfully\n")
			resp.Body.Close()
			return
		}

		log.Printf("[WARN] Loki returned non-200 status: %s, retrying...\n", resp.Status)
		resp.Body.Close()
		time.Sleep(1 * time.Second)
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

// SetGlobalDatabase sets the database for the global logger
func SetGlobalDatabase(db LoggerDBInterface) {
	GlobalLogger.SetDatabase(db)
}

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
func CheckAndLogError(err error, message string, args ...interface{}) {
	if err != nil {
		Error("%s: %v", fmt.Sprintf(message, args...), err)
	}
}
