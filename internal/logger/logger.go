package logger

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const dateFormat = "2006-01-02"

// Entry is one structured JSON Lines log record.
type Entry struct {
	Timestamp         time.Time `json:"ts"`
	RequestID         string    `json:"request_id,omitempty"`
	Worker            string    `json:"worker"`
	Model             string    `json:"model,omitempty"`
	Status            string    `json:"status"`
	DurationMs        int64     `json:"duration_ms,omitempty"`
	QueueWaitMs       int64     `json:"queue_wait_ms,omitempty"`
	PredictedWaitMs   int64     `json:"predicted_wait_ms,omitempty"`
	PredictedTotalMs  int64     `json:"predicted_total_ms,omitempty"`
	RateLimitRPS      float64   `json:"rate_limit_rps,omitempty"`
	ConcurrencyLimit  int       `json:"concurrency_limit,omitempty"`
	QuotaBlockedUntil string    `json:"quota_blocked_until,omitempty"`
	PromptPreview     string    `json:"prompt_preview,omitempty"`
	Files             []string  `json:"files,omitempty"`
	RetryCount        int       `json:"retry_count"`
	ErrorMessage      string    `json:"error,omitempty"`
}

// Logger writes structured JSON Lines logs.
// Logs rotate by UTC date (YYYY-MM-DD.log).
type Logger struct {
	logDir      string
	mu          sync.Mutex
	currentDate string
	currentFile *os.File
}

// New creates a logger and its private log directory when needed.
func New(logDir string) (*Logger, error) {
	if err := os.MkdirAll(logDir, 0700); err != nil {
		return nil, fmt.Errorf("creating log directory %s: %w", logDir, err)
	}
	if err := os.Chmod(logDir, 0700); err != nil {
		return nil, fmt.Errorf("securing log directory %s: %w", logDir, err)
	}
	return &Logger{logDir: logDir}, nil
}

// Log appends one structured entry.
func (l *Logger) Log(entry Entry) error {
	dateStr := entry.Timestamp.Format(dateFormat)

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshaling log entry: %w", err)
	}
	data = append(data, '\n')

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.ensureFile(dateStr); err != nil {
		return err
	}

	if _, err := l.currentFile.Write(data); err != nil {
		return fmt.Errorf("writing log entry: %w", err)
	}

	return nil
}

// Close closes the current log file.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.currentFile != nil {
		err := l.currentFile.Close()
		l.currentFile = nil
		l.currentDate = ""
		return err
	}
	return nil
}

func (l *Logger) ensureFile(dateStr string) error {
	if l.currentDate == dateStr && l.currentFile != nil {
		return nil
	}

	if l.currentFile != nil {
		if err := l.currentFile.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "error closing log file: %v\n", err)
		}
	}

	path := filepath.Join(l.logDir, dateStr+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("opening log file %s: %w", path, err)
	}
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return fmt.Errorf("securing log file %s: %w", path, err)
	}
	l.currentFile = f
	l.currentDate = dateStr
	return nil
}
