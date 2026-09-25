package logger

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLogger_WriteEntry(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")

	lg, err := New(logDir)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer lg.Close()

	entry := Entry{
		Timestamp:     time.Date(2026, 7, 1, 12, 30, 0, 0, time.UTC),
		Worker:        "alice",
		Model:         "TestModel",
		Status:        "success",
		DurationMs:    1200,
		PromptPreview: "Hello world",
		Files:         []string{"file.txt"},
		RetryCount:    0,
	}

	if err := lg.Log(entry); err != nil {
		t.Fatalf("Log() error: %v", err)
	}

	// Verify the expected log file was created.
	expectedFile := filepath.Join(logDir, "2026-07-01.log")
	data, err := os.ReadFile(expectedFile)
	if err != nil {
		t.Fatalf("ReadFile() error: %v", err)
	}

	var parsed Entry
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	if parsed.Worker != "alice" {
		t.Errorf("Worker = %q, want %q", parsed.Worker, "alice")
	}
	if parsed.Status != "success" {
		t.Errorf("Status = %q, want %q", parsed.Status, "success")
	}
	if parsed.DurationMs != 1200 {
		t.Errorf("DurationMs = %d, want 1200", parsed.DurationMs)
	}
}

func TestLogger_MultipleEntries(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")

	lg, err := New(logDir)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer lg.Close()

	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		entry := Entry{
			Timestamp: now,
			Worker:    "alice",
			Status:    "success",
		}
		if err := lg.Log(entry); err != nil {
			t.Fatal(err)
		}
	}

	logFile := filepath.Join(logDir, now.Format("2006-01-02")+".log")
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Errorf("got %d log lines, want 3", len(lines))
	}
}

func TestLogger_CreatesLogDir(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "nested", "logs")

	lg, err := New(logDir)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer lg.Close()

	info, err := os.Stat(logDir)
	if err != nil {
		t.Fatalf("logDir not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("logDir is not a directory")
	}
}

func TestLogger_AdaptiveMetadata(t *testing.T) {
	dir := t.TempDir()
	lg, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer lg.Close()
	now := time.Now().UTC()
	entry := Entry{Timestamp: now, RequestID: "req-123", Worker: "alice", Model: "m", Status: "short_throttle", QueueWaitMs: 12, PredictedWaitMs: 20, PredictedTotalMs: 220, RetryCount: 1}
	if err := lg.Log(entry); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, now.Format("2006-01-02")+".log"))
	if err != nil {
		t.Fatal(err)
	}
	var got Entry
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.RequestID != "req-123" || got.QueueWaitMs != 12 || got.PredictedWaitMs != 20 || got.PredictedTotalMs != 220 {
		t.Fatalf("metadata=%+v", got)
	}
}

func TestLogger_ControllerMetadata(t *testing.T) {
	dir := t.TempDir()
	lg, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer lg.Close()
	now := time.Now().UTC()
	entry := Entry{Timestamp: now, RequestID: "r", Worker: "alice", Model: "m", Status: "success", RateLimitRPS: 17.5, ConcurrencyLimit: 7, QuotaBlockedUntil: "2026-08-08T00:00:00Z"}
	if err := lg.Log(entry); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, now.Format("2006-01-02")+".log"))
	if err != nil {
		t.Fatal(err)
	}
	var got Entry
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.RateLimitRPS != 17.5 || got.ConcurrencyLimit != 7 || got.QuotaBlockedUntil == "" {
		t.Fatalf("got=%+v", got)
	}
}

func TestLoggerHardensExistingDirectoryAndFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "logs")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	logFile := filepath.Join(dir, now.Format(dateFormat)+".log")
	if err := os.WriteFile(logFile, []byte("old\n"), 0644); err != nil {
		t.Fatal(err)
	}
	lg, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer lg.Close()
	if err := lg.Log(Entry{Timestamp: now, Worker: "alice", Status: "success"}); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]os.FileMode{dir: 0700, logFile: 0600} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Fatalf("%s mode=%o want %o", path, got, want)
		}
	}
}
