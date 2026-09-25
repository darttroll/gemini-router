//go:build integration

package commands

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/logger"
	"github.com/darttroll/gemini-router/internal/storage"
)

// TestIntegration_PrintSuccess exercises config through executor and print.
// This test uses the bundled fake AGY command.
func TestIntegration_PrintSuccess(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	logDir := filepath.Join(dir, "logs")
	lg, err := logger.New(logDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lg.Close()

	// Configure the bundled mock AGY instead of a real provider CLI.
	mockScript := `#!/bin/bash
BEHAVIOR="${MOCK_AGY_BEHAVIOR:-success}"
case "$BEHAVIOR" in
  success)
    echo "Mock response to: $*"
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
`
	mockAgyPath := filepath.Join(dir, "mock_agy.sh")
	if err := os.WriteFile(mockAgyPath, []byte(mockScript), 0755); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		AgyPath:      mockAgyPath,
		DataDir:      dir,
		DefaultModel: "TestModel",
		Workers: []config.WorkerConfig{
			// Use the current account for the local integration fixture.
			{Username: os.Getenv("USER"), Enabled: true},
		},
		Balancer: config.BalancerSettings{
			MaxRetries: 3,
		},
		RequestTimeout: 30 * time.Second,
	}

	// Use the real executor against the mock AGY fixture.
	printCmd := NewPrintCommand(cfg, store, lg)

	output, err := printCmd.Run(context.Background(), "Hello integration", "", nil, "", "auto", "/tmp/gemini-router-test", false)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	if !strings.Contains(output, "Mock response to: --print --model TestModel Hello integration") {
		t.Errorf("output = %q, want it to contain 'Mock response to: --print --model TestModel Hello integration'", output)
	}

	// Verify request statistics were recorded.
	stats, err := store.GetWorkerStats(os.Getenv("USER"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalRequests != 1 {
		t.Errorf("TotalRequests = %d, want 1", stats.TotalRequests)
	}
}

// TestIntegration_StatsAfterRequests verifies stats after multiple requests.
func TestIntegration_StatsAfterRequests(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	cfg := &config.Config{
		Workers: []config.WorkerConfig{
			{Username: "testuser", Enabled: true},
		},
	}

	// Seed request history.
	now := time.Now()
	store.InsertRequestLog(storage.RequestLogEntry{
		Worker: "testuser", Model: "M", StartedAt: now, DurationMs: 100, Status: "success",
	})
	store.InsertRequestLog(storage.RequestLogEntry{
		Worker: "testuser", Model: "M", StartedAt: now, DurationMs: 200, Status: "error", ErrorMessage: "fail",
	})

	statsCmd := NewStatsCommand(cfg, store)
	var buf bytes.Buffer
	if err := statsCmd.Run(&buf, 0); err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !strings.Contains(output, "testuser") {
		t.Errorf("stats output missing testuser: %s", output)
	}
	if !strings.Contains(output, "2") {
		t.Errorf("stats output missing total count of 2: %s", output)
	}
}
