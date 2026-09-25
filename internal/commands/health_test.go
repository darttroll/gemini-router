package commands

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/storage"
)

func setupHealthTest(t *testing.T) (*HealthCommand, *storage.Store) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	cfg := &config.Config{
		AgyPath: "/usr/bin/agy",
		Workers: []config.WorkerConfig{
			{Username: "alice", Enabled: true},
			{Username: "bob", Enabled: false},
		},
		Balancer: config.BalancerSettings{},
	}

	// Stub the AGY availability check as healthy.
	cmd := &HealthCommand{
		cfg:     cfg,
		store:   store,
		checkFn: func(worker, agyPath string) (bool, string) { return true, "OK" },
	}
	return cmd, store
}

func TestHealthCommand_Basic(t *testing.T) {
	cmd, _ := setupHealthTest(t)

	var buf bytes.Buffer
	if err := cmd.Run(&buf, false); err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "alice") {
		t.Errorf("output missing alice: %s", output)
	}
	if !strings.Contains(output, "bob") {
		t.Errorf("output missing bob: %s", output)
	}
	if !strings.Contains(output, "disabled") {
		t.Errorf("output missing 'disabled' status for bob: %s", output)
	}
}

func TestHealthCommand_Verbose(t *testing.T) {
	cmd, _ := setupHealthTest(t)

	var buf bytes.Buffer
	if err := cmd.Run(&buf, true); err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	output := buf.String()
	// Verbose output includes adaptive limits.
	if !strings.Contains(output, "Rate") || !strings.Contains(output, "Concurrency") || !strings.Contains(output, "InFlight") {
		t.Errorf("verbose output missing verbose headers: %s", output)
	}
}

func TestHealthCommand_BreakerStatus(t *testing.T) {
	cmd, store := setupHealthTest(t)

	now := time.Now().UTC()
	if err := store.PutControllerState(storage.ControllerState{Worker: "alice", Model: "default", Rate: 12, StableRate: 12, Tokens: 2, TokenUpdatedAt: now, ConcurrencyLimit: 6, QuotaBlockedUntil: now.Add(5 * time.Minute), RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := cmd.Run(&buf, false); err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !strings.Contains(output, "blocked") {
		t.Errorf("output missing breaker status for alice: %s", output)
	}

}
