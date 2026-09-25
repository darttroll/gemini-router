package commands

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/storage"
)

func setupStatsTest(t *testing.T) (*StatsCommand, *storage.Store) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	cfg := &config.Config{
		Workers: []config.WorkerConfig{
			{Username: "alice", Enabled: true},
			{Username: "bob", Enabled: true},
		},
	}

	cmd := NewStatsCommand(cfg, store)
	return cmd, store
}

func TestStatsCommand_Empty(t *testing.T) {
	cmd, _ := setupStatsTest(t)

	var buf bytes.Buffer
	if err := cmd.Run(&buf, 0); err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	output := buf.String()
	if output == "" {
		t.Error("expected non-empty output")
	}
	// The table header should be present.
	if !strings.Contains(output, "Worker") {
		t.Errorf("output missing 'Worker' header: %s", output)
	}
}

func TestStatsCommand_WithData(t *testing.T) {
	cmd, store := setupStatsTest(t)

	now := time.Now()
	store.InsertRequestLog(storage.RequestLogEntry{
		Worker: "alice", Model: "M", StartedAt: now, DurationMs: 500, Status: "success",
	})
	store.InsertRequestLog(storage.RequestLogEntry{
		Worker: "alice", Model: "M", StartedAt: now, DurationMs: 1500, Status: "error", ErrorMessage: "fail",
	})
	store.InsertRequestLog(storage.RequestLogEntry{
		Worker: "bob", Model: "M", StartedAt: now, DurationMs: 200, Status: "success",
	})

	var buf bytes.Buffer
	if err := cmd.Run(&buf, 0); err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !strings.Contains(output, "alice") {
		t.Errorf("output missing alice: %s", output)
	}
	if !strings.Contains(output, "bob") {
		t.Errorf("output missing bob: %s", output)
	}
}

func TestStatsCommand_WithPeriod(t *testing.T) {
	cmd, store := setupStatsTest(t)

	now := time.Now()
	// Request inside the reporting window.
	store.InsertRequestLog(storage.RequestLogEntry{
		Worker: "alice", Model: "M", StartedAt: now.Add(-30 * time.Minute), DurationMs: 100, Status: "success",
	})
	// Request outside the reporting window.
	store.InsertRequestLog(storage.RequestLogEntry{
		Worker: "alice", Model: "M", StartedAt: now.Add(-2 * time.Hour), DurationMs: 100, Status: "success",
	})

	var buf bytes.Buffer
	if err := cmd.Run(&buf, 1*time.Hour); err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	// Verify Alice is present with the expected single-request latency.
	if !strings.Contains(output, "alice") || !strings.Contains(output, "100ms") {
		t.Errorf("output missing expected alice stats (should be 1 request): %s", output)
	}
}

func TestStatsCommand_ShowsFiveHourAndWeeklyQuota(t *testing.T) {
	cmd, store := setupStatsTest(t)
	cmd.cfg.DefaultModel = "gemini-3.6-flash-high"
	cmd.cfg.Quota = config.QuotaSettings{Enabled: true}
	now := time.Now().UTC()
	if err := store.ReplaceQuotaSnapshots("alice", []storage.QuotaSnapshot{
		{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "5h", RemainingFraction: 0.42, ResetTime: now.Add(2 * time.Hour), FetchedAt: now},
		{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "weekly", RemainingFraction: 0.317, ResetTime: now.Add(6 * 24 * time.Hour), FetchedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := cmd.Run(&buf, 0); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "5h Quota") || !strings.Contains(out, "Weekly Quota") {
		t.Fatalf("quota headers missing: %s", out)
	}
	if !strings.Contains(out, "42.0%") || !strings.Contains(out, "31.7%") {
		t.Fatalf("quota values missing: %s", out)
	}
}

func TestStatsCommand_ShowsQueueDepthAndPredictedWaitForNewRequest(t *testing.T) {
	cmd, store := setupStatsTest(t)
	cmd.cfg.DefaultModel = "m"
	cmd.cfg.Adaptive = config.AdaptiveSettings{InitialRate: 2, InitialConcurrency: 4, RetryBudgetCapacity: 10}
	cmd.cfg.RequestTimeout = time.Minute
	now := time.Now().UTC()
	for i := 0; i < 4; i++ {
		if err := store.EnqueueRequest(storage.PendingRequest{
			RequestID: fmt.Sprintf("queued-%d", i), Model: "m", PID: os.Getpid(),
			EnqueuedAt: now.Add(time.Duration(i) * time.Millisecond), Deadline: now.Add(time.Minute), Unbounded: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if err := cmd.Run(&buf, 0); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "Queue total: 4") || !strings.Contains(out, "Queue for model m: 4") {
		t.Fatalf("queue depth missing: %s", out)
	}
	if !strings.Contains(out, "Predicted wait for new request:") {
		t.Fatalf("predicted wait missing: %s", out)
	}
	if strings.Contains(out, "Predicted wait for new request: 0s") {
		t.Fatalf("expected non-zero wait with queued work: %s", out)
	}
}

func TestStatsCommand_ShowsDisabledFiveHourWhenWeeklyExhausted(t *testing.T) {
	cmd, store := setupStatsTest(t)
	cmd.cfg.DefaultModel = "gemini-3.6-flash-high"
	cmd.cfg.Quota = config.QuotaSettings{Enabled: true}
	now := time.Now().UTC()
	if err := store.ReplaceQuotaSnapshots("alice", []storage.QuotaSnapshot{
		{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "5h", RemainingFraction: 1, Disabled: true, FetchedAt: now},
		{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "weekly", RemainingFraction: 0, ResetTime: now.Add(5 * 24 * time.Hour), FetchedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := cmd.Run(&buf, 0); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "disabled") || !strings.Contains(out, "0.0%") || !strings.Contains(out, "quota-blocked") {
		t.Fatalf("weekly-exhausted quota state rendered incorrectly: %s", out)
	}
}
