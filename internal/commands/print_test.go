package commands

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/executor"
	"github.com/darttroll/gemini-router/internal/logger"
	"github.com/darttroll/gemini-router/internal/scheduler"
	"github.com/darttroll/gemini-router/internal/storage"
)

// mockExecutor simulates AGY execution for tests.
type mockExecutor struct {
	results  []*executor.Result
	callIdx  int
	lastArgs mockExecArgs
}

type mockExecArgs struct {
	Worker string
	Prompt string
	Model  string
	Files  []string
}

func (m *mockExecutor) Execute(ctx context.Context, req executor.Request) (*executor.Result, error) {
	m.lastArgs = mockExecArgs{Worker: req.Worker, Prompt: req.Prompt, Model: req.Model, Files: req.Files}
	if m.callIdx < len(m.results) {
		r := m.results[m.callIdx]
		m.callIdx++
		return r, nil
	}
	return &executor.Result{Output: "default response", Duration: 100 * time.Millisecond}, nil
}

func setupPrintTest(t *testing.T) (*PrintCommand, *mockExecutor, *storage.Store) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	store, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	logDir := filepath.Join(dir, "logs")
	lg, err := logger.New(logDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lg.Close() })

	cfg := &config.Config{
		AgyPath:      "/usr/bin/agy",
		DefaultModel: "TestModel",
		Workers: []config.WorkerConfig{
			{Username: "alice", Enabled: true},
			{Username: "bob", Enabled: true},
		},
		Balancer: config.BalancerSettings{
			MaxRetries: 3,
		},
		RequestTimeout: 5 * time.Minute,
	}

	mock := &mockExecutor{}

	cmd := &PrintCommand{
		cfg:    cfg,
		store:  store,
		sched:  scheduler.New(cfg, store),
		logger: lg,
		execFn: mock.Execute,
		idFn:   func() string { return "legacy-test-request" },
	}

	return cmd, mock, store
}

func TestPrintCommand_Success(t *testing.T) {
	cmd, mock, _ := setupPrintTest(t)
	mock.results = []*executor.Result{
		{Output: "Hello from agy", Duration: 100 * time.Millisecond},
	}

	output, err := cmd.Run(context.Background(), "test prompt", "", nil, "", "auto", "/tmp/gemini-router-test", false)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if output != "Hello from agy" {
		t.Errorf("output = %q, want %q", output, "Hello from agy")
	}
}

func TestPrintCommand_RetryOnRateLimit(t *testing.T) {
	cmd, mock, _ := setupPrintTest(t)
	mock.results = []*executor.Result{
		{
			Output:   "",
			Duration: 100 * time.Millisecond,
			Error: &executor.AgyError{
				Type:          executor.ErrorRateLimited,
				Message:       "rate limit",
				ResetDuration: 5 * time.Minute,
			},
		},
		{Output: "Success after retry", Duration: 200 * time.Millisecond},
	}

	output, err := cmd.Run(context.Background(), "test prompt", "", nil, "", "auto", "/tmp/gemini-router-test", false)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if output != "Success after retry" {
		t.Errorf("output = %q, want %q", output, "Success after retry")
	}
}

func TestPrintCommand_FailFast(t *testing.T) {
	cmd, mock, store := setupPrintTest(t)

	// All workers are unavailable because of breaker state.
	now := time.Now().UTC()
	for _, w := range []string{"alice", "bob"} {
		if err := store.PutControllerState(storage.ControllerState{Worker: w, Model: "TestModel", Rate: 2, StableRate: 2, Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 4, QuotaBlockedUntil: now.Add(5 * time.Minute), RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}

	mock.results = []*executor.Result{}

	_, err := cmd.Run(context.Background(), "test prompt", "", nil, "", "failfast", "/tmp/gemini-router-test", false)
	if err == nil {
		t.Fatal("expected error with failfast strategy")
	}
}

func TestPrintCommand_UsesModel(t *testing.T) {
	cmd, mock, _ := setupPrintTest(t)
	mock.results = []*executor.Result{
		{Output: "response", Duration: 100 * time.Millisecond},
	}

	_, err := cmd.Run(context.Background(), "test prompt", "CustomModel", nil, "", "auto", "/tmp/gemini-router-test", false)
	if err != nil {
		t.Fatal(err)
	}
	if mock.lastArgs.Model != "CustomModel" {
		t.Errorf("model = %q, want %q", mock.lastArgs.Model, "CustomModel")
	}
}

func TestPrintCommand_DefaultModel(t *testing.T) {
	cmd, mock, _ := setupPrintTest(t)
	mock.results = []*executor.Result{
		{Output: "response", Duration: 100 * time.Millisecond},
	}

	_, err := cmd.Run(context.Background(), "test prompt", "", nil, "", "auto", "/tmp/gemini-router-test", false)
	if err != nil {
		t.Fatal(err)
	}
	if mock.lastArgs.Model != "TestModel" {
		t.Errorf("model = %q, want default %q", mock.lastArgs.Model, "TestModel")
	}
}

func TestPrintCommand_WaitStrategy(t *testing.T) {
	cmd, mock, store := setupPrintTest(t)

	now := time.Now().UTC()
	if err := store.PutControllerState(storage.ControllerState{Worker: "alice", Model: "TestModel", Rate: 2, StableRate: 2, Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 4, QuotaBlockedUntil: now.Add(time.Minute), RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}

	mock.results = []*executor.Result{
		{Output: "Success after waiting", Duration: 100 * time.Millisecond},
	}

	// This should block, wait for ~10ms, then retry and succeed.
	output, err := cmd.Run(context.Background(), "test prompt", "", nil, "", "wait", "/tmp/gemini-router-test", false)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if output != "Success after waiting" {
		t.Errorf("output = %q, want %q", output, "Success after waiting")
	}
}
