package commands

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/executor"
	"github.com/darttroll/gemini-router/internal/logger"
	"github.com/darttroll/gemini-router/internal/storage"
)

type adaptiveMockExecutor struct {
	mu       sync.Mutex
	results  []*executor.Result
	errors   []error
	requests []executor.Request
}

func (m *adaptiveMockExecutor) Execute(ctx context.Context, req executor.Request) (*executor.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, req)
	i := len(m.requests) - 1
	if i < len(m.errors) && m.errors[i] != nil {
		return nil, m.errors[i]
	}
	if i < len(m.results) {
		return m.results[i], nil
	}
	return &executor.Result{Output: "ok", Duration: 10 * time.Millisecond}, nil
}

func adaptivePrintConfig(workers ...string) *config.Config {
	wc := make([]config.WorkerConfig, 0, len(workers))
	for _, w := range workers {
		wc = append(wc, config.WorkerConfig{Username: w, Enabled: true})
	}
	return &config.Config{
		AgyPath: "/usr/local/bin/agy", DefaultModel: "m", RequestTimeout: time.Second,
		Workers:  wc,
		Balancer: config.BalancerSettings{MaxRetries: 3},
		Queue:    config.QueueSettings{MaxDelay: 200 * time.Millisecond, MaxDepth: 100, PollInterval: 5 * time.Millisecond},
		Adaptive: config.AdaptiveSettings{MinRate: .2, InitialRate: 20, MaxRate: 100, RateBurst: 3, CubicBeta: .7, CubicC: .4, RateWindowSuccesses: 5, InitialConcurrency: 4, MaxConcurrency: 32, ConcurrencyWindow: 10, RetryBudgetCapacity: 10, RetryBudgetRefillPerSec: 0},
	}
}

func setupAdaptivePrint(t *testing.T, cfg *config.Config) (*PrintCommand, *adaptiveMockExecutor, *storage.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := storage.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	lg, err := logger.New(filepath.Join(dir, "logs"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lg.Close() })
	cmd := NewPrintCommand(cfg, st, lg)
	mock := &adaptiveMockExecutor{}
	cmd.execFn = mock.Execute
	cmd.idFn = func() string { return "req-fixed" }
	return cmd, mock, st
}

func TestAdaptivePrintPreservesRequestIDAcrossRetry(t *testing.T) {
	cmd, mock, _ := setupAdaptivePrint(t, adaptivePrintConfig("alice", "bob"))
	mock.results = []*executor.Result{
		{Duration: 10 * time.Millisecond, Error: &executor.AgyError{Type: executor.ErrorRateLimited, Message: "short 429"}},
		{Output: "success", Duration: 10 * time.Millisecond},
	}
	out, err := cmd.Run(context.Background(), "prompt", "", nil, "", "auto", "/tmp/out", false)
	if err != nil {
		t.Fatal(err)
	}
	if out != "success" {
		t.Fatalf("out=%q", out)
	}
	if len(mock.requests) != 2 {
		t.Fatalf("requests=%d", len(mock.requests))
	}
	if mock.requests[0].ID != "req-fixed" || mock.requests[1].ID != "req-fixed" {
		t.Fatalf("ids=%q,%q", mock.requests[0].ID, mock.requests[1].ID)
	}
	if mock.requests[0].Worker == mock.requests[1].Worker {
		t.Fatalf("retry stayed on throttled worker %s", mock.requests[0].Worker)
	}
}

func TestAdaptivePrintLongQuotaDoesNotReduceLearnedRate(t *testing.T) {
	cfg := adaptivePrintConfig("alice", "bob")
	cmd, mock, st := setupAdaptivePrint(t, cfg)
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 20, StableRate: 20, RMax: 22, Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 6, RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	mock.results = []*executor.Result{{Duration: 10 * time.Millisecond, Error: &executor.AgyError{Type: executor.ErrorLongQuota, Message: "quota", ResetDuration: time.Hour}}, {Output: "ok", Duration: 10 * time.Millisecond}}
	if _, err := cmd.Run(context.Background(), "prompt", "", nil, "", "auto", "/tmp/out", false); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate != 20 || got.ConcurrencyLimit != 6 {
		t.Fatalf("long quota changed learned state: %+v", got)
	}
	if got.QuotaBlockedUntil.Before(time.Now().Add(59 * time.Minute)) {
		t.Fatalf("quota block=%v", got.QuotaBlockedUntil)
	}
}

func TestAdaptivePrintBoundedRejectsUnavailableProvider(t *testing.T) {
	cfg := adaptivePrintConfig("alice")
	cmd, mock, st := setupAdaptivePrint(t, cfg)
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 20, StableRate: 20, Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 4, QuotaBlockedUntil: now.Add(time.Hour), RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	_, err := cmd.Run(context.Background(), "prompt", "", nil, "", "auto", "/tmp/out", false)
	if err == nil || !strings.Contains(err.Error(), "router overloaded") {
		t.Fatalf("err=%v", err)
	}
	if len(mock.requests) != 0 {
		t.Fatalf("executor called %d times", len(mock.requests))
	}
}

func TestAdaptivePrintUnboundedWaitsAndCancellationRemovesQueueEntry(t *testing.T) {
	cfg := adaptivePrintConfig("alice")
	cmd, _, st := setupAdaptivePrint(t, cfg)
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 20, StableRate: 20, Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 4, QuotaBlockedUntil: now.Add(time.Hour), RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err := cmd.Run(ctx, "prompt", "", nil, "", "auto", "/tmp/out", true)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v want deadline", err)
	}
	depth, _ := st.QueueDepth()
	if depth != 0 {
		t.Fatalf("queue depth after cancellation=%d", depth)
	}
}

func TestAdaptivePrintRetryBudgetStopsRetryStorm(t *testing.T) {
	cfg := adaptivePrintConfig("alice")
	cfg.Adaptive.RetryBudgetCapacity = 1
	cfg.Adaptive.RetryBudgetRefillPerSec = 0
	cmd, mock, st := setupAdaptivePrint(t, cfg)
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 20, StableRate: 20, Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 4, RetryTokens: 0, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	mock.results = []*executor.Result{{Duration: 10 * time.Millisecond, Error: &executor.AgyError{Type: executor.ErrorNonZeroExit, Message: "transient"}}}
	_, err := cmd.Run(context.Background(), "prompt", "", nil, "", "auto", "/tmp/out", false)
	if err == nil || !strings.Contains(err.Error(), "retry budget exhausted") {
		t.Fatalf("err=%v", err)
	}
	if len(mock.requests) != 1 {
		t.Fatalf("executor calls=%d want 1", len(mock.requests))
	}
}

func TestAdaptivePrintSuccessDoesNotClearNewerQuotaBreaker(t *testing.T) {
	cfg := adaptivePrintConfig("alice")
	cmd, _, st := setupAdaptivePrint(t, cfg)
	now := time.Now().UTC()
	cmd.execFn = func(ctx context.Context, req executor.Request) (*executor.Result, error) {
		cur, err := st.GetControllerState(req.Worker, req.Model, storage.ControllerState{})
		if err != nil {
			return nil, err
		}
		cur.QuotaBlockedUntil = time.Now().Add(time.Hour)
		if err := st.PutControllerState(cur); err != nil {
			return nil, err
		}
		return &executor.Result{Output: "ok", Duration: 10 * time.Millisecond}, nil
	}
	if _, err := cmd.Run(context.Background(), "prompt", "", nil, "", "auto", "/tmp/out", false); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if got.QuotaBlockedUntil.Before(now.Add(50 * time.Minute)) {
		t.Fatalf("newer breaker cleared: %+v", got)
	}
}

func TestAdaptivePrintReturnsSuccessfulOutputWhenReleaseFails(t *testing.T) {
	cfg := adaptivePrintConfig("alice")
	cmd, _, st := setupAdaptivePrint(t, cfg)
	calls := 0
	cmd.execFn = func(ctx context.Context, req executor.Request) (*executor.Result, error) {
		calls++
		_ = st.Close()
		return &executor.Result{Output: "already-generated", Duration: 10 * time.Millisecond}, nil
	}
	out, err := cmd.Run(context.Background(), "prompt", "", nil, "", "auto", "/tmp/out", false)
	if err != nil {
		t.Fatalf("successful upstream result must survive release failure: %v", err)
	}
	if out != "already-generated" {
		t.Fatalf("out=%q", out)
	}
	if calls != 1 {
		t.Fatalf("upstream calls=%d want 1", calls)
	}
}

func TestAdaptivePrintWaitStrategyQueuesUntilDeadline(t *testing.T) {
	cfg := adaptivePrintConfig("alice")
	cmd, _, st := setupAdaptivePrint(t, cfg)
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 20, StableRate: 20, Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 4, QuotaBlockedUntil: now.Add(time.Hour), RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := cmd.Run(ctx, "prompt", "", nil, "", "wait", "/tmp/out", false)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait strategy err=%v", err)
	}
}

func TestAdaptivePrintInvalidAttachmentDoesNotReserveRetryOrTrainController(t *testing.T) {
	cfg := adaptivePrintConfig("alice")
	cmd, mock, st := setupAdaptivePrint(t, cfg)
	now := time.Now().UTC()
	before := storage.ControllerState{
		Worker: "alice", Model: "m", Rate: 20, StableRate: 20, RMax: 20,
		Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 4,
		RetryTokens: 10, RetryUpdatedAt: now,
	}
	if err := st.PutControllerState(before); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(t.TempDir(), "invalid.fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}

	_, err := cmd.Run(context.Background(), "prompt", "", []string{fifo}, "", "auto", "/tmp/out", false)
	if err == nil || !strings.Contains(err.Error(), "only regular files") {
		t.Fatalf("err=%v", err)
	}
	if len(mock.requests) != 0 {
		t.Fatalf("executor calls=%d want 0; invalid input must fail before reservation", len(mock.requests))
	}
	after, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if after.ConcurrencyLimit != before.ConcurrencyLimit || after.RetryTokens != before.RetryTokens || after.Rate != before.Rate {
		t.Fatalf("invalid input trained controller: before=%+v after=%+v", before, after)
	}
	depth, err := st.QueueDepth()
	if err != nil {
		t.Fatal(err)
	}
	if depth != 0 {
		t.Fatalf("queue depth=%d want 0", depth)
	}
}

func TestAdaptivePrintLocalPreparationFailureIsTerminalWithoutProviderPenalty(t *testing.T) {
	cfg := adaptivePrintConfig("alice")
	cmd, mock, st := setupAdaptivePrint(t, cfg)
	now := time.Now().UTC()
	before := storage.ControllerState{
		Worker: "alice", Model: "m", Rate: 20, StableRate: 20, RMax: 20,
		Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 4,
		RetryTokens: 10, RetryUpdatedAt: now,
	}
	if err := st.PutControllerState(before); err != nil {
		t.Fatal(err)
	}
	mock.results = []*executor.Result{{Duration: 10 * time.Millisecond, Error: &executor.AgyError{Type: executor.ErrorLocalPreparation, Message: "local copy failed"}}}

	_, err := cmd.Run(context.Background(), "prompt", "", nil, "", "auto", "/tmp/out", false)
	if err == nil || !strings.Contains(err.Error(), "local copy failed") {
		t.Fatalf("err=%v", err)
	}
	if len(mock.requests) != 1 {
		t.Fatalf("executor calls=%d want 1", len(mock.requests))
	}
	after, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if after.ConcurrencyLimit != before.ConcurrencyLimit || after.RetryTokens != before.RetryTokens || after.Rate != before.Rate {
		t.Fatalf("local failure trained controller: before=%+v after=%+v", before, after)
	}
	if after.Tokens != before.Tokens {
		t.Fatalf("local failure consumed provider start token: before=%v after=%v", before.Tokens, after.Tokens)
	}
}
