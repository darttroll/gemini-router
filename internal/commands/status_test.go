package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/storage"
)

func TestStatusJSONIncludesAdaptiveLoadWithoutMutatingState(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := &config.Config{DefaultModel: "m", RequestTimeout: 5 * time.Minute, Workers: []config.WorkerConfig{{Username: "alice", Enabled: true}, {Username: "bob", Enabled: true}}, Queue: config.QueueSettings{MaxDelay: 10 * time.Second, MaxDepth: 1000, PollInterval: 100 * time.Millisecond}, Adaptive: config.AdaptiveSettings{MinRate: .2, InitialRate: 2, MaxRate: 100, RateBurst: 3, CubicBeta: .7, CubicC: .4, RateWindowSuccesses: 5, InitialConcurrency: 4, MaxConcurrency: 32, ConcurrencyWindow: 10, RetryBudgetCapacity: 10, RetryBudgetRefillPerSec: .1}}
	now := time.Now().UTC()
	alice := storage.ControllerState{Worker: "alice", Model: "m", Rate: 12, StableRate: 11, RMax: 13, Tokens: 2, TokenUpdatedAt: now, ConcurrencyLimit: 6, RetryTokens: 9, RetryUpdatedAt: now}
	bob := storage.ControllerState{Worker: "bob", Model: "m", Rate: 8, StableRate: 8, RMax: 9, Tokens: 2, TokenUpdatedAt: now, ConcurrencyLimit: 4, QuotaBlockedUntil: now.Add(time.Hour), RetryTokens: 9, RetryUpdatedAt: now}
	if err := st.PutControllerState(alice); err != nil {
		t.Fatal(err)
	}
	if err := st.PutControllerState(bob); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := st.EnqueueRequest(storage.PendingRequest{RequestID: fmt.Sprintf("q-%d", i), Model: "m", PID: 99999999, EnqueuedAt: now.Add(time.Duration(i) * time.Millisecond), Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range []storage.RequestLogEntry{{Worker: "alice", Model: "m", StartedAt: now.Add(-2 * time.Second), FinishedAt: now.Add(-1900 * time.Millisecond), DurationMs: 100, Status: "success"}, {Worker: "alice", Model: "m", StartedAt: now.Add(-time.Second), FinishedAt: now.Add(-800 * time.Millisecond), DurationMs: 200, Status: "short_throttle"}} {
		if err := st.InsertRequestLog(e); err != nil {
			t.Fatal(err)
		}
	}
	before, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	cmd := NewStatusCommand(cfg, st)
	var buf bytes.Buffer
	if err := cmd.Run(&buf, true); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("json=%q err=%v", buf.String(), err)
	}
	for _, key := range []string{"queue_depth", "model_queue_depth", "predicted_queue_wait_ms", "predicted_total_time_ms", "active_requests", "ready_providers", "quota_blocked_providers", "observed_throughput_rps", "load_factor", "providers"} {
		if _, ok := got[key]; !ok {
			t.Fatalf("missing %s in %v", key, got)
		}
	}
	providers, ok := got["providers"].([]any)
	if !ok || len(providers) != 2 {
		t.Fatalf("providers=%T %v", got["providers"], got["providers"])
	}
	after, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if before.Rate != after.Rate || before.Tokens != after.Tokens || before.ConcurrencyLimit != after.ConcurrencyLimit || !before.QuotaBlockedUntil.Equal(after.QuotaBlockedUntil) {
		t.Fatalf("status mutated state: before=%+v after=%+v", before, after)
	}
}

func TestStatusSnapshotIncludesQuotaWindows(t *testing.T) {
	cfg := &config.Config{DefaultModel: "gemini-3.6-flash-high", Workers: []config.WorkerConfig{{Username: "alice", Enabled: true}}, Adaptive: config.AdaptiveSettings{InitialRate: 2, InitialConcurrency: 4, RetryBudgetCapacity: 10}, Quota: config.QuotaSettings{Enabled: true}, RequestTimeout: time.Minute}
	st, err := storage.New(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC()
	if err := st.ReplaceQuotaSnapshots("alice", []storage.QuotaSnapshot{{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "5h", RemainingFraction: .25, ResetTime: now.Add(time.Hour), FetchedAt: now}, {Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "weekly", RemainingFraction: .75, ResetTime: now.Add(5 * 24 * time.Hour), FetchedAt: now}}); err != nil {
		t.Fatal(err)
	}
	snap, err := NewStatusCommand(cfg, st).Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Providers) != 1 || snap.Providers[0].FiveHourQuota == nil || snap.Providers[0].WeeklyQuota == nil {
		t.Fatalf("missing quota: %+v", snap.Providers)
	}
	if snap.Providers[0].FiveHourQuota.RemainingFraction != .25 || snap.Providers[0].WeeklyQuota.RemainingFraction != .75 {
		t.Fatalf("bad quota: %+v", snap.Providers[0])
	}
}

func TestStatusPredictsWaitWhenQueueEmptyButAllSlotsBusy(t *testing.T) {
	st, err := storage.New(filepath.Join(t.TempDir(), "busy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := &config.Config{
		DefaultModel: "m", RequestTimeout: time.Minute,
		Workers:  []config.WorkerConfig{{Username: "alice", Enabled: true}},
		Adaptive: config.AdaptiveSettings{InitialRate: 100, InitialConcurrency: 4, RetryBudgetCapacity: 10},
	}
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 100, StableRate: 100, RMax: 100, Tokens: 10, TokenUpdatedAt: now, ConcurrencyLimit: 4, RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	tx, err := st.BeginImmediate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		_, err = tx.InsertActiveRequest(storage.PendingRequest{RequestID: fmt.Sprintf("active-%d", i), Model: "m", PID: os.Getpid()}, "alice", now)
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	snap, err := NewStatusCommand(cfg, st).Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snap.QueueDepth != 0 {
		t.Fatalf("queue=%d want 0", snap.QueueDepth)
	}
	if snap.PredictedQueueWaitMs <= 0 {
		t.Fatalf("predicted wait=%dms want >0 while all slots busy", snap.PredictedQueueWaitMs)
	}
}
