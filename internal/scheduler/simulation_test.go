package scheduler

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/control"
	"github.com/darttroll/gemini-router/internal/storage"
)

func TestSimulationTransientShortThrottleRecoversNearPriorRate(t *testing.T) {
	cfg := schedulerTestConfig().Adaptive
	st := storage.ControllerState{Rate: 20, StableRate: 20, RMax: 20, Tokens: 3, ConcurrencyLimit: 8}
	st = control.OnShortThrottle(st, cfg)
	if st.Rate != 14 {
		t.Fatalf("rate after throttle=%v", st.Rate)
	}
	for i := 0; i < 2*cfg.RateWindowSuccesses; i++ {
		st = control.OnRateSuccess(st, cfg)
	}
	if st.Rate < 18 {
		t.Fatalf("rate after two clean windows=%v want >=18", st.Rate)
	}
	for i := 0; i < cfg.RateWindowSuccesses; i++ {
		st = control.OnRateSuccess(st, cfg)
	}
	if st.Rate < 19 {
		t.Fatalf("rate after three clean windows=%v want >=19", st.Rate)
	}
}

func TestSimulationSustainedOverloadBackpressuresBoundedButAllowsExplicitUnbounded(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	cfg.Queue.MaxDelay = 1500 * time.Millisecond
	cfg.Queue.MaxDepth = 100
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 1, StableRate: 1, RMax: 1, Tokens: 10, TokenUpdatedAt: now, ConcurrencyLimit: 1, RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertRequestLog(storage.RequestLogEntry{Worker: "alice", Model: "m", StartedAt: now.Add(-time.Second), FinishedAt: now, DurationMs: 1000, Status: "success"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := st.EnqueueRequest(storage.PendingRequest{RequestID: fmtID(100 + i), Model: "m", PID: os.Getpid(), EnqueuedAt: now.Add(time.Duration(i) * time.Millisecond), Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
			t.Fatal(err)
		}
	}
	sch := New(cfg, st)
	bounded := storage.PendingRequest{RequestID: "bounded-overload", Model: "m", PID: os.Getpid(), Deadline: now.Add(time.Minute)}
	adm, err := sch.Admit(context.Background(), bounded)
	if err != nil {
		t.Fatal(err)
	}
	if adm.Accepted {
		t.Fatalf("bounded request accepted under sustained overload: %+v", adm)
	}
	unbounded := bounded
	unbounded.RequestID = "unbounded-overload"
	unbounded.Unbounded = true
	adm, err = sch.Admit(context.Background(), unbounded)
	if err != nil {
		t.Fatal(err)
	}
	if !adm.Accepted {
		t.Fatalf("explicit unbounded request rejected: %+v", adm)
	}
}

func TestSimulationLongQuotaLeavesShortRateAndConcurrencyIntact(t *testing.T) {
	cfg := schedulerTestConfig()
	st := newSchedulerStore(t)
	now := time.Now().UTC()
	initial := storage.ControllerState{Worker: "alice", Model: "m", Rate: 20, StableRate: 19, RMax: 22, Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 7, RetryTokens: 10, RetryUpdatedAt: now}
	if err := st.PutControllerState(initial); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueRequest(storage.PendingRequest{RequestID: "quota-sim", Model: "m", PID: os.Getpid(), EnqueuedAt: now, Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
		t.Fatal(err)
	}
	sch := New(cfg, st)
	lease, err := sch.Acquire(context.Background(), "quota-sim")
	if err != nil {
		t.Fatal(err)
	}
	if err := sch.Release(context.Background(), lease, Outcome{Class: control.LongQuotaExhausted, ResetDuration: 5 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate != 20 || got.StableRate != 19 || got.RMax != 22 || got.ConcurrencyLimit != 7 {
		t.Fatalf("long quota poisoned adaptive state: %+v", got)
	}
	if got.QuotaBlockedUntil.Before(time.Now().Add(4*time.Hour + 59*time.Minute)) {
		t.Fatalf("breaker too short: %v", got.QuotaBlockedUntil)
	}
}

func TestSimulationCapacityIncreaseIsLearnedRatherThanFrozen(t *testing.T) {
	cfg := schedulerTestConfig().Adaptive
	st := storage.ControllerState{Rate: 10, StableRate: 10, RMax: 10, ConcurrencyLimit: 4}
	for window := 0; window < 8; window++ {
		for i := 0; i < cfg.RateWindowSuccesses; i++ {
			st = control.OnRateSuccess(st, cfg)
		}
	}
	if st.Rate <= 10 {
		t.Fatalf("rate never probed above old boundary: %v", st.Rate)
	}
	for i := 0; i < 3*cfg.ConcurrencyWindow; i++ {
		st = control.ObserveLatency(st, time.Second, cfg)
	}
	if st.ConcurrencyLimit <= 4 {
		t.Fatalf("concurrency never grew: %d", st.ConcurrencyLimit)
	}
}

func TestSimulationPersistentShortRateDropMovesBoundaryDown(t *testing.T) {
	cfg := schedulerTestConfig().Adaptive
	st := storage.ControllerState{Rate: 20, StableRate: 20, RMax: 20, Tokens: 3, ConcurrencyLimit: 8}
	st = control.OnShortThrottle(st, cfg) // one anomaly: retain old 20 boundary, back off to 14
	if st.RMax != 20 || st.Rate != 14 {
		t.Fatalf("first throttle=%+v", st)
	}
	st = control.OnShortThrottle(st, cfg) // repeated throttle below old boundary => fast convergence
	if st.RMax >= 15 {
		t.Fatalf("persistent lower limit did not move RMax down: %+v", st)
	}
	if st.Rate >= st.RMax {
		t.Fatalf("backed-off rate should be below new boundary: %+v", st)
	}
	for i := 0; i < 2*cfg.RateWindowSuccesses; i++ {
		st = control.OnRateSuccess(st, cfg)
	}
	if st.Rate >= 18 {
		t.Fatalf("controller raced back to obsolete 20 rps boundary: %+v", st)
	}
}
