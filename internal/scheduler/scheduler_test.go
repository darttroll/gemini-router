package scheduler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/control"
	"github.com/darttroll/gemini-router/internal/storage"
)

func schedulerTestConfig() *config.Config {
	return &config.Config{
		Workers:        []config.WorkerConfig{{Username: "alice", Enabled: true}},
		Queue:          config.QueueSettings{MaxDelay: 10 * time.Second, MaxDepth: 1000, PollInterval: 5 * time.Millisecond},
		Adaptive:       config.AdaptiveSettings{MinRate: 0.2, InitialRate: 2, MaxRate: 100, RateBurst: 100, CubicBeta: 0.7, CubicC: 0.4, RateWindowSuccesses: 5, InitialConcurrency: 4, MaxConcurrency: 32, ConcurrencyWindow: 10, RetryBudgetCapacity: 10, RetryBudgetRefillPerSec: 0.1},
		RequestTimeout: time.Minute,
	}
}

func newSchedulerStore(t *testing.T) *storage.Store {
	t.Helper()
	p := filepath.Join(t.TempDir(), "test.db")
	s, err := storage.New(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func seedController(t *testing.T, st *storage.Store, rate float64, tokens float64, concurrency int) {
	t.Helper()
	now := time.Now().UTC()
	err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: rate, StableRate: rate, RMax: rate, Tokens: tokens, TokenUpdatedAt: now, ConcurrencyLimit: concurrency, RetryTokens: 10, RetryUpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionRejectsExcessDelayButUnboundedAccepts(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	seedController(t, st, 1, 100, 1)
	now := time.Now().UTC()
	for i := 0; i < 20; i++ {
		if err := st.EnqueueRequest(storage.PendingRequest{RequestID: fmtID(i), Model: "m", PID: os.Getpid(), EnqueuedAt: now.Add(time.Duration(i) * time.Millisecond), Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.InsertRequestLog(storage.RequestLogEntry{Worker: "alice", Model: "m", StartedAt: now.Add(-time.Second), FinishedAt: now, DurationMs: 1000, Status: "success"}); err != nil {
		t.Fatal(err)
	}
	sch := New(cfg, st)
	bounded := storage.PendingRequest{RequestID: "bounded", Model: "m", PID: os.Getpid(), EnqueuedAt: now, Deadline: now.Add(time.Minute)}
	adm, err := sch.Admit(context.Background(), bounded)
	if err != nil {
		t.Fatal(err)
	}
	if adm.Accepted {
		t.Fatalf("bounded unexpectedly accepted: %+v", adm)
	}
	unbounded := bounded
	unbounded.RequestID = "unbounded"
	unbounded.Unbounded = true
	adm, err = sch.Admit(context.Background(), unbounded)
	if err != nil {
		t.Fatal(err)
	}
	if !adm.Accepted {
		t.Fatalf("unbounded rejected: %+v", adm)
	}
}

func TestAdmissionRejectsExpiredDeadlineEvenUnbounded(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	sch := New(cfg, st)
	adm, err := sch.Admit(context.Background(), storage.PendingRequest{RequestID: "expired", Model: "m", PID: os.Getpid(), Deadline: time.Now().Add(-time.Second), Unbounded: true})
	if err != nil {
		t.Fatal(err)
	}
	if adm.Accepted {
		t.Fatalf("expired request accepted: %+v", adm)
	}
}

func TestAtomicReservationNeverExceedsConcurrencyLimit(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "race.db")
	base, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := schedulerTestConfig()
	seedController(t, base, 100, 100, 4)
	now := time.Now().UTC()
	const n = 20
	for i := 0; i < n; i++ {
		if err := base.EnqueueRequest(storage.PendingRequest{RequestID: fmtID(i), Model: "m", PID: os.Getpid(), EnqueuedAt: now.Add(time.Duration(i) * time.Millisecond), Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
			t.Fatal(err)
		}
	}
	base.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	successes := make(chan Lease, n)
	done := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, err := storage.New(dbPath)
			if err != nil {
				return
			}
			defer st.Close()
			sch := New(cfg, st)
			lease, err := sch.Acquire(ctx, fmtID(i))
			if err == nil {
				successes <- lease
			}
		}(i)
	}
	go func() { wg.Wait(); close(done) }()
	got := 0
	ids := map[string]bool{}
	for got < 4 {
		select {
		case l := <-successes:
			if ids[l.RequestID] {
				t.Fatalf("duplicate lease for %s", l.RequestID)
			}
			ids[l.RequestID] = true
			got++
		case <-done:
			t.Fatalf("workers stopped after %d leases, want 4", got)
		case <-ctx.Done():
			t.Fatalf("timed out after %d leases", got)
		}
	}
	cancel()
	<-done
	for {
		select {
		case l := <-successes:
			if l.RequestID != "" {
				got++
				if ids[l.RequestID] {
					t.Fatalf("duplicate lease for %s", l.RequestID)
				}
				ids[l.RequestID] = true
			}
		default:
			goto drained
		}
	}
drained:
	if got != 4 {
		t.Fatalf("successful leases=%d want 4", got)
	}
	check, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	active, err := check.CountActiveRequests("alice")
	if err != nil {
		t.Fatal(err)
	}
	if active != 4 {
		t.Fatalf("active=%d want 4", active)
	}
}

func fmtID(i int) string { return fmt.Sprintf("req-%03d", i) }

func TestRetryBudgetIsPersistedAtomically(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	cfg.Adaptive.RetryBudgetCapacity = 1
	cfg.Adaptive.RetryBudgetRefillPerSec = 0
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 2, StableRate: 2, Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 4, RetryTokens: 1, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	sch := New(cfg, st)
	ok, err := sch.TryConsumeRetry(context.Background(), "alice", "m")
	if err != nil || !ok {
		t.Fatalf("first retry ok=%v err=%v", ok, err)
	}
	ok, err = sch.TryConsumeRetry(context.Background(), "alice", "m")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("second retry unexpectedly allowed")
	}
}

func TestAuthFailureBlocksProviderWithoutChangingLearnedRate(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 20, StableRate: 20, RMax: 22, Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 6, RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueRequest(storage.PendingRequest{RequestID: "auth-1", Model: "m", PID: os.Getpid(), EnqueuedAt: now, Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
		t.Fatal(err)
	}
	sch := New(cfg, st)
	lease, err := sch.Acquire(context.Background(), "auth-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := sch.Release(context.Background(), lease, Outcome{Class: control.AuthOrEligibilityFailure}); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate != 20 || got.ConcurrencyLimit != 6 {
		t.Fatalf("learned state changed: %+v", got)
	}
	if got.QuotaBlockedUntil.Before(time.Now().Add(23 * time.Hour)) {
		t.Fatalf("provider not blocked long enough: %v", got.QuotaBlockedUntil)
	}
}

func TestSchedulerPrefersFasterWorkerWhenBusyAtEqualUtilization(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "speed.db")
	st, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := schedulerTestConfig()
	cfg.Workers = []config.WorkerConfig{{Username: "alice", Enabled: true}, {Username: "bob", Enabled: true}}
	now := time.Now().UTC()
	for _, w := range []string{"alice", "bob"} {
		if err := st.PutControllerState(storage.ControllerState{Worker: w, Model: "m", Rate: 100, StableRate: 100, Tokens: 100, TokenUpdatedAt: now, ConcurrencyLimit: 4, RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.InsertRequestLog(storage.RequestLogEntry{Worker: "alice", Model: "m", StartedAt: now.Add(-time.Second), FinishedAt: now, DurationMs: 2000, Status: "success"}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertRequestLog(storage.RequestLogEntry{Worker: "bob", Model: "m", StartedAt: now.Add(-time.Second), FinishedAt: now, DurationMs: 100, Status: "success"}); err != nil {
		t.Fatal(err)
	}
	tx, err := st.BeginImmediate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, worker := range []string{"alice", "bob"} {
		if _, err := tx.InsertActiveRequest(storage.PendingRequest{RequestID: "busy-" + worker, Model: "m", PID: os.Getpid()}, worker, now.Add(-time.Second)); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueRequest(storage.PendingRequest{RequestID: "speed", Model: "m", PID: os.Getpid(), EnqueuedAt: now, Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
		t.Fatal(err)
	}
	lease, err := New(cfg, st).Acquire(context.Background(), "speed")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Worker != "bob" {
		t.Fatalf("worker=%s want faster bob", lease.Worker)
	}
}

func TestSchedulerDoesNotStarveIdleWorkersWithDifferentLearnedConcurrency(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	cfg.Workers = []config.WorkerConfig{
		{Username: "alice", Enabled: true},
		{Username: "bob", Enabled: true},
		{Username: "carol", Enabled: true},
	}
	// Keep the learned limits fixed during this regression test. The production
	// failure was caused by selection feeding back into learning, not by the
	// learning functions themselves.
	cfg.Adaptive.RateWindowSuccesses = 1000
	cfg.Adaptive.ConcurrencyWindow = 1000

	now := time.Now().UTC()
	limits := map[string]int{"alice": 11, "bob": 4, "carol": 4}
	for _, worker := range []string{"alice", "bob", "carol"} {
		if err := st.PutControllerState(storage.ControllerState{
			Worker: worker, Model: "m", Rate: 100, StableRate: 100,
			Tokens: 100, TokenUpdatedAt: now, ConcurrencyLimit: limits[worker],
			RetryTokens: 10, RetryUpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
		if err := st.InsertRequestLog(storage.RequestLogEntry{
			Worker: worker, Model: "m", StartedAt: now.Add(-time.Minute), FinishedAt: now.Add(-55 * time.Second),
			DurationMs: 5000, Status: "success",
		}); err != nil {
			t.Fatal(err)
		}
	}

	sch := New(cfg, st)
	counts := map[string]int{}
	for i := 0; i < 9; i++ {
		requestID := fmt.Sprintf("fair-%d", i)
		if err := st.EnqueueRequest(storage.PendingRequest{
			RequestID: requestID, Model: "m", PID: os.Getpid(),
			EnqueuedAt: now.Add(time.Duration(i) * time.Millisecond),
			Deadline:   now.Add(time.Minute), Unbounded: true,
		}); err != nil {
			t.Fatal(err)
		}
		lease, err := sch.Acquire(context.Background(), requestID)
		if err != nil {
			t.Fatal(err)
		}
		counts[lease.Worker]++
		if err := sch.Release(context.Background(), lease, Outcome{Class: control.Success, Duration: 5 * time.Second}); err != nil {
			t.Fatal(err)
		}
		started := now.Add(-20*time.Second + time.Duration(i)*time.Second)
		if err := st.InsertRequestLog(storage.RequestLogEntry{
			Worker: lease.Worker, Model: "m", StartedAt: started, FinishedAt: started.Add(5 * time.Second),
			DurationMs: 5000, Status: "success",
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, worker := range []string{"alice", "bob", "carol"} {
		if counts[worker] != 3 {
			t.Fatalf("unfair idle distribution: counts=%v, want 3 requests per worker", counts)
		}
	}
}

func TestTransientFailureReducesConcurrencyButNotRate(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 20, StableRate: 20, RMax: 20, Tokens: 10, TokenUpdatedAt: now, ConcurrencyLimit: 10, RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueRequest(storage.PendingRequest{RequestID: "drop", Model: "m", PID: os.Getpid(), EnqueuedAt: now, Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
		t.Fatal(err)
	}
	sch := New(cfg, st)
	lease, err := sch.Acquire(context.Background(), "drop")
	if err != nil {
		t.Fatal(err)
	}
	if err := sch.Release(context.Background(), lease, Outcome{Class: control.TransientUpstreamFailure}); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate != 20 {
		t.Fatalf("transient drop changed rate=%v", got.Rate)
	}
	if got.ConcurrencyLimit >= 10 {
		t.Fatalf("transient drop did not reduce concurrency: %d", got.ConcurrencyLimit)
	}
}

func TestSuccessAfterExpiredBreakerClearsProbeHistory(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 20, StableRate: 20, RMax: 20, Tokens: 10, TokenUpdatedAt: now, ConcurrencyLimit: 6, QuotaBlockedUntil: now.Add(-time.Minute), QuotaProbeCount: 24, RetryTokens: 5, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueRequest(storage.PendingRequest{RequestID: "recovered", Model: "m", PID: os.Getpid(), EnqueuedAt: now, Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
		t.Fatal(err)
	}
	sch := New(cfg, st)
	lease, err := sch.Acquire(context.Background(), "recovered")
	if err != nil {
		t.Fatal(err)
	}
	if err := sch.Release(context.Background(), lease, Outcome{Class: control.Success, Duration: time.Second}); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if !got.QuotaBlockedUntil.IsZero() || got.QuotaProbeCount != 0 {
		t.Fatalf("stale quota recovery state remains: %+v", got)
	}
}

func TestSuccessRefillsOneRetryBudgetToken(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	cfg.Adaptive.RetryBudgetCapacity = 10
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 20, StableRate: 20, RMax: 20, Tokens: 10, TokenUpdatedAt: now, ConcurrencyLimit: 6, RetryTokens: 3, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueRequest(storage.PendingRequest{RequestID: "retry-recover", Model: "m", PID: os.Getpid(), EnqueuedAt: now, Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
		t.Fatal(err)
	}
	sch := New(cfg, st)
	lease, err := sch.Acquire(context.Background(), "retry-recover")
	if err != nil {
		t.Fatal(err)
	}
	if err := sch.Release(context.Background(), lease, Outcome{Class: control.Success, Duration: time.Second}); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if got.RetryTokens != 4 {
		t.Fatalf("retry tokens=%v want 4", got.RetryTokens)
	}
}

func TestSchedulerSkipsWorkerWithFreshZeroQuotaSnapshot(t *testing.T) {
	cfg := schedulerTestConfig()
	cfg.Quota = config.QuotaSettings{Enabled: true}
	st := newSchedulerStore(t)
	cfg.Workers = []config.WorkerConfig{{Username: "alice", Enabled: true}, {Username: "bob", Enabled: true}}
	now := time.Now().UTC()
	if err := st.ReplaceQuotaSnapshots("alice", []storage.QuotaSnapshot{{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "5h", RemainingFraction: 0, ResetTime: now.Add(time.Hour), FetchedAt: now}}); err != nil {
		t.Fatal(err)
	}
	req := storage.PendingRequest{RequestID: "quota-skip", Model: "gemini-3.6-flash-high", PID: os.Getpid(), EnqueuedAt: now, Deadline: now.Add(time.Minute), Unbounded: true}
	sch := New(cfg, st)
	if _, err := sch.Admit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	lease, err := sch.Acquire(context.Background(), req.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Worker != "bob" {
		t.Fatalf("worker=%s want bob", lease.Worker)
	}
}

func TestSchedulerIgnoresCachedQuotaWhenWorkerQuotaTelemetryDisabled(t *testing.T) {
	cfg := schedulerTestConfig()
	no := false
	cfg.Quota = config.QuotaSettings{Enabled: true}
	cfg.Workers = []config.WorkerConfig{{Username: "alice", Enabled: true, QuotaEnabled: &no}}
	st := newSchedulerStore(t)
	now := time.Now().UTC()
	if err := st.ReplaceQuotaSnapshots("alice", []storage.QuotaSnapshot{{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "5h", RemainingFraction: 0, ResetTime: now.Add(time.Hour), FetchedAt: now}}); err != nil {
		t.Fatal(err)
	}
	req := storage.PendingRequest{RequestID: "quota-disabled", Model: "gemini-x", PID: os.Getpid(), EnqueuedAt: now, Deadline: now.Add(time.Minute), Unbounded: true}
	sch := New(cfg, st)
	if _, err := sch.Admit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	lease, err := sch.Acquire(context.Background(), req.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Worker != "alice" {
		t.Fatalf("worker=%s", lease.Worker)
	}
}

func TestEstimateNewRequestIncludesQueueAndBusySlots(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	cfg.Adaptive.InitialRate = 100
	cfg.Adaptive.InitialConcurrency = 1
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 100, StableRate: 100, RMax: 100, Tokens: 10, TokenUpdatedAt: now, ConcurrencyLimit: 1, RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	tx, err := st.BeginImmediate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.InsertActiveRequest(storage.PendingRequest{RequestID: "active", Model: "m", PID: os.Getpid()}, "alice", now); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	sch := New(cfg, st)
	est, err := sch.EstimateNewRequest("m")
	if err != nil {
		t.Fatal(err)
	}
	if est.QueueDepth != 0 {
		t.Fatalf("depth=%d want 0", est.QueueDepth)
	}
	if est.PredictedWait <= 0 {
		t.Fatalf("wait=%v want >0 with the only slot busy", est.PredictedWait)
	}
}

func TestEstimateNewRequestUsesNearestQuotaResetWhenAllProvidersBlocked(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	now := time.Now().UTC()
	reset := now.Add(5 * time.Minute)
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 10, StableRate: 10, RMax: 10, Tokens: 10, TokenUpdatedAt: now, ConcurrencyLimit: 4, QuotaBlockedUntil: reset, RetryTokens: 10, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	est, err := New(cfg, st).EstimateNewRequest("m")
	if err != nil {
		t.Fatal(err)
	}
	if est.EffectiveCapacityRPS != 0 {
		t.Fatalf("capacity=%v want 0", est.EffectiveCapacityRPS)
	}
	if est.PredictedWait < 4*time.Minute || est.PredictedWait > 6*time.Minute {
		t.Fatalf("wait=%v want about 5m", est.PredictedWait)
	}
}

func TestEstimateNewRequestUsesModelScopedQueueDepth(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	now := time.Now().UTC()
	seedController(t, st, 100, 100, 4)
	for i := 0; i < 5; i++ {
		if err := st.EnqueueRequest(storage.PendingRequest{RequestID: fmt.Sprintf("other-%d", i), Model: "other", PID: os.Getpid(), EnqueuedAt: now, Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
			t.Fatal(err)
		}
	}
	est, err := New(cfg, st).EstimateNewRequest("m")
	if err != nil {
		t.Fatal(err)
	}
	if est.QueueDepth != 5 || est.ModelQueueDepth != 0 {
		t.Fatalf("depths total=%d model=%d", est.QueueDepth, est.ModelQueueDepth)
	}
	if est.PredictedWait != 0 {
		t.Fatalf("other model queue inflated wait: %v", est.PredictedWait)
	}
}

func TestReleaseIsIdempotentAndDoesNotTrainTwice(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	now := time.Now().UTC()
	if err := st.PutControllerState(storage.ControllerState{Worker: "alice", Model: "m", Rate: 20, StableRate: 20, Tokens: 10, TokenUpdatedAt: now, ConcurrencyLimit: 6, RetryTokens: 3, RetryUpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueRequest(storage.PendingRequest{RequestID: "idem-release", Model: "m", PID: os.Getpid(), Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
		t.Fatal(err)
	}
	sch := New(cfg, st)
	lease, err := sch.Acquire(context.Background(), "idem-release")
	if err != nil {
		t.Fatal(err)
	}
	outcome := Outcome{Class: control.Success, Duration: time.Second}
	if err := sch.Release(context.Background(), lease, outcome); err != nil {
		t.Fatal(err)
	}
	first, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if err := sch.Release(context.Background(), lease, outcome); err != nil {
		t.Fatal(err)
	}
	second, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if first.RetryTokens != second.RetryTokens || first.SuccessesInWindow != second.SuccessesInWindow || first.ConcurrencySamples != second.ConcurrencySamples {
		t.Fatalf("second release trained controller again: first=%+v second=%+v", first, second)
	}
}

func TestConcurrentAdmissionHonorsGlobalHardQueueLimit(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "admission.db")
	base, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := schedulerTestConfig()
	cfg.Queue.MaxDepth = 1
	seedController(t, base, 100, 100, 4)
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}

	const n = 12
	var wg sync.WaitGroup
	accepted := make(chan bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, err := storage.New(dbPath)
			if err != nil {
				accepted <- false
				return
			}
			defer st.Close()
			req := storage.PendingRequest{
				RequestID: fmt.Sprintf("admit-%02d", i), Model: "m", PID: os.Getpid(),
				Deadline: time.Now().Add(time.Minute),
			}
			adm, err := New(cfg, st).Admit(context.Background(), req)
			accepted <- err == nil && adm.Accepted
		}(i)
	}
	wg.Wait()
	close(accepted)
	count := 0
	for ok := range accepted {
		if ok {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("accepted=%d want 1", count)
	}

	check, err := storage.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	depth, err := check.QueueDepth()
	if err != nil {
		t.Fatal(err)
	}
	if depth != 1 {
		t.Fatalf("queue depth=%d want 1", depth)
	}
}

func TestLocalPreparationFailureRefundsStartTokenWithoutTrainingProvider(t *testing.T) {
	st := newSchedulerStore(t)
	cfg := schedulerTestConfig()
	cfg.Adaptive.RateBurst = 3
	now := time.Now().UTC()
	before := storage.ControllerState{
		Worker: "alice", Model: "m", Rate: 20, StableRate: 20, RMax: 20,
		Tokens: 3, TokenUpdatedAt: now, ConcurrencyLimit: 4,
		RetryTokens: 7, RetryUpdatedAt: now,
	}
	if err := st.PutControllerState(before); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueRequest(storage.PendingRequest{RequestID: "local-prep", Model: "m", PID: os.Getpid(), EnqueuedAt: now, Deadline: now.Add(time.Minute), Unbounded: true}); err != nil {
		t.Fatal(err)
	}
	sch := New(cfg, st)
	lease, err := sch.Acquire(context.Background(), "local-prep")
	if err != nil {
		t.Fatal(err)
	}
	mid, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if mid.Tokens >= before.Tokens {
		t.Fatalf("acquire did not consume start token: before=%v mid=%v", before.Tokens, mid.Tokens)
	}
	if err := sch.Release(context.Background(), lease, Outcome{Class: control.LocalPreparationFailure}); err != nil {
		t.Fatal(err)
	}
	after, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if after.Tokens != before.Tokens || after.Rate != before.Rate || after.ConcurrencyLimit != before.ConcurrencyLimit || after.RetryTokens != before.RetryTokens {
		t.Fatalf("local preparation changed provider state: before=%+v after=%+v", before, after)
	}
}
