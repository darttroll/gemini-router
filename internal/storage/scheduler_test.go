package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPendingRequestLifecycle(t *testing.T) {
	s := newTestStore(t)
	deadline := time.Now().Add(time.Minute).UTC().Truncate(time.Millisecond)
	req := PendingRequest{RequestID: "req-1", Model: "m", PID: os.Getpid(), EnqueuedAt: time.Now().UTC(), Deadline: deadline, Unbounded: true}
	if err := s.EnqueueRequest(req); err != nil {
		t.Fatal(err)
	}
	depth, err := s.QueueDepth()
	if err != nil {
		t.Fatal(err)
	}
	if depth != 1 {
		t.Fatalf("depth=%d want 1", depth)
	}
	got, err := s.GetPendingRequest("req-1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.RequestID != req.RequestID || got.Model != "m" || !got.Unbounded || got.Deadline.UnixMilli() != deadline.UnixMilli() {
		t.Fatalf("got=%+v", got)
	}
	if err := s.RemovePendingRequest("req-1"); err != nil {
		t.Fatal(err)
	}
	depth, _ = s.QueueDepth()
	if depth != 0 {
		t.Fatalf("depth=%d want 0", depth)
	}
}

func TestControllerStateRoundTrip(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	in := ControllerState{Worker: "alice", Model: "m", Rate: 12.5, StableRate: 11, RMax: 14, Tokens: 2.25, TokenUpdatedAt: now, RecoveryWindow: 3, SuccessesInWindow: 4, ShortEWMA: 1.2, LongEWMA: 1.1, ConcurrencyLimit: 6, ConcurrencySamples: 7, QuotaBlockedUntil: now.Add(time.Hour), QuotaProbeCount: 2, RetryTokens: 8.5, RetryUpdatedAt: now}
	if err := s.PutControllerState(in); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetControllerState("alice", "m", ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Worker != in.Worker || got.Model != in.Model || got.Rate != in.Rate || got.RMax != in.RMax || got.ConcurrencyLimit != 6 || got.RetryTokens != 8.5 || got.QuotaProbeCount != 2 {
		t.Fatalf("got=%+v want=%+v", got, in)
	}
	if got.TokenUpdatedAt.UnixMilli() != now.UnixMilli() || got.QuotaBlockedUntil.UnixMilli() != now.Add(time.Hour).UnixMilli() {
		t.Fatalf("timestamps got=%+v", got)
	}
}

func TestControllerStateDefaultsWhenMissing(t *testing.T) {
	s := newTestStore(t)
	defaults := ControllerState{Rate: 2, StableRate: 2, Tokens: 3, ConcurrencyLimit: 4, RetryTokens: 10}
	got, err := s.GetControllerState("alice", "m", defaults)
	if err != nil {
		t.Fatal(err)
	}
	if got.Worker != "alice" || got.Model != "m" || got.Rate != 2 || got.ConcurrencyLimit != 4 || got.RetryTokens != 10 {
		t.Fatalf("got=%+v", got)
	}
}

func TestMigrationAddsRequestIdentityColumns(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "legacy.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("INSERT INTO active_requests(worker,pid) VALUES('alice',?)", os.Getpid()); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.db.Exec("INSERT INTO active_requests(worker,pid,request_id,model) VALUES('bob',?,'req-2','m')", os.Getpid()); err != nil {
		t.Fatalf("identity columns unavailable after reopen: %v", err)
	}
}

func TestCleanupStalePendingRemovesExpiredDeadlineEvenWithLivePID(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	if err := s.EnqueueRequest(PendingRequest{RequestID: "expired-live-pid", Model: "m", PID: os.Getpid(), EnqueuedAt: now.Add(-time.Minute), Deadline: now.Add(-time.Second), Unbounded: true}); err != nil {
		t.Fatal(err)
	}
	removed, err := s.CleanupStalePendingRequests()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed=%d want 1", removed)
	}
	depth, err := s.QueueDepth()
	if err != nil {
		t.Fatal(err)
	}
	if depth != 0 {
		t.Fatalf("depth=%d want 0", depth)
	}
}

func TestImmediateTxCommitErrorLeavesConnectionReusable(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	tx, err := s.BeginImmediate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := tx.Commit(); err == nil {
		t.Fatal("expected commit error after context cancellation")
	}

	next, err := s.BeginImmediate(context.Background())
	if err != nil {
		t.Fatalf("connection poisoned after failed commit: %v", err)
	}
	if err := next.Rollback(); err != nil {
		t.Fatal(err)
	}
}
