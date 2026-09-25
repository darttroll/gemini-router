package quota

import (
	"context"
	"path/filepath"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/storage"
	"testing"
	"time"
)

func TestParseAgyQuotaJSON(t *testing.T) {
	raw := []byte(`{"status":"SUCCESS","command":{"name":"usage","data":{"groups":[{"name":"Gemini Models","buckets":[{"id":"gemini-weekly","name":"Weekly Limit Remaining","window":"weekly","remaining_fraction":0.3171,"reset_time":"2026-08-13T13:56:07Z"},{"id":"gemini-5h","name":"Five Hour Limit Remaining","window":"5h","remaining_fraction":0,"reset_time":"2026-08-07T14:56:07Z"}]}]}}}`)
	got, err := Parse("alice", raw, time.Date(2026, 8, 7, 14, 32, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d want 2", len(got))
	}
	if got[0].GroupKey != "gemini" || got[0].Worker != "alice" {
		t.Fatalf("unexpected first bucket: %+v", got[0])
	}
	var five *Bucket
	for i := range got {
		if got[i].Window == "5h" {
			five = &got[i]
		}
	}
	if five == nil || five.RemainingFraction != 0 || five.ResetTime.Format(time.RFC3339) != "2026-08-07T14:56:07Z" {
		t.Fatalf("bad 5h: %+v", five)
	}
}

func TestModelGroup(t *testing.T) {
	cases := map[string]string{"gemini-3.6-flash-high": "gemini", "Claude Opus": "third_party", "gpt-oss": "third_party"}
	for model, want := range cases {
		if got := ModelGroup(model); got != want {
			t.Fatalf("ModelGroup(%q)=%q want %q", model, got, want)
		}
	}
}

func TestSnapshotsFreshZeroQuotaExpiresAtReset(t *testing.T) {
	now := time.Now().UTC()
	cfg := config.QuotaSettings{RefreshInterval: 5 * time.Minute, LowRemainingInterval: 30 * time.Second, LowRemainingThreshold: .1}
	rows := []storage.QuotaSnapshot{
		{Window: "5h", RemainingFraction: 0, ResetTime: now.Add(-time.Second), FetchedAt: now.Add(-time.Minute)},
		{Window: "weekly", RemainingFraction: 0, ResetTime: now.Add(5 * 24 * time.Hour), FetchedAt: now.Add(-time.Minute)},
	}
	if snapshotsFresh(rows, now, cfg) {
		t.Fatal("snapshot must become stale when any exhausted bucket reaches reset")
	}
}

func TestEnsureFreshAllCachesQuota(t *testing.T) {
	db, err := storage.New(filepath.Join(t.TempDir(), "q.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := &config.Config{AgyPath: "/fake/agy", Workers: []config.WorkerConfig{{Username: "alice", Enabled: true}}, Quota: config.QuotaSettings{Enabled: true, RefreshInterval: 5 * time.Minute, LowRemainingInterval: 30 * time.Second, LowRemainingThreshold: .1, CommandTimeout: time.Second}}
	svc := NewService(cfg, db)
	calls := 0
	svc.SetRunnerForTest(func(ctx context.Context, worker, path string) ([]byte, error) {
		calls++
		return []byte(`{"status":"SUCCESS","command":{"name":"usage","data":{"groups":[{"name":"Gemini Models","buckets":[{"id":"gemini-weekly","name":"Weekly Limit Remaining","window":"weekly","remaining_fraction":0.5,"reset_time":"2026-08-14T00:00:00Z"},{"id":"gemini-5h","name":"Five Hour Limit Remaining","window":"5h","remaining_fraction":0.5,"reset_time":"2026-08-08T00:00:00Z"}]}]}}}`), nil
	})
	if errs := svc.EnsureFreshAll(context.Background(), "gemini-x"); len(errs) != 0 {
		t.Fatalf("refresh errors: %v", errs)
	}
	if errs := svc.EnsureFreshAll(context.Background(), "gemini-x"); len(errs) != 0 {
		t.Fatalf("cached errors: %v", errs)
	}
	if calls != 1 {
		t.Fatalf("runner calls=%d want 1", calls)
	}
}

func TestParseDisabledFiveHourBucketWhenWeeklyExhausted(t *testing.T) {
	raw := []byte(`{"status":"SUCCESS","command":{"name":"usage","data":{"groups":[{"name":"Gemini Models","buckets":[{"id":"gemini-weekly","name":"Weekly Limit Remaining","window":"weekly","remaining_fraction":0,"reset_time":"2026-08-13T15:02:49Z"},{"id":"gemini-5h","name":"Five Hour Limit Remaining","window":"5h","disabled":true,"remaining_fraction":1}] }]}}}`)
	got, err := Parse("alice", raw, time.Date(2026, 8, 8, 8, 49, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d want 2", len(got))
	}
	for _, b := range got {
		if b.Window == "5h" {
			if !b.Disabled || !b.ResetTime.IsZero() {
				t.Fatalf("disabled 5h bucket parsed incorrectly: %+v", b)
			}
			return
		}
	}
	t.Fatal("5h bucket missing")
}

func TestEnsureFreshAllGroupsRefreshesEvenWithFreshModelCache(t *testing.T) {
	db, err := storage.New(filepath.Join(t.TempDir(), "q.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	if err := db.ReplaceQuotaSnapshots("alice", []storage.QuotaSnapshot{{
		Worker:            "alice",
		GroupKey:          "gemini",
		GroupName:         "Gemini Models",
		Window:            "weekly",
		RemainingFraction: 0.5,
		ResetTime:         now.Add(24 * time.Hour),
		FetchedAt:         now,
	}}); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		AgyPath: "/fake/agy",
		Workers: []config.WorkerConfig{{Username: "alice", Enabled: true}},
		Quota:   config.QuotaSettings{Enabled: true, CommandTimeout: time.Second},
	}
	svc := NewService(cfg, db)
	calls := 0
	svc.SetRunnerForTest(func(ctx context.Context, worker, path string) ([]byte, error) {
		calls++
		return []byte(`{"status":"SUCCESS","command":{"name":"usage","data":{"groups":[{"name":"Gemini Models","buckets":[{"id":"gemini-weekly","name":"Weekly Limit Remaining","window":"weekly","remaining_fraction":0.4,"reset_time":"2030-08-14T00:00:00Z"},{"id":"gemini-5h","name":"Five Hour Limit Remaining","window":"5h","remaining_fraction":0.8,"reset_time":"2030-08-09T00:00:00Z"}]},{"name":"Claude and GPT models","buckets":[{"id":"3p-weekly","name":"Weekly Limit Remaining","window":"weekly","remaining_fraction":0.9,"reset_time":"2030-08-15T00:00:00Z"},{"id":"3p-5h","name":"Five Hour Limit Remaining","window":"5h","remaining_fraction":1,"reset_time":"2030-08-09T00:00:00Z"}]}]}}}`), nil
	})

	if errs := svc.EnsureFreshAllGroups(context.Background()); len(errs) != 0 {
		t.Fatalf("refresh errors: %v", errs)
	}
	if calls != 1 {
		t.Fatalf("runner calls=%d want 1", calls)
	}
	rows, err := db.GetQuotaSnapshots("alice", "")
	if err != nil {
		t.Fatal(err)
	}
	groups := map[string]bool{}
	for _, r := range rows {
		groups[r.GroupKey] = true
	}
	if !groups["gemini"] || !groups["third_party"] {
		t.Fatalf("stored groups=%v want gemini and third_party", groups)
	}

	if errs := svc.EnsureFreshAllGroups(context.Background()); len(errs) != 0 {
		t.Fatalf("second refresh errors: %v", errs)
	}
	if calls != 2 {
		t.Fatalf("runner calls after second invocation=%d want 2", calls)
	}
}

func TestSnapshotsFreshIgnoresDisabledZeroBucketWithoutReset(t *testing.T) {
	now := time.Now().UTC()
	cfg := config.QuotaSettings{RefreshInterval: 5 * time.Minute, LowRemainingInterval: 30 * time.Second, LowRemainingThreshold: .1}
	rows := []storage.QuotaSnapshot{
		{RemainingFraction: .8, FetchedAt: now},
		{RemainingFraction: 0, Disabled: true, FetchedAt: now},
	}
	if !snapshotsFresh(rows, now, cfg) {
		t.Fatal("disabled zero bucket made fresh active quota stale")
	}
}

func TestSnapshotsFreshDisabledBucketCannotFreezeStaleActiveCache(t *testing.T) {
	now := time.Now().UTC()
	cfg := config.QuotaSettings{RefreshInterval: 5 * time.Minute, LowRemainingInterval: 30 * time.Second, LowRemainingThreshold: .1}
	rows := []storage.QuotaSnapshot{
		{RemainingFraction: .8, FetchedAt: now.Add(-10 * time.Minute)},
		{RemainingFraction: 0, Disabled: true, ResetTime: now.Add(7 * 24 * time.Hour), FetchedAt: now},
	}
	if snapshotsFresh(rows, now, cfg) {
		t.Fatal("disabled exhausted bucket froze stale active quota cache")
	}
}

func TestEnsureFreshAllCachesAllDisabledQuotaSnapshot(t *testing.T) {
	db, err := storage.New(filepath.Join(t.TempDir(), "q.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := &config.Config{
		AgyPath: "/fake/agy",
		Workers: []config.WorkerConfig{{Username: "alice", Enabled: true}},
		Quota: config.QuotaSettings{
			Enabled: true, RefreshInterval: 5 * time.Minute,
			LowRemainingInterval: 30 * time.Second, LowRemainingThreshold: .1,
			CommandTimeout: time.Second,
		},
	}
	svc := NewService(cfg, db)
	calls := 0
	svc.SetRunnerForTest(func(context.Context, string, string) ([]byte, error) {
		calls++
		return []byte(`{"status":"SUCCESS","command":{"name":"usage","data":{"groups":[{"name":"Gemini Models","buckets":[{"id":"gemini-weekly","window":"weekly","disabled":true,"remaining_fraction":1},{"id":"gemini-5h","window":"5h","disabled":true,"remaining_fraction":1}]}]}}}`), nil
	})
	for i := 0; i < 3; i++ {
		if errs := svc.EnsureFreshAll(context.Background(), "gemini-x"); len(errs) > 0 {
			t.Fatal(errs)
		}
	}
	if calls != 1 {
		t.Fatalf("3 immediate requests issued %d /quota calls; want one refresh within TTL", calls)
	}
}

func TestSnapshotsFreshAllDisabledUsesSnapshotTimestampIndependentOfOrder(t *testing.T) {
	now := time.Now().UTC()
	cfg := config.QuotaSettings{RefreshInterval: 5 * time.Minute}
	rows := []storage.QuotaSnapshot{
		{Disabled: true, FetchedAt: now},
		{Disabled: true, FetchedAt: now.Add(-6 * time.Minute)},
	}
	if snapshotsFresh(rows, now, cfg) {
		t.Fatal("all-disabled snapshot with stale bucket timestamp must be stale")
	}
}
