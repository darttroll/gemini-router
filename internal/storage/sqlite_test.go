package storage

import (
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestNew_CreatesDatabase(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer s.Close()

	if _, err := os.Stat(dbPath); err != nil {
		t.Errorf("database file not created: %v", err)
	}
}

func TestActiveRequests_AddAndRemove(t *testing.T) {
	s := newTestStore(t)

	id, err := s.AddActiveRequest("alice", 12345)
	if err != nil {
		t.Fatalf("AddActiveRequest() error: %v", err)
	}
	if id == 0 {
		t.Error("AddActiveRequest() returned id=0")
	}

	count, err := s.CountActiveRequests("alice")
	if err != nil {
		t.Fatalf("CountActiveRequests() error: %v", err)
	}
	if count != 1 {
		t.Errorf("CountActiveRequests() = %d, want 1", count)
	}

	if err := s.RemoveActiveRequest(id); err != nil {
		t.Fatalf("RemoveActiveRequest() error: %v", err)
	}

	count, err = s.CountActiveRequests("alice")
	if err != nil {
		t.Fatalf("CountActiveRequests() error: %v", err)
	}
	if count != 0 {
		t.Errorf("CountActiveRequests() = %d, want 0", count)
	}
}

func TestActiveRequests_CleanupStalePIDs(t *testing.T) {
	s := newTestStore(t)

	// Insert a row owned by a nonexistent PID.
	_, err := s.AddActiveRequest("alice", 999999999)
	if err != nil {
		t.Fatalf("AddActiveRequest() error: %v", err)
	}

	cleaned, err := s.CleanupStaleRequests()
	if err != nil {
		t.Fatalf("CleanupStaleRequests() error: %v", err)
	}
	if cleaned != 1 {
		t.Errorf("CleanupStaleRequests() = %d, want 1", cleaned)
	}

	count, err := s.CountActiveRequests("alice")
	if err != nil {
		t.Fatalf("CountActiveRequests() error: %v", err)
	}
	if count != 0 {
		t.Errorf("after cleanup, CountActiveRequests() = %d, want 0", count)
	}
}

func TestRequestLog_Insert(t *testing.T) {
	s := newTestStore(t)

	now := time.Now()
	entry := RequestLogEntry{
		Worker:        "alice",
		Model:         "TestModel",
		StartedAt:     now,
		FinishedAt:    now.Add(2 * time.Second),
		DurationMs:    2000,
		Status:        "success",
		PromptPreview: "Hello world",
	}

	if err := s.InsertRequestLog(entry); err != nil {
		t.Fatalf("InsertRequestLog() error: %v", err)
	}
}

func TestLastUsedAt(t *testing.T) {
	s := newTestStore(t)

	now := time.Now()
	entry := RequestLogEntry{
		Worker:    "alice",
		Model:     "M",
		StartedAt: now.Add(-30 * time.Second),
		Status:    "success",
	}
	if err := s.InsertRequestLog(entry); err != nil {
		t.Fatal(err)
	}

	lastUsed, err := s.GetLastUsedAt("alice")
	if err != nil {
		t.Fatalf("GetLastUsedAt() error: %v", err)
	}
	if lastUsed.IsZero() {
		t.Error("expected non-zero last_used_at")
	}
}

func TestGetWorkerStats(t *testing.T) {
	s := newTestStore(t)

	now := time.Now()
	entries := []RequestLogEntry{
		{Worker: "alice", Model: "M", StartedAt: now, DurationMs: 100, Status: "success"},
		{Worker: "alice", Model: "M", StartedAt: now, DurationMs: 200, Status: "success"},
		{Worker: "alice", Model: "M", StartedAt: now, DurationMs: 301, Status: "error", ErrorMessage: "fail"},
	}
	for _, e := range entries {
		if err := s.InsertRequestLog(e); err != nil {
			t.Fatal(err)
		}
	}

	stats, err := s.GetWorkerStats("alice", 0)
	if err != nil {
		t.Fatalf("GetWorkerStats() error: %v", err)
	}
	if stats.TotalRequests != 3 {
		t.Errorf("TotalRequests = %d, want 3", stats.TotalRequests)
	}
	if stats.Errors != 1 {
		t.Errorf("Errors = %d, want 1", stats.Errors)
	}
	if stats.AvgDurationMs != 200 {
		t.Errorf("AvgDurationMs = %d, want 200", stats.AvgDurationMs)
	}
}

func TestConcurrentStoreOpenAfterMigration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "concurrent-open.db")
	first, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	first.Close()

	const n = 50
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := New(dbPath)
			if err == nil {
				s.Close()
			}
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent New failed: %v", err)
		}
	}
}

func TestConcurrentFirstStoreOpen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "concurrent-first-open.db")
	const n = 50
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			s, err := New(dbPath)
			if err == nil {
				s.Close()
			}
			errCh <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent first New failed: %v", err)
		}
	}
}

func TestQuotaSnapshotsPersistAndBlockUntilReset(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	rows := []QuotaSnapshot{
		{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "5h", RemainingFraction: 0, ResetTime: now.Add(time.Hour), FetchedAt: now},
		{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "weekly", RemainingFraction: .31, ResetTime: now.Add(6 * 24 * time.Hour), FetchedAt: now},
	}
	if err := s.ReplaceQuotaSnapshots("alice", rows); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetQuotaSnapshots("alice", "gemini")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d want 2", len(got))
	}
	until, err := s.GetQuotaBlockedUntil("alice", "gemini", now)
	if err != nil {
		t.Fatal(err)
	}
	if until.UnixMilli() != now.Add(time.Hour).UnixMilli() {
		t.Fatalf("until=%v want %v", until, now.Add(time.Hour))
	}
}

func TestSchemaV1MigratesQuotaTablesWithoutLosingData(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "v1.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE legacy_marker(value TEXT); INSERT INTO legacy_marker(value) VALUES('keep-me'); PRAGMA user_version=1;`); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var marker string
	if err := s.db.QueryRow(`SELECT value FROM legacy_marker`).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if marker != "keep-me" {
		t.Fatalf("marker=%q", marker)
	}
	var version int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != currentSchemaVersion {
		t.Fatalf("version=%d want %d", version, currentSchemaVersion)
	}
	if _, err := s.GetQuotaSnapshots("alice", "gemini"); err != nil {
		t.Fatalf("quota table unavailable after migration: %v", err)
	}
}

func TestQuotaDisabledFlagPersists(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := s.ReplaceQuotaSnapshots("alice", []QuotaSnapshot{
		{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "5h", RemainingFraction: 1, Disabled: true, FetchedAt: now},
		{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "weekly", RemainingFraction: 0, ResetTime: now.Add(5 * 24 * time.Hour), FetchedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := s.GetQuotaSnapshots("alice", "gemini")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Window == "5h" {
			if !r.Disabled || !r.ResetTime.IsZero() {
				t.Fatalf("disabled quota did not round-trip: %+v", r)
			}
			return
		}
	}
	t.Fatal("5h quota row missing")
}

func TestNewSecuresDatabaseAndWALSidecars(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "secure.db")
	if err := os.WriteFile(dbPath, nil, 0644); err != nil {
		t.Fatal(err)
	}
	store, err := New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, path := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if got := info.Mode().Perm(); got != 0600 {
			t.Fatalf("%s mode=%o want 600", path, got)
		}
	}
}
