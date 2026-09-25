package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

// Store owns the SQLite state used by gemini-router.
type Store struct {
	db     *sql.DB
	dbPath string
}

// RequestLogEntry is one request-history record.
type RequestLogEntry struct {
	Worker        string
	Model         string
	StartedAt     time.Time
	FinishedAt    time.Time
	DurationMs    int64
	Status        string // "success", "error", "rate_limited", "timeout"
	ErrorMessage  string
	PromptPreview string
}

// WorkerStats contains aggregate worker statistics.
type WorkerStats struct {
	TotalRequests int
	Errors        int
	AvgDurationMs int64
}

// New opens the state database and initializes/migrates its schema.
func New(dbPath string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
		return nil, fmt.Errorf("creating database directory: %w", err)
	}
	f, err := os.OpenFile(dbPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("creating database %s securely: %w", dbPath, err)
	}
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return nil, fmt.Errorf("setting database permissions: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("closing precreated database: %w", err)
	}
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(wal)&_pragma=busy_timeout(30000)")
	if err != nil {
		return nil, fmt.Errorf("opening database %s: %w", dbPath, err)
	}

	// Each router CLI invocation has one Store and intentionally serializes its
	// own SQLite work. Cross-process concurrency is coordinated by WAL and
	// BEGIN IMMEDIATE; extra pooled connections only add lock competition.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, dbPath: dbPath}
	if err := s.ensureSchema(); err != nil {
		db.Close()
		return nil, err
	}
	if err := secureSQLiteFiles(dbPath); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func secureSQLiteFiles(dbPath string) error {
	for _, path := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		if err := os.Chmod(path, 0600); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("setting SQLite permissions for %s: %w", path, err)
		}
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

const currentSchemaVersion = 3

// ensureSchema avoids taking SQLite schema/write locks on every short-lived
// router invocation. Only an uninitialized/older database runs DDL. Competing
// first openers retry; after one commits the version, the rest become read-only
// during startup.
func (s *Store) ensureSchema() error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		var version int
		if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
			if isSQLiteBusy(err) && time.Now().Before(deadline) {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			return fmt.Errorf("reading schema version: %w", err)
		}
		if version >= currentSchemaVersion {
			return nil
		}

		if err := s.createSchema(); err != nil {
			if isSQLiteBusy(err) && time.Now().Before(deadline) {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			return fmt.Errorf("creating schema: %w", err)
		}
		if err := s.migrateSchema(); err != nil {
			if isSQLiteBusy(err) && time.Now().Before(deadline) {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			return fmt.Errorf("migrating schema: %w", err)
		}
		if _, err := s.db.Exec(fmt.Sprintf("PRAGMA user_version = %d", currentSchemaVersion)); err != nil {
			if isSQLiteBusy(err) && time.Now().Before(deadline) {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			return fmt.Errorf("setting schema version: %w", err)
		}
		return nil
	}
}

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "sqlite_busy") || strings.Contains(msg, "database is locked") || strings.Contains(msg, "database table is locked")
}

func (s *Store) createSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS active_requests (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		worker TEXT NOT NULL,
		started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		pid INTEGER NOT NULL,
		request_id TEXT NOT NULL DEFAULT '',
		model TEXT NOT NULL DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS request_log (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		worker TEXT NOT NULL,
		model TEXT,
		started_at INTEGER NOT NULL,
		finished_at INTEGER,
		duration_ms INTEGER,
		status TEXT NOT NULL,
		error_message TEXT,
		prompt_preview TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_request_log_worker_started ON request_log(worker, started_at);


	CREATE TABLE IF NOT EXISTS pending_requests (
		request_id TEXT PRIMARY KEY,
		model TEXT NOT NULL,
		pid INTEGER NOT NULL,
		enqueued_at INTEGER NOT NULL,
		deadline INTEGER NOT NULL DEFAULT 0,
		unbounded INTEGER NOT NULL DEFAULT 0
	);
	CREATE INDEX IF NOT EXISTS idx_pending_requests_order ON pending_requests(enqueued_at, request_id);

	CREATE TABLE IF NOT EXISTS quota_snapshots (
		worker TEXT NOT NULL,
		group_key TEXT NOT NULL,
		group_name TEXT NOT NULL,
		window TEXT NOT NULL,
		remaining_fraction REAL NOT NULL,
		disabled INTEGER NOT NULL DEFAULT 0,
		reset_time INTEGER NOT NULL,
		fetched_at INTEGER NOT NULL,
		PRIMARY KEY(worker, group_key, window)
	);
	CREATE INDEX IF NOT EXISTS idx_quota_snapshots_worker_group ON quota_snapshots(worker, group_key);

	CREATE TABLE IF NOT EXISTS quota_refresh (
		worker TEXT PRIMARY KEY,
		owner_pid INTEGER NOT NULL,
		lease_until INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS controller_state (
		worker TEXT NOT NULL,
		model TEXT NOT NULL,
		rate REAL NOT NULL,
		stable_rate REAL NOT NULL,
		rmax REAL NOT NULL,
		tokens REAL NOT NULL,
		token_updated_at INTEGER NOT NULL,
		recovery_window INTEGER NOT NULL,
		successes_in_window INTEGER NOT NULL,
		short_ewma REAL NOT NULL,
		long_ewma REAL NOT NULL,
		concurrency_limit INTEGER NOT NULL,
		concurrency_samples INTEGER NOT NULL,
		quota_blocked_until INTEGER NOT NULL,
		quota_probe_count INTEGER NOT NULL,
		retry_tokens REAL NOT NULL,
		retry_updated_at INTEGER NOT NULL,
		PRIMARY KEY(worker, model)
	);
	`
	_, err := s.db.Exec(schema)
	return err
}

func (s *Store) migrateSchema() error {
	for _, col := range []struct {
		name string
		ddl  string
	}{
		{"request_id", "ALTER TABLE active_requests ADD COLUMN request_id TEXT NOT NULL DEFAULT ''"},
		{"model", "ALTER TABLE active_requests ADD COLUMN model TEXT NOT NULL DEFAULT ''"},
	} {
		has, err := s.tableHasColumn("active_requests", col.name)
		if err != nil {
			return err
		}
		if !has {
			if _, err := s.db.Exec(col.ddl); err != nil {
				return err
			}
		}
	}
	if has, err := s.tableHasColumn("quota_snapshots", "disabled"); err != nil {
		return err
	} else if !has {
		if _, err := s.db.Exec("ALTER TABLE quota_snapshots ADD COLUMN disabled INTEGER NOT NULL DEFAULT 0"); err != nil {
			return err
		}
	}
	_, err := s.db.Exec("CREATE INDEX IF NOT EXISTS idx_active_requests_worker_model ON active_requests(worker, model)")
	return err
}

func (s *Store) tableHasColumn(table, column string) (bool, error) {
	rows, err := s.db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, pk int
		var defaultValue interface{}
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

// --- Active Requests ---

// AddActiveRequest records an active request for a worker and owner PID.
func (s *Store) AddActiveRequest(worker string, pid int) (int64, error) {
	res, err := s.db.Exec(
		"INSERT INTO active_requests (worker, pid) VALUES (?, ?)",
		worker, pid,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// RemoveActiveRequest removes an active request by ID.
func (s *Store) RemoveActiveRequest(id int64) error {
	_, err := s.db.Exec("DELETE FROM active_requests WHERE id = ?", id)
	return err
}

// CountActiveRequests returns active requests for one worker.
func (s *Store) CountActiveRequests(worker string) (int, error) {
	var count int
	err := s.db.QueryRow(
		"SELECT COUNT(*) FROM active_requests WHERE worker = ?",
		worker,
	).Scan(&count)
	return count, err
}

// GetAllActiveRequestCounts returns active request counts by worker.
func (s *Store) GetAllActiveRequestCounts() (map[string]int, error) {
	rows, err := s.db.Query("SELECT worker, COUNT(*) FROM active_requests GROUP BY worker")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int)
	for rows.Next() {
		var worker string
		var count int
		if err := rows.Scan(&worker, &count); err != nil {
			return nil, err
		}
		result[worker] = count
	}
	return result, rows.Err()
}

// CleanupStaleRequests removes leases whose owner PID no longer exists.
// It returns the number of removed rows.
func (s *Store) CleanupStaleRequests() (int, error) {
	rows, err := s.db.Query("SELECT id, pid FROM active_requests")
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var staleIDs []int64
	for rows.Next() {
		var id int64
		var pid int
		if err := rows.Scan(&id, &pid); err != nil {
			return 0, err
		}
		if !processExists(pid) {
			staleIDs = append(staleIDs, id)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	if len(staleIDs) == 0 {
		return 0, nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	query := "DELETE FROM active_requests WHERE id IN ("
	args := make([]interface{}, len(staleIDs))
	for i, id := range staleIDs {
		if i > 0 {
			query += ", "
		}
		query += "?"
		args[i] = id
	}
	query += ")"

	if _, err := tx.Exec(query, args...); err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return len(staleIDs), nil
}

func processExists(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	if err == nil || err == syscall.EPERM {
		return true
	}
	return false
}

// --- Request Log ---

// InsertRequestLog appends one request-history record.
func (s *Store) InsertRequestLog(e RequestLogEntry) error {
	_, err := s.db.Exec(
		`INSERT INTO request_log (worker, model, started_at, finished_at, duration_ms, status, error_message, prompt_preview)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		e.Worker, e.Model, e.StartedAt.UnixMilli(),
		nullableUnixMilli(e.FinishedAt), e.DurationMs, e.Status,
		nullableString(e.ErrorMessage), nullableString(e.PromptPreview),
	)
	return err
}

// GetLastUsedAt returns the worker's most recent request time.
func (s *Store) GetLastUsedAt(worker string) (time.Time, error) {
	var ts sql.NullInt64
	err := s.db.QueryRow(
		"SELECT MAX(started_at) FROM request_log WHERE worker = ?",
		worker,
	).Scan(&ts)
	if err != nil {
		return time.Time{}, err
	}
	if !ts.Valid {
		return time.Time{}, nil
	}
	return time.UnixMilli(ts.Int64).UTC(), nil
}

// GetWorkerStats returns aggregate statistics for one worker.
// A positive period limits statistics to that window.
func (s *Store) GetWorkerStats(worker string, period time.Duration) (*WorkerStats, error) {
	query := `SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END), 0),
		CAST(COALESCE(AVG(duration_ms), 0) AS INTEGER)
		FROM request_log WHERE worker = ?`
	args := []interface{}{worker}

	if period > 0 {
		cutoff := time.Now().Add(-period).UnixMilli()
		query += " AND started_at >= ?"
		args = append(args, cutoff)
	}

	var stats WorkerStats
	err := s.db.QueryRow(query, args...).Scan(
		&stats.TotalRequests, &stats.Errors, &stats.AvgDurationMs,
	)
	return &stats, err
}

// CleanupOldLogs removes request-history rows older than the cutoff.
// It returns the number of removed rows.
func (s *Store) CleanupOldLogs(before time.Time) (int, error) {
	res, err := s.db.Exec("DELETE FROM request_log WHERE started_at < ?", before.UnixMilli())
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(affected), nil
}

func nullableUnixMilli(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.UnixMilli()
}

func nullableString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
