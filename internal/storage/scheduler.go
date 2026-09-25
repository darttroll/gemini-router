package storage

import (
	"context"
	"database/sql"
	"time"
)

// PendingRequest is one synchronous client request waiting for a worker lease.
type PendingRequest struct {
	RequestID  string
	Model      string
	PID        int
	EnqueuedAt time.Time
	Deadline   time.Time
	Unbounded  bool
}

// ActiveRequest is a worker reservation owned by one router invocation.
type ActiveRequest struct {
	ID        int64
	RequestID string
	Worker    string
	Model     string
	PID       int
	StartedAt time.Time
}

// ControllerState persists rate, concurrency, quota and retry feedback per worker/model.
type ControllerState struct {
	Worker             string
	Model              string
	Rate               float64
	StableRate         float64
	RMax               float64
	Tokens             float64
	TokenUpdatedAt     time.Time
	RecoveryWindow     int
	SuccessesInWindow  int
	ShortEWMA          float64
	LongEWMA           float64
	ConcurrencyLimit   int
	ConcurrencySamples int
	QuotaBlockedUntil  time.Time
	QuotaProbeCount    int
	RetryTokens        float64
	RetryUpdatedAt     time.Time
}

func (s *Store) EnqueueRequest(req PendingRequest) error {
	enqueued := req.EnqueuedAt
	if enqueued.IsZero() {
		enqueued = time.Now().UTC()
	}
	_, err := s.db.Exec(`INSERT INTO pending_requests(request_id, model, pid, enqueued_at, deadline, unbounded)
        VALUES(?,?,?,?,?,?)`, req.RequestID, req.Model, req.PID, enqueued.UnixMilli(), unixMilliOrZero(req.Deadline), boolInt(req.Unbounded))
	return err
}

func (s *Store) RemovePendingRequest(requestID string) error {
	_, err := s.db.Exec("DELETE FROM pending_requests WHERE request_id = ?", requestID)
	return err
}

func (s *Store) QueueDepth() (int, error) {
	var n int
	err := s.db.QueryRow("SELECT COUNT(*) FROM pending_requests").Scan(&n)
	return n, err
}

func (s *Store) QueueDepthForModel(model string) (int, error) {
	var n int
	err := s.db.QueryRow("SELECT COUNT(*) FROM pending_requests WHERE model=?", model).Scan(&n)
	return n, err
}

func (s *Store) EnqueueRequestIfBelowLimit(ctx context.Context, req PendingRequest, maxDepth int) (bool, int, error) {
	tx, err := s.BeginImmediate(ctx)
	if err != nil {
		return false, 0, err
	}
	defer tx.Rollback()
	var depth int
	if err := tx.conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM pending_requests").Scan(&depth); err != nil {
		return false, 0, err
	}
	if depth >= maxDepth {
		return false, depth, nil
	}
	enqueued := req.EnqueuedAt
	if enqueued.IsZero() {
		enqueued = time.Now().UTC()
	}
	query := "INSERT INTO pending_requests(request_id, model, pid, enqueued_at, deadline, unbounded) VALUES(?,?,?,?,?,?)"
	if _, err := tx.conn.ExecContext(ctx, query, req.RequestID, req.Model, req.PID, enqueued.UnixMilli(), unixMilliOrZero(req.Deadline), boolInt(req.Unbounded)); err != nil {
		return false, depth, err
	}
	if err := tx.Commit(); err != nil {
		return false, depth, err
	}
	return true, depth + 1, nil
}

func (s *Store) GetPendingRequest(requestID string) (*PendingRequest, error) {
	var r PendingRequest
	var enqueued, deadline int64
	var unbounded int
	err := s.db.QueryRow(`SELECT request_id, model, pid, enqueued_at, deadline, unbounded FROM pending_requests WHERE request_id=?`, requestID).
		Scan(&r.RequestID, &r.Model, &r.PID, &enqueued, &deadline, &unbounded)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.EnqueuedAt = time.UnixMilli(enqueued).UTC()
	if deadline > 0 {
		r.Deadline = time.UnixMilli(deadline).UTC()
	}
	r.Unbounded = unbounded != 0
	return &r, nil
}

func (s *Store) GetControllerState(worker, model string, defaults ControllerState) (ControllerState, error) {
	var st ControllerState
	var tokenAt, quotaUntil, retryAt int64
	err := s.db.QueryRow(`SELECT worker,model,rate,stable_rate,rmax,tokens,token_updated_at,recovery_window,successes_in_window,
        short_ewma,long_ewma,concurrency_limit,concurrency_samples,quota_blocked_until,quota_probe_count,retry_tokens,retry_updated_at
        FROM controller_state WHERE worker=? AND model=?`, worker, model).Scan(
		&st.Worker, &st.Model, &st.Rate, &st.StableRate, &st.RMax, &st.Tokens, &tokenAt, &st.RecoveryWindow, &st.SuccessesInWindow,
		&st.ShortEWMA, &st.LongEWMA, &st.ConcurrencyLimit, &st.ConcurrencySamples, &quotaUntil, &st.QuotaProbeCount, &st.RetryTokens, &retryAt)
	if err == sql.ErrNoRows {
		st = defaults
		st.Worker, st.Model = worker, model
		now := time.Now().UTC()
		if st.TokenUpdatedAt.IsZero() {
			st.TokenUpdatedAt = now
		}
		if st.RetryUpdatedAt.IsZero() {
			st.RetryUpdatedAt = now
		}
		return st, nil
	}
	if err != nil {
		return ControllerState{}, err
	}
	st.TokenUpdatedAt = time.UnixMilli(tokenAt).UTC()
	if quotaUntil > 0 {
		st.QuotaBlockedUntil = time.UnixMilli(quotaUntil).UTC()
	}
	st.RetryUpdatedAt = time.UnixMilli(retryAt).UTC()
	return st, nil
}

func (s *Store) PutControllerState(st ControllerState) error {
	_, err := s.db.Exec(`INSERT INTO controller_state(worker,model,rate,stable_rate,rmax,tokens,token_updated_at,recovery_window,successes_in_window,
        short_ewma,long_ewma,concurrency_limit,concurrency_samples,quota_blocked_until,quota_probe_count,retry_tokens,retry_updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(worker,model) DO UPDATE SET rate=excluded.rate,stable_rate=excluded.stable_rate,rmax=excluded.rmax,tokens=excluded.tokens,
        token_updated_at=excluded.token_updated_at,recovery_window=excluded.recovery_window,successes_in_window=excluded.successes_in_window,
        short_ewma=excluded.short_ewma,long_ewma=excluded.long_ewma,concurrency_limit=excluded.concurrency_limit,
        concurrency_samples=excluded.concurrency_samples,quota_blocked_until=excluded.quota_blocked_until,quota_probe_count=excluded.quota_probe_count,
        retry_tokens=excluded.retry_tokens,retry_updated_at=excluded.retry_updated_at`,
		st.Worker, st.Model, st.Rate, st.StableRate, st.RMax, st.Tokens, unixMilliOrNow(st.TokenUpdatedAt), st.RecoveryWindow, st.SuccessesInWindow,
		st.ShortEWMA, st.LongEWMA, st.ConcurrencyLimit, st.ConcurrencySamples, unixMilliOrZero(st.QuotaBlockedUntil), st.QuotaProbeCount, st.RetryTokens, unixMilliOrNow(st.RetryUpdatedAt))
	return err
}

func unixMilliOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}
func unixMilliOrNow(t time.Time) int64 {
	if t.IsZero() {
		return time.Now().UTC().UnixMilli()
	}
	return t.UnixMilli()
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// CleanupStalePendingRequests removes queued work whose owner is gone or whose deadline expired.
func (s *Store) CleanupStalePendingRequests() (int, error) {
	rows, err := s.db.Query("SELECT request_id,pid,deadline FROM pending_requests")
	if err != nil {
		return 0, err
	}
	type row struct {
		id       string
		pid      int
		deadline int64
	}
	var stale []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.pid, &r.deadline); err != nil {
			rows.Close()
			return 0, err
		}
		expired := r.deadline > 0 && r.deadline <= time.Now().UTC().UnixMilli()
		if expired || !processExists(r.pid) {
			stale = append(stale, r)
		}
	}
	rows.Close()
	if len(stale) == 0 {
		return 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, r := range stale {
		if _, err := tx.Exec("DELETE FROM pending_requests WHERE request_id=?", r.id); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(stale), nil
}

// GetAverageDuration returns recent successful average duration for one worker/model.
func (s *Store) GetAverageDuration(worker, model string, period time.Duration) (time.Duration, error) {
	cutoff := time.Now().Add(-period).UnixMilli()
	var ms sql.NullFloat64
	err := s.db.QueryRow(`SELECT AVG(duration_ms) FROM request_log WHERE worker=? AND model=? AND status='success' AND started_at>=?`, worker, model, cutoff).Scan(&ms)
	if err != nil {
		return 0, err
	}
	if !ms.Valid {
		return 0, nil
	}
	return time.Duration(ms.Float64 * float64(time.Millisecond)), nil
}

// IsQueueHead performs a read-only FIFO precheck. The scheduler rechecks the
// same condition inside BEGIN IMMEDIATE before reserving capacity, so this is
// only a contention optimization, not a correctness boundary.
func (s *Store) IsQueueHead(requestID string) (bool, error) {
	req, err := s.GetPendingRequest(requestID)
	if err != nil {
		return false, err
	}
	if req == nil {
		return false, sql.ErrNoRows
	}
	var head string
	err = s.db.QueryRow(`SELECT request_id FROM pending_requests WHERE model=? ORDER BY enqueued_at,request_id LIMIT 1`, req.Model).Scan(&head)
	if err != nil {
		return false, err
	}
	return head == requestID, nil
}
