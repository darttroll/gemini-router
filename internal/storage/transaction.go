package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ImmediateTx is a SQLite BEGIN IMMEDIATE transaction used for cross-process reservations.
type ImmediateTx struct {
	conn *sql.Conn
	ctx  context.Context
	done bool
}

func (s *Store) BeginImmediate(ctx context.Context) (*ImmediateTx, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		conn.Close()
		return nil, err
	}
	return &ImmediateTx{conn: conn, ctx: ctx}, nil
}

func (tx *ImmediateTx) Commit() error {
	if tx.done {
		return nil
	}
	_, commitErr := tx.conn.ExecContext(tx.ctx, "COMMIT")
	if commitErr != nil {
		_, rollbackErr := tx.conn.ExecContext(context.Background(), "ROLLBACK")
		tx.done = true
		closeErr := tx.conn.Close()
		return errors.Join(commitErr, rollbackErr, closeErr)
	}
	tx.done = true
	return tx.conn.Close()
}

func (tx *ImmediateTx) Rollback() error {
	if tx.done {
		return nil
	}
	_, rollbackErr := tx.conn.ExecContext(context.Background(), "ROLLBACK")
	tx.done = true
	closeErr := tx.conn.Close()
	return errors.Join(rollbackErr, closeErr)
}

func (tx *ImmediateTx) OldestPendingRequestID(model string) (string, error) {
	var id string
	err := tx.conn.QueryRowContext(tx.ctx, `SELECT request_id FROM pending_requests WHERE model=? ORDER BY enqueued_at, request_id LIMIT 1`, model).Scan(&id)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return id, err
}

func (tx *ImmediateTx) GetPendingRequest(requestID string) (*PendingRequest, error) {
	var r PendingRequest
	var enqueued, deadline int64
	var unbounded int
	err := tx.conn.QueryRowContext(tx.ctx, `SELECT request_id,model,pid,enqueued_at,deadline,unbounded FROM pending_requests WHERE request_id=?`, requestID).
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

func (tx *ImmediateTx) DeletePending(requestID string) error {
	_, err := tx.conn.ExecContext(tx.ctx, "DELETE FROM pending_requests WHERE request_id=?", requestID)
	return err
}

func (tx *ImmediateTx) CountActive(worker, model string) (int, error) {
	var n int
	err := tx.conn.QueryRowContext(tx.ctx, "SELECT COUNT(*) FROM active_requests WHERE worker=? AND model=?", worker, model).Scan(&n)
	return n, err
}

func (tx *ImmediateTx) GetControllerState(worker, model string, defaults ControllerState) (ControllerState, error) {
	var st ControllerState
	var tokenAt, quotaUntil, retryAt int64
	err := tx.conn.QueryRowContext(tx.ctx, `SELECT worker,model,rate,stable_rate,rmax,tokens,token_updated_at,recovery_window,successes_in_window,
        short_ewma,long_ewma,concurrency_limit,concurrency_samples,quota_blocked_until,quota_probe_count,retry_tokens,retry_updated_at
        FROM controller_state WHERE worker=? AND model=?`, worker, model).Scan(&st.Worker, &st.Model, &st.Rate, &st.StableRate, &st.RMax, &st.Tokens, &tokenAt, &st.RecoveryWindow, &st.SuccessesInWindow, &st.ShortEWMA, &st.LongEWMA, &st.ConcurrencyLimit, &st.ConcurrencySamples, &quotaUntil, &st.QuotaProbeCount, &st.RetryTokens, &retryAt)
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

func (tx *ImmediateTx) PutControllerState(st ControllerState) error {
	_, err := tx.conn.ExecContext(tx.ctx, `INSERT INTO controller_state(worker,model,rate,stable_rate,rmax,tokens,token_updated_at,recovery_window,successes_in_window,
        short_ewma,long_ewma,concurrency_limit,concurrency_samples,quota_blocked_until,quota_probe_count,retry_tokens,retry_updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(worker,model) DO UPDATE SET rate=excluded.rate,stable_rate=excluded.stable_rate,rmax=excluded.rmax,
        tokens=excluded.tokens,token_updated_at=excluded.token_updated_at,recovery_window=excluded.recovery_window,successes_in_window=excluded.successes_in_window,
        short_ewma=excluded.short_ewma,long_ewma=excluded.long_ewma,concurrency_limit=excluded.concurrency_limit,concurrency_samples=excluded.concurrency_samples,
        quota_blocked_until=excluded.quota_blocked_until,quota_probe_count=excluded.quota_probe_count,retry_tokens=excluded.retry_tokens,retry_updated_at=excluded.retry_updated_at`,
		st.Worker, st.Model, st.Rate, st.StableRate, st.RMax, st.Tokens, unixMilliOrNow(st.TokenUpdatedAt), st.RecoveryWindow, st.SuccessesInWindow, st.ShortEWMA, st.LongEWMA,
		st.ConcurrencyLimit, st.ConcurrencySamples, unixMilliOrZero(st.QuotaBlockedUntil), st.QuotaProbeCount, st.RetryTokens, unixMilliOrNow(st.RetryUpdatedAt))
	return err
}

func (tx *ImmediateTx) GetQuotaBlockedUntil(worker, groupKey string, now time.Time) (time.Time, error) {
	var ms sql.NullInt64
	err := tx.conn.QueryRowContext(tx.ctx, `SELECT MAX(reset_time) FROM quota_snapshots WHERE worker=? AND group_key=? AND disabled=0 AND remaining_fraction<=0 AND reset_time>?`, worker, groupKey, now.UnixMilli()).Scan(&ms)
	if err != nil {
		return time.Time{}, err
	}
	if !ms.Valid || ms.Int64 <= 0 {
		return time.Time{}, nil
	}
	return time.UnixMilli(ms.Int64).UTC(), nil
}

func (tx *ImmediateTx) InsertActiveRequest(req PendingRequest, worker string, started time.Time) (int64, error) {
	res, err := tx.conn.ExecContext(tx.ctx, `INSERT INTO active_requests(worker,started_at,pid,request_id,model) VALUES(?,?,?,?,?)`, worker, started.UnixMilli(), req.PID, req.RequestID, req.Model)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (tx *ImmediateTx) LastUsedAt(worker string) (time.Time, error) {
	var ms sql.NullInt64
	err := tx.conn.QueryRowContext(tx.ctx, "SELECT MAX(started_at) FROM request_log WHERE worker=?", worker).Scan(&ms)
	if err != nil {
		return time.Time{}, err
	}
	if !ms.Valid {
		return time.Time{}, nil
	}
	return time.UnixMilli(ms.Int64).UTC(), nil
}

func (tx *ImmediateTx) DeleteActive(id int64) (bool, error) {
	res, err := tx.conn.ExecContext(tx.ctx, "DELETE FROM active_requests WHERE id=?", id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (tx *ImmediateTx) GetAverageDuration(worker, model string, period time.Duration) (time.Duration, error) {
	cutoff := time.Now().Add(-period).UnixMilli()
	var ms sql.NullFloat64
	err := tx.conn.QueryRowContext(tx.ctx, `SELECT AVG(duration_ms) FROM request_log WHERE worker=? AND model=? AND status='success' AND started_at>=?`, worker, model, cutoff).Scan(&ms)
	if err != nil {
		return 0, err
	}
	if !ms.Valid {
		return 0, nil
	}
	return time.Duration(ms.Float64 * float64(time.Millisecond)), nil
}
