package storage

import (
	"database/sql"
	"time"
)

type QuotaSnapshot struct {
	Worker            string
	GroupKey          string
	GroupName         string
	Window            string
	RemainingFraction float64
	Disabled          bool
	ResetTime         time.Time
	FetchedAt         time.Time
}

func (s *Store) ReplaceQuotaSnapshots(worker string, rows []QuotaSnapshot) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec("DELETE FROM quota_snapshots WHERE worker=?", worker); err != nil {
		return err
	}
	for _, r := range rows {
		if r.Worker == "" {
			r.Worker = worker
		}
		if _, err = tx.Exec(`INSERT INTO quota_snapshots(worker,group_key,group_name,window,remaining_fraction,disabled,reset_time,fetched_at) VALUES(?,?,?,?,?,?,?,?)`,
			r.Worker, r.GroupKey, r.GroupName, r.Window, r.RemainingFraction, boolInt(r.Disabled), unixMilliOrZero(r.ResetTime), unixMilliOrNow(r.FetchedAt)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) GetQuotaSnapshots(worker, groupKey string) ([]QuotaSnapshot, error) {
	query := `SELECT worker,group_key,group_name,window,remaining_fraction,disabled,reset_time,fetched_at FROM quota_snapshots WHERE worker=?`
	args := []any{worker}
	if groupKey != "" {
		query += " AND group_key=?"
		args = append(args, groupKey)
	}
	query += " ORDER BY group_key,window"
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QuotaSnapshot
	for rows.Next() {
		var r QuotaSnapshot
		var reset, fetched int64
		var disabled int
		if err := rows.Scan(&r.Worker, &r.GroupKey, &r.GroupName, &r.Window, &r.RemainingFraction, &disabled, &reset, &fetched); err != nil {
			return nil, err
		}
		r.Disabled = disabled != 0
		if reset > 0 {
			r.ResetTime = time.UnixMilli(reset).UTC()
		}
		r.FetchedAt = time.UnixMilli(fetched).UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) GetQuotaBlockedUntil(worker, groupKey string, now time.Time) (time.Time, error) {
	var ms sql.NullInt64
	err := s.db.QueryRow(`SELECT MAX(reset_time) FROM quota_snapshots WHERE worker=? AND group_key=? AND disabled=0 AND remaining_fraction<=0 AND reset_time>?`, worker, groupKey, now.UnixMilli()).Scan(&ms)
	if err != nil {
		return time.Time{}, err
	}
	if !ms.Valid || ms.Int64 <= 0 {
		return time.Time{}, nil
	}
	return time.UnixMilli(ms.Int64).UTC(), nil
}

func (s *Store) TryClaimQuotaRefresh(worker string, ownerPID int, leaseUntil time.Time) (bool, error) {
	now := time.Now().UTC().UnixMilli()
	res, err := s.db.Exec(`INSERT INTO quota_refresh(worker,owner_pid,lease_until) VALUES(?,?,?)
        ON CONFLICT(worker) DO UPDATE SET owner_pid=excluded.owner_pid,lease_until=excluded.lease_until
        WHERE quota_refresh.lease_until<=?`, worker, ownerPID, leaseUntil.UnixMilli(), now)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *Store) ReleaseQuotaRefresh(worker string, ownerPID int) error {
	_, err := s.db.Exec(`UPDATE quota_refresh SET lease_until=0 WHERE worker=? AND owner_pid=?`, worker, ownerPID)
	return err
}
