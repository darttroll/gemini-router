package storage

import (
	"database/sql"
	"sort"
	"time"
)

type RecentMetrics struct {
	Total          int
	Successes      int
	ShortThrottles int
	AvgLatency     time.Duration
	P95Latency     time.Duration
	ThroughputRPS  float64
}

func (s *Store) CountActiveRequestsForModel(worker, model string) (int, error) {
	var n int
	err := s.db.QueryRow("SELECT COUNT(*) FROM active_requests WHERE worker=? AND model=?", worker, model).Scan(&n)
	return n, err
}

func (s *Store) CountAllActiveRequests() (int, error) {
	var n int
	err := s.db.QueryRow("SELECT COUNT(*) FROM active_requests").Scan(&n)
	return n, err
}

func (s *Store) GetRecentMetrics(worker, model string, period time.Duration) (RecentMetrics, error) {
	cutoff := time.Now().Add(-period).UnixMilli()
	rows, err := s.db.Query(`SELECT duration_ms,status FROM request_log WHERE worker=? AND model=? AND started_at>=? ORDER BY duration_ms`, worker, model, cutoff)
	if err != nil {
		return RecentMetrics{}, err
	}
	defer rows.Close()
	var m RecentMetrics
	var durations []int64
	var totalMs int64
	for rows.Next() {
		var d sql.NullInt64
		var status string
		if err := rows.Scan(&d, &status); err != nil {
			return RecentMetrics{}, err
		}
		m.Total++
		if status == "success" {
			m.Successes++
		}
		if status == "short_throttle" || status == "rate_limited" {
			m.ShortThrottles++
		}
		if d.Valid && d.Int64 >= 0 {
			durations = append(durations, d.Int64)
			totalMs += d.Int64
		}
	}
	if err := rows.Err(); err != nil {
		return RecentMetrics{}, err
	}
	if len(durations) > 0 {
		m.AvgLatency = time.Duration(float64(totalMs) / float64(len(durations)) * float64(time.Millisecond))
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		idx := int(float64(len(durations)-1)*0.95 + 0.5)
		if idx >= len(durations) {
			idx = len(durations) - 1
		}
		m.P95Latency = time.Duration(durations[idx]) * time.Millisecond
	}
	if period > 0 {
		m.ThroughputRPS = float64(m.Successes) / period.Seconds()
	}
	return m, nil
}

func (s *Store) ResetControllerAvailability(worker string, retryCapacity float64) error {
	now := time.Now().UTC().UnixMilli()
	_, err := s.db.Exec(`UPDATE controller_state SET quota_blocked_until=0,quota_probe_count=0,retry_tokens=?,retry_updated_at=? WHERE worker=?`, retryCapacity, now, worker)
	return err
}
