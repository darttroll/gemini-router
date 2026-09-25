package quota

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/proc"
	"github.com/darttroll/gemini-router/internal/storage"
)

type Bucket struct {
	Worker            string
	GroupKey          string
	GroupName         string
	ID                string
	Name              string
	Window            string
	RemainingFraction float64
	Disabled          bool
	ResetTime         time.Time
	FetchedAt         time.Time
}

type agyResponse struct {
	Status  string `json:"status"`
	Command struct {
		Name string `json:"name"`
		Data struct {
			Groups []struct {
				Name    string `json:"name"`
				Buckets []struct {
					ID                string  `json:"id"`
					Name              string  `json:"name"`
					Window            string  `json:"window"`
					RemainingFraction float64 `json:"remaining_fraction"`
					Disabled          bool    `json:"disabled"`
					ResetTime         string  `json:"reset_time"`
				} `json:"buckets"`
			} `json:"groups"`
		} `json:"data"`
	} `json:"command"`
}

func Parse(worker string, raw []byte, fetchedAt time.Time) ([]Bucket, error) {
	var r agyResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("parsing agy quota JSON: %w", err)
	}
	if r.Status != "SUCCESS" {
		return nil, fmt.Errorf("agy quota status %q", r.Status)
	}
	var out []Bucket
	for _, g := range r.Command.Data.Groups {
		for _, b := range g.Buckets {
			var reset time.Time
			if b.ResetTime != "" {
				parsed, err := time.Parse(time.RFC3339, b.ResetTime)
				if err != nil {
					return nil, fmt.Errorf("parsing quota reset %q: %w", b.ResetTime, err)
				}
				reset = parsed.UTC()
			} else if !b.Disabled {
				return nil, fmt.Errorf("quota bucket %q has no reset_time", b.ID)
			}
			key := groupKeyForBucket(b.ID, g.Name)
			out = append(out, Bucket{Worker: worker, GroupKey: key, GroupName: g.Name, ID: b.ID, Name: b.Name, Window: b.Window, RemainingFraction: b.RemainingFraction, Disabled: b.Disabled, ResetTime: reset, FetchedAt: fetchedAt.UTC()})
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("agy quota response contained no buckets")
	}
	return out, nil
}

func groupKeyForBucket(id, groupName string) string {
	id = strings.ToLower(id)
	if strings.HasPrefix(id, "gemini-") {
		return "gemini"
	}
	if strings.HasPrefix(id, "3p-") {
		return "third_party"
	}
	name := strings.ToLower(groupName)
	if strings.Contains(name, "claude") || strings.Contains(name, "gpt") {
		return "third_party"
	}
	return "gemini"
}

func ModelGroup(model string) string {
	m := strings.ToLower(model)
	if strings.Contains(m, "claude") || strings.Contains(m, "gpt") || strings.Contains(m, "oss") {
		return "third_party"
	}
	return "gemini"
}

type Runner func(ctx context.Context, worker, agyPath string) ([]byte, error)

type Service struct {
	cfg    *config.Config
	store  *storage.Store
	runner Runner
}

func NewService(cfg *config.Config, store *storage.Store) *Service {
	return &Service{cfg: cfg, store: store, runner: runAgyQuota}
}

func (s *Service) SetRunnerForTest(r Runner) { s.runner = r }

func (s *Service) EnsureFreshAll(ctx context.Context, model string) map[string]error {
	errs := map[string]error{}
	if s == nil || s.cfg == nil || s.store == nil || !s.cfg.Quota.Enabled {
		return errs
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, w := range s.cfg.EnabledWorkers() {
		if !s.cfg.QuotaEnabledFor(w.Username) {
			continue
		}
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.ensureWorker(ctx, w.Username, ModelGroup(model)); err != nil {
				mu.Lock()
				errs[w.Username] = err
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errs
}

func (s *Service) EnsureFreshAllGroups(ctx context.Context) map[string]error {
	errs := map[string]error{}
	if s == nil || s.cfg == nil || s.store == nil || !s.cfg.Quota.Enabled {
		return errs
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, w := range s.cfg.EnabledWorkers() {
		if !s.cfg.QuotaEnabledFor(w.Username) {
			continue
		}
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.refreshWorker(ctx, w.Username); err != nil {
				mu.Lock()
				errs[w.Username] = err
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errs
}

func (s *Service) ensureWorker(ctx context.Context, worker, groupKey string) error {
	rows, err := s.store.GetQuotaSnapshots(worker, groupKey)
	if err != nil {
		return err
	}
	if snapshotsFresh(rows, time.Now().UTC(), s.cfg.Quota) {
		return nil
	}
	return s.refreshWorker(ctx, worker)
}

func (s *Service) refreshWorker(ctx context.Context, worker string) error {
	now := time.Now().UTC()
	owner := os.Getpid()
	lease := now.Add(s.cfg.Quota.CommandTimeout + 5*time.Second)
	claimed, err := s.store.TryClaimQuotaRefresh(worker, owner, lease)
	if err != nil {
		return err
	}
	if !claimed {
		// Another router process is refreshing. Existing cached data remains usable.
		return nil
	}
	defer s.store.ReleaseQuotaRefresh(worker, owner)
	timeout := s.cfg.Quota.CommandTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	raw, err := s.runner(qctx, worker, s.cfg.GetAgyPath(worker))
	if err != nil {
		return err
	}
	fetched := time.Now().UTC()
	parsed, err := Parse(worker, raw, fetched)
	if err != nil {
		return err
	}
	snapshots := make([]storage.QuotaSnapshot, 0, len(parsed))
	for _, b := range parsed {
		snapshots = append(snapshots, storage.QuotaSnapshot{Worker: b.Worker, GroupKey: b.GroupKey, GroupName: b.GroupName, Window: b.Window, RemainingFraction: b.RemainingFraction, Disabled: b.Disabled, ResetTime: b.ResetTime, FetchedAt: b.FetchedAt})
	}
	return s.store.ReplaceQuotaSnapshots(worker, snapshots)
}

func snapshotsFresh(rows []storage.QuotaSnapshot, now time.Time, cfg config.QuotaSettings) bool {
	if len(rows) == 0 {
		return false
	}
	interval := cfg.RefreshInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	snapshotFetched := rows[0].FetchedAt
	var activeFetched time.Time
	low := false
	var earliestExhaustedReset time.Time
	activeRows := 0
	for _, r := range rows {
		if r.FetchedAt.Before(snapshotFetched) {
			snapshotFetched = r.FetchedAt
		}
		if r.Disabled {
			continue
		}
		activeRows++
		if activeFetched.IsZero() || r.FetchedAt.Before(activeFetched) {
			activeFetched = r.FetchedAt
		}
		if r.RemainingFraction <= 0 {
			// As soon as any exhausted bucket reaches its reset, refresh so the
			// cache and human-facing stats do not keep showing a stale 0% value.
			if !r.ResetTime.After(now) {
				return false
			}
			if earliestExhaustedReset.IsZero() || r.ResetTime.Before(earliestExhaustedReset) {
				earliestExhaustedReset = r.ResetTime
			}
		}
		if r.RemainingFraction <= cfg.LowRemainingThreshold {
			low = true
		}
	}
	if activeRows == 0 {
		return now.Sub(snapshotFetched) < interval
	}
	if !earliestExhaustedReset.IsZero() {
		return now.Before(earliestExhaustedReset)
	}
	if low && cfg.LowRemainingInterval > 0 {
		interval = cfg.LowRemainingInterval
	}
	return now.Sub(activeFetched) < interval
}

func runAgyQuota(ctx context.Context, worker, agyPath string) ([]byte, error) {
	result, err := proc.Run(ctx, nil, "sudo", "-H", "-u", worker, agyPath, "-p", "/quota", "--output-format", "json")
	if err != nil {
		detail := strings.TrimSpace(string(result.Stderr))
		if detail != "" {
			return nil, fmt.Errorf("agy /quota failed: %w: %s", err, detail)
		}
		return nil, fmt.Errorf("agy /quota failed: %w", err)
	}
	return result.Stdout, nil
}
