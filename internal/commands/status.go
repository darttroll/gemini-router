package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/control"
	"github.com/darttroll/gemini-router/internal/quota"
	"github.com/darttroll/gemini-router/internal/scheduler"
	"github.com/darttroll/gemini-router/internal/storage"
)

type QuotaWindowStatus struct {
	RemainingFraction float64 `json:"remaining_fraction"`
	Disabled          bool    `json:"disabled,omitempty"`
	ResetAt           string  `json:"reset_at,omitempty"`
	RefreshedAt       string  `json:"refreshed_at"`
}

type ProviderStatus struct {
	Worker               string             `json:"worker"`
	Model                string             `json:"model"`
	RateLimitRPS         float64            `json:"rate_limit_rps"`
	ConcurrencyLimit     int                `json:"concurrency_limit"`
	InFlight             int                `json:"in_flight"`
	QuotaBlockedUntil    string             `json:"quota_blocked_until,omitempty"`
	RecentThrottleRate   float64            `json:"recent_throttle_rate"`
	AvgLatencyMs         int64              `json:"avg_latency_ms"`
	P95LatencyMs         int64              `json:"p95_latency_ms"`
	EffectiveCapacityRPS float64            `json:"effective_capacity_rps"`
	FiveHourQuota        *QuotaWindowStatus `json:"five_hour_quota,omitempty"`
	WeeklyQuota          *QuotaWindowStatus `json:"weekly_quota,omitempty"`
}

type RouterStatus struct {
	QueueDepth            int              `json:"queue_depth"`
	ModelQueueDepth       int              `json:"model_queue_depth"`
	PredictedQueueWaitMs  int64            `json:"predicted_queue_wait_ms"`
	PredictedTotalTimeMs  int64            `json:"predicted_total_time_ms"`
	ActiveRequests        int              `json:"active_requests"`
	ReadyProviders        int              `json:"ready_providers"`
	QuotaBlockedProviders int              `json:"quota_blocked_providers"`
	ObservedThroughputRPS float64          `json:"observed_throughput_rps"`
	EffectiveCapacityRPS  float64          `json:"effective_capacity_rps"`
	LoadFactor            float64          `json:"load_factor"`
	Providers             []ProviderStatus `json:"providers"`
}

type StatusCommand struct {
	cfg      *config.Config
	store    *storage.Store
	quotaSvc *quota.Service
}

func NewStatusCommand(cfg *config.Config, store *storage.Store) *StatusCommand {
	c := &StatusCommand{cfg: cfg, store: store}
	if cfg != nil && cfg.Quota.Enabled {
		c.quotaSvc = quota.NewService(cfg, store)
	}
	return c
}

func (c *StatusCommand) defaultState(worker, model string) storage.ControllerState {
	now := time.Now().UTC()
	rate := c.cfg.Adaptive.InitialRate
	if rate <= 0 {
		rate = 2
	}
	burst := c.cfg.Adaptive.RateBurst
	if burst <= 0 {
		burst = 3
	}
	conc := c.cfg.Adaptive.InitialConcurrency
	if conc <= 0 {
		conc = 4
	}
	retry := c.cfg.Adaptive.RetryBudgetCapacity
	if retry <= 0 {
		retry = 10
	}
	return storage.ControllerState{Worker: worker, Model: model, Rate: rate, StableRate: rate, Tokens: burst, TokenUpdatedAt: now, ConcurrencyLimit: conc, RetryTokens: retry, RetryUpdatedAt: now}
}

func (c *StatusCommand) Snapshot() (RouterStatus, error) {
	model := c.cfg.DefaultModel
	if model == "" {
		model = "default"
	}
	now := time.Now().UTC()
	estimate, err := scheduler.New(c.cfg, c.store).EstimateNewRequest(model)
	if err != nil {
		return RouterStatus{}, err
	}
	active, err := c.store.CountAllActiveRequests()
	if err != nil {
		return RouterStatus{}, err
	}
	out := RouterStatus{
		QueueDepth:           estimate.QueueDepth,
		ModelQueueDepth:      estimate.ModelQueueDepth,
		PredictedQueueWaitMs: estimate.PredictedWait.Milliseconds(),
		PredictedTotalTimeMs: (estimate.PredictedWait + estimate.PredictedExecution).Milliseconds(),
		ActiveRequests:       active,
		EffectiveCapacityRPS: estimate.EffectiveCapacityRPS,
		Providers:            make([]ProviderStatus, 0, len(c.cfg.Workers)),
	}
	coldStartExec := time.Second
	if c.cfg.RequestTimeout > 0 && c.cfg.RequestTimeout/2 < coldStartExec {
		coldStartExec = c.cfg.RequestTimeout / 2
	}
	if coldStartExec < 100*time.Millisecond {
		coldStartExec = 100 * time.Millisecond
	}
	for _, w := range c.cfg.Workers {
		if !w.Enabled {
			continue
		}
		st, err := c.store.GetControllerState(w.Username, model, c.defaultState(w.Username, model))
		if err != nil {
			return RouterStatus{}, err
		}
		m, err := c.store.GetRecentMetrics(w.Username, model, time.Minute)
		if err != nil {
			return RouterStatus{}, err
		}
		inflight, err := c.store.CountActiveRequestsForModel(w.Username, model)
		if err != nil {
			return RouterStatus{}, err
		}
		capacityAvg, err := c.store.GetAverageDuration(w.Username, model, 10*time.Minute)
		if err != nil {
			return RouterStatus{}, err
		}
		if capacityAvg <= 0 {
			capacityAvg = coldStartExec
		}
		limit := st.ConcurrencyLimit
		if limit <= 0 {
			limit = c.defaultState(w.Username, model).ConcurrencyLimit
		}
		capacity := math.Min(st.Rate, float64(limit)/math.Max(capacityAvg.Seconds(), .1))
		ready := control.QuotaReady(st, now)
		if c.cfg.QuotaEnabledFor(w.Username) {
			if quotaUntil, qerr := c.store.GetQuotaBlockedUntil(w.Username, quota.ModelGroup(model), now); qerr != nil {
				return RouterStatus{}, qerr
			} else if quotaUntil.After(now) {
				ready = false
				if st.QuotaBlockedUntil.Before(quotaUntil) {
					st.QuotaBlockedUntil = quotaUntil
				}
			}
		}
		ps := ProviderStatus{Worker: w.Username, Model: model, RateLimitRPS: st.Rate, ConcurrencyLimit: limit, InFlight: inflight, AvgLatencyMs: m.AvgLatency.Milliseconds(), P95LatencyMs: m.P95Latency.Milliseconds()}
		if c.cfg.QuotaEnabledFor(w.Username) {
			qrows, qerr := c.store.GetQuotaSnapshots(w.Username, quota.ModelGroup(model))
			if qerr != nil {
				return RouterStatus{}, qerr
			}
			for _, qr := range qrows {
				q := &QuotaWindowStatus{RemainingFraction: qr.RemainingFraction, Disabled: qr.Disabled, RefreshedAt: qr.FetchedAt.Format(time.RFC3339Nano)}
				if !qr.ResetTime.IsZero() {
					q.ResetAt = qr.ResetTime.Format(time.RFC3339Nano)
				}
				if qr.Window == "5h" {
					ps.FiveHourQuota = q
				} else if qr.Window == "weekly" {
					ps.WeeklyQuota = q
				}
			}
		}
		if m.Total > 0 {
			ps.RecentThrottleRate = float64(m.ShortThrottles) / float64(m.Total)
		}
		if !ready {
			ps.QuotaBlockedUntil = st.QuotaBlockedUntil.Format(time.RFC3339Nano)
			out.QuotaBlockedProviders++
		} else {
			ps.EffectiveCapacityRPS = capacity
			out.ReadyProviders++
		}
		out.ObservedThroughputRPS += m.ThroughputRPS
		out.Providers = append(out.Providers, ps)
	}
	if out.EffectiveCapacityRPS > 0 {
		out.LoadFactor = out.ObservedThroughputRPS / out.EffectiveCapacityRPS
	}
	return out, nil
}

func (c *StatusCommand) Run(w io.Writer, jsonOutput bool) error {
	if c.quotaSvc != nil {
		timeout := c.cfg.Quota.CommandTimeout + 2*time.Second
		if timeout <= 0 {
			timeout = 12 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_ = c.quotaSvc.EnsureFreshAll(ctx, c.cfg.DefaultModel)
		cancel()
	}
	status, err := c.Snapshot()
	if err != nil {
		return err
	}
	if jsonOutput {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(status)
	}
	fmt.Fprintf(w, "queue_total=%d queue_model=%d active=%d ready=%d blocked=%d predicted_wait=%s throughput=%.2f rps capacity=%.2f rps load=%.2f\n", status.QueueDepth, status.ModelQueueDepth, status.ActiveRequests, status.ReadyProviders, status.QuotaBlockedProviders, time.Duration(status.PredictedQueueWaitMs)*time.Millisecond, status.ObservedThroughputRPS, status.EffectiveCapacityRPS, status.LoadFactor)
	for _, p := range status.Providers {
		state := "ready"
		if p.QuotaBlockedUntil != "" {
			state = "blocked until " + p.QuotaBlockedUntil
		}
		five, weekly := "-", "-"
		if p.FiveHourQuota != nil {
			five = fmt.Sprintf("%.1f%%", p.FiveHourQuota.RemainingFraction*100)
		}
		if p.WeeklyQuota != nil {
			weekly = fmt.Sprintf("%.1f%%", p.WeeklyQuota.RemainingFraction*100)
		}
		fmt.Fprintf(w, "%s: %s rate=%.2f rps concurrency=%d in_flight=%d avg=%dms p95=%dms throttle=%.1f%% quota5h=%s quota_weekly=%s\n", p.Worker, state, p.RateLimitRPS, p.ConcurrencyLimit, p.InFlight, p.AvgLatencyMs, p.P95LatencyMs, p.RecentThrottleRate*100, five, weekly)
	}
	return nil
}
