package commands

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/quota"
	"github.com/darttroll/gemini-router/internal/storage"
)

type StatsCommand struct {
	cfg      *config.Config
	store    *storage.Store
	quotaSvc *quota.Service
}

func NewStatsCommand(cfg *config.Config, store *storage.Store) *StatsCommand {
	c := &StatsCommand{cfg: cfg, store: store}
	if cfg != nil && cfg.Quota.Enabled {
		c.quotaSvc = quota.NewService(cfg, store)
	}
	return c
}

func (s *StatsCommand) Run(w io.Writer, period time.Duration) error {
	model := s.cfg.DefaultModel
	if model == "" {
		model = "default"
	}
	if s.quotaSvc != nil {
		timeout := s.cfg.Quota.CommandTimeout + 2*time.Second
		if timeout <= 0 {
			timeout = 12 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_ = s.quotaSvc.EnsureFreshAll(ctx, model)
		cancel()
	}
	routerStatus, err := NewStatusCommand(s.cfg, s.store).Snapshot()
	if err != nil {
		return fmt.Errorf("getting router queue status: %w", err)
	}
	wait := time.Duration(routerStatus.PredictedQueueWaitMs) * time.Millisecond
	fmt.Fprintf(w, "Queue total: %d\nQueue for model %s: %d\nPredicted wait for new request: %s\n\n", routerStatus.QueueDepth, model, routerStatus.ModelQueueDepth, wait)

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	defer tw.Flush()
	fmt.Fprintf(tw, "Worker\tRequests\tErrors\tAvg Duration\tActive\tStatus\t5h Quota\tWeekly Quota\n")
	fmt.Fprintf(tw, "------\t--------\t------\t------------\t------\t------\t--------\t------------\n")
	group := quota.ModelGroup(model)
	for _, worker := range s.cfg.Workers {
		stats, err := s.store.GetWorkerStats(worker.Username, period)
		if err != nil {
			return fmt.Errorf("getting stats for %s: %w", worker.Username, err)
		}
		active, err := s.store.CountActiveRequests(worker.Username)
		if err != nil {
			return fmt.Errorf("getting active count for %s: %w", worker.Username, err)
		}
		status := s.getWorkerStatus(worker, model, group)
		avg := "-"
		if stats.TotalRequests > 0 {
			avg = fmt.Sprintf("%dms", stats.AvgDurationMs)
		}
		rows, err := s.store.GetQuotaSnapshots(worker.Username, group)
		if err != nil {
			return fmt.Errorf("getting quota for %s: %w", worker.Username, err)
		}
		five, weekly := formatQuotaWindows(rows, time.Now().UTC())
		if !s.cfg.QuotaEnabledFor(worker.Username) {
			five, weekly = "disabled", "disabled"
		} else if len(rows) == 0 {
			five, weekly = "unavailable", "unavailable"
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%d\t%s\t%s\t%s\n", worker.Username, stats.TotalRequests, stats.Errors, avg, active, status, five, weekly)
	}
	return nil
}

func (s *StatsCommand) getWorkerStatus(worker config.WorkerConfig, model, group string) string {
	if !worker.Enabled {
		return "disabled"
	}
	now := time.Now().UTC()
	if s.cfg.QuotaEnabledFor(worker.Username) {
		quotaUntil, err := s.store.GetQuotaBlockedUntil(worker.Username, group, now)
		if err == nil && quotaUntil.After(now) {
			return fmt.Sprintf("quota-blocked (%s)", formatUntil(quotaUntil, now))
		}
	}
	defaults := storage.ControllerState{Worker: worker.Username, Model: model, Rate: s.cfg.Adaptive.InitialRate, ConcurrencyLimit: s.cfg.Adaptive.InitialConcurrency, RetryTokens: s.cfg.Adaptive.RetryBudgetCapacity}
	if st, err := s.store.GetControllerState(worker.Username, model, defaults); err == nil && st.QuotaBlockedUntil.After(now) {
		return fmt.Sprintf("blocked (%s)", formatUntil(st.QuotaBlockedUntil, now))
	}
	return "active"
}

func formatQuotaWindows(rows []storage.QuotaSnapshot, now time.Time) (string, string) {
	five, weekly := "-", "-"
	for _, r := range rows {
		cell := fmt.Sprintf("%.1f%%", r.RemainingFraction*100)
		if r.Disabled {
			cell = "disabled"
		}
		if !r.Disabled && !r.ResetTime.IsZero() {
			cell += " (reset " + formatUntil(r.ResetTime, now) + ")"
		}
		switch r.Window {
		case "5h":
			five = cell
		case "weekly":
			weekly = cell
		}
	}
	return five, weekly
}

func formatUntil(t, now time.Time) string {
	d := t.Sub(now)
	if d <= 0 {
		return "now"
	}
	d = d.Round(time.Minute)
	if d >= 24*time.Hour {
		days := int(d / (24 * time.Hour))
		rest := d - time.Duration(days)*24*time.Hour
		return fmt.Sprintf("%dd%dh", days, int(rest/time.Hour))
	}
	if d >= time.Hour {
		h := int(d / time.Hour)
		m := int((d - time.Duration(h)*time.Hour) / time.Minute)
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh%dm", h, m)
	}
	if d < time.Minute {
		return "<1m"
	}
	return fmt.Sprintf("%dm", int(d/time.Minute))
}
