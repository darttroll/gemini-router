package commands

import (
	"context"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/control"
	"github.com/darttroll/gemini-router/internal/proc"
	"github.com/darttroll/gemini-router/internal/storage"
)

type CheckFn func(worker, agyPath string) (ok bool, detail string)
type HealthCommand struct {
	cfg     *config.Config
	store   *storage.Store
	checkFn CheckFn
}

func NewHealthCommand(cfg *config.Config, store *storage.Store) *HealthCommand {
	return &HealthCommand{cfg: cfg, store: store, checkFn: checkWorkerHealth}
}

func (h *HealthCommand) defaultState(worker, model string) storage.ControllerState {
	now := time.Now().UTC()
	rate := h.cfg.Adaptive.InitialRate
	if rate <= 0 {
		rate = 2
	}
	burst := h.cfg.Adaptive.RateBurst
	if burst <= 0 {
		burst = 3
	}
	conc := h.cfg.Adaptive.InitialConcurrency
	if conc <= 0 {
		conc = 4
	}
	retry := h.cfg.Adaptive.RetryBudgetCapacity
	if retry <= 0 {
		retry = 10
	}
	return storage.ControllerState{Worker: worker, Model: model, Rate: rate, StableRate: rate, Tokens: burst, TokenUpdatedAt: now, ConcurrencyLimit: conc, RetryTokens: retry, RetryUpdatedAt: now}
}
func (h *HealthCommand) Run(w io.Writer, verbose bool) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	defer tw.Flush()
	model := h.cfg.DefaultModel
	if model == "" {
		model = "default"
	}
	now := time.Now().UTC()
	if verbose {
		fmt.Fprintln(tw, "Worker\tEnabled\tAccount\tBreaker\tRate\tConcurrency\tInFlight\tAvgLatency\tP95\tThrottle")
		fmt.Fprintln(tw, "------\t-------\t-------\t-------\t----\t-----------\t--------\t----------\t---\t--------")
	} else {
		fmt.Fprintln(tw, "Worker\tEnabled\tAccount\tBreaker\tRate\tConcurrency\tInFlight")
		fmt.Fprintln(tw, "------\t-------\t-------\t-------\t----\t-----------\t--------")
	}
	for _, worker := range h.cfg.Workers {
		enabled := "yes"
		account := "disabled"
		breaker := "-"
		rate := 0.0
		conc := 0
		inflight := 0
		avg, p95 := time.Duration(0), time.Duration(0)
		throttle := 0.0
		if !worker.Enabled {
			enabled = "no"
		} else {
			ok, detail := h.checkFn(worker.Username, h.cfg.GetAgyPath(worker.Username))
			if ok {
				account = "OK"
			} else {
				account = detail
			}
			st, err := h.store.GetControllerState(worker.Username, model, h.defaultState(worker.Username, model))
			if err != nil {
				return err
			}
			rate = st.Rate
			conc = st.ConcurrencyLimit
			inflight, err = h.store.CountActiveRequestsForModel(worker.Username, model)
			if err != nil {
				return err
			}
			if control.QuotaReady(st, now) {
				breaker = "ready"
			} else {
				breaker = fmt.Sprintf("blocked (%s)", time.Until(st.QuotaBlockedUntil).Round(time.Second))
			}
			if verbose {
				m, err := h.store.GetRecentMetrics(worker.Username, model, time.Minute)
				if err != nil {
					return err
				}
				avg = m.AvgLatency
				p95 = m.P95Latency
				if m.Total > 0 {
					throttle = float64(m.ShortThrottles) / float64(m.Total)
				}
			}
		}
		if verbose {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%.2f rps\t%d\t%d\t%s\t%s\t%.1f%%\n", worker.Username, enabled, account, breaker, rate, conc, inflight, avg.Round(time.Millisecond), p95.Round(time.Millisecond), throttle*100)
		} else {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%.2f rps\t%d\t%d\n", worker.Username, enabled, account, breaker, rate, conc, inflight)
		}
	}
	return nil
}
func checkWorkerHealth(worker, agyPath string) (bool, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := proc.Run(ctx, nil, "sudo", "-i", "-u", worker, agyPath, "models")
	if ctx.Err() == context.DeadlineExceeded {
		return false, "timeout"
	}
	if err != nil {
		detail := strings.TrimSpace(string(result.CombinedOutput()))
		if detail != "" {
			return false, fmt.Sprintf("error: %v: %s", err, detail)
		}
		return false, fmt.Sprintf("error: %v", err)
	}
	if strings.TrimSpace(string(result.Stdout)) == "" {
		return false, "empty response"
	}
	return true, "OK"
}
