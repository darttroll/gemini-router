package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/control"
	"github.com/darttroll/gemini-router/internal/quota"
	"github.com/darttroll/gemini-router/internal/storage"
)

var (
	errNotQueueHead = errors.New("request is not queue head")
	errNoCapacity   = errors.New("no provider capacity")
)

type Admission struct {
	QueueDepth           int
	ModelQueueDepth      int
	PredictedWait        time.Duration
	PredictedExecution   time.Duration
	EffectiveCapacityRPS float64
	Accepted             bool
	Reason               string
}

type Lease struct {
	ActiveID  int64
	RequestID string
	Worker    string
	Model     string
	StartedAt time.Time
}

type Outcome struct {
	Class         control.ErrorClass
	Duration      time.Duration
	ResetDuration time.Duration
}

type Scheduler struct {
	cfg   *config.Config
	store *storage.Store
}

func New(cfg *config.Config, store *storage.Store) *Scheduler {
	return &Scheduler{cfg: cfg, store: store}
}

func (s *Scheduler) defaultState(worker, model string) storage.ControllerState {
	now := time.Now().UTC()
	return storage.ControllerState{Worker: worker, Model: model, Rate: s.cfg.Adaptive.InitialRate, StableRate: s.cfg.Adaptive.InitialRate,
		Tokens: s.cfg.Adaptive.RateBurst, TokenUpdatedAt: now, ConcurrencyLimit: s.cfg.Adaptive.InitialConcurrency,
		RetryTokens: s.cfg.Adaptive.RetryBudgetCapacity, RetryUpdatedAt: now}
}

func (s *Scheduler) EstimateNewRequest(model string) (Admission, error) {
	if model == "" {
		model = s.cfg.DefaultModel
	}
	if model == "" {
		model = "default"
	}
	return s.estimateNewRequest(model, time.Now().UTC())
}

func (s *Scheduler) estimateNewRequest(model string, now time.Time) (Admission, error) {
	totalDepth, err := s.store.QueueDepth()
	if err != nil {
		return Admission{}, err
	}
	modelDepth, err := s.store.QueueDepthForModel(model)
	if err != nil {
		return Admission{}, err
	}
	adm := Admission{QueueDepth: totalDepth, ModelQueueDepth: modelDepth}
	coldStartExec := time.Second
	if s.cfg.RequestTimeout > 0 && s.cfg.RequestTimeout/2 < coldStartExec {
		coldStartExec = s.cfg.RequestTimeout / 2
	}
	if coldStartExec < 100*time.Millisecond {
		coldStartExec = 100 * time.Millisecond
	}
	maxExec := coldStartExec
	hasFreeSlot := false
	var earliestBlockedUntil time.Time

	for _, w := range s.cfg.EnabledWorkers() {
		st, err := s.store.GetControllerState(w.Username, model, s.defaultState(w.Username, model))
		if err != nil {
			return Admission{}, err
		}
		ready := control.QuotaReady(st, now)
		blockedUntil := st.QuotaBlockedUntil
		if s.cfg.QuotaEnabledFor(w.Username) {
			quotaUntil, err := s.store.GetQuotaBlockedUntil(w.Username, quota.ModelGroup(model), now)
			if err != nil {
				return Admission{}, err
			}
			if quotaUntil.After(now) {
				ready = false
				if quotaUntil.After(blockedUntil) {
					blockedUntil = quotaUntil
				}
			}
		}
		if !ready {
			if blockedUntil.After(now) && (earliestBlockedUntil.IsZero() || blockedUntil.Before(earliestBlockedUntil)) {
				earliestBlockedUntil = blockedUntil
			}
			continue
		}

		avg, err := s.store.GetAverageDuration(w.Username, model, 10*time.Minute)
		if err != nil {
			return Admission{}, err
		}
		if avg <= 0 {
			avg = coldStartExec
		}
		if avg > maxExec {
			maxExec = avg
		}
		concurrency := st.ConcurrencyLimit
		if concurrency <= 0 {
			concurrency = s.cfg.Adaptive.InitialConcurrency
		}
		active, err := s.store.CountActiveRequestsForModel(w.Username, model)
		if err != nil {
			return Admission{}, err
		}
		if active < concurrency {
			hasFreeSlot = true
		}
		service := math.Min(st.Rate, float64(concurrency)/math.Max(avg.Seconds(), 0.1))
		if service > 0 {
			adm.EffectiveCapacityRPS += service
		}
	}

	adm.PredictedExecution = maxExec
	if adm.EffectiveCapacityRPS > 0 {
		workAhead := float64(modelDepth)
		if modelDepth == 0 && !hasFreeSlot {
			workAhead = 1
		}
		adm.PredictedWait = time.Duration((workAhead / adm.EffectiveCapacityRPS) * float64(time.Second))
	} else if earliestBlockedUntil.After(now) {
		adm.PredictedWait = earliestBlockedUntil.Sub(now)
	}
	return adm, nil
}

func (s *Scheduler) Admit(ctx context.Context, req storage.PendingRequest) (Admission, error) {
	now := time.Now().UTC()
	if req.EnqueuedAt.IsZero() {
		req.EnqueuedAt = now
	}
	if req.Deadline.IsZero() {
		req.Deadline = now.Add(s.cfg.RequestTimeout)
	}
	if !req.Deadline.After(now) {
		return Admission{Accepted: false, Reason: "deadline expired"}, nil
	}
	_, _ = s.store.CleanupStalePendingRequests()
	adm, err := s.estimateNewRequest(req.Model, now)
	if err != nil {
		return Admission{}, err
	}
	if req.Unbounded {
		adm.Accepted = true
	} else {
		switch {
		case adm.QueueDepth >= s.cfg.Queue.MaxDepth:
			adm.Reason = "queue depth limit exceeded"
		case adm.EffectiveCapacityRPS <= 0:
			adm.Reason = "no ready provider capacity"
		case adm.PredictedWait > s.cfg.Queue.MaxDelay:
			adm.Reason = "predicted queue delay exceeds limit"
		case adm.PredictedWait+adm.PredictedExecution > time.Until(req.Deadline):
			adm.Reason = "predicted completion exceeds deadline"
		default:
			adm.Accepted = true
		}
	}
	if !adm.Accepted {
		return adm, nil
	}
	if req.Unbounded {
		if err := s.store.EnqueueRequest(req); err != nil {
			return Admission{}, err
		}
		adm.QueueDepth++
		adm.ModelQueueDepth++
	} else {
		accepted, depth, err := s.store.EnqueueRequestIfBelowLimit(ctx, req, s.cfg.Queue.MaxDepth)
		if err != nil {
			return Admission{}, err
		}
		adm.QueueDepth = depth
		if !accepted {
			adm.Accepted = false
			adm.Reason = "queue depth limit exceeded"
			return adm, nil
		}
		adm.ModelQueueDepth++
	}
	select {
	case <-ctx.Done():
		_ = s.store.RemovePendingRequest(req.RequestID)
		return Admission{}, ctx.Err()
	default:
	}
	return adm, nil
}

func (s *Scheduler) Acquire(ctx context.Context, requestID string) (Lease, error) {
	for {
		head, headErr := s.store.IsQueueHead(requestID)
		if headErr != nil {
			return Lease{}, headErr
		}
		if !head {
			timer := time.NewTimer(s.cfg.Queue.PollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				_ = s.Cancel(requestID)
				return Lease{}, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		lease, retry, err := s.tryAcquireOnce(ctx, requestID)
		if err == nil {
			return lease, nil
		}
		if !errors.Is(err, errNotQueueHead) && !errors.Is(err, errNoCapacity) {
			return Lease{}, err
		}
		wait := s.cfg.Queue.PollInterval
		if retry > wait {
			wait = retry
		}
		if wait > time.Second {
			wait = time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			_ = s.Cancel(requestID)
			return Lease{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Scheduler) tryAcquireOnce(ctx context.Context, requestID string) (Lease, time.Duration, error) {
	tx, err := s.store.BeginImmediate(ctx)
	if err != nil {
		return Lease{}, 0, err
	}
	defer tx.Rollback()
	req, err := tx.GetPendingRequest(requestID)
	if err != nil {
		return Lease{}, 0, err
	}
	if req == nil {
		return Lease{}, 0, fmt.Errorf("pending request %s not found", requestID)
	}
	now := time.Now().UTC()
	if !req.Deadline.IsZero() && !req.Deadline.After(now) {
		_ = tx.DeletePending(requestID)
		_ = tx.Commit()
		return Lease{}, 0, context.DeadlineExceeded
	}
	head, err := tx.OldestPendingRequestID(req.Model)
	if err != nil {
		return Lease{}, 0, err
	}
	if head != requestID {
		return Lease{}, s.cfg.Queue.PollInterval, errNotQueueHead
	}

	type candidate struct {
		worker string
		state  storage.ControllerState
		score  float64
		last   time.Time
	}
	var best *candidate
	minWait := time.Second
	for _, w := range s.cfg.EnabledWorkers() {
		st, err := tx.GetControllerState(w.Username, req.Model, s.defaultState(w.Username, req.Model))
		if err != nil {
			return Lease{}, 0, err
		}
		if !control.QuotaReady(st, now) {
			continue
		}
		if s.cfg.QuotaEnabledFor(w.Username) {
			quotaUntil, err := tx.GetQuotaBlockedUntil(w.Username, quota.ModelGroup(req.Model), now)
			if err != nil {
				return Lease{}, 0, err
			}
			if quotaUntil.After(now) {
				continue
			}
		}
		limit := st.ConcurrencyLimit
		if limit <= 0 {
			limit = s.cfg.Adaptive.InitialConcurrency
		}
		active, err := tx.CountActive(w.Username, req.Model)
		if err != nil {
			return Lease{}, 0, err
		}
		if active >= limit {
			continue
		}
		next, ok, wait := control.TryConsumeStartToken(st, now, s.cfg.Adaptive)
		if !ok {
			if wait < minWait {
				minWait = wait
			}
			continue
		}
		last, err := tx.LastUsedAt(w.Username)
		if err != nil {
			return Lease{}, 0, err
		}
		avg, err := tx.GetAverageDuration(w.Username, req.Model, 10*time.Minute)
		if err != nil {
			return Lease{}, 0, err
		}
		if avg <= 0 {
			avg = time.Second
		}
		// Prefer the worker with the least currently occupied normalized service
		// capacity. Idle workers therefore tie regardless of their learned
		// concurrency limit and fall back to least-recently-used selection instead
		// of turning a larger limit into a permanent preference bonus.
		score := avg.Seconds() * float64(active) / float64(limit)
		c := candidate{worker: w.Username, state: next, score: score, last: last}
		if best == nil || c.score < best.score || (c.score == best.score && c.last.Before(best.last)) {
			tmp := c
			best = &tmp
		}
	}
	if best == nil {
		return Lease{}, minWait, errNoCapacity
	}
	if err := tx.PutControllerState(best.state); err != nil {
		return Lease{}, 0, err
	}
	id, err := tx.InsertActiveRequest(*req, best.worker, now)
	if err != nil {
		return Lease{}, 0, err
	}
	if err := tx.DeletePending(requestID); err != nil {
		return Lease{}, 0, err
	}
	if err := tx.Commit(); err != nil {
		return Lease{}, 0, err
	}
	return Lease{ActiveID: id, RequestID: requestID, Worker: best.worker, Model: req.Model, StartedAt: now}, 0, nil
}

func (s *Scheduler) Release(ctx context.Context, lease Lease, outcome Outcome) error {
	tx, err := s.store.BeginImmediate(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	deleted, err := tx.DeleteActive(lease.ActiveID)
	if err != nil {
		return err
	}
	if !deleted {
		return nil
	}
	st, err := tx.GetControllerState(lease.Worker, lease.Model, s.defaultState(lease.Worker, lease.Model))
	if err != nil {
		return err
	}
	switch outcome.Class {
	case control.Success:
		st = control.OnRateSuccess(st, s.cfg.Adaptive)
		st = control.ObserveLatency(st, outcome.Duration, s.cfg.Adaptive)
		// Success that started after an older breaker expired proves recovery.
		// A breaker created after this request started is newer evidence and stays.
		if !st.QuotaBlockedUntil.IsZero() && !st.QuotaBlockedUntil.After(lease.StartedAt) {
			st.QuotaBlockedUntil = time.Time{}
			st.QuotaProbeCount = 0
		}
		// Clean traffic restores retry headroom promptly after an incident.
		st.RetryTokens = math.Min(s.cfg.Adaptive.RetryBudgetCapacity, st.RetryTokens+1)
		st.RetryUpdatedAt = time.Now().UTC()
	case control.ShortThrottle:
		st = control.OnShortThrottle(st, s.cfg.Adaptive)
	case control.LongQuotaExhausted:
		st = control.SetLongQuota(st, time.Now().UTC(), outcome.ResetDuration)
	case control.AuthOrEligibilityFailure:
		st.QuotaBlockedUntil = time.Now().UTC().Add(24 * time.Hour)
	case control.TransientUpstreamFailure:
		st = control.OnConcurrencyDrop(st, s.cfg.Adaptive)
	case control.LocalPreparationFailure:
		// No provider call happened. Undo the start-pacing reservation and keep
		// learned provider health/concurrency/retry state unchanged.
		st = control.RefundStartToken(st, s.cfg.Adaptive)
	}
	if err := tx.PutControllerState(st); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Scheduler) Cancel(requestID string) error { return s.store.RemovePendingRequest(requestID) }

// TryConsumeRetry atomically consumes one retry-budget token for a worker/model.
func (s *Scheduler) TryConsumeRetry(ctx context.Context, worker, model string) (bool, error) {
	tx, err := s.store.BeginImmediate(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	st, err := tx.GetControllerState(worker, model, s.defaultState(worker, model))
	if err != nil {
		return false, err
	}
	st, ok := control.TryConsumeRetry(st, time.Now().UTC(), s.cfg.Adaptive)
	if err := tx.PutControllerState(st); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return ok, nil
}

// TryAcquire performs one atomic reservation attempt without waiting.
func (s *Scheduler) TryAcquire(ctx context.Context, requestID string) (Lease, bool, error) {
	lease, _, err := s.tryAcquireOnce(ctx, requestID)
	if err == nil {
		return lease, true, nil
	}
	if errors.Is(err, errNotQueueHead) || errors.Is(err, errNoCapacity) {
		return Lease{}, false, nil
	}
	return Lease{}, false, err
}
