package commands

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/control"
	"github.com/darttroll/gemini-router/internal/executor"
	"github.com/darttroll/gemini-router/internal/logger"
	"github.com/darttroll/gemini-router/internal/quota"
	"github.com/darttroll/gemini-router/internal/scheduler"
	"github.com/darttroll/gemini-router/internal/storage"
)

// ExecFunc runs one request-owned AGI invocation. Tests replace it with a fake.
type ExecFunc func(ctx context.Context, req executor.Request) (*executor.Result, error)

// PrintCommand implements the synchronous one-shot request path.
type PrintCommand struct {
	cfg      *config.Config
	store    *storage.Store
	logger   *logger.Logger
	sched    *scheduler.Scheduler
	quotaSvc *quota.Service
	execFn   ExecFunc
	idFn     func() string
	rand     *rand.Rand
}

// NewPrintCommand creates the adaptive print path. Worker dispatch is owned by Scheduler.
func NewPrintCommand(cfg *config.Config, store *storage.Store, lg *logger.Logger) *PrintCommand {
	return &PrintCommand{
		cfg: cfg, store: store, logger: lg,
		sched: scheduler.New(cfg, store),
		quotaSvc: func() *quota.Service {
			if cfg.Quota.Enabled {
				return quota.NewService(cfg, store)
			}
			return nil
		}(),
		idFn: func() string { return uuid.NewString() },
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (p *PrintCommand) ensureRuntimeDefaults() {
	if p.sched == nil {
		p.sched = scheduler.New(p.cfg, p.store)
	}
	if p.quotaSvc == nil && p.cfg.Quota.Enabled {
		p.quotaSvc = quota.NewService(p.cfg, p.store)
	}
	if p.idFn == nil {
		p.idFn = func() string { return uuid.NewString() }
	}
	if p.rand == nil {
		p.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	// Tests and old direct constructors may bypass config.Load().
	if p.cfg.Queue.MaxDelay <= 0 {
		p.cfg.Queue.MaxDelay = 10 * time.Second
	}
	if p.cfg.Queue.MaxDepth <= 0 {
		p.cfg.Queue.MaxDepth = 1000
	}
	if p.cfg.Queue.PollInterval <= 0 {
		p.cfg.Queue.PollInterval = 100 * time.Millisecond
	}
	if p.cfg.Adaptive.MinRate <= 0 {
		p.cfg.Adaptive.MinRate = .2
	}
	if p.cfg.Adaptive.InitialRate <= 0 {
		p.cfg.Adaptive.InitialRate = 2
	}
	if p.cfg.Adaptive.MaxRate <= 0 {
		p.cfg.Adaptive.MaxRate = 100
	}
	if p.cfg.Adaptive.RateBurst <= 0 {
		p.cfg.Adaptive.RateBurst = 3
	}
	if p.cfg.Adaptive.CubicBeta <= 0 {
		p.cfg.Adaptive.CubicBeta = .7
	}
	if p.cfg.Adaptive.CubicC <= 0 {
		p.cfg.Adaptive.CubicC = .4
	}
	if p.cfg.Adaptive.RateWindowSuccesses <= 0 {
		p.cfg.Adaptive.RateWindowSuccesses = 5
	}
	if p.cfg.Adaptive.InitialConcurrency <= 0 {
		p.cfg.Adaptive.InitialConcurrency = 4
	}
	if p.cfg.Adaptive.MaxConcurrency <= 0 {
		p.cfg.Adaptive.MaxConcurrency = 32
	}
	if p.cfg.Adaptive.ConcurrencyWindow <= 0 {
		p.cfg.Adaptive.ConcurrencyWindow = 10
	}
	if p.cfg.Adaptive.RetryBudgetCapacity <= 0 {
		p.cfg.Adaptive.RetryBudgetCapacity = 10
	}
	if p.cfg.RequestTimeout <= 0 {
		p.cfg.RequestTimeout = 5 * time.Minute
	}
}

// Run admits, queues, dispatches and executes one synchronous request.
func (p *PrintCommand) Run(ctx context.Context, prompt, modelOverride string, files []string, stdinContent, retryStrategy, outDir string, queueUnbounded bool) (string, error) {
	p.ensureRuntimeDefaults()
	model := modelOverride
	if model == "" {
		model = p.cfg.DefaultModel
	}
	if model == "" {
		model = "default"
	}

	_, _ = p.store.CleanupStaleRequests()
	_, _ = p.store.CleanupStalePendingRequests()

	runCtx, cancel := withOverallTimeout(ctx, p.cfg.RequestTimeout)
	defer cancel()
	if err := executor.ValidateAttachments(runCtx, files); err != nil {
		return "", fmt.Errorf("validating attachments: %w", err)
	}
	if p.quotaSvc != nil {
		_ = p.quotaSvc.EnsureFreshAll(runCtx, model)
	}
	deadline, _ := runCtx.Deadline()
	requestID := p.idFn()
	req := storage.PendingRequest{RequestID: requestID, Model: model, PID: os.Getpid(), EnqueuedAt: time.Now().UTC(), Deadline: deadline, Unbounded: queueUnbounded || retryStrategy == "wait"}

	admission, err := p.sched.Admit(runCtx, req)
	if err != nil {
		return "", fmt.Errorf("admitting request: %w", err)
	}
	if !admission.Accepted {
		return "", fmt.Errorf("router overloaded: %s", admission.Reason)
	}
	// From this point the request may exist in pending_requests. A lease
	// acquisition removes it atomically, so this is a no-op on the normal path
	// and a safety net for every cancellation/error return path.
	defer func() { _ = p.sched.Cancel(requestID) }()

	maxRetries := p.cfg.Balancer.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		queueStarted := time.Now()
		var lease scheduler.Lease
		if retryStrategy == "failfast" {
			var ok bool
			lease, ok, err = p.sched.TryAcquire(runCtx, requestID)
			if err == nil && !ok {
				_ = p.sched.Cancel(requestID)
				return "", fmt.Errorf("no available worker slot")
			}
		} else {
			lease, err = p.sched.Acquire(runCtx, requestID)
		}
		if err != nil {
			return "", err
		}
		queueWait := time.Since(queueStarted)

		execReq := executor.Request{ID: requestID, Worker: lease.Worker, Prompt: prompt, Model: model, Files: files, StdinContent: stdinContent, OutDir: outDir}
		var result *executor.Result
		var execErr error
		if p.execFn != nil {
			result, execErr = p.execFn(runCtx, execReq)
		} else {
			workerExec := executor.New(p.cfg.GetAgyPath(lease.Worker), p.cfg.RequestTimeout)
			result, execErr = workerExec.ExecuteRequest(runCtx, execReq)
		}

		outcome, terminalErr := classifyAttempt(result, execErr)
		releaseErr := p.releaseLease(lease, outcome)
		recordErr := terminalErr
		if releaseErr != nil {
			recordErr = errors.Join(recordErr, fmt.Errorf("releasing worker lease: %w", releaseErr))
		}

		p.recordAttempt(requestID, lease, model, prompt, files, attempt, queueWait, admission, result, outcome, recordErr)

		if outcome.Class == control.Success && terminalErr == nil {
			return result.Output, nil
		}
		if releaseErr != nil {
			return "", recordErr
		}
		if terminalErr == nil {
			terminalErr = errors.New("request failed")
		}
		lastErr = terminalErr

		if outcome.Class == control.ClientCancelled || outcome.Class == control.Timeout {
			return "", terminalErr
		}
		if outcome.Class == control.InvalidRequestOrModel || outcome.Class == control.LocalPreparationFailure {
			return "", terminalErr
		}
		if attempt >= maxRetries {
			break
		}

		retryCtx, retryCancel := context.WithTimeout(context.Background(), 8*time.Second)
		allowed, budgetErr := p.sched.TryConsumeRetry(retryCtx, lease.Worker, model)
		retryCancel()
		if budgetErr != nil {
			return "", fmt.Errorf("checking retry budget: %w", budgetErr)
		}
		if !allowed {
			return "", fmt.Errorf("retry budget exhausted after %s: %w", outcome.Class, terminalErr)
		}

		// Provider pacing/breakers handle throttle/quota recovery. Only transient failures add local jitter.
		if outcome.Class == control.TransientUpstreamFailure {
			delay := control.FullJitter(50*time.Millisecond, attempt, 2*time.Second, p.rand)
			timer := time.NewTimer(delay)
			select {
			case <-runCtx.Done():
				timer.Stop()
				return "", runCtx.Err()
			case <-timer.C:
			}
		}

		req.EnqueuedAt = time.Now().UTC()
		admission, err = p.sched.Admit(runCtx, req)
		if err != nil {
			return "", fmt.Errorf("re-admitting retry: %w", err)
		}
		if !admission.Accepted {
			return "", fmt.Errorf("router overloaded while retrying: %s: %w", admission.Reason, lastErr)
		}
	}
	if lastErr == nil {
		lastErr = errors.New("request failed without result")
	}
	return "", fmt.Errorf("all %d retries exhausted: %w", maxRetries, lastErr)
}

func (p *PrintCommand) releaseLease(lease scheduler.Lease, outcome scheduler.Outcome) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		err := p.sched.Release(cleanupCtx, lease, outcome)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

func withOverallTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= timeout {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

func classifyAttempt(result *executor.Result, execErr error) (scheduler.Outcome, error) {
	if execErr != nil {
		class := control.TransientUpstreamFailure
		if errors.Is(execErr, context.Canceled) {
			class = control.ClientCancelled
		}
		if errors.Is(execErr, context.DeadlineExceeded) {
			class = control.Timeout
		}
		return scheduler.Outcome{Class: class}, execErr
	}
	if result == nil {
		return scheduler.Outcome{Class: control.TransientUpstreamFailure}, errors.New("executor returned nil result")
	}
	out := scheduler.Outcome{Class: control.Success, Duration: result.Duration}
	if result.Error == nil {
		return out, nil
	}
	out.ResetDuration = result.Error.ResetDuration
	switch result.Error.Type {
	case executor.ErrorRateLimited:
		out.Class = control.ShortThrottle
	case executor.ErrorLongQuota:
		out.Class = control.LongQuotaExhausted
	case executor.ErrorAccountIneligible:
		out.Class = control.AuthOrEligibilityFailure
	case executor.ErrorInvalidRequest:
		out.Class = control.InvalidRequestOrModel
	case executor.ErrorInvalidAttachment, executor.ErrorLocalPreparation:
		out.Class = control.LocalPreparationFailure
	case executor.ErrorTimeout:
		out.Class = control.Timeout
	case executor.ErrorCancelled:
		out.Class = control.ClientCancelled
	default:
		out.Class = control.TransientUpstreamFailure
	}
	return out, result.Error
}

func (p *PrintCommand) recordAttempt(requestID string, lease scheduler.Lease, model, prompt string, files []string, attempt int, queueWait time.Duration, admission scheduler.Admission, result *executor.Result, outcome scheduler.Outcome, err error) {
	now := time.Now().UTC()
	duration := time.Duration(0)
	if result != nil {
		duration = result.Duration
	}
	preview := ""
	if p.cfg.Logging.PromptPreview {
		preview = prompt
		runes := []rune(preview)
		if len(runes) > 100 {
			preview = string(runes[:100])
		}
	}
	message := ""
	if err != nil {
		message = err.Error()
	}
	if p.logger != nil {
		entry := logger.Entry{Timestamp: now, RequestID: requestID, Worker: lease.Worker, Model: model, Status: string(outcome.Class), DurationMs: duration.Milliseconds(), QueueWaitMs: queueWait.Milliseconds(), PredictedWaitMs: admission.PredictedWait.Milliseconds(), PredictedTotalMs: (admission.PredictedWait + admission.PredictedExecution).Milliseconds(), PromptPreview: preview, Files: files, RetryCount: attempt, ErrorMessage: message}
		defaults := storage.ControllerState{Worker: lease.Worker, Model: model, Rate: p.cfg.Adaptive.InitialRate, StableRate: p.cfg.Adaptive.InitialRate, ConcurrencyLimit: p.cfg.Adaptive.InitialConcurrency, RetryTokens: p.cfg.Adaptive.RetryBudgetCapacity}
		if st, stateErr := p.store.GetControllerState(lease.Worker, model, defaults); stateErr == nil {
			entry.RateLimitRPS = st.Rate
			entry.ConcurrencyLimit = st.ConcurrencyLimit
			if !st.QuotaBlockedUntil.IsZero() {
				entry.QuotaBlockedUntil = st.QuotaBlockedUntil.Format(time.RFC3339Nano)
			}
		}
		_ = p.logger.Log(entry)
	}

	_ = p.store.InsertRequestLog(storage.RequestLogEntry{Worker: lease.Worker, Model: model, StartedAt: now.Add(-duration), FinishedAt: now, DurationMs: duration.Milliseconds(), Status: string(outcome.Class), ErrorMessage: message, PromptPreview: preview})
}
