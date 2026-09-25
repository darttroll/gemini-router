package control

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/storage"
)

func testAdaptive() config.AdaptiveSettings {
	return config.AdaptiveSettings{
		MinRate: 0.2, InitialRate: 2, MaxRate: 100, RateBurst: 3,
		CubicBeta: 0.7, CubicC: 0.4, RateWindowSuccesses: 5,
		InitialConcurrency: 4, MaxConcurrency: 32, ConcurrencyWindow: 10,
		RetryBudgetCapacity: 10, RetryBudgetRefillPerSec: 0.1,
	}
}

func TestCubicTransientThrottleRecoversQuickly(t *testing.T) {
	cfg := testAdaptive()
	st := storage.ControllerState{Rate: 20, StableRate: 20, RMax: 20, Tokens: 3, ConcurrencyLimit: 4, RetryTokens: 10}
	st = OnShortThrottle(st, cfg)
	if st.Rate != 14 {
		t.Fatalf("rate after throttle=%v want 14", st.Rate)
	}
	if st.RMax != 20 || st.RecoveryWindow != 0 || st.SuccessesInWindow != 0 {
		t.Fatalf("state after throttle=%+v", st)
	}
	for i := 0; i < 10; i++ {
		st = OnRateSuccess(st, cfg)
	}
	if st.Rate < 18 {
		t.Fatalf("rate after two clean windows=%v want >=18", st.Rate)
	}
	for i := 0; i < 5; i++ {
		st = OnRateSuccess(st, cfg)
	}
	if st.Rate < 19 {
		t.Fatalf("rate after three clean windows=%v want >=19", st.Rate)
	}
}

func TestCubicDoesNotRecoverWhileIdle(t *testing.T) {
	cfg := testAdaptive()
	st := storage.ControllerState{Rate: 14, StableRate: 20, RMax: 20, RecoveryWindow: 0}
	got := AdvanceIdle(st, 24*time.Hour, cfg)
	if got.Rate != st.Rate || got.RecoveryWindow != st.RecoveryWindow {
		t.Fatalf("idle changed state: before=%+v after=%+v", st, got)
	}
}

func TestTokenBucketRefillAndWait(t *testing.T) {
	cfg := testAdaptive()
	now := time.Unix(100, 0)
	st := storage.ControllerState{Rate: 2, Tokens: 0, TokenUpdatedAt: now}
	st, ok, wait := TryConsumeStartToken(st, now, cfg)
	if ok || wait != 500*time.Millisecond {
		t.Fatalf("ok=%v wait=%v want false,500ms", ok, wait)
	}
	st, ok, wait = TryConsumeStartToken(st, now.Add(time.Second), cfg)
	if !ok || wait != 0 {
		t.Fatalf("ok=%v wait=%v want true,0", ok, wait)
	}
	if st.Tokens != 1 {
		t.Fatalf("tokens=%v want 1", st.Tokens)
	}
	st.Tokens = 10
	st.TokenUpdatedAt = now
	st, _, _ = TryConsumeStartToken(st, now.Add(10*time.Second), cfg)
	if st.Tokens > cfg.RateBurst-1+1e-9 {
		t.Fatalf("burst cap failed tokens=%v", st.Tokens)
	}
}

func TestGradient2StableLatencyGrows(t *testing.T) {
	cfg := testAdaptive()
	st := storage.ControllerState{ConcurrencyLimit: 4}
	for i := 0; i < cfg.ConcurrencyWindow; i++ {
		st = ObserveLatency(st, 2*time.Second, cfg)
	}
	if st.ConcurrencyLimit <= 4 {
		t.Fatalf("limit=%d want growth", st.ConcurrencyLimit)
	}
}

func TestGradient2RisingLatencyReduces(t *testing.T) {
	cfg := testAdaptive()
	st := storage.ControllerState{ConcurrencyLimit: 8, ShortEWMA: 1, LongEWMA: 1}
	for i := 0; i < cfg.ConcurrencyWindow; i++ {
		st = ObserveLatency(st, 5*time.Second, cfg)
	}
	if st.ConcurrencyLimit >= 8 {
		t.Fatalf("limit=%d want <8", st.ConcurrencyLimit)
	}
}

func TestClassifyLongQuotaSeparateFromShortThrottle(t *testing.T) {
	long := ClassifyProcessOutput(errors.New("exit 1"), "", "", "RESOURCE_EXHAUSTED (code 429): Individual quota reached. Resets in 55m58s")
	if long.Class != LongQuotaExhausted || long.ResetDuration != 55*time.Minute+58*time.Second {
		t.Fatalf("long=%+v", long)
	}
	short := ClassifyProcessOutput(errors.New("exit 1"), "", "429 too many requests", "")
	if short.Class != ShortThrottle || short.ResetDuration != 0 {
		t.Fatalf("short=%+v", short)
	}
}

func TestClassifyCancellation(t *testing.T) {
	got := ClassifyProcessOutput(context.Canceled, "", "", "")
	if got.Class != ClientCancelled {
		t.Fatalf("class=%s", got.Class)
	}
}

func TestLongQuotaWithKnownResetDoesNotChangeRate(t *testing.T) {
	now := time.Unix(1000, 0)
	st := storage.ControllerState{Rate: 20, StableRate: 20, RMax: 22, ConcurrencyLimit: 6}
	got := SetLongQuota(st, now, time.Hour)
	if got.Rate != 20 || got.ConcurrencyLimit != 6 {
		t.Fatalf("quota mutated adaptive state: %+v", got)
	}
	if got.QuotaBlockedUntil.Before(now.Add(time.Hour)) {
		t.Fatalf("blocked until=%v", got.QuotaBlockedUntil)
	}
}

func TestUnknownQuotaProbeSchedule(t *testing.T) {
	want := []time.Duration{time.Minute, 5 * time.Minute, 10 * time.Minute, 30 * time.Minute, time.Hour, time.Hour, 24 * time.Hour}
	counts := []int{0, 1, 2, 3, 4, 23, 24}
	for i, c := range counts {
		if got := QuotaProbeDelay(c); got != want[i] {
			t.Fatalf("count=%d got=%v want=%v", c, got, want[i])
		}
	}
}

func TestRetryBudgetRefillAndConsume(t *testing.T) {
	cfg := testAdaptive()
	now := time.Unix(100, 0)
	st := storage.ControllerState{RetryTokens: 0, RetryUpdatedAt: now}
	st, ok := TryConsumeRetry(st, now, cfg)
	if ok {
		t.Fatal("unexpected retry token")
	}
	st, ok = TryConsumeRetry(st, now.Add(10*time.Second), cfg)
	if !ok {
		t.Fatal("expected refilled retry token")
	}
	if st.RetryTokens > 1e-9 {
		t.Fatalf("tokens=%v want 0 after consuming refilled token", st.RetryTokens)
	}
}

func TestFullJitterBounds(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for attempt := 0; attempt < 8; attempt++ {
		got := FullJitter(100*time.Millisecond, attempt, 5*time.Second, r)
		capd := 100 * time.Millisecond * time.Duration(1<<attempt)
		if capd > 5*time.Second {
			capd = 5 * time.Second
		}
		if got < 0 || got > capd {
			t.Fatalf("attempt=%d got=%v cap=%v", attempt, got, capd)
		}
	}
}

func TestShortThrottleDropsBurstTokens(t *testing.T) {
	cfg := testAdaptive()
	st := storage.ControllerState{Rate: 20, StableRate: 20, RMax: 20, Tokens: 3}
	got := OnShortThrottle(st, cfg)
	if got.Tokens != 0 {
		t.Fatalf("tokens after short throttle=%v want 0", got.Tokens)
	}
}

func TestCubicRepeatedThrottleLowersObsoleteBoundary(t *testing.T) {
	cfg := testAdaptive()
	st := storage.ControllerState{Rate: 14, StableRate: 20, RMax: 20, Tokens: 2}
	got := OnShortThrottle(st, cfg)
	if got.RMax >= 20 {
		t.Fatalf("obsolete RMax not lowered: %+v", got)
	}
	if got.RMax <= got.Rate {
		t.Fatalf("new boundary=%v should remain above backed-off rate=%v", got.RMax, got.Rate)
	}
}

func TestConcurrencyDropReducesLimitWithoutChangingRate(t *testing.T) {
	cfg := testAdaptive()
	st := storage.ControllerState{Rate: 20, ConcurrencyLimit: 10}
	got := OnConcurrencyDrop(st, cfg)
	if got.ConcurrencyLimit >= 10 || got.ConcurrencyLimit < 1 {
		t.Fatalf("limit=%d", got.ConcurrencyLimit)
	}
	if got.Rate != 20 {
		t.Fatalf("rate changed to %v", got.Rate)
	}
}

func TestClassifierSuccessfulStdoutMentioning429IsSuccess(t *testing.T) {
	got := ClassifyProcessOutput(nil, "HTTP 429 means Too Many Requests in this explanation.", "", "")
	if got.Class != Success {
		t.Fatalf("class=%s message=%q", got.Class, got.Message)
	}
}
