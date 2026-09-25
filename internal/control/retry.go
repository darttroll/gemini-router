package control

import (
	"math"
	"math/rand"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/storage"
)

func TryConsumeRetry(st storage.ControllerState, now time.Time, cfg config.AdaptiveSettings) (storage.ControllerState, bool) {
	if st.RetryUpdatedAt.IsZero() {
		st.RetryUpdatedAt = now
		if st.RetryTokens <= 0 {
			st.RetryTokens = cfg.RetryBudgetCapacity
		}
	}
	elapsed := now.Sub(st.RetryUpdatedAt).Seconds()
	if elapsed > 0 {
		st.RetryTokens = math.Min(cfg.RetryBudgetCapacity, st.RetryTokens+elapsed*cfg.RetryBudgetRefillPerSec)
		st.RetryUpdatedAt = now
	}
	if st.RetryTokens < 1 {
		return st, false
	}
	st.RetryTokens--
	return st, true
}

func FullJitter(base time.Duration, attempt int, max time.Duration, r *rand.Rand) time.Duration {
	capd := base
	for i := 0; i < attempt && capd < max; i++ {
		if capd > max/2 {
			capd = max
			break
		}
		capd *= 2
	}
	if capd > max {
		capd = max
	}
	if capd <= 0 {
		return 0
	}
	return time.Duration(r.Int63n(int64(capd) + 1))
}
