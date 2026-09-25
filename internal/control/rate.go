package control

import (
	"math"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/storage"
)

func OnShortThrottle(st storage.ControllerState, cfg config.AdaptiveSettings) storage.ControllerState {
	if st.Rate <= 0 {
		st.Rate = cfg.InitialRate
	}
	if st.StableRate <= 0 {
		st.StableRate = st.Rate
	}
	current := st.Rate
	if st.RMax > 0 && current < st.RMax {
		// CUBIC fast convergence: repeated congestion below the old maximum
		// means that maximum is stale. Move the remembered boundary down
		// instead of repeatedly racing back to an obsolete limit.
		st.RMax = current * (1 + cfg.CubicBeta) / 2
	} else {
		st.RMax = current
	}
	st.Rate = clampFloat(current*cfg.CubicBeta, cfg.MinRate, cfg.MaxRate)
	st.RecoveryWindow = 0
	st.SuccessesInWindow = 0
	st.Tokens = 0
	return st
}

func OnRateSuccess(st storage.ControllerState, cfg config.AdaptiveSettings) storage.ControllerState {
	if st.Rate <= 0 {
		st.Rate = cfg.InitialRate
	}
	if st.StableRate <= 0 {
		st.StableRate = st.Rate
	}
	st.SuccessesInWindow++
	if st.SuccessesInWindow < cfg.RateWindowSuccesses {
		return st
	}
	st.SuccessesInWindow = 0
	st.RecoveryWindow++

	old := st.Rate
	var target float64
	if st.RMax > 0 {
		k := math.Cbrt(st.RMax * (1 - cfg.CubicBeta) / cfg.CubicC)
		t := float64(st.RecoveryWindow)
		target = cfg.CubicC*math.Pow(t-k, 3) + st.RMax
	} else {
		target = old*1.10 + 0.1
	}
	if target < old {
		target = old
	}
	maxStep := old * 1.5
	if target > maxStep {
		target = maxStep
	}
	st.Rate = clampFloat(target, cfg.MinRate, cfg.MaxRate)
	if st.Rate > st.StableRate {
		st.StableRate = st.Rate
	}
	return st
}

// AdvanceIdle intentionally does not advance CUBIC recovery: learning is observation-driven.
func AdvanceIdle(st storage.ControllerState, _ time.Duration, _ config.AdaptiveSettings) storage.ControllerState {
	return st
}

func TryConsumeStartToken(st storage.ControllerState, now time.Time, cfg config.AdaptiveSettings) (storage.ControllerState, bool, time.Duration) {
	if st.Rate <= 0 {
		st.Rate = cfg.InitialRate
	}
	if st.TokenUpdatedAt.IsZero() {
		st.TokenUpdatedAt = now
		if st.Tokens <= 0 {
			st.Tokens = cfg.RateBurst
		}
	}
	elapsed := now.Sub(st.TokenUpdatedAt).Seconds()
	if elapsed > 0 {
		st.Tokens = math.Min(cfg.RateBurst, st.Tokens+elapsed*st.Rate)
		st.TokenUpdatedAt = now
	}
	if st.Tokens >= 1 {
		st.Tokens -= 1
		return st, true, 0
	}
	missing := 1 - st.Tokens
	wait := time.Duration((missing / st.Rate) * float64(time.Second))
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	return st, false, wait
}

func clampFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func RefundStartToken(st storage.ControllerState, cfg config.AdaptiveSettings) storage.ControllerState {
	st.Tokens = math.Min(cfg.RateBurst, st.Tokens+1)
	return st
}
