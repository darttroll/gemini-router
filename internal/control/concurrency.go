package control

import (
	"math"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/storage"
)

func ObserveLatency(st storage.ControllerState, latency time.Duration, cfg config.AdaptiveSettings) storage.ControllerState {
	if st.ConcurrencyLimit <= 0 {
		st.ConcurrencyLimit = cfg.InitialConcurrency
	}
	x := latency.Seconds()
	if x <= 0 {
		return st
	}
	if st.ShortEWMA == 0 {
		st.ShortEWMA = x
	} else {
		st.ShortEWMA = 0.2*x + 0.8*st.ShortEWMA
	}
	if st.LongEWMA == 0 {
		st.LongEWMA = x
	} else {
		st.LongEWMA = 0.05*x + 0.95*st.LongEWMA
	}
	st.ConcurrencySamples++
	if st.ConcurrencySamples < cfg.ConcurrencyWindow {
		return st
	}
	st.ConcurrencySamples = 0
	gradient := 1.0
	if st.ShortEWMA > 0 {
		gradient = clampFloat(st.LongEWMA/st.ShortEWMA, 0.5, 1.0)
	}
	next := int(math.Round(gradient*float64(st.ConcurrencyLimit) + 1))
	if next < 1 {
		next = 1
	}
	if next > cfg.MaxConcurrency {
		next = cfg.MaxConcurrency
	}
	st.ConcurrencyLimit = next
	return st
}

// OnConcurrencyDrop reacts to a request-level upstream/process drop that may
// indicate the current in-flight limit is too aggressive. It is independent
// from the short request-rate controller.
func OnConcurrencyDrop(st storage.ControllerState, cfg config.AdaptiveSettings) storage.ControllerState {
	if st.ConcurrencyLimit <= 0 {
		st.ConcurrencyLimit = cfg.InitialConcurrency
	}
	next := int(math.Floor(float64(st.ConcurrencyLimit) * 0.8))
	if next >= st.ConcurrencyLimit {
		next = st.ConcurrencyLimit - 1
	}
	if next < 1 {
		next = 1
	}
	st.ConcurrencyLimit = next
	st.ConcurrencySamples = 0
	return st
}
