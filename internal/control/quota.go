package control

import (
	"time"

	"github.com/darttroll/gemini-router/internal/storage"
)

func SetLongQuota(st storage.ControllerState, now time.Time, reset time.Duration) storage.ControllerState {
	if reset > 0 {
		st.QuotaBlockedUntil = now.Add(reset + 5*time.Second)
		st.QuotaProbeCount = 0
		return st
	}
	delay := QuotaProbeDelay(st.QuotaProbeCount)
	st.QuotaBlockedUntil = now.Add(delay)
	st.QuotaProbeCount++
	return st
}

func QuotaProbeDelay(probeCount int) time.Duration {
	switch probeCount {
	case 0:
		return time.Minute
	case 1:
		return 5 * time.Minute
	case 2:
		return 10 * time.Minute
	case 3:
		return 30 * time.Minute
	}
	if probeCount < 24 {
		return time.Hour
	}
	return 24 * time.Hour
}

func QuotaReady(st storage.ControllerState, now time.Time) bool {
	return st.QuotaBlockedUntil.IsZero() || !st.QuotaBlockedUntil.After(now)
}
