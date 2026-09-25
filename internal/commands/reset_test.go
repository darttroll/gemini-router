package commands

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/storage"
)

func TestResetPreservesLearnedCapacityAndClearsAvailabilityState(t *testing.T) {
	st, err := storage.New(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC()
	before := storage.ControllerState{Worker: "alice", Model: "m", Rate: 17, StableRate: 16, RMax: 20, Tokens: 0, TokenUpdatedAt: now, ConcurrencyLimit: 7, QuotaBlockedUntil: now.Add(time.Hour), QuotaProbeCount: 8, RetryTokens: 0, RetryUpdatedAt: now}
	if err := st.PutControllerState(before); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Adaptive: config.AdaptiveSettings{RetryBudgetCapacity: 13}}
	cmd := NewResetCommand(cfg, st)
	if err := cmd.Run("alice"); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetControllerState("alice", "m", storage.ControllerState{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Rate != 17 || got.StableRate != 16 || got.RMax != 20 || got.ConcurrencyLimit != 7 {
		t.Fatalf("learned capacity changed: %+v", got)
	}
	if !got.QuotaBlockedUntil.IsZero() || got.QuotaProbeCount != 0 || got.RetryTokens != 13 {
		t.Fatalf("availability state not reset: %+v", got)
	}
}
