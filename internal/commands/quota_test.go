package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/storage"
)

func boolPtr(v bool) *bool { return &v }

func setupQuotaTest(t *testing.T) (*QuotaCommand, *storage.Store) {
	t.Helper()
	store, err := storage.New(filepath.Join(t.TempDir(), "quota.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	cfg := &config.Config{
		AgyPath: "/fake/agy",
		Workers: []config.WorkerConfig{
			{Username: "alice", Enabled: true},
			{Username: "bob", Enabled: true},
			{Username: "carol", Enabled: true, QuotaEnabled: boolPtr(false)},
		},
		Quota: config.QuotaSettings{Enabled: true, CommandTimeout: time.Second},
	}
	cmd := NewQuotaCommand(cfg, store)
	return cmd, store
}

func TestQuotaCommandTableShowsAllGroupsAndDynamicColumns(t *testing.T) {
	cmd, store := setupQuotaTest(t)
	cmd.quotaSvc = nil // render deterministic cached fixtures; refresh behavior has separate tests
	now := time.Now().UTC()

	if err := store.ReplaceQuotaSnapshots("alice", []storage.QuotaSnapshot{
		{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "5h", RemainingFraction: 1, Disabled: true, FetchedAt: now},
		{Worker: "alice", GroupKey: "gemini", GroupName: "Gemini Models", Window: "weekly", RemainingFraction: 0, ResetTime: now.Add(5 * 24 * time.Hour), FetchedAt: now},
		{Worker: "alice", GroupKey: "third_party", GroupName: "Claude and GPT models", Window: "5h", RemainingFraction: 1, ResetTime: now.Add(4 * time.Hour), FetchedAt: now},
		{Worker: "alice", GroupKey: "third_party", GroupName: "Claude and GPT models", Window: "weekly", RemainingFraction: .875, ResetTime: now.Add(6 * 24 * time.Hour), FetchedAt: now},
		{Worker: "alice", GroupKey: "zeta", GroupName: "Zeta Models", Window: "daily", RemainingFraction: .5, ResetTime: now.Add(12 * time.Hour), FetchedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceQuotaSnapshots("bob", []storage.QuotaSnapshot{
		{Worker: "bob", GroupKey: "gemini", GroupName: "Gemini Models", Window: "5h", RemainingFraction: .25, ResetTime: now.Add(2 * time.Hour), FetchedAt: now},
		{Worker: "bob", GroupKey: "gemini", GroupName: "Gemini Models", Window: "weekly", RemainingFraction: .5, ResetTime: now.Add(3 * 24 * time.Hour), FetchedAt: now},
		{Worker: "bob", GroupKey: "third_party", GroupName: "Claude and GPT models", Window: "weekly", RemainingFraction: .75, ResetTime: now.Add(4 * 24 * time.Hour), FetchedAt: now},
	}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := cmd.Run(&buf, false); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	headers := []string{"Worker", "Gemini 5h", "Gemini Weekly", "Claude/GPT 5h", "Claude/GPT Weekly", "Zeta Models daily"}
	last := -1
	for _, h := range headers {
		idx := strings.Index(out, h)
		if idx < 0 {
			t.Fatalf("header %q missing:\n%s", h, out)
		}
		if idx <= last {
			t.Fatalf("header %q out of order:\n%s", h, out)
		}
		last = idx
	}
	for _, worker := range []string{"alice", "bob", "carol"} {
		if strings.Count(out, worker) != 1 {
			t.Fatalf("worker %q must appear exactly once:\n%s", worker, out)
		}
	}
	if !strings.Contains(out, "disabled") || !strings.Contains(out, "0.0% (reset") || !strings.Contains(out, "87.5% (reset") {
		t.Fatalf("expected quota cell formats missing:\n%s", out)
	}
	bobLine := lineContaining(out, "bob")
	if !strings.Contains(bobLine, "unavailable") {
		t.Fatalf("missing bucket should render unavailable: %s", bobLine)
	}
	carolLine := lineContaining(out, "carol")
	if strings.Count(carolLine, "disabled") != len(headers)-1 {
		t.Fatalf("quota-disabled worker should have disabled in every quota column: %s", carolLine)
	}
}

func TestQuotaCommandJSONIncludesAllBucketsAndRefreshError(t *testing.T) {
	cmd, store := setupQuotaTest(t)
	cmd.cfg.Workers = cmd.cfg.Workers[:2]
	now := time.Now().UTC()
	if err := store.ReplaceQuotaSnapshots("bob", []storage.QuotaSnapshot{
		{Worker: "bob", GroupKey: "third_party", GroupName: "Claude and GPT models", Window: "weekly", RemainingFraction: .33, ResetTime: now.Add(24 * time.Hour), FetchedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	cmd.quotaSvc.SetRunnerForTest(func(ctx context.Context, worker, path string) ([]byte, error) {
		if worker == "bob" {
			return nil, errors.New("quota endpoint unavailable")
		}
		return []byte(`{"status":"SUCCESS","command":{"name":"usage","data":{"groups":[{"name":"Gemini Models","buckets":[{"id":"gemini-weekly","name":"Weekly Limit Remaining","window":"weekly","remaining_fraction":0,"reset_time":"2030-08-13T15:02:49Z"},{"id":"gemini-5h","name":"Five Hour Limit Remaining","window":"5h","disabled":true,"remaining_fraction":1}]},{"name":"Claude and GPT models","buckets":[{"id":"3p-weekly","name":"Weekly Limit Remaining","window":"weekly","remaining_fraction":1,"reset_time":"2030-08-15T00:00:00Z"},{"id":"3p-5h","name":"Five Hour Limit Remaining","window":"5h","remaining_fraction":1,"reset_time":"2030-08-09T00:00:00Z"}]}]}}}`), nil
	})

	var buf bytes.Buffer
	if err := cmd.Run(&buf, true); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Providers []struct {
			Worker       string `json:"worker"`
			RefreshError string `json:"refresh_error"`
			Quotas       []struct {
				GroupKey          string  `json:"group_key"`
				GroupName         string  `json:"group_name"`
				Window            string  `json:"window"`
				RemainingFraction float64 `json:"remaining_fraction"`
				Disabled          bool    `json:"disabled"`
				ResetAt           string  `json:"reset_at"`
				RefreshedAt       string  `json:"refreshed_at"`
			} `json:"quotas"`
		} `json:"providers"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
	}
	if len(got.Providers) != 2 {
		t.Fatalf("providers=%d want 2: %s", len(got.Providers), buf.String())
	}
	if got.Providers[0].Worker != "alice" || len(got.Providers[0].Quotas) != 4 {
		t.Fatalf("alice quotas unexpected: %+v", got.Providers[0])
	}
	var sawDisabled bool
	for _, q := range got.Providers[0].Quotas {
		if q.GroupKey == "gemini" && q.Window == "5h" {
			sawDisabled = q.Disabled && q.RefreshedAt != ""
		}
	}
	if !sawDisabled {
		t.Fatalf("disabled/refreshed fields missing: %+v", got.Providers[0])
	}
	if got.Providers[1].Worker != "bob" || !strings.Contains(got.Providers[1].RefreshError, "quota endpoint unavailable") {
		t.Fatalf("bob refresh error missing: %+v", got.Providers[1])
	}
	if len(got.Providers[1].Quotas) != 1 || got.Providers[1].Quotas[0].RemainingFraction != .33 {
		t.Fatalf("bob cached quota fallback missing: %+v", got.Providers[1])
	}
}

func lineContaining(s, needle string) string {
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}
	return ""
}
