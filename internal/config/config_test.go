package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig_ValidFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")

	yamlContent := `
agy_path: "/usr/local/bin/agy"
data_dir: "/var/lib/gemini-router"
balancer:
  max_retries: 2
default_model: "TestModel"
request_timeout: 3m
workers:
  - username: alice
    enabled: true
  - username: bob
    enabled: false
`
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.AgyPath != "/usr/local/bin/agy" {
		t.Errorf("AgyPath = %q, want %q", cfg.AgyPath, "/usr/local/bin/agy")
	}
	if cfg.DataDir != "/var/lib/gemini-router" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/var/lib/gemini-router")
	}
	if cfg.Balancer.MaxRetries != 2 {
		t.Errorf("MaxRetries = %d, want 2", cfg.Balancer.MaxRetries)
	}
	if cfg.DefaultModel != "TestModel" {
		t.Errorf("DefaultModel = %q, want %q", cfg.DefaultModel, "TestModel")
	}
	if cfg.RequestTimeout != 3*time.Minute {
		t.Errorf("RequestTimeout = %v, want 3m", cfg.RequestTimeout)
	}
	if len(cfg.Workers) != 2 {
		t.Fatalf("len(Workers) = %d, want 2", len(cfg.Workers))
	}
	if cfg.Workers[0].Username != "alice" || !cfg.Workers[0].Enabled {
		t.Errorf("Workers[0] = %+v, want alice/enabled", cfg.Workers[0])
	}
	if cfg.Workers[1].Username != "bob" || cfg.Workers[1].Enabled {
		t.Errorf("Workers[1] = %+v, want bob/disabled", cfg.Workers[1])
	}
}

func TestLoadConfig_WorkerAgyPathOverride(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")

	yamlContent := `
agy_path: "/global/agy"
data_dir: "/tmp/data"
workers:
  - username: alice
    enabled: true
    agy_path: "/custom/agy"
`
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Workers[0].AgyPath != "/custom/agy" {
		t.Errorf("Workers[0].AgyPath = %q, want %q", cfg.Workers[0].AgyPath, "/custom/agy")
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")

	yamlContent := `
agy_path: "/usr/bin/agy"
data_dir: "/tmp/data"
workers:
  - username: test
    enabled: true
`
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Balancer.MaxRetries != 3 {
		t.Errorf("default MaxRetries = %d, want 3", cfg.Balancer.MaxRetries)
	}
	if cfg.DefaultModel != "gemini-3.6-flash-high" {
		t.Errorf("default DefaultModel = %q, want %q", cfg.DefaultModel, "gemini-3.6-flash-high")
	}
	if cfg.RequestTimeout != 5*time.Minute {
		t.Errorf("default RequestTimeout = %v, want 5m", cfg.RequestTimeout)
	}
	if !cfg.Quota.Enabled || cfg.Quota.RefreshInterval != 5*time.Minute || cfg.Quota.LowRemainingInterval != 30*time.Second || cfg.Quota.LowRemainingThreshold != 0.10 || cfg.Quota.CommandTimeout != 10*time.Second {
		t.Errorf("unexpected default quota settings: %+v", cfg.Quota)
	}
}

func TestLoadConfig_ValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "missing agy_path",
			yaml:    "data_dir: \"/tmp\"\nworkers:\n  - username: a\n    enabled: true",
			wantErr: "agy_path is required",
		},
		{
			name:    "missing data_dir",
			yaml:    "agy_path: \"/bin/agy\"\nworkers:\n  - username: a\n    enabled: true",
			wantErr: "data_dir is required",
		},
		{
			name:    "no workers",
			yaml:    "agy_path: \"/bin/agy\"\ndata_dir: \"/tmp\"",
			wantErr: "at least one worker is required",
		},
		{
			name:    "no enabled workers",
			yaml:    "agy_path: \"/bin/agy\"\ndata_dir: \"/tmp\"\nworkers:\n  - username: a\n    enabled: false",
			wantErr: "at least one worker must be enabled",
		},
		{
			name:    "empty worker username",
			yaml:    "agy_path: \"/bin/agy\"\ndata_dir: \"/tmp\"\nworkers:\n  - username: \"\"\n    enabled: true",
			wantErr: "invalid worker username",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfgPath := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(cfgPath, []byte(tt.yaml), 0644); err != nil {
				t.Fatal(err)
			}
			_, err := Load(cfgPath)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want containing %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestFindConfigPath(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("agy_path: x"), 0644); err != nil {
		t.Fatal(err)
	}

	found, err := FindConfigPath(cfgPath)
	if err != nil {
		t.Fatalf("FindConfigPath() error: %v", err)
	}
	if found != cfgPath {
		t.Errorf("FindConfigPath() = %q, want %q", found, cfgPath)
	}
}

func TestFindConfigPath_NotFound(t *testing.T) {
	_, err := FindConfigPath("")
	// With no explicit path, FindConfigPath searches standard locations.
	// The test normally has no config in those standard locations.
	if err == nil {
		// Skip if the host unexpectedly provides a default config.
		t.Skip("config file found in default location")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestLoadConfig_Adaptive(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	yamlContent := `
agy_path: "/usr/bin/agy"
data_dir: "/tmp/data"
queue:
  max_delay: 10s
  max_depth: 1000
  poll_interval: 100ms
adaptive:
  min_rate: 0.2
  initial_rate: 2.0
  max_rate: 100.0
  rate_burst: 3
  cubic_beta: 0.7
  cubic_c: 0.4
  rate_window_successes: 5
  initial_concurrency: 4
  max_concurrency: 32
  concurrency_window: 10
  retry_budget_capacity: 10
  retry_budget_refill_per_sec: 0.1
workers:
  - username: alice
    enabled: true
`
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Queue.MaxDelay != 10*time.Second || cfg.Queue.MaxDepth != 1000 || cfg.Queue.PollInterval != 100*time.Millisecond {
		t.Fatalf("Queue = %+v", cfg.Queue)
	}
	if cfg.Adaptive.MinRate != 0.2 || cfg.Adaptive.InitialRate != 2 || cfg.Adaptive.MaxRate != 100 || cfg.Adaptive.RateBurst != 3 {
		t.Fatalf("Adaptive rate settings = %+v", cfg.Adaptive)
	}
	if cfg.Adaptive.CubicBeta != 0.7 || cfg.Adaptive.CubicC != 0.4 || cfg.Adaptive.RateWindowSuccesses != 5 {
		t.Fatalf("Adaptive cubic settings = %+v", cfg.Adaptive)
	}
	if cfg.Adaptive.InitialConcurrency != 4 || cfg.Adaptive.MaxConcurrency != 32 || cfg.Adaptive.ConcurrencyWindow != 10 {
		t.Fatalf("Adaptive concurrency settings = %+v", cfg.Adaptive)
	}
	if cfg.Adaptive.RetryBudgetCapacity != 10 || cfg.Adaptive.RetryBudgetRefillPerSec != 0.1 {
		t.Fatalf("Adaptive retry settings = %+v", cfg.Adaptive)
	}
}

func TestLoadConfig_AdaptiveDefaults(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("agy_path: /usr/bin/agy\ndata_dir: /tmp/data\nworkers:\n  - username: alice\n    enabled: true\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Queue.MaxDelay != 10*time.Second || cfg.Queue.MaxDepth != 1000 || cfg.Queue.PollInterval != 100*time.Millisecond {
		t.Fatalf("Queue defaults = %+v", cfg.Queue)
	}
	want := AdaptiveSettings{MinRate: 0.2, InitialRate: 2, MaxRate: 100, RateBurst: 3, CubicBeta: 0.7, CubicC: 0.4, RateWindowSuccesses: 5, InitialConcurrency: 4, MaxConcurrency: 32, ConcurrencyWindow: 10, RetryBudgetCapacity: 10, RetryBudgetRefillPerSec: 0.1}
	if cfg.Adaptive != want {
		t.Fatalf("Adaptive defaults = %+v, want %+v", cfg.Adaptive, want)
	}
}

func TestQuotaEnabledForWorkerOverride(t *testing.T) {
	no := false
	cfg := &Config{Quota: QuotaSettings{Enabled: true}, Workers: []WorkerConfig{{Username: "a", Enabled: true}, {Username: "b", Enabled: true, QuotaEnabled: &no}}}
	if !cfg.QuotaEnabledFor("a") {
		t.Fatal("default worker quota should inherit enabled")
	}
	if cfg.QuotaEnabledFor("b") {
		t.Fatal("worker quota override false ignored")
	}
}

func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := "agy_path: /usr/bin/agy\ndata_dir: /tmp/data\nunknown_option: true\nworkers:\n  - username: alice\n    enabled: true\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestLoadConfigPreservesExplicitZeroRetrySettings(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := "agy_path: /usr/bin/agy\ndata_dir: /tmp/data\nbalancer:\n  max_retries: 0\nadaptive:\n  retry_budget_refill_per_sec: 0\nworkers:\n  - username: alice\n    enabled: true\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Balancer.MaxRetries != 0 {
		t.Fatalf("max_retries=%d", cfg.Balancer.MaxRetries)
	}
	if cfg.Adaptive.RetryBudgetRefillPerSec != 0 {
		t.Fatalf("retry refill=%v", cfg.Adaptive.RetryBudgetRefillPerSec)
	}
}

func TestLoadConfigRejectsDuplicateAndInvalidWorkers(t *testing.T) {
	for name, data := range map[string]string{
		"duplicate": "agy_path: /usr/bin/agy\ndata_dir: /tmp/data\nworkers:\n  - username: alice\n    enabled: true\n  - username: alice\n    enabled: true\n",
		"invalid":   "agy_path: /usr/bin/agy\ndata_dir: /tmp/data\nworkers:\n  - username: --bad\n    enabled: true\n",
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestLoadConfigRejectsRateBurstBelowOne(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := "agy_path: /usr/bin/agy\ndata_dir: /tmp/data\nadaptive:\n  rate_burst: 0.5\nworkers:\n  - username: alice\n    enabled: true\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected rate_burst validation error")
	}
}

func TestLoadConfigRejectsNonPositiveRequestTimeout(t *testing.T) {
	for _, value := range []string{"0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			data := "agy_path: /usr/bin/agy\ndata_dir: /tmp/data\nrequest_timeout: " + value + "\nworkers:\n  - username: alice\n    enabled: true\n"
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil || !contains(err.Error(), "request_timeout must be positive") {
				t.Fatalf("Load(%s) err=%v", value, err)
			}
		})
	}
}
