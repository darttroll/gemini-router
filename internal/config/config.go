package config

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration and accepts YAML strings such as "60s" and "5m".
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = parsed
	return nil
}

// WorkerConfig describes one Linux worker account.
type WorkerConfig struct {
	Username     string `yaml:"username"`
	Enabled      bool   `yaml:"enabled"`
	AgyPath      string `yaml:"agy_path,omitempty"`
	QuotaEnabled *bool  `yaml:"quota_enabled,omitempty"`
}

// BalancerConfig contains retry-related compatibility settings.
type BalancerConfig struct {
	MaxRetries *int `yaml:"max_retries"`
}

// QueueConfig describes queue admission settings as represented in YAML.
type QueueConfig struct {
	MaxDelayRaw     Duration `yaml:"max_delay"`
	MaxDepth        int      `yaml:"max_depth"`
	PollIntervalRaw Duration `yaml:"poll_interval"`
}

type LoggingConfig struct {
	PromptPreview bool `yaml:"prompt_preview"`
}

type LoggingSettings struct {
	PromptPreview bool
}

// AdaptiveConfig describes adaptive rate/concurrency settings as represented in YAML.
type QuotaConfig struct {
	Enabled                 *bool    `yaml:"enabled"`
	RefreshIntervalRaw      Duration `yaml:"refresh_interval"`
	LowRemainingIntervalRaw Duration `yaml:"low_remaining_interval"`
	LowRemainingThreshold   float64  `yaml:"low_remaining_threshold"`
	CommandTimeoutRaw       Duration `yaml:"command_timeout"`
}

type QuotaSettings struct {
	Enabled               bool
	RefreshInterval       time.Duration
	LowRemainingInterval  time.Duration
	LowRemainingThreshold float64
	CommandTimeout        time.Duration
}

type AdaptiveConfig struct {
	MinRate                 float64  `yaml:"min_rate"`
	InitialRate             float64  `yaml:"initial_rate"`
	MaxRate                 float64  `yaml:"max_rate"`
	RateBurst               float64  `yaml:"rate_burst"`
	CubicBeta               float64  `yaml:"cubic_beta"`
	CubicC                  float64  `yaml:"cubic_c"`
	RateWindowSuccesses     int      `yaml:"rate_window_successes"`
	InitialConcurrency      int      `yaml:"initial_concurrency"`
	MaxConcurrency          int      `yaml:"max_concurrency"`
	ConcurrencyWindow       int      `yaml:"concurrency_window"`
	RetryBudgetCapacity     float64  `yaml:"retry_budget_capacity"`
	RetryBudgetRefillPerSec *float64 `yaml:"retry_budget_refill_per_sec"`
}

// QueueSettings are normalized queue settings used by the scheduler.
type QueueSettings struct {
	MaxDelay     time.Duration
	MaxDepth     int
	PollInterval time.Duration
}

// AdaptiveSettings are normalized adaptive-controller settings.
type AdaptiveSettings struct {
	MinRate                 float64
	InitialRate             float64
	MaxRate                 float64
	RateBurst               float64
	CubicBeta               float64
	CubicC                  float64
	RateWindowSuccesses     int
	InitialConcurrency      int
	MaxConcurrency          int
	ConcurrencyWindow       int
	RetryBudgetCapacity     float64
	RetryBudgetRefillPerSec float64
}

// Config is the root configuration structure.
type Config struct {
	AgyPath           string         `yaml:"agy_path"`
	DataDir           string         `yaml:"data_dir"`
	BalancerRaw       BalancerConfig `yaml:"balancer"`
	QueueRaw          QueueConfig    `yaml:"queue"`
	AdaptiveRaw       AdaptiveConfig `yaml:"adaptive"`
	QuotaRaw          QuotaConfig    `yaml:"quota"`
	LoggingRaw        LoggingConfig  `yaml:"logging"`
	DefaultModel      string         `yaml:"default_model"`
	RequestTimeoutRaw *Duration      `yaml:"request_timeout"`
	Workers           []WorkerConfig `yaml:"workers"`

	// Normalized fields populated after parsing.
	Balancer       BalancerSettings `yaml:"-"`
	Queue          QueueSettings    `yaml:"-"`
	Adaptive       AdaptiveSettings `yaml:"-"`
	Quota          QuotaSettings    `yaml:"-"`
	Logging        LoggingSettings  `yaml:"-"`
	RequestTimeout time.Duration    `yaml:"-"`
}

// BalancerSettings contains normalized retry-related settings.
type BalancerSettings struct {
	MaxRetries int
}

// Load reads, defaults, normalizes, and validates a configuration file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %s: %w", path, err)
	}

	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", path, err)
	}

	applyDefaults(&cfg)

	if err := Validate(&cfg); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}

	return &cfg, nil
}

// FindConfigPath searches configuration locations in priority order:
// 1. Explicit --config path, when provided.
// 2. /etc/gemini-router/config.yaml
// 3. ~/.config/gemini-router/config.yaml
func FindConfigPath(flagPath string) (string, error) {
	if flagPath != "" {
		if _, err := os.Stat(flagPath); err != nil {
			return "", fmt.Errorf("config file not found: %s", flagPath)
		}
		return flagPath, nil
	}

	candidates := []string{
		"/etc/gemini-router/config.yaml",
	}

	home, err := os.UserHomeDir()
	if err == nil {
		candidates = append(candidates, home+"/.config/gemini-router/config.yaml")
	}

	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}

	return "", fmt.Errorf("config file not found: tried %v", candidates)
}

// EnabledWorkers returns workers with enabled=true.
func (c *Config) EnabledWorkers() []WorkerConfig {
	var result []WorkerConfig
	for _, w := range c.Workers {
		if w.Enabled {
			result = append(result, w)
		}
	}
	return result
}

// GetAgyPath returns the AGY path for a worker.
// A worker-specific agy_path overrides the global path.
func (c *Config) QuotaEnabledFor(worker string) bool {
	if c == nil || !c.Quota.Enabled {
		return false
	}
	for _, w := range c.Workers {
		if w.Username == worker {
			return w.QuotaEnabled == nil || *w.QuotaEnabled
		}
	}
	return false
}

func (c *Config) GetAgyPath(worker string) string {
	for _, w := range c.Workers {
		if w.Username == worker && w.AgyPath != "" {
			return w.AgyPath
		}
	}
	return c.AgyPath
}

func applyDefaults(cfg *Config) {
	maxRetries := 3
	if cfg.BalancerRaw.MaxRetries != nil {
		maxRetries = *cfg.BalancerRaw.MaxRetries
	}
	if cfg.QueueRaw.MaxDelayRaw.Duration == 0 {
		cfg.QueueRaw.MaxDelayRaw.Duration = 10 * time.Second
	}
	if cfg.QueueRaw.MaxDepth == 0 {
		cfg.QueueRaw.MaxDepth = 1000
	}
	if cfg.QueueRaw.PollIntervalRaw.Duration == 0 {
		cfg.QueueRaw.PollIntervalRaw.Duration = 100 * time.Millisecond
	}
	if cfg.QuotaRaw.Enabled == nil {
		v := true
		cfg.QuotaRaw.Enabled = &v
	}
	if cfg.QuotaRaw.RefreshIntervalRaw.Duration == 0 {
		cfg.QuotaRaw.RefreshIntervalRaw.Duration = 5 * time.Minute
	}
	if cfg.QuotaRaw.LowRemainingIntervalRaw.Duration == 0 {
		cfg.QuotaRaw.LowRemainingIntervalRaw.Duration = 30 * time.Second
	}
	if cfg.QuotaRaw.LowRemainingThreshold == 0 {
		cfg.QuotaRaw.LowRemainingThreshold = 0.10
	}
	if cfg.QuotaRaw.CommandTimeoutRaw.Duration == 0 {
		cfg.QuotaRaw.CommandTimeoutRaw.Duration = 10 * time.Second
	}
	if cfg.AdaptiveRaw.MinRate == 0 {
		cfg.AdaptiveRaw.MinRate = 0.2
	}
	if cfg.AdaptiveRaw.InitialRate == 0 {
		cfg.AdaptiveRaw.InitialRate = 2.0
	}
	if cfg.AdaptiveRaw.MaxRate == 0 {
		cfg.AdaptiveRaw.MaxRate = 100.0
	}
	if cfg.AdaptiveRaw.RateBurst == 0 {
		cfg.AdaptiveRaw.RateBurst = 3.0
	}
	if cfg.AdaptiveRaw.CubicBeta == 0 {
		cfg.AdaptiveRaw.CubicBeta = 0.7
	}
	if cfg.AdaptiveRaw.CubicC == 0 {
		cfg.AdaptiveRaw.CubicC = 0.4
	}
	if cfg.AdaptiveRaw.RateWindowSuccesses == 0 {
		cfg.AdaptiveRaw.RateWindowSuccesses = 5
	}
	if cfg.AdaptiveRaw.InitialConcurrency == 0 {
		cfg.AdaptiveRaw.InitialConcurrency = 4
	}
	if cfg.AdaptiveRaw.MaxConcurrency == 0 {
		cfg.AdaptiveRaw.MaxConcurrency = 32
	}
	if cfg.AdaptiveRaw.ConcurrencyWindow == 0 {
		cfg.AdaptiveRaw.ConcurrencyWindow = 10
	}
	if cfg.AdaptiveRaw.RetryBudgetCapacity == 0 {
		cfg.AdaptiveRaw.RetryBudgetCapacity = 10
	}
	retryBudgetRefillPerSec := 0.1
	if cfg.AdaptiveRaw.RetryBudgetRefillPerSec != nil {
		retryBudgetRefillPerSec = *cfg.AdaptiveRaw.RetryBudgetRefillPerSec
	}
	if cfg.DefaultModel == "" {
		cfg.DefaultModel = "gemini-3.6-flash-high"
	}
	if cfg.RequestTimeoutRaw == nil {
		cfg.RequestTimeoutRaw = &Duration{Duration: 5 * time.Minute}
	}

	// Populate normalized runtime settings.
	cfg.Balancer = BalancerSettings{MaxRetries: maxRetries}
	cfg.Queue = QueueSettings{
		MaxDelay:     cfg.QueueRaw.MaxDelayRaw.Duration,
		MaxDepth:     cfg.QueueRaw.MaxDepth,
		PollInterval: cfg.QueueRaw.PollIntervalRaw.Duration,
	}
	cfg.Quota = QuotaSettings{
		Enabled:               cfg.QuotaRaw.Enabled != nil && *cfg.QuotaRaw.Enabled,
		RefreshInterval:       cfg.QuotaRaw.RefreshIntervalRaw.Duration,
		LowRemainingInterval:  cfg.QuotaRaw.LowRemainingIntervalRaw.Duration,
		LowRemainingThreshold: cfg.QuotaRaw.LowRemainingThreshold,
		CommandTimeout:        cfg.QuotaRaw.CommandTimeoutRaw.Duration,
	}
	cfg.Logging = LoggingSettings{PromptPreview: cfg.LoggingRaw.PromptPreview}
	cfg.Adaptive = AdaptiveSettings{
		MinRate:                 cfg.AdaptiveRaw.MinRate,
		InitialRate:             cfg.AdaptiveRaw.InitialRate,
		MaxRate:                 cfg.AdaptiveRaw.MaxRate,
		RateBurst:               cfg.AdaptiveRaw.RateBurst,
		CubicBeta:               cfg.AdaptiveRaw.CubicBeta,
		CubicC:                  cfg.AdaptiveRaw.CubicC,
		RateWindowSuccesses:     cfg.AdaptiveRaw.RateWindowSuccesses,
		InitialConcurrency:      cfg.AdaptiveRaw.InitialConcurrency,
		MaxConcurrency:          cfg.AdaptiveRaw.MaxConcurrency,
		ConcurrencyWindow:       cfg.AdaptiveRaw.ConcurrencyWindow,
		RetryBudgetCapacity:     cfg.AdaptiveRaw.RetryBudgetCapacity,
		RetryBudgetRefillPerSec: retryBudgetRefillPerSec,
	}
	cfg.RequestTimeout = cfg.RequestTimeoutRaw.Duration
}

var workerUsernameRE = regexp.MustCompile(`^[a-z_][a-z0-9_-]*$`)

func finite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func Validate(cfg *Config) error {
	if cfg.AgyPath == "" {
		return fmt.Errorf("agy_path is required")
	}
	if cfg.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}
	if len(cfg.Workers) == 0 {
		return fmt.Errorf("at least one worker is required")
	}

	hasEnabled := false
	seenWorkers := make(map[string]struct{}, len(cfg.Workers))
	for i, w := range cfg.Workers {
		if !workerUsernameRE.MatchString(w.Username) {
			return fmt.Errorf("invalid worker username %q (index %d)", w.Username, i)
		}
		if _, exists := seenWorkers[w.Username]; exists {
			return fmt.Errorf("duplicate worker username %q", w.Username)
		}
		seenWorkers[w.Username] = struct{}{}
		if w.Enabled {
			hasEnabled = true
		}
	}

	if !hasEnabled {
		return fmt.Errorf("at least one worker must be enabled")
	}
	if cfg.RequestTimeout <= 0 {
		return fmt.Errorf("request_timeout must be positive")
	}
	if cfg.Balancer.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be >= 0")
	}
	if cfg.Queue.MaxDelay <= 0 || cfg.Queue.MaxDepth <= 0 || cfg.Queue.PollInterval <= 0 {
		return fmt.Errorf("queue settings must be positive")
	}
	if cfg.Quota.Enabled && (cfg.Quota.RefreshInterval <= 0 || cfg.Quota.LowRemainingInterval <= 0 || !finite(cfg.Quota.LowRemainingThreshold) || cfg.Quota.LowRemainingThreshold <= 0 || cfg.Quota.LowRemainingThreshold >= 1 || cfg.Quota.CommandTimeout <= 0) {
		return fmt.Errorf("quota settings are invalid")
	}
	if !finite(cfg.Adaptive.MinRate) || !finite(cfg.Adaptive.InitialRate) || !finite(cfg.Adaptive.MaxRate) || cfg.Adaptive.MinRate <= 0 || cfg.Adaptive.InitialRate < cfg.Adaptive.MinRate || cfg.Adaptive.MaxRate < cfg.Adaptive.InitialRate {
		return fmt.Errorf("adaptive rate bounds are invalid")
	}
	if !finite(cfg.Adaptive.RateBurst) || !finite(cfg.Adaptive.CubicBeta) || !finite(cfg.Adaptive.CubicC) || cfg.Adaptive.RateBurst < 1 || cfg.Adaptive.CubicBeta <= 0 || cfg.Adaptive.CubicBeta >= 1 || cfg.Adaptive.CubicC <= 0 || cfg.Adaptive.RateWindowSuccesses <= 0 {
		return fmt.Errorf("adaptive rate controller settings are invalid")
	}
	if cfg.Adaptive.InitialConcurrency <= 0 || cfg.Adaptive.MaxConcurrency < cfg.Adaptive.InitialConcurrency || cfg.Adaptive.ConcurrencyWindow <= 0 {
		return fmt.Errorf("adaptive concurrency settings are invalid")
	}
	if !finite(cfg.Adaptive.RetryBudgetCapacity) || !finite(cfg.Adaptive.RetryBudgetRefillPerSec) || cfg.Adaptive.RetryBudgetCapacity <= 0 || cfg.Adaptive.RetryBudgetRefillPerSec < 0 {
		return fmt.Errorf("adaptive retry budget settings are invalid")
	}

	return nil
}
