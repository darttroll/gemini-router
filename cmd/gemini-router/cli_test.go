package main

import (
	"testing"
)

func TestParseCLI_Print(t *testing.T) {
	args := []string{"--print", "Hello world"}
	opts, err := parseCLI(args)
	if err != nil {
		t.Fatalf("parseCLI() error: %v", err)
	}
	if opts.Command != "print" {
		t.Errorf("Command = %q, want 'print'", opts.Command)
	}
	if opts.Prompt != "Hello world" {
		t.Errorf("Prompt = %q, want 'Hello world'", opts.Prompt)
	}
}

func TestParseCLI_PrintShort(t *testing.T) {
	args := []string{"-p", "Hello world"}
	opts, err := parseCLI(args)
	if err != nil {
		t.Fatalf("parseCLI() error: %v", err)
	}
	if opts.Command != "print" {
		t.Errorf("Command = %q, want 'print'", opts.Command)
	}
	if opts.Prompt != "Hello world" {
		t.Errorf("Prompt = %q, want 'Hello world'", opts.Prompt)
	}
}

func TestParseCLI_PrintWithModel(t *testing.T) {
	args := []string{"-p", "--model", "CustomModel", "Hello"}
	opts, err := parseCLI(args)
	if err != nil {
		t.Fatalf("parseCLI() error: %v", err)
	}
	if opts.Model != "CustomModel" {
		t.Errorf("Model = %q, want 'CustomModel'", opts.Model)
	}
	if opts.Prompt != "Hello" {
		t.Errorf("Prompt = %q, want 'Hello'", opts.Prompt)
	}
}

func TestParseCLI_PrintWithFiles(t *testing.T) {
	args := []string{"-p", "Describe", "image.png", "doc.txt"}
	opts, err := parseCLI(args)
	if err != nil {
		t.Fatalf("parseCLI() error: %v", err)
	}
	if opts.Prompt != "Describe" {
		t.Errorf("Prompt = %q, want 'Describe'", opts.Prompt)
	}
	if len(opts.Files) != 2 {
		t.Fatalf("len(Files) = %d, want 2", len(opts.Files))
	}
	if opts.Files[0] != "image.png" || opts.Files[1] != "doc.txt" {
		t.Errorf("Files = %v, want [image.png doc.txt]", opts.Files)
	}
}

func TestParseCLI_PrintWithTimeout(t *testing.T) {
	args := []string{"-p", "--timeout", "3m", "Hello"}
	opts, err := parseCLI(args)
	if err != nil {
		t.Fatalf("parseCLI() error: %v", err)
	}
	if opts.Timeout != "3m" {
		t.Errorf("Timeout = %q, want '3m'", opts.Timeout)
	}
}

func TestParseCLI_PrintWithRetryStrategy(t *testing.T) {
	args := []string{"-p", "--retry-strategy", "failfast", "Hello"}
	opts, err := parseCLI(args)
	if err != nil {
		t.Fatalf("parseCLI() error: %v", err)
	}
	if opts.RetryStrategy != "failfast" {
		t.Errorf("RetryStrategy = %q, want 'failfast'", opts.RetryStrategy)
	}
}

func TestParseCLI_Stats(t *testing.T) {
	args := []string{"stats"}
	opts, err := parseCLI(args)
	if err != nil {
		t.Fatalf("parseCLI() error: %v", err)
	}
	if opts.Command != "stats" {
		t.Errorf("Command = %q, want 'stats'", opts.Command)
	}
}

func TestParseCLI_StatsWithPeriod(t *testing.T) {
	args := []string{"stats", "--period", "24h"}
	opts, err := parseCLI(args)
	if err != nil {
		t.Fatalf("parseCLI() error: %v", err)
	}
	if opts.Command != "stats" {
		t.Errorf("Command = %q, want 'stats'", opts.Command)
	}
	if opts.Period != "24h" {
		t.Errorf("Period = %q, want '24h'", opts.Period)
	}
}

func TestParseCLI_Health(t *testing.T) {
	args := []string{"health"}
	opts, err := parseCLI(args)
	if err != nil {
		t.Fatalf("parseCLI() error: %v", err)
	}
	if opts.Command != "health" {
		t.Errorf("Command = %q, want 'health'", opts.Command)
	}
}

func TestParseCLI_HealthVerbose(t *testing.T) {
	args := []string{"health", "--verbose"}
	opts, err := parseCLI(args)
	if err != nil {
		t.Fatalf("parseCLI() error: %v", err)
	}
	if !opts.Verbose {
		t.Error("expected Verbose=true")
	}
}

func TestParseCLI_Setup(t *testing.T) {
	args := []string{"setup"}
	opts, err := parseCLI(args)
	if err != nil {
		t.Fatalf("parseCLI() error: %v", err)
	}
	if opts.Command != "setup" {
		t.Errorf("Command = %q, want 'setup'", opts.Command)
	}
}

func TestParseCLI_Config(t *testing.T) {
	args := []string{"--config", "/etc/custom.yaml", "-p", "Hello"}
	opts, err := parseCLI(args)
	if err != nil {
		t.Fatalf("parseCLI() error: %v", err)
	}
	if opts.ConfigPath != "/etc/custom.yaml" {
		t.Errorf("ConfigPath = %q, want '/etc/custom.yaml'", opts.ConfigPath)
	}
}

func TestParseCLI_NoArgs(t *testing.T) {
	args := []string{}
	_, err := parseCLI(args)
	if err == nil {
		t.Fatal("expected error for empty args")
	}
}

func TestParseCLI_QueueUnbounded(t *testing.T) {
	opts, err := parseCLI([]string{"-p", "--queue-unbounded", "Hello"})
	if err != nil {
		t.Fatal(err)
	}
	if !opts.QueueUnbounded || opts.Command != "print" {
		t.Fatalf("opts=%+v", opts)
	}
}

func TestParseCLI_StatusJSON(t *testing.T) {
	opts, err := parseCLI([]string{"status", "--json"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Command != "status" || !opts.JSON {
		t.Fatalf("opts=%+v", opts)
	}
}

func TestParseCLI_QueueUnboundedRejectedForStatus(t *testing.T) {
	_, err := parseCLI([]string{"status", "--queue-unbounded"})
	if err == nil {
		t.Fatal("expected queue-unbounded validation error")
	}
}

func TestParseCLI_Quota(t *testing.T) {
	opts, err := parseCLI([]string{"quota"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Command != "quota" || opts.JSON {
		t.Fatalf("opts=%+v", opts)
	}
}

func TestParseCLI_QuotaJSON(t *testing.T) {
	opts, err := parseCLI([]string{"quota", "--json"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Command != "quota" || !opts.JSON {
		t.Fatalf("opts=%+v", opts)
	}
}

func TestParseCLI_JSONRejectedForStats(t *testing.T) {
	_, err := parseCLI([]string{"stats", "--json"})
	if err == nil {
		t.Fatal("expected --json validation error for stats")
	}
}

func TestParseCLI_PrintCommandWordIsPrompt(t *testing.T) {
	opts, err := parseCLI([]string{"-p", "setup"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Command != "print" || opts.Prompt != "setup" {
		t.Fatalf("opts=%+v", opts)
	}
}

func TestParseCLI_InvalidRetryStrategyRejected(t *testing.T) {
	if _, err := parseCLI([]string{"-p", "--retry-strategy", "banana", "hello"}); err == nil {
		t.Fatal("expected invalid retry strategy error")
	}
}

func TestParseCLI_UnknownPrintFlagRejected(t *testing.T) {
	if _, err := parseCLI([]string{"-p", "--wat", "hello"}); err == nil {
		t.Fatal("expected unknown flag error")
	}
}

func TestParseCLI_DoubleDashAllowsFlagLikePrompt(t *testing.T) {
	opts, err := parseCLI([]string{"-p", "--", "--model"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Prompt != "--model" {
		t.Fatalf("prompt=%q", opts.Prompt)
	}
}

func TestParseCLIRejectsNonPositiveTimeout(t *testing.T) {
	for _, value := range []string{"0s", "-1s"} {
		if _, err := parseCLI([]string{"-p", "--timeout", value, "hello"}); err == nil {
			t.Fatalf("timeout %s accepted", value)
		}
	}
}
