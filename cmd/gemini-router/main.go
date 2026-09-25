package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/darttroll/gemini-router/internal/commands"
	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/executor"
	"github.com/darttroll/gemini-router/internal/logger"
	"github.com/darttroll/gemini-router/internal/storage"
)

const usageText = `Usage: gemini-router [flags] --print "prompt" [files...]
       gemini-router <command> [flags]

Flags:
  -p, --print              Run a one-shot request (main mode)
  --model <name>           Model for the request (overrides default_model)
  --out-dir <path>         Output directory for artifacts (default: /tmp/gemini-router)
  --timeout <duration>     Request timeout (default: from config, 5m)
  --retry-strategy <str>   Error strategy: auto|failfast|wait (default: auto)
  --queue-unbounded        Bypass queue delay/depth admission limits
  --config <path>          Path to config file
  -h, --help               Show help

Commands:
  stats [--period <dur>]   Worker history + queue depth/predicted wait + 5h/weekly quotas
  health [--verbose]       Check worker availability
  status [--json]          Current queue, adaptive capacity, quota and provider load
  quota [--json]           all provider quota groups and windows
  setup                    Initialize (create dirs, generate sudoers)
  reset <worker>           Reset provider breaker/retry budget (preserve learned capacity)
`

// CLIOptions contains parsed command-line arguments.
type CLIOptions struct {
	Command        string // "print", "stats", "health", "status", "quota", "setup", "reset"
	Worker         string // Used by the reset command.
	Prompt         string
	Model          string
	OutDir         string
	Timeout        string
	RetryStrategy  string
	ConfigPath     string
	Files          []string
	Period         string
	Verbose        bool
	QueueUnbounded bool
	JSON           bool
	Help           bool
}

func main() {
	opts, err := parseCLI(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprint(os.Stderr, usageText)
		os.Exit(1)
	}

	if opts.Help {
		fmt.Print(usageText)
		os.Exit(0)
	}

	// Load configuration.
	cfgPath, err := config.FindConfigPath(opts.ConfigPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Apply CLI overrides.
	if opts.Timeout != "" {
		d, err := time.ParseDuration(opts.Timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid timeout: %v\n", err)
			os.Exit(1)
		}
		cfg.RequestTimeout = d
	}
	if err := config.Validate(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: config validation after CLI overrides: %v\n", err)
		os.Exit(1)
	}

	switch opts.Command {
	case "print":
		exitCode := runPrint(cfg, opts)
		os.Exit(exitCode)
	case "stats":
		exitCode := runStats(cfg, opts)
		os.Exit(exitCode)
	case "health":
		exitCode := runHealth(cfg, opts)
		os.Exit(exitCode)
	case "status":
		exitCode := runStatus(cfg, opts)
		os.Exit(exitCode)
	case "quota":
		exitCode := runQuota(cfg, opts)
		os.Exit(exitCode)
	case "setup":
		exitCode := runSetup(cfg)
		os.Exit(exitCode)
	case "reset":
		exitCode := runReset(cfg, opts)
		os.Exit(exitCode)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", opts.Command)
		fmt.Fprint(os.Stderr, usageText)
		os.Exit(1)
	}
}

func initStorage(cfg *config.Config) (*storage.Store, error) {
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}
	if err := os.Chmod(cfg.DataDir, 0700); err != nil {
		return nil, fmt.Errorf("securing data directory: %w", err)
	}
	dbPath := filepath.Join(cfg.DataDir, "gemini-router.db")
	return storage.New(dbPath)
}

func runPrint(cfg *config.Config, opts *CLIOptions) int {
	store, err := initStorage(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		return 1
	}
	defer store.Close()

	logDir := filepath.Join(cfg.DataDir, "logs")
	lg, err := logger.New(logDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating logger: %v\n", err)
		return 1
	}
	defer lg.Close()

	printCmd := commands.NewPrintCommand(cfg, store, lg)

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, cfg.RequestTimeout)
	defer cancel()

	// Stdin belongs to the same request deadline as queueing and model execution.
	stdinContent, err := executor.ReadStdinContext(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
		return 1
	}

	retryStrategy := opts.RetryStrategy
	if retryStrategy == "" {
		retryStrategy = "auto"
	}

	output, err := printCmd.Run(ctx, opts.Prompt, opts.Model, opts.Files, stdinContent, retryStrategy, opts.OutDir, opts.QueueUnbounded)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	if len(output) > 0 && !strings.HasSuffix(output, "\n") {
		fmt.Println(output)
	} else {
		fmt.Print(output)
	}
	return 0
}

func runStats(cfg *config.Config, opts *CLIOptions) int {
	store, err := initStorage(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		return 1
	}
	defer store.Close()

	var period time.Duration
	if opts.Period != "" {
		period, err = time.ParseDuration(opts.Period)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid period: %v\n", err)
			return 1
		}
	}

	statsCmd := commands.NewStatsCommand(cfg, store)
	if err := statsCmd.Run(os.Stdout, period); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

func runHealth(cfg *config.Config, opts *CLIOptions) int {
	store, err := initStorage(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		return 1
	}
	defer store.Close()

	healthCmd := commands.NewHealthCommand(cfg, store)
	if err := healthCmd.Run(os.Stdout, opts.Verbose); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

func runStatus(cfg *config.Config, opts *CLIOptions) int {
	store, err := initStorage(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		return 1
	}
	defer store.Close()
	cmd := commands.NewStatusCommand(cfg, store)
	if err := cmd.Run(os.Stdout, opts.JSON); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

func runQuota(cfg *config.Config, opts *CLIOptions) int {
	store, err := initStorage(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		return 1
	}
	defer store.Close()

	cmd := commands.NewQuotaCommand(cfg, store)
	if err := cmd.Run(os.Stdout, opts.JSON); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

func runSetup(cfg *config.Config) int {
	setupCmd := commands.NewSetupCommand(cfg)
	if err := setupCmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

func runReset(cfg *config.Config, opts *CLIOptions) int {
	store, err := initStorage(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		return 1
	}
	defer store.Close()

	resetCmd := commands.NewResetCommand(cfg, store)
	if err := resetCmd.Run(opts.Worker); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

// parseCLI parses command-line arguments.
func parseCLI(args []string) (*CLIOptions, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("no arguments provided")
	}
	opts := &CLIOptions{}
	i := 0

	for i < len(args) {
		switch args[i] {
		case "--config":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("--config requires a path argument")
			}
			opts.ConfigPath = args[i]
			i++
		case "-h", "--help":
			opts.Help = true
			opts.Command = "help"
			return opts, nil
		default:
			goto operation
		}
	}

operation:
	if i >= len(args) {
		return nil, fmt.Errorf("no command specified")
	}
	subcommands := map[string]bool{"stats": true, "health": true, "status": true, "quota": true, "setup": true, "reset": true}
	if subcommands[args[i]] {
		opts.Command = args[i]
		i++
		for i < len(args) {
			arg := args[i]
			switch arg {
			case "--config":
				i++
				if i >= len(args) {
					return nil, fmt.Errorf("--config requires a path argument")
				}
				opts.ConfigPath = args[i]
			case "-h", "--help":
				opts.Help = true
				return opts, nil
			case "--period":
				if opts.Command != "stats" {
					return nil, fmt.Errorf("--period is only valid with stats")
				}
				i++
				if i >= len(args) {
					return nil, fmt.Errorf("--period requires a duration argument")
				}
				opts.Period = args[i]
			case "--verbose":
				if opts.Command != "health" {
					return nil, fmt.Errorf("--verbose is only valid with health")
				}
				opts.Verbose = true
			case "--json":
				if opts.Command != "status" && opts.Command != "quota" {
					return nil, fmt.Errorf("--json is only valid with status or quota")
				}
				opts.JSON = true
			default:
				if strings.HasPrefix(arg, "-") {
					return nil, fmt.Errorf("unknown flag for %s: %s", opts.Command, arg)
				}
				if opts.Command == "reset" && opts.Worker == "" {
					opts.Worker = arg
				} else {
					return nil, fmt.Errorf("unexpected argument for %s: %s", opts.Command, arg)
				}
			}
			i++
		}
		if opts.Command == "reset" && opts.Worker == "" {
			return nil, fmt.Errorf("reset requires a worker")
		}
		return opts, nil
	}

	opts.Command = "print"
	seenPrint := false
	literal := false
	for i < len(args) {
		arg := args[i]
		if literal {
			if opts.Prompt == "" {
				opts.Prompt = arg
			} else {
				opts.Files = append(opts.Files, arg)
			}
			i++
			continue
		}
		switch arg {
		case "--":
			literal = true
		case "-p", "--print":
			seenPrint = true
		case "--config":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("--config requires a path argument")
			}
			opts.ConfigPath = args[i]
		case "--model":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("--model requires a name argument")
			}
			opts.Model = args[i]
		case "--out-dir":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("--out-dir requires a path argument")
			}
			opts.OutDir = args[i]
		case "--timeout":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("--timeout requires a duration argument")
			}
			opts.Timeout = args[i]
			d, err := time.ParseDuration(opts.Timeout)
			if err != nil || d <= 0 {
				return nil, fmt.Errorf("--timeout must be a positive duration")
			}
		case "--retry-strategy":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("--retry-strategy requires a value (auto|failfast|wait)")
			}
			opts.RetryStrategy = args[i]
			if opts.RetryStrategy != "auto" && opts.RetryStrategy != "failfast" && opts.RetryStrategy != "wait" {
				return nil, fmt.Errorf("invalid retry strategy %q (want auto|failfast|wait)", opts.RetryStrategy)
			}
		case "--queue-unbounded":
			opts.QueueUnbounded = true
		case "-h", "--help":
			opts.Help = true
			return opts, nil
		default:
			if strings.HasPrefix(arg, "-") {
				return nil, fmt.Errorf("unknown print flag: %s (use -- before a literal argument beginning with '-')", arg)
			}
			if opts.Prompt == "" {
				opts.Prompt = arg
			} else {
				opts.Files = append(opts.Files, arg)
			}
		}
		i++
	}
	if !seenPrint {
		return nil, fmt.Errorf("no command specified (use --print/-p or a subcommand)")
	}
	if opts.RetryStrategy == "" {
		opts.RetryStrategy = "auto"
	}
	if opts.OutDir == "" {
		opts.OutDir = "/tmp/gemini-router"
	}
	return opts, nil
}
