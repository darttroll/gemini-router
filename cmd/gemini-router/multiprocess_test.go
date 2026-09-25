//go:build linux

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/storage"
)

func TestCLIProcessesRespectSharedSQLiteConcurrency(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("multi-process sudo integration test requires root")
	}

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	tmp := t.TempDir()
	bin := os.Getenv("GEMINI_ROUTER_TEST_BINARY")
	if bin == "" {
		bin = filepath.Join(tmp, "gemini-router")
		build := exec.Command("go", "build", "-trimpath", "-o", bin, "./cmd/gemini-router")
		build.Dir = repoRoot
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("building CLI: %v\n%s", err, out)
		}
	}

	agy := filepath.Join(tmp, "fake-agy.sh")
	if err := os.WriteFile(agy, []byte("#!/bin/sh\nsleep 0.25\nprintf 'ok\\n'\n"), 0755); err != nil {
		t.Fatal(err)
	}

	dataDir := filepath.Join(tmp, "state")
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(tmp, "config.yaml")
	cfg := fmt.Sprintf(`agy_path: %q
data_dir: %q
balancer:
  max_retries: 0
queue:
  max_delay: 5s
  max_depth: 100
  poll_interval: 5ms
adaptive:
  min_rate: 100
  initial_rate: 100
  max_rate: 100
  rate_burst: 100
  cubic_beta: 0.7
  cubic_c: 0.4
  rate_window_successes: 1000
  initial_concurrency: 2
  max_concurrency: 2
  concurrency_window: 1000
  retry_budget_capacity: 10
  retry_budget_refill_per_sec: 0
quota:
  enabled: false
default_model: m
request_timeout: 5s
logging:
  prompt_preview: false
workers:
  - username: root
    enabled: true
`, agy, dataDir)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}

	store, err := storage.New(filepath.Join(dataDir, "gemini-router.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const clients = 6
	type result struct {
		i      int
		output string
		err    error
	}
	results := make(chan result, clients)
	var wg sync.WaitGroup

	for i := 0; i < clients; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			var stdout, stderr bytes.Buffer
			cmd := exec.CommandContext(ctx, bin, "--config", cfgPath, "-p", fmt.Sprintf("request-%d", i))
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			err := cmd.Run()
			results <- result{i: i, output: stdout.String() + stderr.String(), err: err}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	maxActive := 0
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			goto finished
		case <-ticker.C:
			n, err := store.CountAllActiveRequests()
			if err != nil {
				t.Fatal(err)
			}
			if n > maxActive {
				maxActive = n
			}
			if n > 2 {
				t.Fatalf("cross-process concurrency exceeded limit: active=%d", n)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

finished:
	close(results)
	for r := range results {
		if r.err != nil {
			t.Errorf("client %d failed: %v\n%s", r.i, r.err, r.output)
			continue
		}
		if strings.TrimSpace(r.output) != "ok" {
			t.Errorf("client %d output=%q, want ok", r.i, r.output)
		}
	}
	if t.Failed() {
		return
	}
	if maxActive != 2 {
		t.Fatalf("max active requests=%d, want 2 to prove concurrent execution", maxActive)
	}
	if depth, err := store.QueueDepth(); err != nil {
		t.Fatal(err)
	} else if depth != 0 {
		t.Fatalf("pending queue not empty after clients finished: %d", depth)
	}
	if active, err := store.CountAllActiveRequests(); err != nil {
		t.Fatal(err)
	} else if active != 0 {
		t.Fatalf("active leases remain after clients finished: %d", active)
	}
}
