package commands

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/darttroll/gemini-router/internal/config"
	"github.com/darttroll/gemini-router/internal/executor"
	"github.com/darttroll/gemini-router/internal/storage"
)

func TestParallelIdentityStress50Requests(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "parallel.db")
	cfg := &config.Config{DefaultModel: "m", RequestTimeout: 2 * time.Minute, Workers: []config.WorkerConfig{{Username: "alice", Enabled: true}, {Username: "bob", Enabled: true}, {Username: "carol", Enabled: true}}, Balancer: config.BalancerSettings{MaxRetries: 0}, Queue: config.QueueSettings{MaxDelay: time.Second, MaxDepth: 1000, PollInterval: 2 * time.Millisecond}, Adaptive: config.AdaptiveSettings{MinRate: .2, InitialRate: 100, MaxRate: 200, RateBurst: 100, CubicBeta: .7, CubicC: .4, RateWindowSuccesses: 5, InitialConcurrency: 20, MaxConcurrency: 40, ConcurrencyWindow: 10, RetryBudgetCapacity: 10, RetryBudgetRefillPerSec: .1}}
	const n = 50
	results := make([]string, n)
	errs := make([]error, n)
	seen := make([]executor.Request, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := storage.New(dbPath)
			if err != nil {
				errs[i] = err
				return
			}
			defer st.Close()
			id := fmt.Sprintf("req-%03d", i)
			token := fmt.Sprintf("TOKEN_%03d", i)
			cmd := NewPrintCommand(cfg, st, nil)
			cmd.idFn = func() string { return id }
			cmd.execFn = func(ctx context.Context, req executor.Request) (*executor.Result, error) {
				seen[i] = req
				time.Sleep(time.Duration(i%5) * time.Millisecond)
				return &executor.Result{Output: token, ConversationID: "conv-" + id, Duration: 5 * time.Millisecond}, nil
			}
			out, err := cmd.Run(context.Background(), token, "", nil, "", "auto", "/tmp/out", true)
			results[i] = out
			errs[i] = err
		}()
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("req-%03d", i)
		token := fmt.Sprintf("TOKEN_%03d", i)
		if errs[i] != nil {
			t.Fatalf("request %d err=%v", i, errs[i])
		}
		if results[i] != token {
			t.Fatalf("request %d output=%q want %q", i, results[i], token)
		}
		if seen[i].ID != id || seen[i].Prompt != token {
			t.Fatalf("request %d identity=%+v", i, seen[i])
		}
		for j := 0; j < n; j++ {
			if j != i && strings.Contains(results[i], fmt.Sprintf("TOKEN_%03d", j)) {
				t.Fatalf("request %d contains token from %d: %q", i, j, results[i])
			}
		}
	}
}
