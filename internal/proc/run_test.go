//go:build linux

package proc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRunCancellationTerminatesDescendants(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_, err := Run(ctx, nil, "sh", "-c", "sleep 30 & echo $! > "+shellQuote(pidFile)+"; wait")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run error=%v, want deadline exceeded", err)
	}

	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("reading child pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("parsing child pid: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if processGoneOrZombie(pid) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("descendant pid %d remained alive after group cancellation", pid)
}

func processGoneOrZombie(pid int) bool {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if os.IsNotExist(err) {
		return true
	}
	if err != nil {
		return false
	}
	fields := strings.Fields(string(data))
	return len(fields) >= 3 && fields[2] == "Z"
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func TestRunCancellationKillsSIGTERMIgnoringDetachedStdioChild(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "stubborn.pid")
	script := filepath.Join(dir, "stubborn.py")
	body := "import os, signal, sys, time\n" +
		"signal.signal(signal.SIGTERM, signal.SIG_IGN)\n" +
		"dn=os.open(os.devnull, os.O_RDWR)\n" +
		"os.dup2(dn,0); os.dup2(dn,1); os.dup2(dn,2)\n" +
		"tmp=sys.argv[1]+'.tmp'\n" +
		"f=open(tmp,'w'); f.write(str(os.getpid())); f.flush(); os.fsync(f.fileno()); f.close()\n" +
		"os.replace(tmp,sys.argv[1])\n" +
		"while True: time.sleep(1)\n"
	if err := os.WriteFile(script, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := Run(ctx, nil, "sh", "-c", "python3 "+shellQuote(script)+" "+shellQuote(pidFile)+" & wait")
		done <- err
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(pidFile); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("stubborn child did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	data, err := os.ReadFile(pidFile)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		cancel()
		t.Fatal(err)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v, want context canceled", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if processGoneOrZombie(pid) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("SIGTERM-ignoring descendant pid %d remained alive", pid)
}
