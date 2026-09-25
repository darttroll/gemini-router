package proc

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"syscall"
	"time"
)

// Result contains the captured output of one subprocess.
type Result struct {
	Stdout []byte
	Stderr []byte
}

// CombinedOutput returns stdout followed by stderr.
func (r Result) CombinedOutput() []byte {
	out := make([]byte, 0, len(r.Stdout)+len(r.Stderr))
	out = append(out, r.Stdout...)
	out = append(out, r.Stderr...)
	return out
}

// Run executes a command in its own Unix process group. Cancellation terminates
// the whole group so sudo wrappers cannot leave provider/helper children behind.
func Run(ctx context.Context, stdin io.Reader, name string, args ...string) (Result, error) {
	cmd := exec.Command(name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdin = stdin

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return Result{}, err
	}

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	select {
	case err := <-waitCh:
		return Result{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}, err
	case <-ctx.Done():
		TerminateGroup(cmd.Process.Pid, waitCh)
		// If a process is stuck in kernel I/O, cmd.Wait may finish after the
		// bounded cancellation grace. Do not read capture buffers while that
		// reaper can still be draining process pipes.
		return Result{}, ctx.Err()
	}
}

// TerminateGroup asks a process group to exit, then escalates to SIGKILL.
//
// cmd.Wait() only proves that the process-group leader exited. Descendants can
// outlive it, so group existence rather than leader exit is the termination
// condition. Conversely, PGID disappearance does not prove cmd.Wait() has
// finished draining its pipes, so both conditions are handled independently.
func TerminateGroup(pid int, waitCh <-chan error) {
	// Cancellation must not hold the request lease indefinitely when a helper is
	// stuck in slow or uninterruptible filesystem I/O. Give cooperative
	// shutdown a short grace period, escalate, then bound how long the caller
	// waits for kernel/process cleanup. cmd.Wait keeps running in its reaper
	// goroutine if the process cannot be reaped within this window.
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	if !waitForGroupGone(pid, 50*time.Millisecond) {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		_ = waitForGroupGone(pid, 100*time.Millisecond)
	}

	waitTimer := time.NewTimer(50 * time.Millisecond)
	defer waitTimer.Stop()
	select {
	case <-waitCh:
	case <-waitTimer.C:
	}
}

func waitForGroupGone(pid int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !processGroupExists(pid) {
			return true
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			return !processGroupExists(pid)
		}
	}
}

func processGroupExists(pid int) bool {
	err := syscall.Kill(-pid, 0)
	return err == nil || err == syscall.EPERM
}
