package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/darttroll/gemini-router/internal/proc"
)

type ErrorType string

const (
	ErrorRateLimited       ErrorType = "rate_limited"
	ErrorLongQuota         ErrorType = "long_quota_exhausted"
	ErrorAccountIneligible ErrorType = "account_ineligible"
	ErrorInvalidRequest    ErrorType = "invalid_request"
	ErrorInvalidAttachment ErrorType = "invalid_attachment"
	ErrorLocalPreparation  ErrorType = "local_preparation_failure"
	ErrorNonZeroExit       ErrorType = "non_zero_exit"
	ErrorEmptyOutput       ErrorType = "empty_output"
	ErrorTimeout           ErrorType = "timeout"
	ErrorCancelled         ErrorType = "client_cancelled"
)

type AgyError struct {
	Type          ErrorType
	Message       string
	ResetDuration time.Duration
}

func (e *AgyError) Error() string { return fmt.Sprintf("%s: %s", e.Type, e.Message) }

type Result struct {
	Output         string
	Duration       time.Duration
	Error          *AgyError
	ConversationID string
	OwnedLogPath   string
}

type Request struct {
	ID           string
	Worker       string
	Prompt       string
	Model        string
	Files        []string
	StdinContent string
	OutDir       string
}

type Executor struct {
	agyPath      string
	timeout      time.Duration
	workerHomeFn func(string) string
}

type invalidAttachmentError struct {
	message string
}

func (e *invalidAttachmentError) Error() string { return e.message }

// ValidateAttachments checks user-supplied attachments inside a cancellable
// subprocess boundary so slow metadata I/O cannot block the router process.
func ValidateAttachments(ctx context.Context, files []string) error {
	for _, path := range files {
		if err := validateRegularAttachment(ctx, path); err != nil {
			return err
		}
	}
	return nil
}

func validateRegularAttachment(ctx context.Context, path string) error {
	result, err := proc.Run(ctx, nil, "stat", "-c", "%f", "--", path)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		detail := strings.TrimSpace(string(result.CombinedOutput()))
		if detail == "" {
			detail = err.Error()
		}
		return &invalidAttachmentError{message: fmt.Sprintf("reading attachment metadata %s: %s", path, detail)}
	}
	mode, err := strconv.ParseUint(strings.TrimSpace(string(result.Stdout)), 16, 32)
	if err != nil {
		return fmt.Errorf("parsing attachment metadata for %s: %w", path, err)
	}
	if uint32(mode)&uint32(syscall.S_IFMT) != uint32(syscall.S_IFREG) {
		return &invalidAttachmentError{message: fmt.Sprintf("unsupported attachment %s: only regular files are allowed", path)}
	}
	return nil
}

func New(agyPath string, timeout time.Duration) *Executor {
	return &Executor{agyPath: agyPath, timeout: timeout}
}

// Execute is the legacy-compatible wrapper. New code should use ExecuteRequest so request identity is explicit.
func (e *Executor) Execute(ctx context.Context, worker, prompt, model string, files []string, stdinContent string, outDir string) (*Result, error) {
	return e.ExecuteRequest(ctx, Request{ID: generateUUID(), Worker: worker, Prompt: prompt, Model: model, Files: files, StdinContent: stdinContent, OutDir: outDir})
}

func (e *Executor) ExecuteRequest(ctx context.Context, req Request) (*Result, error) {
	if matched, _ := regexp.MatchString(`^[a-z_][a-z0-9_-]*$`, req.Worker); !matched && req.Worker != "root" {
		return nil, fmt.Errorf("invalid worker username: %s", req.Worker)
	}
	if req.ID == "" {
		req.ID = generateUUID()
	}
	start := time.Now()
	execCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	logPath := ownedLogPath(req.ID)
	_ = os.Remove(logPath)
	result := &Result{OwnedLogPath: logPath}
	defer os.Remove(logPath)

	var tmpDir string
	mapped := req.Files
	if len(req.Files) > 0 {
		var err error
		tmpDir, mapped, err = e.prepareFiles(execCtx, req.Worker, req.Files)
		if err != nil {
			result.Duration = time.Since(start)
			var invalid *invalidAttachmentError
			switch {
			case errors.Is(err, context.Canceled):
				result.Error = &AgyError{Type: ErrorCancelled, Message: "client cancelled request"}
			case errors.Is(err, context.DeadlineExceeded):
				result.Error = &AgyError{Type: ErrorTimeout, Message: fmt.Sprintf("request timed out after %s", e.timeout)}
			case errors.As(err, &invalid):
				result.Error = &AgyError{Type: ErrorInvalidAttachment, Message: invalid.Error()}
			default:
				result.Error = &AgyError{Type: ErrorLocalPreparation, Message: fmt.Sprintf("preparing files: %v", err)}
			}
			return result, nil
		}
		defer e.cleanupDir(tmpDir)
	}

	args := e.buildArgsWithLog(req.Worker, req.Prompt, req.Model, mapped, logPath)
	var stdin io.Reader
	if req.StdinContent != "" {
		stdin = strings.NewReader(req.StdinContent)
	}
	processResult, runErr := proc.Run(execCtx, stdin, "sudo", args...)
	if runErr != nil && execCtx.Err() == nil {
		// The command started and failed; classification below inspects diagnostics.
	}

	result.Duration = time.Since(start)
	result.Output = string(processResult.Stdout)
	logBytes, _ := os.ReadFile(logPath)
	ownedLog := string(logBytes)
	result.ConversationID = extractConversationIDFromText(ownedLog)

	if ctx.Err() == context.Canceled {
		result.Error = &AgyError{Type: ErrorCancelled, Message: "client cancelled request"}
		return result, nil
	}
	if execCtx.Err() == context.DeadlineExceeded {
		result.Error = &AgyError{Type: ErrorTimeout, Message: fmt.Sprintf("request timed out after %s", e.timeout)}
		return result, nil
	}
	// Provider diagnostics belong to this request's stderr and owned AGI log.
	// Do not scan a successful model stdout for strings such as "HTTP 429":
	// the model may legitimately be discussing rate limits in its answer.
	errorText := string(processResult.Stderr) + "\n" + ownedLog
	if runErr != nil {
		// Some CLIs print failure diagnostics to stdout only when exiting non-zero.
		errorText += "\n" + string(processResult.Stdout)
	}
	if parsed := parseTextForError(errorText); parsed != nil {
		result.Error = parsed
		return result, nil
	}
	if runErr != nil {
		result.Error = &AgyError{Type: ErrorNonZeroExit, Message: fmt.Sprintf("agy exited with error: %v, stderr: %s", runErr, string(processResult.Stderr))}
		return result, nil
	}
	if strings.TrimSpace(result.Output) == "" {
		result.Error = &AgyError{Type: ErrorEmptyOutput, Message: "agy returned empty output without detectable error"}
		return result, nil
	}

	if result.ConversationID != "" {
		if err := e.copyArtifacts(execCtx, req.ID, req.Worker, result.ConversationID, &result.Output, req.OutDir); err != nil {
			result.Output += fmt.Sprintf("\n\n[gemini-router warning: artifact copy failed: %v]", err)
		}
	}
	return result, nil
}

func ownedLogPath(requestID string) string {
	safe := regexp.MustCompile(`[^A-Za-z0-9_-]+`).ReplaceAllString(requestID, "_")
	if safe == "" {
		safe = generateUUID()
	}
	return filepath.Join(os.TempDir(), "gemini-router-agy-"+safe+".log")
}

func (e *Executor) buildArgs(worker, prompt, model string, files []string) []string {
	return e.buildArgsInternal(worker, prompt, model, files, "")
}
func (e *Executor) buildArgsWithLog(worker, prompt, model string, files []string, logFile string) []string {
	return e.buildArgsInternal(worker, prompt, model, files, logFile)
}
func (e *Executor) buildArgsInternal(worker, prompt, model string, files []string, logFile string) []string {
	args := []string{"-H", "-u", worker, e.agyPath}
	if logFile != "" {
		args = append(args, "--log-file", logFile)
	}
	if model != "" {
		args = append(args, "--model", model)
	}
	args = append(args, "--print")
	finalPrompt := prompt
	if len(files) > 0 {
		if finalPrompt != "" {
			finalPrompt += "\n\nFiles attached:\n" + strings.Join(files, "\n")
		} else {
			finalPrompt = strings.Join(files, "\n")
		}
	}
	if finalPrompt != "" {
		args = append(args, finalPrompt)
	}
	return args
}

func (e *Executor) prepareFiles(ctx context.Context, worker string, files []string) (string, []string, error) {
	if err := ValidateAttachments(ctx, files); err != nil {
		return "", nil, err
	}
	tmpDir := filepath.Join(os.TempDir(), "gemini-router-"+generateUUID())
	if err := os.MkdirAll(tmpDir, 0700); err != nil {
		return "", nil, fmt.Errorf("creating temp dir: %w", err)
	}
	var mapped []string
	for i, f := range files {
		if err := ctx.Err(); err != nil {
			os.RemoveAll(tmpDir)
			return "", nil, err
		}
		dst := filepath.Join(tmpDir, fmt.Sprintf("%d_%s", i, filepath.Base(f)))
		result, err := proc.Run(ctx, nil, "cp", "--", f, dst)
		if err != nil {
			os.RemoveAll(tmpDir)
			detail := strings.TrimSpace(string(result.CombinedOutput()))
			if detail != "" {
				return "", nil, fmt.Errorf("copying file %s: %w: %s", f, err, detail)
			}
			return "", nil, fmt.Errorf("copying file %s: %w", f, err)
		}
		if err := os.Chmod(dst, 0600); err != nil {
			os.RemoveAll(tmpDir)
			return "", nil, fmt.Errorf("securing temp file %s: %w", dst, err)
		}
		mapped = append(mapped, dst)
	}
	if _, err := proc.Run(ctx, nil, "sudo", "chown", "-R", worker, tmpDir); err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("chown temp dir: %w", err)
	}
	return tmpDir, mapped, nil
}

func (e *Executor) cleanupDir(path string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = proc.Run(ctx, nil, "sudo", "rm", "-rf", "--", path)
}

var (
	reResetsIn       = regexp.MustCompile(`(?i)Resets in\s+([\dhms]+)`)
	reConversationID = regexp.MustCompile(`conversation=([a-fA-F0-9\-]{36})`)
)

func parseTextForError(content string) *AgyError {
	lower := strings.ToLower(content)
	reset := time.Duration(0)
	if m := reResetsIn.FindStringSubmatch(content); len(m) >= 2 {
		reset = parseResetDuration(m[1])
	}
	if strings.Contains(lower, "account ineligible") {
		return &AgyError{Type: ErrorAccountIneligible, Message: "account ineligible"}
	}
	if strings.Contains(lower, "invalid model") || strings.Contains(lower, "unknown model") || strings.Contains(lower, "invalid request") {
		return &AgyError{Type: ErrorInvalidRequest, Message: strings.TrimSpace(content)}
	}
	if strings.Contains(lower, "individual quota reached") || strings.Contains(lower, "weekly quota") || strings.Contains(lower, "five-hour quota") {
		return &AgyError{Type: ErrorLongQuota, Message: "long quota exhausted", ResetDuration: reset}
	}
	if strings.Contains(lower, "resource_exhausted") || strings.Contains(lower, "too many requests") || strings.Contains(lower, "rate limit exceeded") || strings.Contains(lower, "rate_limit_exceeded") {
		return &AgyError{Type: ErrorRateLimited, Message: "rate limit exceeded", ResetDuration: reset}
	}
	return nil
}

func parseLogFileForError(logFile string, offset int64) *AgyError {
	f, err := os.Open(logFile)
	if err != nil {
		return nil
	}
	defer f.Close()
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil
		}
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil
	}
	return parseTextForError(string(data))
}
func extractConversationIDFromFile(logFile string) string {
	data, err := os.ReadFile(logFile)
	if err != nil {
		return ""
	}
	return extractConversationIDFromText(string(data))
}
func extractConversationIDFromText(content string) string {
	matches := reConversationID.FindAllStringSubmatch(content, -1)
	if len(matches) == 0 {
		return ""
	}
	return matches[len(matches)-1][1]
}
func parseResetDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}
func readFromReader(r io.Reader) (string, error) {
	data, err := io.ReadAll(r)
	return string(data), err
}

func readFromReaderContext(ctx context.Context, r io.Reader) (string, error) {
	type readResult struct {
		content string
		err     error
	}
	done := make(chan readResult, 1)
	go func() {
		content, err := readFromReader(r)
		done <- readResult{content: content, err: err}
	}()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case result := <-done:
		return result.content, result.err
	}
}

func ReadStdinContext(ctx context.Context) (string, error) {
	info, err := os.Stdin.Stat()
	if err != nil {
		return "", nil
	}
	if info.Mode()&os.ModeCharDevice != 0 {
		return "", nil
	}
	return readFromReaderContext(ctx, os.Stdin)
}

func ReadStdin() (string, error) {
	return ReadStdinContext(context.Background())
}
func generateUUID() string { return uuid.New().String() }

func workerHome(worker string) string {
	if worker == "root" {
		return "/root"
	}
	return "/home/" + worker
}
func safeRequestID(requestID string) string {
	safe := regexp.MustCompile(`[^A-Za-z0-9_-]+`).ReplaceAllString(requestID, "_")
	if safe == "" {
		return generateUUID()
	}
	return safe
}

func artifactDestination(outDir, requestID string, index int, source string) string {
	return filepath.Join(outDir, safeRequestID(requestID), fmt.Sprintf("%02d_%s", index, filepath.Base(source)))
}

func (e *Executor) copyArtifacts(ctx context.Context, requestID, worker, convID string, output *string, outDir string) error {
	if outDir == "" {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	home := workerHome(worker)
	if e.workerHomeFn != nil {
		home = e.workerHomeFn(worker)
	}
	brainDir := filepath.Join(home, ".gemini", "antigravity-cli", "brain", convID)
	findCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	findResult, err := proc.Run(findCtx, nil, "sudo", "find", brainDir, "-maxdepth", "1", "-type", "f", "(", "-name", "*.jpg", "-o", "-name", "*.png", "-o", "-name", "*.mp4", "-o", "-name", "*.webm", ")")
	cancel()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("listing artifacts: %w: %s", err, strings.TrimSpace(string(findResult.CombinedOutput())))
	}
	out := findResult.Stdout

	requestDir := filepath.Join(outDir, safeRequestID(requestID))
	mkdirCtx, mkdirCancel := context.WithTimeout(ctx, 10*time.Second)
	mkdirResult, err := proc.Run(mkdirCtx, nil, "mkdir", "-p", "--", requestDir)
	mkdirCancel()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("creating request artifact directory: %w: %s", err, strings.TrimSpace(string(mkdirResult.CombinedOutput())))
	}
	chmodCtx, chmodCancel := context.WithTimeout(ctx, 10*time.Second)
	chmodResult, err := proc.Run(chmodCtx, nil, "chmod", "0700", "--", requestDir)
	chmodCancel()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("securing request artifact directory: %w: %s", err, strings.TrimSpace(string(chmodResult.CombinedOutput())))
	}

	uid, gid := os.Getuid(), os.Getgid()
	for i, f := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		dest := artifactDestination(outDir, requestID, i, f)
		copyCtx, copyCancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := proc.Run(copyCtx, nil, "sudo", "cp", "--", f, dest)
		copyCancel()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("copying artifact %s: %w", filepath.Base(f), err)
		}
		chownCtx, chownCancel := context.WithTimeout(ctx, 10*time.Second)
		_, err = proc.Run(chownCtx, nil, "sudo", "chown", fmt.Sprintf("%d:%d", uid, gid), dest)
		chownCancel()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("setting artifact ownership for %s: %w", filepath.Base(dest), err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		*output += fmt.Sprintf("\n\n[gemini-router: generated artifact saved to %s]", dest)
	}
	return nil
}
