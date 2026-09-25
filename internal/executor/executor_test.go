package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestBuildCommand(t *testing.T) {
	e := &Executor{
		agyPath: "/usr/bin/agy",
	}

	args := e.buildArgs("worker1", "Hello world", "TestModel", nil)
	expected := []string{"-H", "-u", "worker1", "/usr/bin/agy", "--model", "TestModel", "--print", "Hello world"}
	if len(args) != len(expected) {
		t.Fatalf("args = %v, want %v", args, expected)
	}
	for i := range expected {
		if args[i] != expected[i] {
			t.Fatalf("args[%d] = %q, want %q; full args=%v", i, args[i], expected[i], args)
		}
	}
}

func TestPrepareFiles(t *testing.T) {
	// Create a test input file.
	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(testFile, []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}

	e := &Executor{}
	// Use the current user for file-preparation tests.
	currentUser := os.Getenv("USER")
	if currentUser == "" {
		currentUser = "root" // fallback
	}
	tmpDir, mappedFiles, err := e.prepareFiles(context.Background(), currentUser, []string{testFile})
	if err != nil {
		// Skip when the test environment cannot perform sudo chown.
		if strings.Contains(err.Error(), "chown temp dir") {
			t.Skipf("skipping test because sudo authentication is unavailable: %v", err)
		}
		t.Fatalf("prepareFiles() error: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	if len(mappedFiles) != 1 {
		t.Fatalf("mappedFiles len = %d, want 1", len(mappedFiles))
	}

	// Verify the file was copied.
	data, err := os.ReadFile(mappedFiles[0])
	if err != nil {
		t.Fatalf("reading copied file: %v", err)
	}
	if string(data) != "content" {
		t.Errorf("copied content = %q, want %q", string(data), "content")
	}
}

func TestPrepareFiles_NonexistentFile(t *testing.T) {
	e := &Executor{}
	_, _, err := e.prepareFiles(context.Background(), "root", []string{"/nonexistent/file.txt"})
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestParseAgyLogForError_LongQuota(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "cli-test.log")

	logContent := `I0701 12:46:39.000000 12345 log.go:100] Starting request
E0701 12:46:40.769850 12345 log.go:398] RESOURCE_EXHAUSTED (code 429): Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 18h42m41s.
`
	if err := os.WriteFile(logFile, []byte(logContent), 0644); err != nil {
		t.Fatal(err)
	}

	errInfo := parseLogFileForError(logFile, 0)
	if errInfo == nil {
		t.Fatal("expected error info from log")
	}
	if errInfo.Type != ErrorLongQuota {
		t.Errorf("Type = %q, want %q", errInfo.Type, ErrorLongQuota)
	}
	expectedReset := 18*time.Hour + 42*time.Minute + 41*time.Second
	if errInfo.ResetDuration != expectedReset {
		t.Errorf("ResetDuration = %v, want %v", errInfo.ResetDuration, expectedReset)
	}
}

func TestParseAgyLogForError_ShortThrottle(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "cli-short.log")
	if err := os.WriteFile(logFile, []byte("RESOURCE_EXHAUSTED (code 429): too many requests\n"), 0644); err != nil {
		t.Fatal(err)
	}
	errInfo := parseLogFileForError(logFile, 0)
	if errInfo == nil || errInfo.Type != ErrorRateLimited || errInfo.ResetDuration != 0 {
		t.Fatalf("errInfo=%+v", errInfo)
	}
}

func TestParseAgyLogForError_AccountIneligible(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "cli-test.log")

	logContent := `E0701 12:46:40.000000 12345 log.go:398] Account ineligible for this service.
`
	if err := os.WriteFile(logFile, []byte(logContent), 0644); err != nil {
		t.Fatal(err)
	}

	errInfo := parseLogFileForError(logFile, 0)
	if errInfo == nil {
		t.Fatal("expected error info from log")
	}
	if errInfo.Type != ErrorAccountIneligible {
		t.Errorf("Type = %q, want %q", errInfo.Type, ErrorAccountIneligible)
	}
}

func TestParseAgyLogForError_NoError(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "cli-test.log")

	logContent := `I0701 12:46:39.000000 12345 log.go:100] Request completed successfully
`
	if err := os.WriteFile(logFile, []byte(logContent), 0644); err != nil {
		t.Fatal(err)
	}

	errInfo := parseLogFileForError(logFile, 0)
	if errInfo != nil {
		t.Errorf("expected nil error info, got %+v", errInfo)
	}
}

func TestParseResetDuration(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"18h42m41s", 18*time.Hour + 42*time.Minute + 41*time.Second},
		{"5m30s", 5*time.Minute + 30*time.Second},
		{"1h0m0s", 1 * time.Hour},
		{"30s", 30 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseResetDuration(tt.input)
			if got != tt.want {
				t.Errorf("parseResetDuration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestReadStdin(t *testing.T) {
	content := "stdin content here"
	r, w, _ := os.Pipe()
	w.WriteString(content)
	w.Close()

	result, err := readFromReader(r)
	if err != nil {
		t.Fatalf("readFromReader() error: %v", err)
	}
	if result != content {
		t.Errorf("readFromReader() = %q, want %q", result, content)
	}
}

func TestExecuteBasic(t *testing.T) {
	e := New("echo", 2*time.Second)

	res, err := e.Execute(context.Background(), "root", "test prompt", "", nil, "", "/tmp/gemini-router-test")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if res.Error != nil {
		// Skip when the test environment cannot run sudo.
		if res.Error.Type == ErrorNonZeroExit && strings.Contains(res.Error.Message, "sudo") {
			t.Skip("Skipping because sudo failed")
		}
	}
}

func TestOwnedLogPathIsUniquePerRequest(t *testing.T) {
	a := ownedLogPath("req-a")
	b := ownedLogPath("req-b")
	if a == b {
		t.Fatalf("owned log paths collide: %s", a)
	}
	if !strings.Contains(a, "req-a") || !strings.Contains(b, "req-b") {
		t.Fatalf("paths do not preserve request identity: %q %q", a, b)
	}
}

func TestBuildArgsWithOwnedLog(t *testing.T) {
	e := New("/usr/bin/agy", time.Minute)
	args := e.buildArgsWithLog("worker1", "Hello", "Model", nil, "/tmp/gemini-router-agy-req-1.log")
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--log-file /tmp/gemini-router-agy-req-1.log") {
		t.Fatalf("args missing owned log: %v", args)
	}
}

func TestOwnedLogsNeverCrossAttribute(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.log")
	b := filepath.Join(dir, "b.log")
	convA := "11111111-1111-1111-1111-111111111111"
	convB := "22222222-2222-2222-2222-222222222222"
	if err := os.WriteFile(a, []byte("conversation="+convA+"\nrequest A ok\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("conversation="+convB+"\nRESOURCE_EXHAUSTED (code 429): too many requests\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if got := extractConversationIDFromFile(a); got != convA {
		t.Fatalf("A conversation=%q", got)
	}
	if got := extractConversationIDFromFile(b); got != convB {
		t.Fatalf("B conversation=%q", got)
	}
	if errInfo := parseLogFileForError(a, 0); errInfo != nil {
		t.Fatalf("A saw B error: %+v", errInfo)
	}
	if errInfo := parseLogFileForError(b, 0); errInfo == nil || errInfo.Type != ErrorRateLimited {
		t.Fatalf("B error=%+v", errInfo)
	}
}

func TestExecuteRequestCancellationKillsProcessGroup(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("process-group sudo test requires root")
	}
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "pids")
	script := filepath.Join(dir, "mock-agy.sh")
	body := `#!/bin/bash
log=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--log-file" ]]; then log="$2"; shift 2; continue; fi
  shift
done
sleep 30 &
child=$!
echo "$$ $child" > "` + pidFile + `"
echo "conversation=33333333-3333-3333-3333-333333333333" > "$log"
wait $child
`
	if err := os.WriteFile(script, []byte(body), 0755); err != nil {
		t.Fatal(err)
	}
	e := New(script, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *Result, 1)
	go func() {
		r, _ := e.ExecuteRequest(ctx, Request{ID: "cancel-test", Worker: "root", Prompt: "x", OutDir: dir})
		done <- r
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(pidFile); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mock agy did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	r := <-done
	if r == nil || r.Error == nil || r.Error.Type != ErrorCancelled {
		t.Fatalf("result=%+v", r)
	}
	data, _ := os.ReadFile(pidFile)
	var parent, child int
	if _, err := fmt.Sscanf(string(data), "%d %d", &parent, &child); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	for _, pid := range []int{parent, child} {
		if err := syscall.Kill(pid, 0); err == nil {
			t.Fatalf("pid %d still alive after cancellation", pid)
		}
	}
}

func TestExecuteRequestSuccessfulOutputMentioning429IsNotThrottle(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires sudo/root test environment")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "mock-agy.sh")
	body := `#!/bin/bash
log=""
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--log-file" ]]; then log="$2"; shift 2; continue; fi
  shift
done
echo "conversation=44444444-4444-4444-4444-444444444444" > "$log"
echo "HTTP 429 means Too Many Requests; this is a normal model answer."
`
	if err := os.WriteFile(script, []byte(body), 0755); err != nil {
		t.Fatal(err)
	}
	e := New(script, time.Second)
	r, err := e.ExecuteRequest(context.Background(), Request{ID: "normal-429", Worker: "root", Prompt: "Explain HTTP 429", OutDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if r.Error != nil {
		t.Fatalf("successful model output misclassified as provider error: %+v", r.Error)
	}
	if !strings.Contains(r.Output, "HTTP 429") {
		t.Fatalf("output=%q", r.Output)
	}
}

func TestArtifactDestinationIsolatedByRequestAndIndex(t *testing.T) {
	base := t.TempDir()
	a := artifactDestination(base, "req-a", 0, "/somewhere/image.png")
	b := artifactDestination(base, "req-b", 0, "/somewhere/image.png")
	c := artifactDestination(base, "req-a", 1, "/elsewhere/image.png")
	if a == b || a == c || b == c {
		t.Fatalf("artifact destinations collide: %q %q %q", a, b, c)
	}
	if filepath.Dir(a) == filepath.Dir(b) {
		t.Fatalf("different requests share artifact directory: %q %q", a, b)
	}
	if filepath.Base(a) != "00_image.png" || filepath.Base(c) != "01_image.png" {
		t.Fatalf("unexpected artifact names: %q %q", a, c)
	}
}

func TestSafeRequestIDDoesNotEscapeArtifactDirectory(t *testing.T) {
	got := safeRequestID("../../bad/request")
	if strings.Contains(got, "/") || strings.Contains(got, "..") {
		t.Fatalf("unsafe request id %q", got)
	}
}

func TestReadFromReaderContextCanBeCancelled(t *testing.T) {
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := readFromReaderContext(ctx, r)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v want deadline exceeded", err)
	}
}

func TestPrepareFilesRejectsFIFOWithoutBlocking(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "input.fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := (&Executor{}).prepareFiles(ctx, "root", []string{fifo})
	if err == nil || !strings.Contains(err.Error(), "only regular files") {
		t.Fatalf("err=%v, want regular-file rejection", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("special attachment rejection took %s", elapsed)
	}
}

func TestCopyArtifactsParallelRequestsDoNotOverwrite(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("real artifact copy integration requires root/sudo")
	}
	home := t.TempDir()
	outDir := t.TempDir()
	convA := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	convB := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	for conv, content := range map[string]string{convA: "A", convB: "B"} {
		d := filepath.Join(home, ".gemini", "antigravity-cli", "brain", conv)
		if err := os.MkdirAll(d, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "image.png"), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	e := New("/bin/true", time.Second)
	e.workerHomeFn = func(string) string { return home }
	type result struct {
		out string
		err error
	}
	ch := make(chan result, 2)
	for _, tc := range []struct{ id, conv string }{{"req-a", convA}, {"req-b", convB}} {
		tc := tc
		go func() {
			out := ""
			err := e.copyArtifacts(context.Background(), tc.id, "root", tc.conv, &out, outDir)
			ch <- result{out: out, err: err}
		}()
	}
	for range 2 {
		r := <-ch
		if r.err != nil {
			t.Fatal(r.err)
		}
	}
	a, err := os.ReadFile(filepath.Join(outDir, "req-a", "00_image.png"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(outDir, "req-b", "00_image.png"))
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != "A" || string(b) != "B" {
		t.Fatalf("artifact contents crossed: A=%q B=%q", a, b)
	}
}

func TestPrepareFilesMetadataRespectsContext(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "input.txt")
	if err := os.WriteFile(src, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	stat := filepath.Join(bin, "stat")
	if err := os.WriteFile(stat, []byte("#!/bin/sh\nexec /bin/sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := (&Executor{}).prepareFiles(ctx, "root", []string{src})
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v want deadline exceeded", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("metadata validation ignored deadline: elapsed=%s", elapsed)
	}
}

func TestPrepareFilesRejectsSymlinkToRegularFile(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.txt")
	if err := os.WriteFile(target, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "input-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	_, _, err := (&Executor{}).prepareFiles(context.Background(), "root", []string{link})
	if err == nil || !strings.Contains(err.Error(), "only regular files") {
		t.Fatalf("err=%v want symlink rejection", err)
	}
}

func TestExecuteRequestArtifactCopyHonorsRequestDeadline(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	sudo := filepath.Join(bin, "sudo")
	sudoBody := "#!/bin/sh\ncase \"$1\" in\n-H) shift 3; exec \"$@\";;\ncp) shift; /bin/sleep 0.8; exec /bin/cp \"$@\";;\n*) exec \"$@\";;\nesac\n"
	if err := os.WriteFile(sudo, []byte(sudoBody), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	const conv = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	brain := filepath.Join(dir, ".gemini", "antigravity-cli", "brain", conv)
	if err := os.MkdirAll(brain, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(brain, "image.png"), []byte("A"), 0600); err != nil {
		t.Fatal(err)
	}
	agy := filepath.Join(dir, "agy")
	agyBody := "#!/bin/sh\nprintf 'conversation=" + conv + "\\n' > \"$2\"\nprintf 'MODEL_OK\\n'\n"
	if err := os.WriteFile(agy, []byte(agyBody), 0700); err != nil {
		t.Fatal(err)
	}

	e := New(agy, 5*time.Second)
	e.workerHomeFn = func(string) string { return dir }
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	result, err := e.ExecuteRequest(ctx, Request{ID: "artifact-deadline", Worker: "root", Prompt: "test", OutDir: filepath.Join(dir, "out")})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ExecuteRequest err=%v", err)
	}
	if result == nil || result.Error != nil {
		t.Fatalf("result=%+v", result)
	}
	if !strings.Contains(result.Output, "MODEL_OK") {
		t.Fatalf("model output lost: %q", result.Output)
	}
	if !strings.Contains(result.Output, "artifact copy failed") {
		t.Fatalf("missing artifact warning after deadline: %q", result.Output)
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("parent err=%v", ctx.Err())
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("request deadline=100ms but returned after %s", elapsed)
	}
}

func TestExecuteRequestUnsupportedAttachmentIsInvalidRequest(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "input.fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	e := New("/bin/true", time.Second)
	result, err := e.ExecuteRequest(context.Background(), Request{ID: "invalid-input", Worker: "root", Prompt: "test", Files: []string{fifo}})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result == nil || result.Error == nil || result.Error.Type != ErrorInvalidAttachment {
		t.Fatalf("result=%+v want invalid attachment", result)
	}
}

func TestExecuteRequestLocalPreparationFailureIsNotProviderFailure(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "input.txt")
	if err := os.WriteFile(src, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "stat"), []byte("#!/bin/sh\nprintf 'regular file\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "cp"), []byte("#!/bin/sh\necho local-copy-failed >&2\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	e := New("/bin/true", time.Second)
	result, err := e.ExecuteRequest(context.Background(), Request{ID: "local-failure", Worker: "root", Prompt: "test", Files: []string{src}})
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if result == nil || result.Error == nil || result.Error.Type != ErrorLocalPreparation {
		t.Fatalf("result=%+v want local preparation failure", result)
	}
}

func TestPrepareFilesRealMetadataSyscallRespectsContext(t *testing.T) {
	f := os.Getenv("REAUDIT_STAT_FILE")
	if f == "" {
		t.Skip("requires syscall-delay harness")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	dir, _, err := (&Executor{}).prepareFiles(ctx, "root", []string{f})
	if dir != "" {
		defer os.RemoveAll(dir)
	}
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v want deadline exceeded", err)
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("metadata check exceeded 50ms deadline: elapsed=%s err=%v", elapsed, err)
	}
}

func TestPrepareFilesRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.txt")
	if err := os.WriteFile(target, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.txt")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	_, _, err := (&Executor{}).prepareFiles(context.Background(), "root", []string{link})
	if err == nil || !strings.Contains(err.Error(), "only regular files") {
		t.Fatalf("err=%v, want symlink rejection", err)
	}
}
