# Contributing

## Development environment

Requirements:

- Linux;
- Go 1.25.14 or newer;
- SQLite support provided by the pure-Go `modernc.org/sqlite` dependency.

Normal unit tests do not need real provider credentials.

## Required checks

Before submitting a change:

```bash
files=$(find . -type f -name '*.go' -not -path './.git/*' -print)
test -z "$(gofmt -l $files)"
go mod tidy -diff
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
go build -trimpath -o /tmp/gemini-router ./cmd/gemini-router
```

CI also runs `govulncheck` and a secret scan.

## Privileged integration tests

A small set of Linux process-isolation tests requires root because production
execution uses `sudo` and Unix process groups. Those tests use only temporary
fake AGY binaries and temporary users/state; they must never use production
worker accounts.

The privileged CI job compiles the relevant test binaries as an unprivileged
runner and then executes only the explicitly selected tests with `sudo`.

## Design rules

Changes should preserve these boundaries:

- request-rate, concurrency, long-quota, and retry control are independent;
- cross-process reservations are atomic in SQLite;
- a successful upstream result must not be generated again solely because
  local bookkeeping failed;
- observability failures must not corrupt a successful model result;
- request-owned logs and artifacts must not be inferred from shared
  "latest" files;
- bounded admission must keep the global queue ceiling atomic;
- model-specific traffic must not inflate another model's wait prediction.

Add a regression test for every repaired failure mode.
