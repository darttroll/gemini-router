# Gemini Router — Multi-Account Gemini CLI Scheduler

**Quota-aware multi-account scheduler and load balancer for Google Gemini / Antigravity CLI (`agy`) on Linux.**

`gemini-router` is a Go CLI for running Gemini workloads across multiple independently authenticated Linux worker accounts without letting parallel scripts, automation, or AI agents fight over the same rate limits, concurrency slots, or long-window quotas.

It combines **cross-process scheduling**, **adaptive rate limiting**, **concurrency control**, **quota-aware worker selection**, **bounded retries**, **request cancellation**, **file attachments**, and **generated-artifact collection** behind a simple one-shot command:

```bash
sudo gemini-router -p "Review this deployment plan and identify the highest-risk failure modes"
```

> **Not an HTTP proxy.** Gemini Router does not expose an OpenAI-compatible API, does not implement the provider protocol, and does not bypass authentication or provider quotas. It schedules calls to the external [Google Antigravity CLI](https://github.com/google-antigravity/antigravity-cli) from short-lived local processes.

## Why Gemini Router

Many automation systems do not have one long-running client. They have cron jobs, CI tasks, autonomous agents, shell scripts, background workers, and interactive commands all starting independently.

If those processes invoke the same AI CLI directly, they can race each other:

- too many requests start at once;
- one worker gets overloaded while another is idle;
- short-term rate limits get confused with long-window quota exhaustion;
- retries amplify provider failures;
- cancellation leaves subprocesses behind;
- parallel requests overwrite logs or generated artifacts;
- every process independently refreshes the same quota state.

Gemini Router gives those independent processes a shared coordination layer without introducing a daemon or network service.

## Key capabilities

| Capability | What Gemini Router does |
| --- | --- |
| **Multi-account scheduling** | Routes requests across multiple authenticated Linux worker accounts. |
| **Cross-process coordination** | Independent CLI processes coordinate through one SQLite WAL database. |
| **Adaptive rate limiting** | Uses token-bucket pacing with learned request-rate limits and CUBIC-style recovery. |
| **Adaptive concurrency** | Learns safe per-worker/per-model concurrency from observed latency and failures. |
| **Quota-aware routing** | Tracks provider quota windows such as 5-hour and weekly Gemini quotas and temporarily excludes exhausted workers. |
| **Bounded queueing** | Applies queue depth and predicted-delay limits before admitting work. |
| **Retry isolation** | Uses persisted retry budgets so repeated failures do not create retry storms. |
| **Correct failure classification** | Invalid attachments and local preparation failures do not train the provider controller or reduce healthy worker capacity. |
| **Cancellation** | Propagates request deadlines through attachment handling, AGY execution, and artifact collection; terminates the request process group on cancellation. |
| **Attachments** | Safely copies regular files into request-owned temporary storage; FIFOs, devices, sockets, and symlinks are rejected. |
| **Generated artifacts** | Collects images and other AGY-generated files into request-specific output directories. |
| **Observability** | Reports queue depth, provider readiness, learned limits, latency, throttle observations, quota windows, and recent request history. |
| **No daemon required** | Each invocation is a normal CLI process; SQLite is the coordination boundary. |

## Typical use cases

Gemini Router is designed for Linux hosts where several local tools need reliable access to Gemini through Antigravity CLI, for example:

- autonomous AI agents and agent orchestrators;
- CI/CD and build automation;
- batch processing and research pipelines;
- shell scripts and cron jobs;
- parallel coding or review jobs;
- long-context requests with file attachments;
- image-generation workflows that need generated artifacts copied to a stable output directory.

If you are looking for a **Gemini CLI router**, **multi-account Gemini scheduler**, **LLM request load balancer**, or **quota-aware local AI worker pool**, this is the problem Gemini Router is intended to solve.

## Architecture

Every `gemini-router` invocation is short-lived. Multiple processes share scheduler state in SQLite and execute AGY under separate Linux users.

```text
script / CI job / AI agent
          |
          v
   gemini-router process
          |
          +---- queue admission / predicted wait
          |
          +---- SQLite WAL
          |       |
          |       +-- active leases
          |       +-- adaptive rate state
          |       +-- concurrency state
          |       +-- retry budgets
          |       +-- quota snapshots
          |
          +---- select worker + reserve capacity
          |
          v
 sudo -H -u <worker> agy --print ...
          |
          +---- request-owned log
          +---- generated artifacts
```

Worker selection and lease creation happen inside `BEGIN IMMEDIATE`, so independent router processes cannot reserve the same capacity concurrently.

For the full control flow and failure model, see [Architecture](docs/architecture.md).

## Requirements

- Linux
- Go **1.25.14 or newer** to build from source
- a local filesystem suitable for SQLite WAL
- root access for the supported deployment model
- one authenticated Antigravity CLI installation per enabled Linux worker

Do **not** place the live SQLite/WAL database on Dropbox, NFS, or another network/FUSE filesystem. Source code may live there; runtime scheduler state should not.

### Antigravity CLI compatibility

The pinned compatibility baseline is **AGY 1.2.8**. A real end-to-end Gemini request has been verified with that release.

Gemini Router relies on these AGY interfaces:

- `--log-file`
- `--model`
- `--print`
- `--output-format json`
- the read-only `/quota` command

AGY evolves independently, so newer releases should be revalidated before production deployment. The reproducible 1.2.8 installation below uses the official release assets and SHA-256 digests published by the upstream repository.

Official upstream:

- [Antigravity CLI repository](https://github.com/google-antigravity/antigravity-cli)
- [Antigravity CLI documentation](https://antigravity.google/docs/cli/overview/)
- [Antigravity CLI changelog](https://antigravity.google/changelog)

## Quick start

### 1. Install Gemini Router

Once the repository has a tagged public release:

```bash
go install github.com/darttroll/gemini-router/cmd/gemini-router@latest
sudo install -m 0755 "$(go env GOPATH)/bin/gemini-router" /usr/local/bin/gemini-router
```

Or build a checkout:

```bash
go build -trimpath -o /tmp/gemini-router ./cmd/gemini-router
sudo install -m 0755 /tmp/gemini-router /usr/local/bin/gemini-router
```

### 2. Create worker accounts

```bash
sudo useradd --create-home --shell /bin/bash gemini_a
sudo useradd --create-home --shell /bin/bash gemini_b
```

Each worker has its own AGY authentication state and provider quota.

### 3. Install and authenticate AGY

Install AGY for each worker, then authenticate once as that worker. A reproducible pinned installation is shown in [Installing AGY workers](#installing-agy-workers).

### 4. Configure the router

Copy the example configuration:

```bash
sudo mkdir -p /etc/gemini-router
sudo cp configs/config.example.yaml /etc/gemini-router/config.yaml
sudo chmod 0600 /etc/gemini-router/config.yaml
```

Minimal worker configuration:

```yaml
agy_path: "/usr/local/bin/agy"
data_dir: "/var/lib/gemini-router"

default_model: "gemini-3.6-flash-high"
request_timeout: 5m

workers:
  - username: gemini_a
    enabled: true
    agy_path: "/home/gemini_a/.local/bin/agy"
  - username: gemini_b
    enabled: true
    agy_path: "/home/gemini_b/.local/bin/agy"
```

### 5. Initialize runtime state

```bash
sudo gemini-router setup
```

`setup` creates or tightens private runtime directories, initializes SQLite, validates the candidate sudoers rule with `visudo`, and installs it atomically.

### 6. Run the first request

```bash
sudo gemini-router -p "Explain Linux namespaces in five concise bullets"
```

Inspect the scheduler:

```bash
sudo gemini-router status
sudo gemini-router health --verbose
sudo gemini-router quota
```

## Installing AGY workers

Gemini Router's supported deployment model is **root-managed**: the router itself runs as root and executes AGY through `sudo -H -u <worker>`. Authentication remains inside the worker account's home directory.

The following example installs the tested **AGY 1.2.8** release from official GitHub release assets and verifies its SHA-256 digest before installation:

```bash
for worker in gemini_a gemini_b; do
  sudo -H -u "$worker" bash <<'EOF'
set -euo pipefail

version=1.2.8

case "$(uname -m)" in
  x86_64)
    asset=agy_cli_linux_x64.tar.gz
    sha256=244752206d1f65c01aff489628f1df51f1a3fddacaa8ed74984661ebb6d09136
    ;;
  aarch64|arm64)
    asset=agy_cli_linux_arm64.tar.gz
    sha256=85ea71929436711e4b0332026508ee4e7ddb05192f24f1774dfbed879255fc6e
    ;;
  *)
    echo "unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

curl -fL --retry 3 \
  -o "$tmp/agy.tar.gz" \
  "https://github.com/google-antigravity/antigravity-cli/releases/download/$version/$asset"

echo "$sha256  $tmp/agy.tar.gz" | sha256sum -c -
tar -xzf "$tmp/agy.tar.gz" -C "$tmp"

install -d -m 0755 "$HOME/.local/bin"
install -m 0755 "$tmp/antigravity" "$HOME/.local/bin/agy"

"$HOME/.local/bin/agy" --version
EOF
done
```

Authenticate each worker:

```bash
sudo -H -u gemini_a /home/gemini_a/.local/bin/agy
sudo -H -u gemini_b /home/gemini_b/.local/bin/agy
```

On SSH, AGY prints an authorization URL when browser sign-in cannot happen locally.

If you intentionally upgrade AGY, verify the interfaces listed above and rerun the router integration tests before production deployment.

## Usage

### Normal request

```bash
sudo gemini-router -p "Review this incident report and propose a remediation plan"
```

### Select a model

```bash
sudo gemini-router --model gemini-3.6-flash-high -p "Summarize this design"
```

### Pipe stdin

```bash
cat README.md | sudo gemini-router -p "Review this document"
```

### Attach files

```bash
sudo gemini-router -p "Review these files" main.go config.yaml
```

Attachments must be regular files. Special files such as FIFOs, devices, sockets, and symlinks are rejected before AGY is launched.

### Collect generated images and artifacts

```bash
sudo gemini-router \
  --out-dir /var/lib/gemini-router/artifacts \
  -p "Generate a PNG architecture diagram for this system"
```

Generated artifacts discovered in the request's AGY workspace are copied into a request-specific directory under `--out-dir`.

### Retry strategies

`--retry-strategy` accepts:

- `auto` — normal bounded admission; retry eligible failures within the persisted retry budget.
- `failfast` — bounded admission, then fail immediately if no worker slot is available at dispatch time.
- `wait` — bypass queue delay/depth admission and wait for provider capacity until the request deadline.

`--queue-unbounded` bypasses queue delay/depth admission without disabling provider pacing, concurrency limits, quota breakers, retry budgets, or the overall request deadline.

Set `balancer.max_retries: 0` to disable retries.

## Configuration

The complete reference configuration is [`configs/config.example.yaml`](configs/config.example.yaml).

Important settings:

```yaml
data_dir: "/var/lib/gemini-router"
default_model: "gemini-3.6-flash-high"
request_timeout: 5m

balancer:
  max_retries: 3

queue:
  max_delay: 10s
  max_depth: 1000
  poll_interval: 100ms

adaptive:
  min_rate: 0.2
  initial_rate: 2.0
  max_rate: 100.0
  initial_concurrency: 4
  max_concurrency: 32

quota:
  enabled: true
  refresh_interval: 5m
  low_remaining_interval: 30s
  low_remaining_threshold: 0.10
  command_timeout: 10s

logging:
  prompt_preview: false
```

Configuration parsing is strict. Unknown YAML fields, duplicate workers, malformed worker names, invalid bounds, invalid retry strategies, and non-positive request timeouts are rejected before work starts.

## Status, health, quotas, and metrics

```bash
sudo gemini-router status
sudo gemini-router status --json
sudo gemini-router health --verbose
sudo gemini-router quota
sudo gemini-router quota --json
sudo gemini-router stats --period 24h
```

`status` reports global and per-model queue depth, predicted wait, active requests, provider readiness, effective capacity, load factor, learned rate/concurrency limits, recent latency and throttling observations, and quota windows.

Quota refresh is coordinated across processes so concurrent CLI invocations do not all issue the same read-only `/quota` request. Successful snapshots containing only disabled quota buckets are still cached for the configured refresh interval.

## Reliability model

A successful upstream model response is final: failure to update local scheduler bookkeeping does **not** cause the model request to be generated again.

Request deadlines propagate through:

1. attachment metadata validation;
2. attachment copies;
3. AGY execution;
4. artifact discovery and copies.

Attachment validation is performed before scheduler admission, so invalid local input does not consume a provider lease, retry budget, or adaptive-concurrency signal.

Queued requests are removed on cancellation or deadline. Lease release is idempotent.

Each AGY subprocess is placed in its own process group. Cancellation sends `SIGTERM`, then escalates surviving members to `SIGKILL` after a bounded grace period.

A process blocked in uninterruptible kernel I/O may remain until the kernel operation returns; the kill remains pending, but the router does not keep the scheduler lease indefinitely. Processes that deliberately create a new session or process group leave the router's PGID boundary.

Each request owns its AGY log and artifact directory, so concurrent requests using identical generated filenames cannot overwrite one another.

If the model response succeeds but the request deadline expires while artifacts are being collected, the model response remains final and is returned with an artifact-copy warning instead of triggering another model generation.

## Real-world validation

In addition to unit, integration, race, vulnerability, and secret-scanning gates, the current public candidate has been exercised against real authenticated AGY sessions.

The validation campaign included:

- concurrent independent router processes sharing one SQLite scheduler;
- mixed engineering and operations prompts;
- a roughly 55 KB long prompt;
- a roughly 220 KB file attachment;
- real image generation with PNG/JPEG artifact collection;
- predictive deadline rejection before provider execution;
- cancellation of an already-running real AGY request;
- post-run checks for leaked active/pending scheduler state.

The mixed concurrent burst completed all requested tasks without provider throttling or leaked scheduler entries. The image workflow produced real request-isolated artifacts. A separate end-to-end smoke request passed with the pinned AGY **1.2.8** release.

These observations are validation evidence, **not performance guarantees**. Provider latency and quota behavior vary by account, model, region, and upstream release.

## Security and privacy

Gemini Router is a privileged local scheduler, not a sandbox.

Runtime directories are private by default, SQLite/log files use restrictive permissions, prompt previews are disabled unless explicitly enabled, worker names are validated before reaching `sudo`, and sudoers changes are validated with `visudo` before atomic installation.

Read [SECURITY.md](SECURITY.md) before deploying on a multi-user host.

Use only accounts you are authorized to operate and comply with the terms and quota policies of the upstream provider. Gemini Router coordinates independently authenticated workers; it does not circumvent provider authentication or quota enforcement.

## Limitations

- Linux only.
- The supported deployment model is root-managed.
- No daemon, HTTP API, or OpenAI-compatible endpoint is provided.
- AGY owns provider authentication, network transport, model execution, and upstream retries.
- SQLite is the cross-process coordination boundary and must live on suitable local storage.
- Exactly-once upstream execution cannot be guaranteed across every possible OS crash boundary.
- Compatibility with future AGY releases is not assumed automatically.

## Development

```bash
go mod tidy -diff
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
go build -trimpath -o dist/gemini-router ./cmd/gemini-router
```

CI also runs static analysis, vulnerability scanning, secret scanning, and privileged Linux integration tests.

## Documentation

- [Architecture](docs/architecture.md)
- [Deployment and rollback](docs/deployment.md)
- [Security policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)

## License

MIT. See [LICENSE](LICENSE).
