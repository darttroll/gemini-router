# Architecture

## Scope

`gemini-router` is a one-shot Linux CLI scheduler for the Antigravity CLI
(`agy`). It is not an HTTP proxy and it does not implement provider
protocols. Every invocation is a short-lived client process. Independent
clients coordinate through a shared SQLite database.

```text
client process
    |
    v
gemini-router
    |
    +-- admission / FIFO queue -----------+
    |                                     |
    +-- SQLite WAL <-> controller state   |
    |                                     |
    +-- worker lease ---------------------+
    |
    v
sudo -u <worker> agy --print ...
```

## Request identity

Each request receives a UUID that follows the request through:

- the pending queue;
- the active lease;
- the request-owned AGY log;
- structured router logging;
- the request-specific artifact directory.

The router never discovers request metadata by reading the newest
account-global AGY log.

## Admission and queueing

Two queue scopes intentionally coexist:

- **total queue depth** is the global safety ceiling;
- **model queue depth** is used to predict wait for a request of that model.

For bounded requests, the final global depth check and the queue insertion are
performed in one `BEGIN IMMEDIATE` transaction. Concurrent router processes
therefore cannot all observe the same free final slot and overfill the hard
limit.

Predicted wait uses recent execution latency plus learned rate/concurrency
capacity for providers that are currently ready for the selected model.

`--queue-unbounded` bypasses only queue depth/delay admission. It does not
bypass request deadlines, provider pacing, concurrency limits, quota breakers,
or retry budgets. Retry strategy `wait` uses the same unbounded admission
semantics.

## Worker acquisition

Only the FIFO head for a model can reserve capacity. The actual reservation is
rechecked inside `BEGIN IMMEDIATE`.

For each enabled worker the scheduler evaluates:

1. controller breaker state;
2. cached provider quota for the model group;
3. active requests versus learned concurrency;
4. start-rate token availability;
5. recent execution latency and normalized occupied capacity;
6. least-recently-used ordering as the final tie-breaker.

The selected worker's start token, the new active lease, and removal from the
pending queue are committed atomically.

## Adaptive control

Controller state is persisted per worker/model.

### Short request rate

A CUBIC-style controller reacts to short throttling independently of long
quota. A throttle backs off the learned request rate and drains start-burst
tokens. Recovery advances only after observed successful requests; idle wall
clock time does not manufacture evidence of recovery.

### Start pacing

A token bucket smooths process launches while allowing a small configured
burst.

### Concurrency

A latency-gradient controller maintains a separate in-flight limit. Transient
process/upstream failures can reduce concurrency without poisoning the learned
short request rate.

### Long quota

Read-only `agy -p "/quota" --output-format json` calls populate cached
5-hour/weekly quota buckets. An exhausted enabled bucket removes that worker
from scheduling until its real reset time. Disabled buckets do not block admission or dispatch. A successful quota snapshot whose buckets are all disabled is still cached for the normal refresh TTL; disabled rows do not extend or shorten the freshness of active buckets.

Quota refresh itself uses a cross-process SQLite lease so concurrent router invocations do not issue duplicate refreshes.

### Retries

Retry capacity is a separate persisted token budget. Eligible transient
failures use exponential full-jitter backoff. Long quota and short throttling
are handled primarily by their own pacing/breaker mechanisms.

## Completion and lease release

Lease release is idempotent. The active row is deleted before controller
training within the same transaction. Repeating release after a completed
commit sees no active row and therefore cannot train the controller twice.

If a model call already produced a successful result, failure to persist local
release/bookkeeping is recorded diagnostically but does **not** trigger another
model generation.

A SQLite `COMMIT` failure is followed by a best-effort rollback before the
connection is returned. This prevents an accidentally active transaction from
leaking back into the connection pool.

## Process lifecycle

The model call and filesystem helper subprocesses run in their own Unix process groups. Request cancellation or deadline expiry sends `SIGTERM`, escalates to `SIGKILL`, and uses a bounded reaping grace rather than holding the request lease indefinitely.

Stdin reading, attachment metadata validation, attachment copying, and artifact collection inherit the same request cancellation context. Artifact collection that is interrupted after a successful model answer is reported as a warning; it does not convert the completed model call into a retry.

A helper stuck in uninterruptible kernel I/O can outlive the request return until the kernel operation completes. In that case the kill is already pending and `cmd.Wait` continues only in its reaper goroutine. The router therefore bounds its own wait, not the kernel's completion time.

Health, quota, artifact-copy, and other helper commands use bounded contexts rather than unbounded `exec.Command` calls.

## Artifact isolation

Input files are copied to a request-owned temporary directory with collision-
safe names. Generated files are copied to:

```text
<out-dir>/<request-id>/<index>_<basename>
```

so parallel requests producing the same basename do not overwrite each other.

## Persistent state

The SQLite schema contains:

- `pending_requests` — queued work;
- `active_requests` — worker leases;
- `controller_state` — adaptive feedback per worker/model;
- `request_log` — operational history;
- `quota_snapshots` — provider quota cache;
- `quota_refresh` — refresh leases.

Old schema columns/tables may remain for migration compatibility even after
legacy scheduling code is removed. Runtime code should have only one active
scheduling path.

## Failure model

The router distinguishes at least:

- short throttle;
- long quota exhaustion;
- transient upstream/process failure;
- authentication/eligibility failure;
- invalid request/model;
- local preparation failure before the provider call;
- timeout;
- client cancellation;
- router overload.

These classes intentionally have different effects on controller state.

The system cannot guarantee exactly-once provider execution across every OS or
hardware crash boundary. Its guarantee is narrower: known local bookkeeping
failures after a successful model call do not cause an intentional retry.
