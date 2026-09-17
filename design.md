# lua-resty-adaptive-limit — Design

Adaptive concurrency limiting and load shedding for OpenResty. The allowed
number of concurrent requests against a protected capacity pool adjusts
automatically from observed completion latency and explicit overload signals,
so excess load is shed *early* instead of piling onto a saturating upstream.

Design principles, in priority order: correctness; predictable behavior under
failure; extremely low request-path overhead; controller stability; bounded
CPU and memory; multi-worker correctness; operational visibility; API
simplicity. No claim in this document is unfalsifiable: every property is
either trivially inspectable in the code or backed by a test, a simulation,
or a benchmark in this repository.

## 1. The two planes

### Fast path — per request, O(1), allocation-free

Runs in `access_by_lua` / `log_by_lua` (or the low-level API from any Lua
phase). Responsibilities: read the current limit, atomically take a
concurrency slot, admit or reject, release on completion, accumulate one
worker-local observation. It never performs: network/DNS/file I/O, Redis,
JSON, regex, `string.format`, controller math, histogram merges, timer
creation, `resty.lock`, per-request logging, dynamic key construction, or
configuration parsing.

Admission cost budget (shared-memory ops): 1 read of `limit` + 1 atomic
`incr` of `inflight`; a rejected request pays one extra `incr` to roll the
reservation back. A released request pays 1 atomic `incr` (decrement). The
observation update touches only a fixed-size worker-local Lua structure.

### Control path — timer driven, never per request

One `ngx.timer.every` per worker (the *scheduler*) services **all** limiter
instances: flush worker-local aggregates into shared per-window accumulators,
refresh worker heartbeats, and — for at most one worker per limiter per
window — run the controller update. No per-request timers are ever created;
no per-limiter timers beyond the one scheduler.

## 2. Public API

```lua
local adaptive = require "resty.adaptive_limit"

local limiter, err = adaptive.new({
    name         = "payments",        -- required
    shared_dict  = "adaptive_limit",  -- required (lua_shared_dict zone name)
    algorithm    = "gradient2",       -- "gradient2" (default) | "aimd"
    -- ... options below ...
})

-- Low-level API (maximum performance; no ngx.ctx involvement):
local ok, err = limiter:try_acquire()
--   true                 admitted
--   nil, "rejected"      concurrency limit reached (caller picks the response)
--   nil, <internal err>  limiter failure ("internal_error", "not_started",
--                        "invalid_state"); governed by failure_mode
local ok, err = limiter:release(latency, outcome)
--   latency: seconds (number) or nil when the outcome carries no sample
--   outcome: "success" | "timeout" | "connect_error" | "overload" |
--            "error" | "aborted" | "ignored"  (default "success")

-- Request-lifecycle convenience API:
local ok, err = limiter:access()          -- call in access_by_lua
local ok, err = limiter:log()             -- call in log_by_lua; idempotent

-- Convenience rejection helper (outside the core; optional):
limiter:enforce(err)                      -- sets Retry-After, ngx.exit(status)

-- Observability (not on the hot path):
local state = limiter:state()

-- Module-level lifecycle (call once per worker from init_worker_by_lua):
local ok, err = adaptive.start({ flush_interval = 0.2 })
local ok, err = adaptive.stop()           -- test/shutdown use

-- Errors are stable string constants, interned and identity-comparable:
adaptive.errors.REJECTED        == "rejected"
adaptive.errors.INTERNAL_ERROR  == "internal_error"
adaptive.errors.NOT_STARTED     == "not_started"
adaptive.errors.INVALID_STATE   == "invalid_state"
```

`err == "rejected"` (string compare) and `err == adaptive.errors.REJECTED`
are the same comparison; errors are plain interned strings so no error
object is ever allocated per rejection.

### `access()` / `log()` contract

- `access()` stores a single scalar under a **precomputed** `ngx.ctx` key
  (`"alim:" .. name`); no per-request table is allocated by the library.
- `log()` order of operations (telemetry can never block accounting):
  1. validate the request was admitted by *this* limiter (ctx flag)
  2. release the concurrency slot (shared decrement + worker-local mirror)
  3. clear the admission flag, set the released flag
  4. record the latency/outcome observation into worker-local stats
  5. optional hooks (`on_anomaly`) — pcall-protected
- `log()` is idempotent: a second call sees the released flag and returns
  without touching counters. `log()` without a matching `access()` is a
  no-op (supports mixed low-level usage in the same location).
- Internal redirects: `ngx.ctx` survives error_page-style redirects, so a
  second `access()` for the same limiter in the same request returns
  `true` **without acquiring a second slot**. `ngx.exec` targets and
  subrequests are guarded separately (`ngx.req.is_internal()`): their ctx
  is fresh and their log phase never runs / runs only at the chain's end,
  so they bypass admission by default; `allow_internal = true` re-enables
  admission for limiters that live on exec-fronted locations. A request
  is admitted at most once per limiter unless the caller clears the ctx
  flag deliberately.
- **The location that admits must run the log phase**: in `error_page` internal
  redirects, nginx resets `ngx.ctx` and executes only the fallback location's log
  phase. To avoid leaking the admitted slot, the fallback location must explicitly
  call `limiter:release(nil, outcome)` or the error must be handled in the admitting
  location without redirect.
- `access({ bypass = true })` skips admission entirely (health checks,
  streaming endpoints documented in §12).

## 3. Configuration reference

Validation happens once, in `new()`. Invalid configuration aborts startup
with a descriptive error — never silently accepted.

| Option | Type | Default | Notes |
|---|---|---|---|
| `name` | string | required | `[a-z][a-z0-9_.-]{0,62}`; becomes part of precomputed dict keys |
| `shared_dict` | string | required | name of a dedicated `lua_shared_dict` zone |
| `algorithm` | string | `"gradient2"` | `"gradient2"` or `"aimd"` |
| `profile` | string | `"balanced"` | `"conservative"`, `"balanced"`, `"responsive"`; explicit options override profile values |
| `initial_limit` | number | 50 | cold-start limit; `min_limit <= initial_limit <= max_limit` |
| `min_limit` | number | 1 | `>= 1` |
| `max_limit` | number | 2000 | `>= min_limit` |
| `sample_window` | number | 1.0 | controller window seconds; `> 0` |
| `aggregation_grace` | number | 0.35 | seconds after window close before it is processed |
| `min_samples` | number | 20 | below this many completions in a window: hold limit |
| `failure_mode` | string | `"fail_open"` | or `"fail_closed"`, see §9 |
| `rtt_tolerance` | number | 2.0 | tolerated short/long RTT ratio before shedding (gradient2) |
| `min_gradient` | number | 0.5 | lower clamp of the gradient (gradient2) |
| `smoothing` | number | 0.5 | limit EWMA factor, `(0, 1]` |
| `headroom_min` / `headroom_max` | number | 1 / 50 | bounds on the `sqrt(limit)` headroom |
| `baseline_alpha` | number | 0.05 | long-RTT (baseline) EWMA speed, `(0, 1)` |
| `sample_alpha` | number | 0.5 | short-RTT EWMA speed, `(0, 1]` |
| `overload.min_samples` | number | 20 | minimum completions before backoff may trigger |
| `overload.failure_ratio` | number | 0.10 | strong-signal ratio that triggers backoff |
| `overload.backoff` | number | 0.80 | multiplicative backoff, `(0, 1)` |
| `latency_source` | string | `"request_time"` | `"request_time"` \| `"upstream_response_time"` \| `"manual"` |
| `upstream_time_choice` | string | `"last"` | when retries produced several upstream timings: `"last"` \| `"max"` \| `"sum"` |
| `rejection_status` | number | 503 | used only by `enforce()` |
| `retry_after` | string/number | 1 | used only by `enforce()` |
| `stale_threshold` | number | 30 | seconds without completions before `controller_stalled` is reported |
| `on_update` | function | nil | `function(snapshot)` after each controller publication (held windows fire nothing) |
| `allow_internal` | boolean | false | admit on internal requests (exec-fronted locations; see §2) |
| `on_anomaly` | function | nil | `function(kind, detail)` for counter anomalies / internal errors |

Profiles (tuned via simulation, see `spec/simulation_spec.lua`):
`conservative` = lower `rtt_tolerance`, stronger smoothing, earlier backoff;
`responsive` = the opposite. Explicit options always override profiles; no
undocumented values exist — profile expansions are listed in the README.

## 4. Shared state model

A dedicated `lua_shared_dict` is required and the module never writes keys
outside its namespace. All key strings are interned Lua strings built once
in `new()`; the request path performs no concatenation.

Namespace prefix: `al:<schema_version>:<name>:` (currently `al:1:`).
One dict-global schema marker, `adaptive_limit:schema`, lives *outside*
the versioned namespaces: a future library version would otherwise write
under `al:2:` while old workers kept `al:1:` and neither would ever
detect the other (split-brain counters). With the marker, an
incompatible shared state is a loud startup error.

| Key | Type | Writer | Lifetime |
|---|---|---|---|
| `adaptive_limit:schema` | `"1"` | init_worker (first worker) | permanent |
| `...:limit` | number (integer) | controller lease holder | permanent |
| `...:inflight` | number | admission (incr), release (incr), exit_worker | permanent |
| `...:long_rtt` / `...:short_rtt` / `...:gradient` | number | controller | permanent |
| `...:last_window` | number | controller | permanent |
| `...:last_update` | number | controller | permanent |
| `...:w:<n>:c` / `:s` | number | worker flush (atomic incr) | exptime + explicit delete |
| `...:w:<n>:ovl` / `:tmo` / `:cer` / `:err` / `:abt` / `:rej` | number | worker flush (atomic incr) | exptime + explicit delete |
| `...:lease:<n>` | string `"<pid>:<seq>"` | lease contender (add) | TTL (`lease_ttl`) |
| `...:hb:<worker_id>` | number (timestamp) | scheduler heartbeat | TTL 5s |

Invariants of the shared model:

- Window accumulator keys exist only for windows younger than
  `window + aggregation_grace + lease_ttl + 10s`; they carry an `exptime`
  so nothing can leak even if a controller never runs.
- Worker heartbeat slots are indexed by `ngx.worker.id()`, bounded by
  `ngx.worker.count()`; an expired heartbeat identifies a lost worker.
- The schema marker identifies the shared state; a version mismatch at
  `init_worker` reinitializes state with a WARN rather than interpreting
  foreign data.
- No JSON, no blobs, no Lua-serialized tables: every shared value is a
  number or short string, parseable by any tool.

## 5. Admission algorithm

```lua
-- The shared-dict increment is the admission linearization point.
-- Every successful incr returns a distinct resulting inflight value, so
-- concurrent admissions can never observe the same slot. A result above
-- the current limit is rolled back immediately and the request is rejected.
local limit = dict:get(limit_key)          -- (1)
if limit == nil then
    -- Critical state missing (eviction/resize): re-seed from the worker's
    -- last observed limit, count an anomaly, rate-limited WARN.
    limit = reseed_limit()
end
local n = dict:incr(inflight_key, 1, 0)    -- (2) linearization point
if not n then return internal_error() end
if n <= limit then
    return true                            -- admitted
end
dict:incr(inflight_key, -1)                -- (3) rollback
return nil, REJECTED
```

- The classic `get`-then-`set` race cannot exist: the counter moves only by
  atomic `incr`, and each admission consumes exactly one increment. For a
  **stable** limit `L`, more than `L` requests cannot pass admission
  simultaneously (proven by `t/admission.t` under 1/2/4 workers with
  thousands of repetitions).
- A rejected request can never permanently raise `inflight`: the rollback
  decrement is unconditional; if the decrement ever returns a negative
  value the counter is snapped to 0 and a counter anomaly is counted (this
  surfaces lost admissions rather than hiding them).

### Semantics while the limit changes

The limit is read *before* the increment, so admission decisions use a limit
value that may be up to one increment-old. Consequences (documented, tested,
bounded):

- *Increase*: a request may be rejected using the stale lower limit for at
  most the instant before the next read; no over-admission is possible.
- *Decrease*: requests admitted just before publication used the old, higher
  limit, so `inflight` may transiently exceed the new limit by the number of
  admissions that crossed the publication point (bounded by live worker
  concurrency). No running request is ever terminated; new admissions are
  rejected until `inflight <= new_limit`. Steady-state behavior after the
  transition satisfies the stable-limit invariant.

## 6. Request accounting and worker-local bookkeeping

Each limiter keeps, in plain module-local Lua state per worker:

- `local_inflight` — number of slots this worker holds (mirrors its share of
  the shared counter). The worker VM is cooperative and single-threaded per
  Lua state; the increment/decrement pairs contain no yield points, so the
  mirror is exact without synchronization.
- Fixed-size stat struct (no arrays, no per-request tables):
  `sample_count, latency_sum, success, timeout, connect_error, overload,
  error, aborted, rejected, admitted_total, rejected_total, last_completion`
- Rate-limited log state (max one message per kind per second per worker).
- Anomaly counters: `internal_errors, counter_anomalies, timer_failures,
  dict_read_failures, workers_lost, controller_skips`.

## 7. Statistics pipeline

```text
request completes (log phase / release)
   ↓  (fast, non-yielding, fixed-size)
worker-local stat struct
   ↓  every flush_interval (scheduler tick)
shared per-window accumulators w:<n>:*  — atomic incr, race-free by design
   ↓  window N-1 closed + aggregation_grace elapsed
controller lease holder aggregates window N-1, computes, publishes
```

- Flushes are partial aggregates: `incr` on the window's accumulator keys.
  Atomic increments mean worker flushes never need locks and never lose
  updates, regardless of worker count.
- The controller reads a window only after it is closed *and* the grace
  period passed, so straggler flushes are accounted for. After processing,
  the accumulator keys are deleted explicitly (exptime is the backstop).
- Windows with `sample_count < min_samples` (or zero samples) **hold** the
  current limit. Low-traffic services do not oscillate; idle periods change
  nothing (no manufactured samples, no baseline reset, no drift toward
  min/max). `last_window` advances so the backlog is not reprocessed later.
- After a long pause (e.g. laptop sleep, SIGSTOP), the controller processes
  at most the two most recent closed windows and skips older ones
  (`controller_skips` counter), so a timer backlog cannot cause a burst of
  stale decisions.

## 8. Controller

### Interface (pure, ngx-free, deterministic)

```lua
local g2 = require "resty.adaptive_limit.controller.gradient2"
local state = { limit = 100.0, long_rtt = 0.020, short_rtt = 0.020 }
local m = { sample_count = 900, mean_rtt = 0.024,
            overload_count = 2, timeout_count = 0, error_count = 1,
            aborted_count = 0 }
local next_state, err = g2:update(state, m, config)
```

Algorithms never touch `ngx`, shared dicts, or I/O; all inputs are
validated numbers. They are unit-tested with hand-computed values and fuzzed
for the §54 properties (finite, positive, bounded, no movement on empty
input, corrupted input cannot corrupt state).

### Gradient2 (default)

Given window measurement `m` and current state:

1. If `m.sample_count < min_samples` → hold (no update at all).
2. `short_rtt' = sample_alpha * m.mean_rtt + (1 - sample_alpha) * short_rtt`
3. Baseline update is **frozen during overload episodes**: if the
   strong-signal ratio `(overload + timeout + connect_error) /
   sample_count > overload.failure_ratio`, `long_rtt` is not updated
   this window (an overload must not be normalized into the baseline;
   application errors such as HTTP 500 are *not* strong signals and do
   not freeze it). Otherwise
   `long_rtt' = baseline_alpha * short_rtt' + (1 - baseline_alpha) * long_rtt`.
4. `gradient = clamp(rtt_tolerance * long_rtt' / short_rtt', min_gradient, 1.0)`
   (guard: `short_rtt' <= 0` → hold).
5. `headroom = clamp(sqrt(limit), headroom_min, headroom_max)` — bounded
   upward pressure so a healthy backend can keep growing; growth is
   ~`smoothing * sqrt(limit)` per window, never a jump.
6. `candidate = limit * gradient + headroom`
7. Overload backoff: if `sample_count >= overload.min_samples` and the
   strong-signal ratio exceeds `overload.failure_ratio`:
   `candidate = min(candidate, limit * overload.backoff)`.
8. `limit' = clamp(limit * (1 - smoothing) + candidate * smoothing,
   min_limit, max_limit)`
9. Publish `floor(limit')` (integer at the publication boundary; the float
   state is kept internally for smooth EWMA).
10. Any non-finite intermediate (NaN/inf from corrupted inputs) → hold the
    previous valid limit, count `internal_errors`, rate-limited error log.
    NaN/inf are never published.

### AIMD (reference)

Healthy window (strong-signal ratio ≤ threshold **and**
`mean_rtt <= rtt_tolerance * long_rtt`): `limit += additive_increment`
(default 1). Congested window: `limit *= multiplicative_decrease`
(default 0.8). Same sample/baseline handling, clamps, publish and safety
rules as Gradient2.

### Publication

The lease holder writes `limit`, `long_rtt`, `short_rtt`, `gradient`,
`last_window`, `last_update` (6 `set` ops) and deletes the processed window
keys. Controller work per limiter per window is ~10–20 shared-dict
operations, executed by at most one worker, once per `sample_window`.

## 9. Failure model

### Distinct error classes

- `rejected` — the limiter works, the pool is full. Not a failure.
- `internal_error` — shared dict failures (missing zone, no memory,
  unexpected errors), invalid state. Governed by `failure_mode`:
  `fail_open` (default) admits the request *without holding a slot* (so
  there is nothing to release; `log()` still records the observation) —
  availability over protection; `fail_closed` rejects. The limiter never
  crashes a request because of its own errors.
- `not_started` — `adaptive.start()` was not called (or `new()` was never
  completed). Returned as an error, never masked by `fail_open`.
- `invalid_state` — shared state failed validation (schema mismatch,
  non-numeric limit). Counted, rate-limited logged, state re-seeded from
  configuration.

### Worker crash

- **Graceful exit / HUP reload drain**: `exit_worker_by_lua` reconciles the
  worker's `local_inflight` into the shared counter (subtract-only, floored
  at 0, anomaly counted if non-zero). The exact ordering of `exit_worker`
  versus late `log_by_lua` calls was verified empirically (see
  `t/resilience.t` and `benchmark/resilience.sh`); reconciliation is coded to be safe under both orderings.
- **Abrupt death (SIGKILL / segfault)**: nothing runs; the victim's slots
  leak — the shared counter stays high, permanently reducing effective
  capacity until an operator acts. This is stated honestly rather than
  papered over:
  - the worker's heartbeat key expires → `workers_lost` counter;
  - `inflight >= limit` combined with no completions for
    `stale_threshold` seconds → `controller_stalled` in `state()`;
  - no automatic reset ever runs (an unsynchronized "repair" while live
    requests modify the counter would corrupt it). The README documents the
    operator procedure: quiesce (or accept the shedding), then reset
    `inflight` from an admin snippet. A lease/quota-based backend that
    recovers automatically is a **separate, optional** backend and is not
    part of the default shared-counter path.

### Graceful reload

The shared dictionary survives HUP. New workers validate the schema marker
and adopt the learned `limit`/RTT state — a healthy limit is never
deliberately discarded by a reload. Old workers drain and reconcile via
`exit_worker`. `t/resilience.t` and `benchmark/resilience.sh` hammer this with continuous traffic: no
permanent lockout, no negative counters, no runaway admission, no schema
corruption.

**Reload-overlap note:** during a graceful reload where code changes the
measurement schema (such as the addition of `completions`), draining old
workers do not write the new field. A mixed transition window may fail invariant
validation and hold the previous limit once (failing safe); subsequent windows
self-heal immediately as old workers finish draining.

## 10. Latency and outcome classification

- `request_time` (default): `ngx.now() - ngx.req.start_time()` measured in
  the log phase. Includes client-body download / response streaming —
  documented trade-off; appropriate when the protected resource is the
  whole request.
- `upstream_response_time`: parsed from `ngx.var.upstream_response_time` in
  the log phase. With upstream retries the variable holds multiple values
  (`"0.005, 0.010"` or `"0.005 , 0.010"`); the parser splits on
  commas/spaces, validates each token as a finite non-negative number, and
  applies `upstream_time_choice` (`last` = final attempt, default; `max`;
  `sum`). Garbage/empty values yield no sample — never a guessed number.
- `manual`: the low-level `release(latency, outcome)` is authoritative.
- Outcomes (default classifier, overridable via `outcome_classifier`):
  `499` → `aborted` (slot always released; not an overload signal; latency
  not sampled — it is a truncation artifact), `503` → `overload`,
  `504` → `timeout`, `502` → `connect_error`, other `5xx` → `error`
  (counted, not a strong overload signal), `2xx/3xx/4xx` → `success`.
  HTTP 400-class errors are *not* capacity signals.

## 11. Scheduler and leadership

- `adaptive.start()` creates exactly one `ngx.timer.every(flush_interval)`
  per worker, shared by all limiters (registered automatically by `new()`).
  The callback body is fully pcall-wrapped: the timer cannot die from a
  limiter error; per-tick errors are counted and logged rate-limited.
- Controller leadership per window: `dict:add(lease:<n>, "<pid>:<seq>",
  lease_ttl)`. The winner runs window `n`'s update; losers do nothing.
  Leases are keyed per window and **never released explicitly** (TTL
  expiry only) — the successor's lease for window `n+1` can therefore never
  be disturbed. `last_window` makes the update idempotent: if leadership
  changes mid-window, the new holder sees `last_window >= n` and skips.
- No `resty.lock`, no network, no yielding anywhere in the scheduler.

## 12. Scope and documented limitations

- Node-local: each OpenResty instance learns its own limit. Four instances
  × 100 ⇒ ≈400 cluster capacity — an emergent approximation, not a global
  semaphore.
- One limiter = one named capacity pool; no tenant fairness inside the
  pool (use separate limiters).
- Default lifecycle mode assumes request-scoped work: WebSocket/SSE/gRPC
  streaming/long uploads bypass the limiter (`access({bypass=true})`) and
  manage their own admission, or define an explicit completion signal via
  the low-level API.
- Completion-based observation sees no samples when all in-flight requests
  hang; the limiter keeps shedding at the limit (backpressure holds) and
  surfaces `controller_stalled` for alerting.
- Abrupt worker death leaks that worker's slots (§9) — surfaced, not hidden.
- Shared-dict sizing guidance (README): ≈50 small entries per limiter
  plus ≤ 3 live windows × 7 accumulator keys and `worker_count` heartbeat
  slots; a 10m zone comfortably serves dozens of limiters. `state()`
  exposes `dict_capacity`/`dict_free` for monitoring.

## 13. Invariants (and where they are enforced)

| # | Invariant | Enforced by |
|---|---|---|
| 1 | Stable limit L ⇒ simultaneous admissions ≤ L | atomic incr linearization (§5); `t/admission.t`, `t/multi_worker.t` |
| 2 | Every admitted request releases exactly once | ctx released-flag; rollback on reject; `t/lifecycle.t` |
| 3 | A rejected request cannot permanently raise inflight | unconditional rollback; negative-snap anomaly; `t/admission.t` |
| 4 | `min_limit <= published limit <= max_limit` | clamp at publication; fuzz spec |
| 5 | Insufficient samples ⇒ no limit movement | hold rule in both controllers; `t/controller.t`, simulations F/G |
| 6 | Controller state never NaN/inf | input/output validation; hold-on-invalid; fuzz spec |
| 7 | No request-path external I/O or yielding | construction (only dict get/incr + fixed-size math); code review §76 |
| 8 | Memory does not grow with total requests | fixed-size structs; TTL'd window keys; soak test |
| 9 | Controller failure never silently maximizes the limit | hold-on-invalid + clamp; internal_errors counter |
| 10 | Observability failures never prevent release | release-before-observe ordering (§2); pcall-protected extras |

## 14. Verification matrix

| Concern | Method |
|---|---|
| Algorithm math (hand-computed values, edge inputs) | busted `spec/*_spec.lua` |
| §54 properties under generated inputs | busted fuzz specs (seeded PRNG) |
| §53 scenarios A–H | deterministic simulations (`spec/simulation_spec.lua`) |
| Admission races, multi-worker caps | Test::Nginx `t/admission.t`, `t/multi_worker.t` (1/2/4 workers, 100 concurrent vs limit 10, thousands of reps) |
| Lifecycle (§52 list) | `t/lifecycle.t` |
| Reload under traffic | `t/resilience.t`, `benchmark/resilience.sh` (HUP × N while wrk drives requests) |
| Worker SIGKILL | `benchmark/resilience.sh` |
| Controller integration (windows, lease, backoff) | `t/controller.t` |
| Bounded memory / no leaks | `benchmark/soak.sh` + `t/stats.t` assertions |
| Overhead vs baseline | `benchmark/run.sh` (baseline vs fixed-counter vs adaptive; 1/2/4/8 workers) |
