# lua-resty-adaptive-limit

Adaptive concurrency limiting and load shedding for OpenResty.

The allowed number of concurrent requests against a protected upstream
adjusts automatically from observed completion latency and explicit
overload signals, so excess load is shed **early** — before the upstream
saturates, queues explode, requests time out, and clients retry — instead
of after.

```text
request
   ↓
admission counter          (atomic shared-dict increment, O(1))
   ↓
current adaptive limit  ←── controller (once per window, one leader)
   ↓                          ↑
allow / reject            latency + outcome observations
   ↓
upstream
```

Status: **0.1.0** — initial release, with
multi-worker race tests, deterministic controller simulations, a
real-traffic reload/SIGKILL harness, and published benchmarks. It is young
software; read the failure behavior and limitations before deploying it.

- [Installation](#installation)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [API](#api)
- [Configuration reference](#configuration-reference)
- [Failure behavior](#failure-behavior)
- [Performance](#performance)
- [Limitations](#limitations)
- [Security considerations](#security-considerations)
- [Development](#development)

## Installation

OpenResty ≥ **1.15.8.1** (the library uses `exit_worker_by_lua*`,
available since lua-nginx-module 0.10.15). No other runtime dependencies.

After a LuaRocks release: `luarocks install lua-resty-adaptive-limit`.

Or copy `lib/resty/adaptive_limit.lua` and `lib/resty/adaptive_limit/`
into your `lua_package_path`.

## Quick start

Three things: a dedicated shared dict, a module that defines your
limiters, and five one-line Lua blocks.

```nginx
lua_shared_dict adaptive_limit 10m;   # dedicated zone; sizing below

init_by_lua_block        { require("app.limiters") }   -- validate at startup
init_worker_by_lua_block { assert(require("resty.adaptive_limit").start()) }
exit_worker_by_lua_block { require("resty.adaptive_limit").exit() }

server {
    location /api/ {
        access_by_lua_block { require("resty.adaptive_limit").get("payments"):guard() }
        proxy_pass http://payments_backend;
        log_by_lua_block    { require("resty.adaptive_limit").get("payments"):log() }
    }
}
```

`app/limiters.lua` ([examples/basic.lua](examples/basic.lua)):

```lua
local adaptive = require "resty.adaptive_limit"

assert(adaptive.new({
    name          = "payments",        -- required
    shared_dict   = "adaptive_limit",  -- required
    initial_limit = 50,
    min_limit     = 5,
    max_limit     = 2000,
}))
```

`guard()` admits the request or answers `503` + `Retry-After` (500 on a
limiter-internal error); `log()` releases the slot and records the
observation. Call `log()` before any other `log_by_lua` work in the same
location. Everything else is tuning, and the defaults are the `balanced`
profile.

A runnable end-to-end config lives in [examples/nginx.conf](examples/nginx.conf);
[examples/proxy.lua](examples/proxy.lua) shows the low-level API.

## How it works

A fixed concurrency limit is a guess; rate limits count requests and say
nothing about how hard each one hits the backend. Latency starts rising
as soon as a queue starts forming — long before failures — so it is the
earliest congestion signal there is:

```text
concurrency: 20  latency: 20 ms      healthy
concurrency: 120 latency: 24 ms      still healthy → limit grows
concurrency: 150 latency: 40 ms      queue forming → limit stops growing
concurrency: 180 latency: 120 ms     saturation → limit shrinks, load shed
```

The baseline the controller compares against is learned only while the
limit is *not* the bottleneck (windows without rejections). Under
saturation the limit itself shapes the latency, so learning from those
windows would normalize the queue and ratchet the limit up until failures
appear; instead, every `probe_interval` windows (30 s by default) the
limit is published at `probe_fraction` (half) for two windows and the
baseline is re-measured from the second — the same idea as Envoy's
adaptive-concurrency minRTT recalculation and BBR's PROBE_RTT. Expect a
short, periodic dip in the published limit under sustained saturation;
`state().probe_restore` shows the limit it will return to.

Two planes, strictly separated:

**Fast path** (per request): read the current limit, take one atomic
shared-dict increment — the admission linearization point — admit, or
roll the increment back and reject. No locks, no queues, no regex, no
JSON, no logging, no allocation: **0.22 µs** per admission, **0.125 µs**
per release ([Performance](#performance)). For a stable limit `L`, more
than `L` requests can never pass admission simultaneously (proven under
real multi-worker load in `t/admission.t` and `t/multi_worker.t`).

**Control path** (one timer per worker, never per request): workers flush
fixed-size local aggregates into shared per-window accumulators; once a
window closes, exactly one worker aggregates it, runs the controller
(a windowed Gradient2-inspired algorithm by default, AIMD as a reference)
and publishes the new limit.
Windows with insufficient latency samples hold unless explicit failures
independently justify backoff; nothing moves on idle.
Healthy windows grow only after a rejection proves offered demand reached
the current cap, preventing low-concurrency traffic from drifting the limit
upward. The controller math and invariants are in
[design.md §8](design.md).

## API

Errors are stable interned string constants:
`adaptive.errors.REJECTED` (`"rejected"`), `errors.INTERNAL_ERROR`,
`errors.NOT_STARTED`, `errors.INVALID_STATE` — comparing with `==` against
either form is the same comparison.

### `adaptive.new(options) -> limiter | nil, err`

*Where:* `init_by_lua` (validation) and `init_worker_by_lua` (per-worker
instance). *Returns:* a limiter instance, or `nil` + a message for invalid
configuration, an unknown shared dict, or a duplicate name in this worker.
*Side effects:* registers the instance with the per-worker scheduler.
Validates everything once; nothing on the request path ever parses
configuration.

### `limiter:try_acquire() -> true | nil, err`

*Allowed contexts:* any request phase (`access_by_lua`, `content_by_lua`).
*Yields:* no. *Cost:* 1 dict read + 1 atomic incr (+1 incr on rejection).

Returns `true` (a slot is held — you **must** `release` exactly once) or
`nil, err`:

* `rejected` — the pool is full; the application chooses the response.
* `internal_error` — shared-dict failure; the slot was **not** taken.
* `not_started` — `adaptive.start()` has not run in this worker.

The low-level API always surfaces internal errors and implements no
failure policy of its own; `failure_mode` applies to the lifecycle helper.

### `limiter:release(latency, outcome) -> true | nil, err`

*Allowed contexts:* any request phase (`log_by_lua` typically). *Yields:*
no. *Side effects:* decrements the shared counter first, then records the
observation — an observation failure can never block the release.

`latency`: seconds (number) or `nil`. Non-finite/negative values are
dropped with an anomaly; they cannot corrupt statistics. `outcome`:
`"success"`, `"timeout"`, `"connect_error"`, `"overload"`, `"error"`,
`"aborted"`, `"ignored"` (default `"success"`). Aborted requests release
their slot but contribute no latency sample (a truncated duration is not a
capacity signal); `ignored` contributes nothing at all (health checks).
An unknown outcome string (a typo) still releases the slot; the
observation is dropped with a `bad_outcome` anomaly and a rate-limited
error log, so a typo in one code path can never leak concurrency.
A negative counter result — double release — is compensated with an
atomic increment (never a `set`, which would erase a sibling worker's
concurrent admission) and counted as an anomaly; it is surfaced, never
hidden.

### `limiter:access(options) -> true | nil, err`

*Allowed contexts:* `access_by_lua` (or the first serving phase).
*Yields:* no. `options.bypass = true` admits nothing and tracks nothing
(streaming endpoints, health checks).

Returns `true`, or `nil, "rejected"`, or `nil, err` for limiter-internal
failures — under `fail_open` (default) an internal failure admits the
request **without holding a slot** (so `log()` releases nothing);
`fail_closed` returns the error instead. `not_started` and
`invalid_state` are configuration-level problems and are never masked by
`fail_open`.

Internal re-entries — subrequests, `error_page` targets, `ngx.exec`
targets — **bypass admission by default**: subrequests never get a log
phase and exec chains run their log phase only at the final location, so
admitting there would leak a slot per request. Locations that are only
reached through `ngx.exec`/X-Accel-Redirect should own the limiter with
`allow_internal = true`.

**Important lifecycle rule:** *the location that admits must be the location whose log phase runs.*
When an admitting location redirects internally (e.g. `ngx.exec` or `error_page 502 = /fallback`), nginx executes only the final location's log phase and resets `ngx.ctx`. If an admitting location uses `error_page` redirect, the fallback location must explicitly release the slot using `limiter:release(nil, outcome)` to prevent leaking concurrency slots. (All of this behavior is pinned by tests; see `t/lifecycle.t`.)

### `limiter:log() -> true`

*Allowed contexts:* `log_by_lua`. *Yields:* no. Idempotent; releases only
what `access()` admitted; releases **before** any other work, with
optional parts pcall-protected. Call it before any other `log_by_lua`
extensions in the same location. If the location crashed after admission,
the log phase still runs and the slot is still released.

Latency source (`latency_source`): `request_time` (default,
`ngx.now() - ngx.req.start_time()`), `upstream_response_time` (parsed
explicitly — upstream retries produce compound values like
`"0.005, 0.010"`; redirects across upstream groups also introduce colons.
The parser validates every token and applies `upstream_time_choice`
`last`/`max`/`sum`), or `manual` (low-level `release` only).

Outcome classification defaults: `499` → aborted, `503` → overload,
`504` → timeout, `502` → connect_error, other 5xx → error (not a capacity
signal), everything else → success. Override with
`outcome_classifier = function(status) return "overload" or nil end`
(pcall-protected; errors fall back to the default).

### `limiter:guard(options) -> true | nil, err`

`access(options)` followed by `enforce(err)` on failure: admits, or
produces the rejection response and returns `nil, err`. The one-liner
for the common case; use `access()` + `enforce()` to choose the response
yourself.

### `limiter:enforce(err)`

Convenience helper for producing the standard rejection response:
maps `"rejected"` to the configured status (503 by default) with a
`Retry-After` header; internal errors map to 500.

### `limiter:state() -> table`

*Yields:* no; performs a handful of shared-dict reads — **not** for the
request path. Returns shared state (`limit`, `float_limit`, `inflight`,
`short_rtt`, `long_rtt`, `gradient`, `probe_restore` (set only during a
baseline probe), `last_window`, `last_update`, last
closed window's raw accumulators — note: accumulators are deleted once
processed, so `last_sample_count` is non-zero only during the grace window),
worker-local counters (`admitted_total`, `rejected_total`, `controller_updates`,
`controller_skips`, `internal_errors`, `counter_anomalies`, `timer_failures`),
and diagnostics: `controller_stalled` (pool exhausted with no recent completions on any
worker — the hung-backend scenario; backpressure still holds and this makes
it visible),
`workers_active`/`workers_expected` (heartbeat liveness),
`last_completion_age`, `shared_dict_capacity`/`shared_dict_free`.

### `adaptive.start(options) -> true | nil, err`

*Where:* `init_worker_by_lua` only. Validates and adopts shared state (a
learned limit **survives** a graceful reload), writes the first heartbeat,
and starts the single scheduler timer. `options.flush_interval` (default
0.2 s) sets the scheduler cadence.

### `adaptive.exit()`

*Where:* `exit_worker_by_lua`. Subtracts the slots this worker still holds
from the shared counter: graceful shutdown and reload drains cannot leak
them. If a straggler log phase still fires afterwards, its excess
decrement is compensated and raises an anomaly — visible, never corrupting.

### `adaptive.get(name) -> limiter`

The limiter registered under `name` in this worker. An unknown name is a
configuration typo, so it **raises** with a clear message rather than
returning `nil`.

### `adaptive.limiters()`, `adaptive.stop()`, `adaptive.errors`

Registry names in this worker; scheduler halt (tests/shutdown); the error
constants.

## Configuration reference

Explicit options override `profile`; profiles are only pre-tuned bundles
(behavior differences are pinned by the simulations).

| Option | Type | Default | Range / notes |
|---|---|---|---|
| `name` | string | required | `[a-z][a-z0-9_.-]{0,62}`; part of shared-dict keys |
| `shared_dict` | string | required | name of a **dedicated** `lua_shared_dict` zone |
| `algorithm` | string | `"gradient2"` | `"gradient2"` \| `"aimd"` |
| `profile` | string | `"balanced"` | `conservative` sheds earlier/grows slower; `responsive` tolerates bursts, reacts fast |
| `initial_limit` | integer | 50 | `[min_limit, max_limit]`; cold-start value |
| `min_limit` / `max_limit` | integer | 1 / 2000 | `≥ 1`; hard clamps on every publication |
| `sample_window` | number | 1.0 | seconds; controller window |
| `aggregation_grace` | number | 0.35 | seconds; wait after window close before processing (late flushes) |
| `min_samples` | integer | 20 | minimum usable latency samples for an RTT update |
| `failure_mode` | string | `"fail_open"` | lifecycle behavior on limiter-internal failures |
| `rtt_tolerance` | number | 2.0 | `> 1`; tolerated short/long RTT ratio before shedding |
| `min_gradient` | number | 0.5 | `(0, 1)`; floor on per-window decrease |
| `smoothing` | number | 0.5 | `(0, 1]`; limit EWMA factor |
| `headroom_min`/`headroom_max` | number | 1 / 50 | bounds of the `sqrt(limit)` growth pressure |
| `baseline_alpha` | number | 0.05 | `(0, 1)`; baseline EWMA speed on app-limited windows (slow by design) |
| `sample_alpha` | number | 0.5 | `(0, 1]`; observed-RTT EWMA speed |
| `overload_min_samples` | integer | 20 | minimum completed outcomes before explicit-signal backoff may trigger |
| `overload_failure_ratio` | number | 0.10 | `(0, 1]`; strong-signal/completion ratio that triggers backoff and freezes the baseline |
| `overload_backoff` | number | 0.80 | `(0, 1)`; multiplicative backoff |
| `probe_interval` | integer | 30 | windows between baseline probes under saturation (`>= 3`; `0` disables) |
| `probe_fraction` | number | 0.5 | `(0, 1)`; limit published during the two probe windows |
| `latency_source` | string | `"request_time"` | \| `upstream_response_time` \| `manual` |
| `upstream_time_choice` | string | `"last"` | `last` \| `max` \| `sum` for multi-attempt upstream timings |
| `allow_internal` | boolean | false | admit on `ngx.req.is_internal()` (exec-fronted locations) |
| `rejection_status` / `retry_after` | number | 503 / 1 | used only by `enforce()` |
| `stale_threshold` | number | 30 | seconds without completions before `controller_stalled` |
| `on_update` | function | nil | `function(snapshot)` after each controller publication (control path; must not yield) |
| `on_anomaly` | function | nil | `function(kind, detail)` on anomalies/internal errors |
| `outcome_classifier` | function | nil | `function(status) -> outcome or nil`; errors use the default classifier |

### Shared dict sizing

Per limiter: ~10 permanent keys + up to ~3 live windows × 9 accumulator
keys (TTL-bounded) + one heartbeat key per worker. All entries are small
numbers/short strings; a limiter idles well under 2 KB. A 10m zone serves
dozens of limiters comfortably. `state().shared_dict_free` exposes usage —
alert on it. Critical keys are never written with TTLs; if the zone is
evicted/undersized the limiter re-seeds from the last observed value and
raises `limit_missing` anomalies rather than admitting unbounded.

## Failure behavior

* **Pool full** is not a failure: `rejected` is the limiter working.
* **Shared-dict failures** on the request path surface as
  `internal_error` (low-level) or follow `failure_mode` (lifecycle):
  `fail_open` keeps the site available without holding slots,
  `fail_closed` sheds. Both count `internal_errors` and rate-limit one
  error log per second per kind — normal traffic never logs.
* **Missing/corrupted critical state** (eviction, operator flush,
  corruption): re-seeded from the worker's last observed limit — fresh to
  within one window — with a `limit_missing`/`limit_corrupted` anomaly;
  a limit outside `[min_limit, max_limit]` (including `inf`) is treated
  the same way. An `inflight` counter that vanished under memory pressure
  (OpenResty may evict unexpired keys) is **not** recreated from zero —
  that would forget every held slot: it is rebuilt from this worker's own
  held count (`inflight_missing` anomaly; the discovering acquire fails as
  an internal error under `failure_mode`), and sibling workers' earlier
  admissions reconcile as they drain. A corrupted `inflight` counter (non-numeric or
  non-finite — `-inf + 1` must never admit) is **not** auto-repaired: the
  library cannot know how many slots other workers hold, and an
  unsynchronized reset would erase their admissions. It is surfaced
  (`inflight_corrupted` anomaly, rate-limited error log) and treated as
  an internal error under `failure_mode` until an operator resets the
  key with traffic quiesced:
  `ngx.shared.<zone>:set("al:1:<name>:inflight", 0)`.
  The limiter never crashes a request comparing against garbage.
* **Graceful reload (`nginx -s reload`)**: the shared dictionary
  survives; new workers validate the schema marker and adopt the learned
  limit. Verified under continuous wrk load across repeated reloads
  (`benchmark/resilience.sh`): no lockout, no reset, no drift.
* **Worker exit (graceful)**: `adaptive.exit()` reconciles held slots.
  Late log phases are safe (the excess decrement is compensated, anomaly counted).
* **Worker death (SIGKILL/segfault)**: nothing runs. The victim's held
  slots leak — the shared counter stays high, reducing capacity until an
  operator acts. An unsynchronized "repair" while live requests
  modify the counter would corrupt it. It is surfaced (heartbeat expiry,
  `controller_stalled`, inflight/throughput divergence) and the operator
  procedure is: quiesce or accept the shedding, then reset `inflight` via
  an admin snippet. A lease/quota backend that auto-recovers is a
  separate feature and is not part of this release.
* **Hung backend** (all in-flight requests stuck): a completion-based
  controller sees no samples, so the limit **holds** — backpressure
  remains, the limiter keeps shedding at the last healthy limit, and
  `controller_stalled` reports the condition for alerting.
* **Schema mismatch**: a hard startup error, loudly — silently
  re-initializing would discard a learned limit.

## Performance

Measured with the harness in `benchmark/` (wrk 4.2.0, 2 threads / 64
connections, median of 3x15 s, Docker VM: linuxkit aarch64 reporting 1
core, 978 MiB, OpenResty 1.31.1.1 -- see `benchmark/results/*/machine.txt`).
Two caveats: on a single-core VM wrk's two threads
compete with nginx for CPU (absolute numbers are depressed and non-monotone
in worker count), and every comparison below is against an identical
no-limiter baseline run on the same VM. Treat relative deltas as the claim.

Below-limit throughput (limiter enabled, never rejecting -- median req/s):

| scenario | 1 worker | 2 workers | 4 workers |
|---|---|---|---|
| baseline (no limiter) | 13748 | 23556 | 36211 |
| fixed shared-dict counter | 12854 | 24998 | 35842 |
| **adaptive limiter** | **12747** | **24481** | **35317** |
| adaptive vs baseline | -7.3% | +3.9% | -2.5% |
| adaptive vs fixed counter | -0.8% | -2.1% | -1.5% |
| adaptive, 16 limiters / 1 scheduler | 12426 | 25581 | 33339 |

The irreducible cost is the shared-dict admission itself (the fixed
counter, the `resty.limit.conn` shape); the full adaptive stack -- worker
statistics, log-phase observation, window flushing, controller -- adds
roughly **0-2%** on top of that floor in this run. The 16-limiter results
show the scheduler remains small relative to request processing, but this
single-host benchmark is too noisy for a stronger claim. Under heavy rejection (limit 2 under 64
connections) the limiter serves ~41-46k req/s -- the 503 short-circuit is
cheaper than proxying, which is the point of early load shedding.

Per-operation cost inside OpenResty (`benchmark/microbench.lua`,
200k ops):

```text
raw dict get+incr           0.095 us/op
try_acquire (admit)         0.220 us/op
release                     0.125 us/op
try_acquire (reject)        0.345 us/op
```

The complete admission path adds ~0.125 us over the raw shared-dict
floor. Sustained-load behavior (10-minute soak, ~2.9 M requests): worker
RSS flat at ~21.5 MB (-502 kB between first and last quarter), Lua GC
oscillating 0.7-1.2 MB with zero trend -- memory does not grow with
requests served. No benchmark methodology was tuned toward a target; every
repetition's number, including noisy runs, is listed in
`benchmark/results/<run>/summary.txt`.

## Limitations

* **Node-local.** Each nginx instance learns its own limit; 4 instances ×
  ~100 ≈ 400 cluster capacity is an emergent approximation, not a global
  semaphore. Cross-instance coordination (Redis etc.) is deliberately out
  of scope for admission.
* **One limiter = one pool.** No tenant fairness inside a pool; use
  separate limiters or a fairness layer.
* **Request-scoped work.** WebSockets, SSE, gRPC streaming, and
  multi-hour uploads don't fit a completion-based model: `bypass` them
  and manage admission explicitly with the low-level API.
* **Low traffic.** Below `min_samples` usable latency samples per window the
  limit holds unless explicit failures trigger backoff; a healthy service
  seeing 2 req/s learns almost nothing.
* **Abrupt worker death** leaks that worker's slots (see above).
* **Wrong latency sources give wrong limits.** `request_time` includes
  slow clients and large bodies; prefer `upstream_response_time` when the
  protected resource is backend processing.
* **Config changes** take effect on reload; the learned limit persists and
  is immediately clamped to a changed `min_limit`/`max_limit` range.
* **`lua_code_cache on` is required** (the default). With it off, every
  request runs in a fresh Lua VM that has no registry and never saw
  `start()`: `get()` raises and `try_acquire()` returns `not_started`.
* `log_by_lua` does not run for subrequests (platform behavior); the
  lifecycle helper relies on the documented redirect semantics pinned in
  `t/lifecycle.t`.

## Security considerations

Request data is never trusted: no attacker-controlled strings become
shared-dict keys (names are config-validated), no user input reaches the
controller (latencies are sanitized, outcomes are fixed constants), no
NaN/inf can be published, and no header value influences the limit.
Configuration is trusted and validated once at startup. Treat the shared
dict as sensitive-only-in-summary; no request payloads are ever stored.

## Development

```bash
make test-unit                          # busted specs (local busted if installed, else Docker)
make test-unit SPEC=spec/gradient2_spec.lua
make test-integration T=t/admission.t   # Test::Nginx (always Docker)
make test                               # both
make sim                                # deterministic controller simulations A–H
make shell                              # a shell inside the harness image
make bench / make resilience / SOAK_SECONDS=600 make soak
```

The harness image (OpenResty + busted + Test::Nginx + wrk, the same one
CI uses) is built on first use. See [CONTRIBUTING.md](CONTRIBUTING.md).

The ten invariants in [design.md](design.md) each map to a test or a
simulated property; the verification matrix lists the relevant checks.

## License

MIT — see [LICENSE](LICENSE).
