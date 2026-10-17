# Changelog

All notable changes are documented here. Format based on
[Keep a Changelog](https://keepachangelog.com/); versioning follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.0] — 2026-10-17

### Fixed
- Baseline probes no longer adopt a still-queued RTT. When the probe
  limit was itself above capacity, the probe re-seeded `long_rtt` with
  queueing latency; the gradient then read 1 and headroom ratcheted the
  limit far above capacity (32x in simulation J, limit above offered
  load in real-traffic tests). A probe still lowers the baseline freely
  and raises it only when its RTT stayed within 25% of the pre-probe RTT:
  a probe still above capacity reads lower (Little's law), one that
  straddles a capacity drop reads higher; neither is service time. Probes
  start only while latency is steady, so that reference does not lag a
  sudden change.
- A cold start into an already saturated backend no longer pins a
  queue-inflated baseline. Probes cut to `probe_fraction` x the
  Little's-law capacity estimate (`limit * long_rtt / short_rtt`) instead
  of `probe_fraction` x the limit, so an inflated baseline is lowered on
  every probe until it reads the service time (simulation K: 34x capacity
  before, near capacity after).
- The library never evicts keys from its zone. Window accumulators were
  created with evicting writes, so an undersized zone dropped limiters'
  `inflight` counters (uncapped admission under `fail_open`) and
  `sample_window` markers (a fresh worker start refused as legacy state).
  A full zone now fails startup with an explicit "increase its size"
  error, and runtime flush failures say the same.
- Startup contention between workers no longer counts a
  `stale_controller_window` anomaly.
- Missing inflight counters remain errors until safely reset; recovery
  no longer forgets sibling workers' holdings and exceeds the cap.
- A limiter-wide controller lease orders publications across windows,
  including concurrent startup ticks, with owner checks before publishing.
- Stored window-size metadata prevents reloads from freezing adaptation
  after `sample_window` changes.
- Active probes back off on explicit overload and restore only after
  enough healthy, positive latency observations.
- Startup propagates shared-state initialization and adoption write
  failures; concurrent initialization preserves already published state.

### Upgrade notes
- The minimum supported OpenResty is 1.19.3.1 (lua-nginx-module 0.10.18).
  Earlier releases documented 1.15.8.1, but `exit_worker_by_lua*`, which
  the setup requires, does not exist before 0.10.18.
- Changing `sample_window` requires a full stop/start. Upgrading from
  0.1.1 or earlier also requires a full stop/start because existing shared
  state has no window-size metadata. Matching-window reloads preserve the
  learned limit after that upgrade.

## [0.1.1] — 2026-10-04

Bug-fix release. No new options, no API changes, and no shared-state
schema change: upgrading with a graceful reload keeps the learned limit.

### Fixed
- Controller: a window of zero RTTs (sub-millisecond completions recorded
  as 0) no longer seeds the baseline. The next real sample was flooring
  the gradient and, with rejections freezing that zero, pinning the limit
  at `min_limit` until the next probe. A cold limiter whose first
  sufficient window already rejected now seeds its baseline from that
  window instead of waiting for an app-limited one.
- Admission: a negative inflight repair adds back only the excess this
  decrement introduced. Repairing the whole hole across workers left
  phantom slots and could shed all traffic after a reload.
- Controller: a baseline probe no longer re-seeds `long_rtt` from a window
  that still contains completions admitted before the probe limit was
  published. The reduced limit is held until one RTT has drained, then
  restored without learning if the sample is still queued.
- `start()` refuses a shared dict with no `expire()` (lua-resty-core is
  required and is now loaded by the library). A missing `expire` used to
  throw inside the scheduler, skip every controller update, and add the
  same window deltas again on the next tick.
- `controller_stalled` waits out `stale_threshold` after the pool fills
  instead of firing on the first full look with no completion yet.
- `log()` releases the concurrency slot before running the outcome
  classifier, so a failing or yielding classifier can never hold a slot.

### Upgrade notes
- Builds with `lua_load_resty_core off` (or older OpenResty builds that do
  not preload lua-resty-core) now have it loaded by the library; if
  lua-resty-core is genuinely unavailable, `start()` returns an error
  instead of starting a limiter whose controller could never run.

## [0.1.0] — 2026-09-20

### Added
- Adaptive concurrency admission on top of atomic `lua_shared_dict`
  operations: `try_acquire`/`release` (low-level), `access`/`log`, and `guard`
  (request-lifecycle) APIs with idempotent release and internal-redirect
  safety.
- Windowed Gradient2-inspired controller (default) and AIMD reference controller,
  both pure and deterministic, with hand-computed unit tests, seeded fuzz
  property tests, and scenario A–I simulations.
- Saturation-safe latency baseline: learned only from app-limited windows
  and re-measured under sustained saturation by a periodic probe
  (`probe_interval`, `probe_fraction`; `probe_restore` in `state()`), so
  a backend that queues without failing cannot ratchet the limit to
  `max_limit` (simulation I).
- Per-worker scheduler (single `ngx.timer.every`), fixed-size worker-local
  statistics, per-window shared accumulators with atomic-increment
  flushing, deterministic windows with grace period, per-window controller
  leases with `last_window` idempotence; a window superseded by a sibling
  after the lease is abandoned (`stale_controller_window`).
- Graceful-reload state preservation (learned limit survives), schema
  version marker, worker-exit slot reconciliation, worker heartbeats.
- Observability: `state()` snapshot, `on_update`/`on_anomaly` hooks,
  stuck-pool diagnostics (`controller_stalled`), worker liveness,
  shared-dict usage; optional nginx-lua-prometheus example.
- Failure modes: `fail_open`/`fail_closed`, missing/corrupted state
  re-seeding with anomaly counters, distinct error constants; an evicted
  `inflight` counter is rebuilt from the worker's held slots
  (`inflight_missing`) rather than from zero.
- Test suite: busted specs (unit, fuzz, simulations) and Test::Nginx
  integration tests (admission races on 1/2/4 workers, lifecycle,
  errors, stats, controller loop with asserted growth/shedding/recovery,
  resilience); Docker harness identical to CI; benchmark matrix,
  micro-benchmark, soak and reload/SIGKILL harnesses.
- Named limiter lookup through `adaptive.get(name)` and local test selection
  through the `SPEC=`/`T=` Make variables.

[Unreleased]: https://github.com/mrjavadseydi/lua-resty-adaptive-limit/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/mrjavadseydi/lua-resty-adaptive-limit/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/mrjavadseydi/lua-resty-adaptive-limit/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/mrjavadseydi/lua-resty-adaptive-limit/releases/tag/v0.1.0
