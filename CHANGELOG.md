# Changelog

All notable changes are documented here. Format based on
[Keep a Changelog](https://keepachangelog.com/); versioning follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed
- Controller: a window of zero RTTs (sub-millisecond completions recorded
  as 0) no longer seeds the baseline. The next real sample was flooring
  the gradient and, with rejections freezing that zero, pinning the limit
  at `min_limit` until the next probe.
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
- `log()` releases the concurrency slot before the outcome classifier.

- Controller: the latency baseline no longer learns from saturated windows,
  which normalized sustained congestion and ratcheted the limit to
  `max_limit` when the backend queued without failing (simulation I).
  Under saturation the baseline is re-measured by a periodic probe
  (`probe_interval`, `probe_fraction`; new `probe_restore` state key).
- Admission: an evicted `inflight` counter is rebuilt from this worker's
  held slots instead of zero (`inflight_missing` anomaly), so the cap
  survives shared-dict memory pressure.
- Controller: a window superseded by a sibling after the lease is abandoned
  (`stale_controller_window`) instead of publishing older state.
- `t/controller.t` asserts actual growth, shedding, recovery and the
  hand-computed single-update value; a frozen controller now fails it.

## [0.1.0] — 2026-09-20

### Added
- Adaptive concurrency admission on top of atomic `lua_shared_dict`
  operations: `try_acquire`/`release` (low-level), `access`/`log`, and `guard`
  (request-lifecycle) APIs with idempotent release and internal-redirect
  safety.
- Windowed Gradient2-inspired controller (default) and AIMD reference controller,
  both pure and deterministic, with hand-computed unit tests, seeded fuzz
  property tests, and scenario A–H simulations.
- Per-worker scheduler (single `ngx.timer.every`), fixed-size worker-local
  statistics, per-window shared accumulators with atomic-increment
  flushing, deterministic windows with grace period, per-window controller
  leases with `last_window` idempotence.
- Graceful-reload state preservation (learned limit survives), schema
  version marker, worker-exit slot reconciliation, worker heartbeats.
- Observability: `state()` snapshot, `on_update`/`on_anomaly` hooks,
  stuck-pool diagnostics (`controller_stalled`), worker liveness,
  shared-dict usage; optional nginx-lua-prometheus example.
- Failure modes: `fail_open`/`fail_closed`, missing/corrupted state
  re-seeding with anomaly counters, distinct error constants.
- Test suite: busted specs (unit, fuzz, simulations) and Test::Nginx
  integration tests (admission races on 1/2/4 workers, lifecycle,
  errors, stats, controller loop, resilience); Docker harness identical
  to CI; benchmark matrix, micro-benchmark, soak and reload/SIGKILL
  harnesses.
- Named limiter lookup through `adaptive.get(name)` and local test selection
  through the `SPEC=`/`T=` Make variables.
