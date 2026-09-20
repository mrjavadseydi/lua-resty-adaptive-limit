# Changelog

All notable changes are documented here. Format based on
[Keep a Changelog](https://keepachangelog.com/); versioning follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed
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
