# Changelog

All notable changes are documented here. Format based on
[Keep a Changelog](https://keepachangelog.com/); versioning follows
[Semantic Versioning](https://semver.org/).

## [0.1.0] — 2026-09-19

Initial release.

### Added
- Adaptive concurrency admission on top of atomic `lua_shared_dict`
  operations: `try_acquire`/`release` (low-level) and `access`/`log`
  (request-lifecycle) APIs with idempotent release and internal-redirect
  safety.
- Gradient2 adaptive controller (default) and AIMD reference controller,
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
