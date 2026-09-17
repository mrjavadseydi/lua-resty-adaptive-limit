# Contributing

Thanks for considering a contribution. The library has a strict ordering
of priorities that reviews will enforce, in order: correctness;
predictable behavior under failure; request-path overhead; controller
stability; bounded CPU/memory; multi-worker correctness; observability;
API simplicity.

## Ground rules

- **Run the tests.** Everything runs in the Docker harness — local runs
  and CI are identical:
  ```bash
  make image && make test
  ```
  Controller changes must additionally pass `make sim` (scenarios A–H).
- **No request-path costs without proof.** The fast path is measured in
  `benchmark/microbench.lua`. An allocation, a string concatenation, an
  extra dict operation, a regex, or a yield on the admission/release path
  needs a benchmark showing why it is unavoidable — and usually it is
  avoidable.
- **Concurrency assumptions go in comments.** Any code relying on
  atomicity or on the absence of yield points documents it (see the
  existing comments around `try_acquire`).
- **Documented behavior is pinned by tests.** If you change behavior —
  redirect handling, failure modes, classification — the corresponding
  test in `t/` or `spec/` must be updated in the same change.
- **Honest documentation.** Limitations are documented, not papered over.
  Claims need a test, a simulation, or a benchmark behind them.
- LuaJIT-compatible Lua 5.1 syntax only (no Lua 5.3+ features). No FFI in
  the core. No new runtime dependencies.

## Workflow

1. Fork/branch from `main`.
2. Small, focused changes with a clear message ("what" and "why").
3. `make test` green for every commit.
4. Pull requests: describe the behavior change and its effect on the
   invariants in design.md.

## Reporting bugs

Include: OpenResty version (`resty -V`), a minimal nginx.conf, the
relevant `state()` snapshot, and error-log excerpts. For races, note the
worker count and whether reloads were involved.
