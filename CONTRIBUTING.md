# Contributing

Thanks for considering a contribution. The library has a strict ordering
of priorities that reviews will enforce, in order: correctness;
predictable behavior under failure; request-path overhead; controller
stability; bounded CPU/memory; multi-worker correctness; observability;
API simplicity.

## Ground rules

- **Run the tests.**
  ```bash
  make test-unit                          # specs; local busted if installed, else Docker
  make test-unit SPEC=spec/gradient2_spec.lua
  make test-integration T=t/admission.t   # Test::Nginx, always Docker
  make test                               # both
  make shell                              # tightest loop: busted / prove inside the image
  ```
  The Docker harness (the image CI uses) is built on first use. The
  specs are pure Lua against `spec/mock_ngx.lua`, so a local busted runs
  them in well under a second — it must be built for LuaJIT/Lua 5.1
  (`luarocks --lua-version=5.1 install busted`), since the code is
  Lua 5.1 only; `.busted` sets the paths. Controller changes must
  additionally pass `make sim` (scenarios A–H).
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
- **Evidence-backed documentation.** Claims need a test, simulation, or
  benchmark behind them, and limitations belong in the README.
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
