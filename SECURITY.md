# Security Policy

## Threat model

The library treats **configuration as trusted** and **request data as
untrusted**:

- Shared-dict keys are built exclusively from configuration-validated
  limiter names (`[a-z][a-z0-9_.-]{0,62}`) plus internal constants. No
  attacker-controlled string ever becomes a dict key, so key cardinality
  is bounded by the configuration, never by traffic.
- Latencies and outcomes enter the controller through validation: NaN,
  infinity, negative, or oversized values are dropped and counted as
  anomalies; outcome names are fixed constants. Malformed input cannot
  corrupt controller state and NaN/inf are never published to the shared
  dictionary.
- No request payload, header, or URI component is ever stored in the
  shared dictionary, logged, or echoed.
- No `loadstring`, dynamic module loading, or metatable tricks in the hot
  path; the library performs no network I/O of its own.

## Reporting a vulnerability

Open a GitHub issue marked `security` or contact the maintainer directly
(see the repository). Please include a minimal reproduction and the
OpenResty version. Security-relevant fixes will be released as patch
versions with a CHANGELOG entry.

## Known non-issues by design

- The limiter does not implement multi-tenant fairness; a tenant can
  consume the whole pool by design (use one limiter per tenant class).
- Fail-open mode trades protection for availability on limiter-internal
  failures; that is a documented, deliberate choice per limiter.
- Abrupt worker death (SIGKILL) leaks the victim's held slots (surfaced
  via diagnostics; see README "Failure behavior"). This is an accepted
  trade-off against per-request synchronization cost.
