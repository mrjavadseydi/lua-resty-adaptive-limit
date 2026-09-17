-- Stable error constants.
--
-- Errors are plain interned strings: `err == "rejected"` and
-- `err == adaptive.errors.REJECTED` are the same comparison, and no error
-- object is ever allocated per rejection on the request path.
--
-- Distinguishing classes (design.md §11):
--   REJECTED       the limiter works; the concurrency pool is full.
--   INTERNAL_ERROR limiter-internal failure (shared dict errors, no
--                  memory, ...); governed by failure_mode for the
--                  lifecycle helpers, surfaced to low-level callers.
--   NOT_STARTED    adaptive.start() was never called in this worker.
--   INVALID_STATE  shared state failed validation (schema mismatch,
--                  corruption); re-seeded and surfaced, never hidden.

return {
    REJECTED       = "rejected",
    INTERNAL_ERROR = "internal_error",
    NOT_STARTED    = "not_started",
    INVALID_STATE  = "invalid_state",
}
