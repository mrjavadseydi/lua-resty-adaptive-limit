-- Per-worker module runtime shared between the entry module, the limiter
-- instances and the scheduler. One Lua value per OpenResty worker VM —
-- never shared between workers (cross-worker state lives exclusively in
-- the shared dictionary).
--
-- Kept in its own file so the entry module and the limiter can require it
-- without a circular require.

return {
    -- set to true by adaptive.start(); admission refuses to run before
    -- that (errors.NOT_STARTED)
    started = false,

    -- scheduler tick in seconds, set by adaptive.start()
    flush_interval = 0.2,

    -- name -> limiter instance; one scheduler tick services all of them
    registry = {},

    -- registration order for deterministic tick iteration
    order = {},
}
