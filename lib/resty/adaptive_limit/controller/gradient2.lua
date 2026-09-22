-- Windowed Gradient2-inspired concurrency controller (default algorithm).
--
-- Pure and deterministic: no ngx, no I/O, no clocks. Inputs are validated
-- by controller.common.safe_update() before update() is called; the
-- output is validated again on return. state fields:
--
--   limit     current limit (float internally; published as an integer
--             by the wiring at the publication boundary)
--   long_rtt  slow EWMA of healthy RTT (the capacity baseline); nil
--             before the first sufficient non-overloaded window
--   short_rtt fast EWMA of observed RTT; nil before the first sufficient
--             window
--   gradient  last computed gradient (diagnostics)
--   held      true when the window produced no update (insufficient
--             samples) — the wiring counts this as a skipped update
--   probe_restore  the limit to return to after a baseline probe; nil
--             outside probes (see controller.common)
--
-- Config fields used (flattened by the limiter's config builder):
--   min_samples, sample_alpha, baseline_alpha, rtt_tolerance,
--   min_gradient, smoothing, headroom_min, headroom_max, min_limit,
--   max_limit, overload_min_samples, overload_failure_ratio,
--   overload_backoff, probe_interval, probe_fraction
--
-- Equations (per closed window):
--   short'  = sample_alpha   * mean_rtt + (1 - sample_alpha)   * short
--   long'   = baseline_alpha * short'  + (1 - baseline_alpha) * long
--             -- only on app-limited windows (no rejections): under
--             -- saturation the limit shapes the latency, and learning
--             -- it would normalize the queue (ratchet). Frozen as well
--             -- on windows whose strong-signal ratio exceeds
--             -- overload_failure_ratio. Under saturation the baseline
--             -- is re-measured by the periodic probe instead.
--   grad    = clamp(rtt_tolerance * long' / short', min_gradient, 1.0)
--   head    = rejected_count > 0
--             and clamp(sqrt(limit), headroom_min, headroom_max) or 0
--   cand    = limit * grad + head
--   cand    = min(cand, limit * overload_backoff)   -- on overload windows
--   limit'  = clamp(limit * (1 - smoothing) + cand * smoothing,
--                   min_limit, max_limit)
--
-- Insufficient latency samples hold the RTT state and limit unless enough
-- explicit failures independently trigger overload backoff.
-- A non-positive mean (sub-millisecond completions recorded as 0) does
-- not seed or move RTT state. With no positive baseline the gradient
-- stays 1 and no division is performed.

local clamp = require("resty.adaptive_limit.util.clamp")
local common = require("resty.adaptive_limit.controller.common")

local math_sqrt = math.sqrt
local math_min = math.min

local _M = {
    name = "gradient2",
}

function _M.update(state, m, cfg)
    local limit = state.limit
    local sc = m.sample_count

    local failure_count = m.overload_count + m.timeout_count
        + m.connect_error_count
    local completions = m.completions or sc
    local overloaded = completions > 0
        and failure_count / completions > cfg.overload_failure_ratio
    local backoff = overloaded and completions >= cfg.overload_min_samples

    local probe = common.probe_step(state, m, cfg, overloaded)
    if probe then
        return probe
    end

    if sc < cfg.min_samples and not backoff then
        return {
            limit = limit,
            long_rtt = common.usable_rtt(state.long_rtt) and state.long_rtt
                or nil,
            short_rtt = common.usable_rtt(state.short_rtt) and state.short_rtt
                or nil,
            gradient = state.gradient,
            held = true,
        }
    end

    -- strong overload signals only: 503s, timeouts and upstream connect
    -- failures; plain application errors (500-class) are never treated
    -- as capacity signals (design.md §4). The baseline is frozen on
    -- overloaded windows — including the very first one: seeding it from
    -- an overloaded RTT (restart mid-incident) would teach the controller
    -- that the overload is "healthy". The first usable non-overloaded
    -- window seeds it, rejections included, so a cold limiter that is
    -- already the bottleneck still learns a healthy backend.
    local short_rtt, long_rtt = common.observe_rtt(state, m, cfg, overloaded)

    local gradient = state.gradient
    if sc >= cfg.min_samples then
        gradient = 1.0
        if common.usable_rtt(long_rtt) and common.usable_rtt(short_rtt) then
            gradient = clamp(cfg.rtt_tolerance * long_rtt / short_rtt,
                             cfg.min_gradient, 1.0)
        end
    end

    -- Rejections prove offered demand reached the current cap. Without one,
    -- suppress positive headroom so low-concurrency traffic cannot drift the
    -- learned limit to max_limit; latency and overload can still reduce it.
    local headroom = 0
    if m.rejected_count > 0 then
        headroom = clamp(math_sqrt(limit), cfg.headroom_min, cfg.headroom_max)
    end
    local candidate = limit * (gradient or 1.0) + headroom

    if backoff then
        candidate = math_min(candidate, limit * cfg.overload_backoff)
    end

    local next_limit = limit * (1 - cfg.smoothing) + candidate * cfg.smoothing
    next_limit = clamp(next_limit, cfg.min_limit, cfg.max_limit)

    return common.probe_start({
        limit = next_limit,
        long_rtt = long_rtt,
        short_rtt = short_rtt,
        gradient = gradient,
        held = false,
    }, m, cfg)
end

return _M
