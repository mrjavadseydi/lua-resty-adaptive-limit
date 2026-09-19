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
--
-- Config fields used (flattened by the limiter's config builder):
--   min_samples, sample_alpha, baseline_alpha, rtt_tolerance,
--   min_gradient, smoothing, headroom_min, headroom_max, min_limit,
--   max_limit, overload_min_samples, overload_failure_ratio,
--   overload_backoff
--
-- Equations (per closed window):
--   short'  = sample_alpha   * mean_rtt + (1 - sample_alpha)   * short
--   long'   = baseline_alpha * short'  + (1 - baseline_alpha) * long
--             -- frozen on windows whose strong-signal ratio exceeds
--             -- overload_failure_ratio: an overload must not be
--             -- normalized into the baseline
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
-- short' == 0 (all zero-RTT samples, e.g. a mock backend) is treated as
-- perfectly healthy: gradient 1, no division performed.

local clamp = require("resty.adaptive_limit.util.clamp")
local ewma = require("resty.adaptive_limit.util.ewma")

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

    if sc < cfg.min_samples and not backoff then
        return {
            limit = limit,
            long_rtt = state.long_rtt,
            short_rtt = state.short_rtt,
            gradient = state.gradient,
            held = true,
        }
    end

    local short_rtt = state.short_rtt
    local long_rtt = state.long_rtt
    if sc >= cfg.min_samples and short_rtt == nil then
        -- First sufficient window seeds the RTT state directly instead
        -- of pretending some default RTT was observed.
        short_rtt = m.mean_rtt
    elseif sc >= cfg.min_samples then
        short_rtt = ewma(short_rtt, m.mean_rtt, cfg.sample_alpha)
    end

    -- strong overload signals only: 503s, timeouts and upstream connect
    -- failures; plain application errors (500-class) are never treated
    -- as capacity signals (design.md §4)
    -- The baseline is frozen on overloaded windows — including the very
    -- first one: seeding it from an overloaded RTT (restart mid-incident)
    -- would teach the controller that the overload is "healthy". With no
    -- baseline yet the gradient stays 1.0 and only the overload backoff
    -- acts; the first healthy window seeds it.
    if sc >= cfg.min_samples and not overloaded then
        long_rtt = ewma(long_rtt, short_rtt, cfg.baseline_alpha)
    end

    local gradient = state.gradient
    if sc >= cfg.min_samples then
        gradient = 1.0
        if long_rtt ~= nil and short_rtt > 0 then
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

    return {
        limit = next_limit,
        long_rtt = long_rtt,
        short_rtt = short_rtt,
        gradient = gradient,
        held = false,
    }
end

return _M
