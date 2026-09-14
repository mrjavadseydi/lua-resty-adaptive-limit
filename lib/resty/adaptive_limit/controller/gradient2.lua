-- Gradient2 adaptive concurrency controller (default algorithm).
--
-- Pure and deterministic: no ngx, no I/O, no clocks. Inputs are validated
-- by controller.common.safe_update() before update() is called; the
-- output is validated again on return. state fields:
--
--   limit     current limit (float internally; published as an integer
--             by the wiring at the publication boundary)
--   long_rtt  slow EWMA of healthy RTT (the capacity baseline); nil
--             before the first sufficient window
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
--   head    = clamp(sqrt(limit), headroom_min, headroom_max)
--   cand    = limit * grad + head
--   cand    = min(cand, limit * overload_backoff)   -- on overload windows
--   limit'  = clamp(limit * (1 - smoothing) + cand * smoothing,
--                   min_limit, max_limit)
--
-- Insufficient samples: no update at all (invariant 5) — low-traffic
-- windows must not move the limit, the baseline, or anything else.
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

    if sc < cfg.min_samples then
        return {
            limit = limit,
            long_rtt = state.long_rtt,
            short_rtt = state.short_rtt,
            gradient = state.gradient,
            held = true,
        }
    end

    local mean = m.mean_rtt

    local short_rtt
    if state.short_rtt == nil then
        -- First sufficient window seeds the RTT state directly instead
        -- of pretending some default RTT was observed.
        short_rtt = mean
    else
        short_rtt = ewma(state.short_rtt, mean, cfg.sample_alpha)
    end

    local failure_count = m.overload_count + m.timeout_count
        + m.connect_error_count
    -- strong overload signals only: 503s, timeouts and upstream connect
    -- failures; plain application errors (500-class) are never treated
    -- as capacity signals (spec §14)
    local overloaded = failure_count / sc > cfg.overload_failure_ratio

    local long_rtt = state.long_rtt
    if not overloaded then
        long_rtt = ewma(long_rtt, short_rtt, cfg.baseline_alpha)
    end
    if long_rtt == nil then
        long_rtt = short_rtt
    end

    local gradient = 1.0
    if short_rtt > 0 then
        gradient = clamp(cfg.rtt_tolerance * long_rtt / short_rtt,
                         cfg.min_gradient, 1.0)
    end

    local headroom = clamp(math_sqrt(limit), cfg.headroom_min, cfg.headroom_max)
    local candidate = limit * gradient + headroom

    if overloaded and sc >= cfg.overload_min_samples then
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
