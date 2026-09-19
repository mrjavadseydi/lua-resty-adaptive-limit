-- AIMD adaptive concurrency controller (reference / fallback algorithm).
--
-- Deliberately simple: healthy window → additive increase; congested
-- window → multiplicative decrease. Used for comparison against
-- Gradient2, for testing, and for deployments that prefer a dead-simple
-- controller. Shares the RTT bookkeeping, validation and safety rules of
-- Gradient2 (same state shape, same config fields plus:
--   aimd_increment  -- additive increase per healthy window (default 1)
--   aimd_decrease   -- multiplicative decrease factor (default 0.8)
-- ).

local clamp = require("resty.adaptive_limit.util.clamp")
local ewma = require("resty.adaptive_limit.util.ewma")

local _M = {
    name = "aimd",
}

function _M.update(state, m, cfg)
    local limit = state.limit
    local sc = m.sample_count

    local failure_count = m.overload_count + m.timeout_count
        + m.connect_error_count
    local completions = m.completions or sc
    local overloaded = completions > 0
        and failure_count / completions > cfg.overload_failure_ratio
    local strong_overload = overloaded
        and completions >= cfg.overload_min_samples

    if sc < cfg.min_samples and not strong_overload then
        return {
            limit = limit,
            long_rtt = state.long_rtt,
            short_rtt = state.short_rtt,
            gradient = state.gradient,
            held = true,
        }
    end

    local short_rtt = state.short_rtt
    if sc >= cfg.min_samples and short_rtt == nil then
        short_rtt = m.mean_rtt
    elseif sc >= cfg.min_samples then
        short_rtt = ewma(short_rtt, m.mean_rtt, cfg.sample_alpha)
    end

    -- baseline frozen on congested windows, the first one included (see
    -- gradient2.lua): never learn an overloaded RTT as healthy
    local long_rtt = state.long_rtt
    if sc >= cfg.min_samples and not overloaded then
        long_rtt = ewma(long_rtt, short_rtt, cfg.baseline_alpha)
    end

    -- Latency congestion: the smoothed RTT exceeded rtt_tolerance times
    -- the baseline (the same shedding trigger Gradient2 uses, as a
    -- boolean instead of a gradient).
    local congested = strong_overload
    if not congested and sc >= cfg.min_samples
        and long_rtt ~= nil and long_rtt > 0
        and short_rtt > cfg.rtt_tolerance * long_rtt then
        congested = true
    end

    local next_limit
    if congested then
        next_limit = limit * cfg.aimd_decrease
    elseif m.rejected_count == 0 then
        next_limit = limit
    else
        next_limit = limit + cfg.aimd_increment
    end

    next_limit = clamp(next_limit, cfg.min_limit, cfg.max_limit)

    return {
        limit = next_limit,
        long_rtt = long_rtt,
        short_rtt = short_rtt,
        gradient = congested and 0 or 1,
        held = false,
    }
end

return _M
