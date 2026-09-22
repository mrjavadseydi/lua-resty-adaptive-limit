-- Validation boundary shared by all controllers.
--
-- Controllers are pure and assume validated input (documented in each
-- module); every caller — the controller wiring in the limiter and the
-- test/simulation harnesses — goes through safe_update(), which validates
-- the state and the measurement, runs the update, and validates the
-- result. A controller failure therefore can never reach the shared
-- state: the wiring holds the previous limit and counts an internal
-- error instead (invariant 9).

local ewma = require("resty.adaptive_limit.util.ewma")

local _M = {}

local math_huge = math.huge
local math_ceil = math.ceil

-- How many windows a probe may stay reduced while waiting for pre-probe
-- admissions to drain. Past this, restore the limit and keep the old
-- baseline rather than learn a still-queued RTT.
local PROBE_OFFSET_CAP = 4

function _M.is_finite(v)
    -- NaN fails the first check (NaN ~= NaN); +-inf fail the others.
    return v == v and v < math_huge and v > -math_huge
end

-- A latency the controller may learn from. Zero is not a baseline:
-- ngx.now() has millisecond resolution, so a window of sub-millisecond
-- completions records mean_rtt = 0, and the next real sample then floors
-- the gradient. Non-positive stored state is treated as "not seeded".
function _M.usable_rtt(v)
    return type(v) == "number" and _M.is_finite(v) and v > 0
end

-- Update short/long RTT from one window. A non-positive mean updates
-- nothing. long_rtt still seeds from the first usable window even when
-- that window rejected: demand above the limit with a healthy backend
-- is how a cold limiter learns, and refusing it leaves the gradient at
-- 1 until the next probe.
function _M.observe_rtt(state, m, cfg, overloaded)
    local short_rtt = _M.usable_rtt(state.short_rtt) and state.short_rtt or nil
    local long_rtt = _M.usable_rtt(state.long_rtt) and state.long_rtt or nil
    if m.sample_count < cfg.min_samples or not _M.usable_rtt(m.mean_rtt) then
        return short_rtt, long_rtt
    end
    if short_rtt == nil then
        short_rtt = m.mean_rtt
    else
        short_rtt = ewma(short_rtt, m.mean_rtt, cfg.sample_alpha)
    end
    if not overloaded and (m.rejected_count == 0 or long_rtt == nil) then
        long_rtt = ewma(long_rtt, short_rtt, cfg.baseline_alpha)
    end
    return short_rtt, long_rtt
end

local function is_count(v)
    return type(v) == "number" and _M.is_finite(v)
        and v >= 0 and v % 1 == 0
end

-- Validate controller state. long_rtt/short_rtt may be nil before the
-- first sufficient window (controllers seed them from the first real
-- measurement; nothing is invented before that).
function _M.validate_state(state, cfg)
    if type(state) ~= "table" then
        return nil, "state must be a table"
    end
    local limit = state.limit
    if type(limit) ~= "number" or not _M.is_finite(limit)
        or limit < cfg.min_limit or limit > cfg.max_limit then
        return nil, "invalid limit"
    end
    local rtt = state.long_rtt
    if rtt ~= nil and (type(rtt) ~= "number" or not _M.is_finite(rtt) or rtt < 0) then
        return nil, "invalid long_rtt"
    end
    rtt = state.short_rtt
    if rtt ~= nil and (type(rtt) ~= "number" or not _M.is_finite(rtt) or rtt < 0) then
        return nil, "invalid short_rtt"
    end
    local pr = state.probe_restore
    if pr ~= nil and (type(pr) ~= "number" or not _M.is_finite(pr)
        or pr < cfg.min_limit or pr > cfg.max_limit) then
        return nil, "invalid probe_restore"
    end
    return true
end

-- Validate a window measurement, returning a normalized measurement or
-- nil + reason. Corruption detection bounds the outcome-class counts and
-- the sample count by the number of completed (non-ignored) outcomes —
-- never by the sample count itself: aborted outcomes and unusable
-- latencies are completions but not samples, so a legitimate abort-heavy
-- window has class counts exceeding sample_count. The wiring always
-- supplies completions and rejected_count; direct callers may omit them,
-- in which case the sum checks are skipped.
function _M.validate_measurement(m, cfg)
    if type(m) ~= "table" then
        return nil, "measurement must be a table"
    end
    local sc = m.sample_count
    if not is_count(sc) then
        return nil, "invalid sample_count"
    end

    local mean = m.mean_rtt or 0
    if sc > 0 then
        if type(mean) ~= "number" or not _M.is_finite(mean) or mean < 0 then
            return nil, "invalid mean_rtt"
        end
    else
        mean = 0
    end

    local ovl = m.overload_count or 0
    local tmo = m.timeout_count or 0
    local cer = m.connect_error_count or 0
    local err = m.error_count or 0
    local abt = m.aborted_count or 0
    local rej = m.rejected_count or 0
    if not is_count(ovl) or not is_count(tmo) or not is_count(cer)
        or not is_count(err) or not is_count(abt) or not is_count(rej) then
        return nil, "invalid outcome counts"
    end

    local win = m.window
    if win ~= nil and not is_count(win) then
        return nil, "invalid window"
    end

    local cmp = m.completions
    if cmp ~= nil then
        if not is_count(cmp) then
            return nil, "invalid completions"
        end
        if sc > cmp then
            return nil, "sample_count exceeds completions"
        end
        if ovl + tmo + cer + err + abt > cmp then
            return nil, "outcome counts exceed completions"
        end
    end

    return {
        sample_count = sc,
        mean_rtt = mean,
        overload_count = ovl,
        timeout_count = tmo,
        connect_error_count = cer,
        error_count = err,
        aborted_count = abt,
        rejected_count = rej,
        completions = cmp,
        window = win,
    }
end

-- Baseline probe (design.md §8). Under saturated demand (rejections in
-- the window) the limit itself shapes the observed latency, so a baseline
-- learned from such windows normalizes the very queue the controller is
-- supposed to shed: gradient 1.0, headroom growth, more queue, higher
-- baseline — a ratchet that only failures stop. Both controllers
-- therefore learn the baseline only from app-limited windows, and under
-- saturation re-measure it by probing: every probe_interval windows the
-- limit is published at limit * probe_fraction for two windows; the
-- first is polluted by requests admitted under the old limit (the
-- publish lands aggregation_grace into it), the second is the clean
-- measurement and re-seeds the baseline. Window phases come from the
-- shared window number, so every worker agrees without extra state.
--
-- probe_step(state, m, cfg, overloaded) returns the complete next state
-- when this window belongs to a probe (the caller returns it as-is), or
-- nil when the normal update runs. probe_start(next_state, m, cfg)
-- turns a normal update into the start of a probe when one is due.
-- The window the reduced limit is published into is polluted. Later
-- windows reseed only once sample_window has had time to drain one RTT
-- of pre-probe admissions; otherwise the probe limit is held (up to
-- PROBE_OFFSET_CAP) and then restored without learning the queued RTT.
local function hold_probe(state, restore)
    return {
        limit = state.limit,
        long_rtt = _M.usable_rtt(state.long_rtt) and state.long_rtt or nil,
        short_rtt = _M.usable_rtt(state.short_rtt) and state.short_rtt or nil,
        gradient = state.gradient,
        probe_restore = restore,
        held = true,
    }
end

local function restore_probe(state, restore, long_rtt)
    return {
        limit = restore,
        long_rtt = long_rtt,
        short_rtt = _M.usable_rtt(state.short_rtt) and state.short_rtt or nil,
        gradient = state.gradient,
        held = false,
    }
end

-- True when completions in this probe window were admitted under the
-- reduced limit. A completion was admitted about one RTT earlier, so
-- window offset k (1 = the window the probe was published into) is
-- clean only once k >= ceil(rtt / sample_window) + 1. Callers that do
-- not pass sample_window keep the historical "offset >= 2" rule.
local function probe_drained(mean, sample_window, offset)
    if type(sample_window) ~= "number" or not (sample_window > 0) then
        return offset >= 2
    end
    if not _M.usable_rtt(mean) then
        return false
    end
    local need = math_ceil(mean / sample_window) + 1
    if need < 2 then
        need = 2
    end
    return offset >= need
end

function _M.probe_step(state, m, cfg, overloaded)
    local restore = state.probe_restore
    local interval = cfg.probe_interval or 0
    if restore == nil or interval <= 0 or m.window == nil then
        return nil
    end
    local offset = m.window % interval
    -- The publish lands in the next window; that window is polluted by
    -- admissions taken at the old limit.
    if offset == 1 then
        return hold_probe(state, restore)
    end
    local long_rtt = _M.usable_rtt(state.long_rtt) and state.long_rtt or nil
    -- offset 0 with a probe still open is a skipped cycle: restore,
    -- do not learn from whatever window happened to land there.
    if offset == 0 then
        return restore_probe(state, restore, long_rtt)
    end

    local mean = m.mean_rtt
    local learn = m.sample_count >= cfg.min_samples and not overloaded
        and _M.usable_rtt(mean)
        and probe_drained(mean, cfg.sample_window, offset)
    if learn then
        return restore_probe(state, restore, mean)
    end
    -- RTT still longer than the windows since the cut: keep the reduced
    -- limit so a later window can be the clean sample. Give up at the
    -- cap and restore the previous baseline unchanged.
    if m.sample_count >= cfg.min_samples and not overloaded
        and _M.usable_rtt(mean) and offset < PROBE_OFFSET_CAP then
        return hold_probe(state, restore)
    end
    return restore_probe(state, restore, long_rtt)
end

function _M.probe_start(next_state, m, cfg)
    local interval = cfg.probe_interval or 0
    if interval > 0 and m.window ~= nil and m.window % interval == 0
        and m.rejected_count > 0 and next_state.long_rtt ~= nil then
        next_state.probe_restore = next_state.limit
        next_state.limit = math.max(cfg.min_limit,
            next_state.limit * cfg.probe_fraction)
    end
    return next_state
end

-- Repair a state table that failed validate_state (or wraps a limit whose
-- policy shrank across a reload) into one validate_state accepts: clamp
-- limit into [min_limit, max_limit] (falling back to cfg.initial_limit
-- when non-numeric/non-finite), and drop long_rtt/short_rtt when invalid
-- rather than inventing a value. Returns the repaired state and whether
-- anything was actually changed.
function _M.repair_state(state, cfg)
    local repaired = false

    local limit = state.limit
    if type(limit) ~= "number" or not _M.is_finite(limit) then
        limit = cfg.initial_limit
        repaired = true
    elseif limit < cfg.min_limit then
        limit = cfg.min_limit
        repaired = true
    elseif limit > cfg.max_limit then
        limit = cfg.max_limit
        repaired = true
    end

    local long_rtt = state.long_rtt
    if long_rtt ~= nil and (type(long_rtt) ~= "number"
        or not _M.is_finite(long_rtt) or long_rtt < 0) then
        long_rtt = nil
        repaired = true
    end

    local short_rtt = state.short_rtt
    if short_rtt ~= nil and (type(short_rtt) ~= "number"
        or not _M.is_finite(short_rtt) or short_rtt < 0) then
        short_rtt = nil
        repaired = true
    end

    local pr = state.probe_restore
    if pr ~= nil and (type(pr) ~= "number" or not _M.is_finite(pr)
        or pr < cfg.min_limit or pr > cfg.max_limit) then
        pr = nil
        repaired = true
    end

    return { limit = limit, long_rtt = long_rtt, short_rtt = short_rtt,
        probe_restore = pr }, repaired
end

-- safe_update(algorithm, state, measurement, cfg) -> next_state | nil, err
--
-- The single entry point the wiring (and tests) use. On any validation
-- failure of the input or of the algorithm's output, the caller receives
-- nil + reason and must hold the previous state.
function _M.safe_update(algorithm, state, m, cfg)
    local ok, err = _M.validate_state(state, cfg)
    if not ok then
        return nil, "state: " .. (err or "?")
    end

    local mm, merr = _M.validate_measurement(m, cfg)
    if not mm then
        return nil, "measurement: " .. (merr or "?")
    end

    local next_state, aerr = algorithm.update(state, mm, cfg)
    if type(next_state) ~= "table" then
        return nil, "algorithm returned no state: " .. tostring(aerr)
    end

    ok, err = _M.validate_state(next_state, cfg)
    if not ok then
        return nil, "algorithm output invalid: " .. (err or "?")
    end
    local rtt = next_state.long_rtt
    if rtt ~= nil and not _M.is_finite(rtt) then
        return nil, "algorithm output invalid: long_rtt not finite"
    end
    rtt = next_state.short_rtt
    if rtt ~= nil and not _M.is_finite(rtt) then
        return nil, "algorithm output invalid: short_rtt not finite"
    end

    return next_state
end

return _M
