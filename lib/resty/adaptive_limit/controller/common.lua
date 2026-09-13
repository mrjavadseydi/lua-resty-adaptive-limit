-- Validation boundary shared by all controllers.
--
-- Controllers are pure and assume validated input (documented in each
-- module); every caller — the controller wiring in the limiter and the
-- test/simulation harnesses — goes through safe_update(), which validates
-- the state and the measurement, runs the update, and validates the
-- result. A controller failure therefore can never reach the shared
-- state: the wiring holds the previous limit and counts an internal
-- error instead (invariant 9).

local _M = {}

local math_huge = math.huge

function _M.is_finite(v)
    -- NaN fails the first check (NaN ~= NaN); +-inf fail the others.
    return v == v and v < math_huge and v > -math_huge
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
    return true
end

-- Validate a window measurement, returning a normalized measurement or
-- nil + reason. Corruption detection includes class counts exceeding the
-- sample count (impossible without shared-state corruption).
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
    local err = m.error_count or 0
    local abt = m.aborted_count or 0
    if not is_count(ovl) or not is_count(tmo)
        or not is_count(err) or not is_count(abt) then
        return nil, "invalid outcome counts"
    end
    if ovl + tmo + err + abt > sc then
        return nil, "outcome counts exceed sample_count"
    end

    return {
        sample_count = sc,
        mean_rtt = mean,
        overload_count = ovl,
        timeout_count = tmo,
        error_count = err,
        aborted_count = abt,
    }
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
