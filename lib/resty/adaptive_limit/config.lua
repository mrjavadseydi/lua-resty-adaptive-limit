-- Configuration building and validation (spec §30).
--
-- Everything here runs once, in adaptive.new() — never per request. The
-- result is a *flattened* config of scalar fields: the request path and
-- the controllers only ever touch cfg.<scalar>, no nested table lookups.
--
-- Invalid configuration is a startup error with a descriptive message,
-- never a silently accepted value.

local _M = {}

local ALGORITHMS = {
    gradient2 = true,
    aimd = true,
}

local PROFILES = {
    -- conservative: sheds early, grows slowly, backs off hard — for
    -- critical backends where queue formation is expensive
    conservative = {
        rtt_tolerance = 1.5,
        min_gradient = 0.5,
        smoothing = 0.35,
        overload_failure_ratio = 0.05,
        overload_backoff = 0.7,
        min_samples = 30,
    },
    -- balanced: the documented defaults
    balanced = {},
    -- responsive: tolerates bursts before shedding (2.5x RTT), then reacts
    -- and recovers quickly — for spiky workloads with cheap queueing
    responsive = {
        rtt_tolerance = 2.5,
        smoothing = 0.65,
        overload_failure_ratio = 0.15,
        overload_backoff = 0.85,
        min_samples = 10,
    },
}

local FAILURE_MODES = {
    fail_open = true,
    fail_closed = true,
}

local LATENCY_SOURCES = {
    request_time = true,
    upstream_response_time = true,
    manual = true,
}

local UPSTREAM_CHOICES = {
    last = true,
    max = true,
    sum = true,
}

-- Limiter names become shared-dict key components and log prefixes.
-- Bounded, lowercase, no whitespace/control characters. Lua patterns, no
-- regex engine: validation runs only at startup anyway.
local NAME_PATTERN = "^%l[%l%d%.%-%_]*$"
local NAME_MAX = 63

local function fail(msg)
    return nil, "adaptive_limit config: " .. msg
end

local function positive_num(v, name)
    if type(v) ~= "number" or v ~= v or v <= 0 or v == math.huge then
        return fail(name .. " must be a positive number")
    end
end

local function integer_in_range(v, name, lo, hi)
    if type(v) ~= "number" or v % 1 ~= 0 or v < lo or v > hi then
        return fail(name .. string.format(" must be an integer in [%d, %d]", lo, hi))
    end
end

function _M.build(user)
    if type(user) ~= "table" then
        return fail("options table required")
    end

    local name = user.name
    if type(name) ~= "string" or #name == 0 or #name > NAME_MAX
        or not name:match(NAME_PATTERN) then
        return fail("name must match [a-z][a-z0-9_.-]{0,62}")
    end

    local shared_dict = user.shared_dict
    if type(shared_dict) ~= "string" or #shared_dict == 0 then
        return fail("shared_dict must be the name of a lua_shared_dict zone")
    end

    local algorithm = user.algorithm or "gradient2"
    if not ALGORITHMS[algorithm] then
        return fail("algorithm must be \"gradient2\" or \"aimd\"")
    end

    local profile_name = user.profile or "balanced"
    local profile = PROFILES[profile_name]
    if not profile then
        return fail("profile must be \"conservative\", \"balanced\" or \"responsive\"")
    end

    -- helper: option > profile (explicit configuration always wins)
    local function opt(key, default)
        local v = user[key]
        if v == nil then
            v = profile[key]
        end
        if v == nil then
            v = default
        end
        return v
    end

    local failure_mode = user.failure_mode or "fail_open"
    if not FAILURE_MODES[failure_mode] then
        return fail("failure_mode must be \"fail_open\" or \"fail_closed\"")
    end

    local cfg = {
        name = name,
        shared_dict = shared_dict,
        algorithm = algorithm,
        failure_mode = failure_mode,
        latency_source = user.latency_source or "request_time",
        upstream_time_choice = user.upstream_time_choice or "last",
        rejection_status = user.rejection_status or 503,
        retry_after = user.retry_after or 1,
        stale_threshold = opt("stale_threshold", 30),
        on_update = user.on_update,
        on_anomaly = user.on_anomaly,
        outcome_classifier = user.outcome_classifier,
    }

    if not LATENCY_SOURCES[cfg.latency_source] then
        return fail("latency_source must be \"request_time\", " ..
            "\"upstream_response_time\" or \"manual\"")
    end
    if not UPSTREAM_CHOICES[cfg.upstream_time_choice] then
        return fail("upstream_time_choice must be \"last\", \"max\" or \"sum\"")
    end

    local status_err = integer_in_range(cfg.rejection_status,
        "rejection_status", 400, 599)
    if status_err then return status_err end

    if type(cfg.retry_after) ~= "number" or cfg.retry_after < 0 then
        return fail("retry_after must be a non-negative number")
    end

    if type(cfg.stale_threshold) ~= "number" or cfg.stale_threshold <= 0 then
        return fail("stale_threshold must be a positive number")
    end
    if cfg.on_update ~= nil and type(cfg.on_update) ~= "function" then
        return fail("on_update must be a function")
    end
    if cfg.on_anomaly ~= nil and type(cfg.on_anomaly) ~= "function" then
        return fail("on_anomaly must be a function")
    end
    if cfg.outcome_classifier ~= nil
        and type(cfg.outcome_classifier) ~= "function" then
        return fail("outcome_classifier must be a function")
    end

    -- limits
    cfg.min_limit = opt("min_limit", 1)
    cfg.max_limit = opt("max_limit", 2000)
    cfg.initial_limit = opt("initial_limit", 50)

    local err = integer_in_range(cfg.min_limit, "min_limit", 1, 2 ^ 31)
    if err then return err end
    err = integer_in_range(cfg.max_limit, "max_limit", 1, 2 ^ 31)
    if err then return err end
    err = integer_in_range(cfg.initial_limit, "initial_limit", 1, 2 ^ 31)
    if err then return err end
    if cfg.min_limit > cfg.max_limit then
        return fail("min_limit must be <= max_limit")
    end
    if cfg.initial_limit < cfg.min_limit
        or cfg.initial_limit > cfg.max_limit then
        return fail("initial_limit must be within [min_limit, max_limit]")
    end

    -- windows
    cfg.sample_window = opt("sample_window", 1.0)
    err = positive_num(cfg.sample_window, "sample_window")
    if err then return err end

    cfg.min_samples = opt("min_samples", 20)
    err = integer_in_range(cfg.min_samples, "min_samples", 1, 2 ^ 31)
    if err then return err end

    -- controller knobs
    cfg.rtt_tolerance = opt("rtt_tolerance", 2.0)
    if type(cfg.rtt_tolerance) ~= "number" or cfg.rtt_tolerance <= 1
        or cfg.rtt_tolerance ~= cfg.rtt_tolerance then
        return fail("rtt_tolerance must be a number > 1")
    end

    cfg.min_gradient = opt("min_gradient", 0.5)
    if type(cfg.min_gradient) ~= "number" or cfg.min_gradient <= 0
        or cfg.min_gradient >= 1 then
        return fail("min_gradient must be in (0, 1)")
    end

    cfg.smoothing = opt("smoothing", 0.5)
    if type(cfg.smoothing) ~= "number" or cfg.smoothing <= 0
        or cfg.smoothing > 1 then
        return fail("smoothing must be in (0, 1]")
    end

    cfg.headroom_min = opt("headroom_min", 1)
    cfg.headroom_max = opt("headroom_max", 50)
    if type(cfg.headroom_min) ~= "number" or cfg.headroom_min < 0 then
        return fail("headroom_min must be a non-negative number")
    end
    if type(cfg.headroom_max) ~= "number"
        or cfg.headroom_max < cfg.headroom_min then
        return fail("headroom_max must be >= headroom_min")
    end

    cfg.baseline_alpha = opt("baseline_alpha", 0.05)
    if type(cfg.baseline_alpha) ~= "number" or cfg.baseline_alpha <= 0
        or cfg.baseline_alpha >= 1 then
        return fail("baseline_alpha must be in (0, 1)")
    end

    cfg.sample_alpha = opt("sample_alpha", 0.5)
    if type(cfg.sample_alpha) ~= "number" or cfg.sample_alpha <= 0
        or cfg.sample_alpha > 1 then
        return fail("sample_alpha must be in (0, 1]")
    end

    -- overload backoff
    cfg.overload_min_samples = opt("overload_min_samples", 20)
    err = integer_in_range(cfg.overload_min_samples,
        "overload.min_samples", 1, 2 ^ 31)
    if err then return err end

    cfg.overload_failure_ratio = opt("overload_failure_ratio", 0.10)
    if type(cfg.overload_failure_ratio) ~= "number"
        or cfg.overload_failure_ratio <= 0
        or cfg.overload_failure_ratio > 1 then
        return fail("overload.failure_ratio must be in (0, 1]")
    end

    cfg.overload_backoff = opt("overload_backoff", 0.80)
    if type(cfg.overload_backoff) ~= "number" or cfg.overload_backoff <= 0
        or cfg.overload_backoff >= 1 then
        return fail("overload.backoff must be in (0, 1)")
    end

    -- aimd-specific
    cfg.aimd_increment = opt("aimd_increment", 1)
    if type(cfg.aimd_increment) ~= "number" or cfg.aimd_increment <= 0 then
        return fail("aimd_increment must be a positive number")
    end
    cfg.aimd_decrease = opt("aimd_decrease", 0.8)
    if type(cfg.aimd_decrease) ~= "number" or cfg.aimd_decrease <= 0
        or cfg.aimd_decrease >= 1 then
        return fail("aimd_decrease must be in (0, 1)")
    end

    return cfg
end

return _M
