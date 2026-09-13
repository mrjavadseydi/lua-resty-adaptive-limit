-- The limiter instance: admission, release, and (from later phases)
-- lifecycle helpers, statistics and controller wiring.
--
-- Concurrency notes that the whole file depends on:
--
--  * An OpenResty worker runs Lua cooperatively in a single VM. A
--    sequence of Lua statements with no yield point is effectively
--    atomic within the worker; every yield-capable call here is in the
--    shared dictionary, and none of our read-modify-write pairs on
--    worker-local state contain one.
--  * Cross-worker state lives exclusively in the shared dictionary and
--    moves only through its atomic operations. The admission
--    linearization point is dict:incr(inflight, 1): every successful
--    increment receives a distinct resulting value, so more than `limit`
--    requests can never hold slots at a stable limit.

local errors = require("resty.adaptive_limit.errors")
local config_mod = require("resty.adaptive_limit.config")
local state_mod = require("resty.adaptive_limit.state")
local runtime = require("resty.adaptive_limit.runtime")

local ngx_now = ngx.now
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_WARN = ngx.WARN
local ngx_shared = ngx.shared

-- Outcomes accepted by release(). "ignored" excludes the sample from
-- controller statistics entirely (health checks, synthetic probes).
local OUTCOMES = {
    success = 1, timeout = 1, connect_error = 1, overload = 1,
    error = 1, aborted = 1, ignored = 1,
}

-- Sanity clamp for externally supplied latency values (spec §31: a
-- malformed latency must not corrupt the stats). Values outside
-- [0, LATENCY_CAP] are counted as anomalies and not sampled.
local LATENCY_CAP = 3600

-- Rate-limited logging: at most one message per kind per LOG_INTERVAL
-- seconds per worker (spec §11: surface violations, never spam).
local LOG_INTERVAL = 1.0

local _M = {}
local mt = { __index = _M }

-- rate_limited_log(self, kind, level, ...)
-- Returns true if the message was emitted within the budget.
local function rate_limited_log(self, kind, level, msg)
    local now = ngx_now()
    local last = self._log_last[kind]
    if last and now - last < LOG_INTERVAL then
        return false
    end
    self._log_last[kind] = now
    ngx_log(level, "adaptive_limit[", self.cfg.name, "] ", msg)
    return true
end

local function count_anomaly(self, kind)
    self.anomalies[kind] = (self.anomalies[kind] or 0) + 1
    local hook = self.cfg.on_anomaly
    if hook then
        hook(kind, nil)
    end
end

-- Classify a raw shared-dict error into a limiter-internal failure.
local function internal_error(self, where, err)
    self.internal_errors = self.internal_errors + 1
    rate_limited_log(self, "internal", ngx_ERR,
        where, " failed: ", err or "unknown")
    return nil, errors.INTERNAL_ERROR
end

function _M.new(user_cfg)
    local cfg, err = config_mod.build(user_cfg)
    if not cfg then
        return nil, err
    end

    local dict = ngx_shared and ngx_shared[cfg.shared_dict] or nil
    local st, serr = state_mod.new(dict, cfg.name)
    if not st then
        return nil, "adaptive_limit: shared dict \"" .. cfg.shared_dict
            .. "\": " .. tostring(serr)
    end

    local algorithm = require("resty.adaptive_limit.controller."
        .. (cfg.algorithm == "aimd" and "aimd" or "gradient2"))

    if runtime.registry[cfg.name] then
        return nil, "adaptive_limit: duplicate limiter name \""
            .. cfg.name .. "\""
    end

    -- Precomputed, hot-path-only fields come first; the fast path reads
    -- nothing it does not need.
    local limiter = {
        cfg = cfg,
        st = st,
        algorithm = algorithm,

        -- shared-dict key handles
        K_limit = st.K.limit,
        K_inflight = st.K.inflight,

        -- worker-local inflight mirror (see top comment: exact without
        -- synchronization because no yield sits between the paired
        -- updates)
        _inflight = 0,
        _last_limit = nil,

        -- fixed-size worker-local statistics. Nothing here is ever
        -- indexed by request; no structure grows with traffic.
        stats = {
            sample_count = 0,
            latency_sum = 0,
            success = 0,
            timeout = 0,
            connect_error = 0,
            overload = 0,
            error = 0,
            aborted = 0,
            admitted_total = 0,
            rejected_total = 0,
            last_completion = nil,
        },

        internal_errors = 0,
        anomalies = {},

        _log_last = {},
        -- ngx.ctx key, precomputed once ("alim:" .. name)
        _ctx_key = "alim:" .. cfg.name,

        -- flush bookkeeping (scheduler)
        _flush_window = nil,
    }

    runtime.registry[cfg.name] = limiter
    runtime.order[#runtime.order + 1] = limiter

    return setmetatable(limiter, mt)
end

function _M:reseed_shared_state()
    local st = self.st
    local cfg = self.cfg

    st:write_schema()

    -- A fresh limiter starts at initial_limit. A restart with existing
    -- state adopts it (checked by the caller before calling this).
    self.st.dict:set(self.K_limit, cfg.initial_limit)
    self.st.dict:set(st.K.inflight, 0)
    self.st.dict:set(st.K.last_window, 0)
    self.st.dict:set(st.K.last_update, ngx_now())
    self._last_limit = cfg.initial_limit
end

-- Validate the shared state schema marker; reseed if absent, refuse on
-- mismatch. Called from adaptive.start() (init_worker) and after schema
-- errors detected at runtime.
function _M:check_schema()
    local st = self.st
    local v, err = st:read_schema()
    if v == nil then
        if err and err ~= "not found" then
            return nil, errors.INTERNAL_ERROR
        end
        -- no marker: first worker here, or the dict was flushed
        self:reseed_shared_state()
        return true
    end
    if v ~= tostring(state_mod.SCHEMA_VERSION) then
        rate_limited_log(self, "schema", ngx_ERR,
            "shared state schema version ", tostring(v),
            " is not supported (expected ", tostring(state_mod.SCHEMA_VERSION),
            "); refusing to reuse incompatible state")
        return nil, errors.INVALID_STATE
    end
    return true
end

-- Adopt existing shared controller state after a reload (graceful HUP):
-- the learned limit must survive. Returns the adopted limit.
function _M:adopt_shared_state()
    local st = self.st
    local cs, err = st:read_controller_state()
    if not cs then
        -- genuine dict error: let the caller surface it; the learned
        -- state is unknown, not absent
        return nil, errors.INTERNAL_ERROR
    end
    if cs.limit == nil or type(cs.limit) ~= "number"
        or cs.limit ~= cs.limit then
        -- absent or corrupt: fall back to a reseed
        self:reseed_shared_state()
        return self.cfg.initial_limit
    end
    self._last_limit = cs.limit
    return cs.limit
end

------------------------------------------------------------------------
-- Admission (fast path)
------------------------------------------------------------------------

--- Try to acquire one concurrency slot.
-- Returns true, or nil + one of errors.REJECTED / errors.INTERNAL_ERROR /
-- errors.NOT_STARTED / errors.INVALID_STATE.
function _M:try_acquire()
    if not runtime.started then
        return nil, errors.NOT_STARTED
    end

    local dict = self.st.dict

    local limit, err = dict:get(self.K_limit)
    if limit == nil then
        if err and err ~= "not found" then
            return internal_error(self, "get(limit)", err)
        end
        -- Critical key missing (eviction, flushed dict, or first boot).
        -- Re-seed from this worker's last observed limit (fresh to within
        -- one window) and surface the anomaly instead of admitting
        -- unbounded.
        self.anomalies.limit_missing = (self.anomalies.limit_missing or 0) + 1
        limit = self._last_limit or self.cfg.initial_limit
        dict:set(self.K_limit, limit)
        rate_limited_log(self, "limit_missing", ngx_WARN,
            "limit key missing; re-seeded from last observed value")
    end
    self._last_limit = limit

    -- Admission linearization point (see top-of-file comment).
    local n, ierr = dict:incr(self.K_inflight, 1, 0)
    if not n then
        return internal_error(self, "incr(inflight)", ierr)
    end

    if n <= limit then
        self._inflight = self._inflight + 1
        self.stats.admitted_total = self.stats.admitted_total + 1
        return true
    end

    -- Over the limit: roll the reservation back unconditionally.
    dict:incr(self.K_inflight, -1)
    self.stats.rejected_total = self.stats.rejected_total + 1
    return nil, errors.REJECTED
end

------------------------------------------------------------------------
-- Release (fast path)
------------------------------------------------------------------------

-- Validate and normalize a latency sample; returns usable number or nil.
local function sanitize_latency(self, latency)
    if latency == nil then
        return nil
    end
    if type(latency) ~= "number" or latency ~= latency
        or latency < 0 or latency > LATENCY_CAP then
        count_anomaly(self, "bad_latency")
        return nil
    end
    return latency
end

--- Release one slot and record the completion observation.
-- latency: seconds (number) or nil; outcome: see design.md §10.
-- Accounting happens before observation; observation failures can never
-- prevent the release (invariant 10).
function _M:release(latency, outcome)
    if not runtime.started then
        return nil, errors.NOT_STARTED
    end
    if outcome ~= nil and not OUTCOMES[outcome] then
        return nil, errors.INVALID_STATE
    end
    outcome = outcome or "success"

    local dict = self.st.dict

    -- 1. Release the slot. This must succeed for accounting to hold.
    local n, err = dict:incr(self.K_inflight, -1)
    if not n then
        -- The slot leaks; surfaced via internal_errors and the stuck
        -- diagnostics. The caller may retry the release once.
        return internal_error(self, "incr(inflight, -1)", err)
    end
    if n < 0 then
        -- Double release (or a lost admission): snap to zero and surface.
        self.anomalies.negative_inflight =
            (self.anomalies.negative_inflight or 0) + 1
        dict:set(self.K_inflight, 0)
        rate_limited_log(self, "negative", ngx_WARN,
            "inflight went negative; double release suspected")
    end
    if self._inflight > 0 then
        self._inflight = self._inflight - 1
    end

    -- 2. Record the observation (never blocks the release: fixed-size
    --    struct updates only).
    if outcome ~= "ignored" then
        local s = self.stats
        local latency_n = sanitize_latency(self, latency)
        if outcome ~= "aborted" then
            -- Client aborts release the slot but their (truncated)
            -- duration is not a capacity signal.
            s.sample_count = s.sample_count + 1
            if latency_n then
                s.latency_sum = s.latency_sum + latency_n
            end
        end
        if outcome == "success" then
            s.success = s.success + 1
        elseif outcome == "timeout" then
            s.timeout = s.timeout + 1
        elseif outcome == "connect_error" then
            s.connect_error = s.connect_error + 1
        elseif outcome == "overload" then
            s.overload = s.overload + 1
        elseif outcome == "error" then
            s.error = s.error + 1
        elseif outcome == "aborted" then
            s.aborted = s.aborted + 1
        end
        s.last_completion = ngx_now()
    end

    return true
end

-- Exposed for tests and the scheduler.
_M.rate_limited_log = rate_limited_log
_M.count_anomaly = count_anomaly
_M.OUTCOMES = OUTCOMES

return _M
