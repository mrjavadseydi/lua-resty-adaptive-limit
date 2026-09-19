-- lua-resty-adaptive-limit
--
-- Adaptive concurrency limiting and load shedding for OpenResty: the
-- allowed number of concurrent requests adjusts automatically from
-- observed completion latency and explicit overload signals.
--
-- See design.md for the architecture, README.md for usage, and
-- spec/ + t/ for verification. Entry points:
--
--   local adaptive = require "resty.adaptive_limit"
--   local limiter  = assert(adaptive.new({ name = "payments",
--       shared_dict = "adaptive_limit" }))
--   -- init_worker_by_lua:  adaptive.start()
--   -- access_by_lua:       adaptive.get("payments"):guard()
--   -- log_by_lua:          adaptive.get("payments"):log()
--   -- exit_worker_by_lua:  adaptive.exit()

local errors = require("resty.adaptive_limit.errors")
local limiter_mod = require("resty.adaptive_limit.limiter")
local state_mod = require("resty.adaptive_limit.state")
local scheduler = require("resty.adaptive_limit.scheduler")
local runtime = require("resty.adaptive_limit.runtime")

local ngx_get_phase = ngx.get_phase

local _M = {
    _VERSION = "0.1.0",
    errors = errors,
}

--- Create (or, per worker VM, re-create) a named limiter.
-- Configuration is validated here, once; an invalid configuration is a
-- startup error, never a runtime surprise. Typically called from a module
-- required by init_by_lua (early validation) and re-executed by every
-- worker (fresh per-worker instance registered with the scheduler).
--
-- Returns nil + message on invalid configuration, an unusable shared
-- dictionary, or a duplicate name within this worker.
function _M.new(opts)
    return limiter_mod.new(opts)
end

--- Start the per-worker scheduler. Call once from init_worker_by_lua*.
-- Validates and adopts shared state (learned limits survive a graceful
-- reload), then starts the single timer that drives statistics flushing,
-- heartbeats and controller updates for every registered limiter.
function _M.start(opts)
    if ngx_get_phase() == "init" then
        return nil, "adaptive_limit: start() must be called from " ..
            "init_worker_by_lua*, not init_by_lua*"
    end

    opts = opts or {}
    if opts.flush_interval ~= nil then
        if type(opts.flush_interval) ~= "number"
            or opts.flush_interval ~= opts.flush_interval
            or opts.flush_interval <= 0
            or opts.flush_interval == math.huge then
            return nil, "adaptive_limit: flush_interval must be a positive number"
        end
        runtime.flush_interval = opts.flush_interval
    end

    local order = runtime.order
    for i = 1, #order do
        local limiter = order[i]
        local ok, err = limiter:check_schema()
        if not ok then
            -- INVALID_STATE (foreign schema) is a hard error: silently
            -- re-initializing would discard a learned limit.
            if err == errors.INVALID_STATE then
                runtime.started = false
                return nil, "adaptive_limit: limiter \"" .. limiter.cfg.name ..
                    "\": incompatible shared state schema (expected version " ..
                    tostring(state_mod.SCHEMA_VERSION) .. ")"
            end
            return nil, "adaptive_limit: limiter \"" .. limiter.cfg.name ..
                "\": schema check failed: " .. tostring(err)
        end
        local adopted, aerr = limiter:adopt_shared_state()
        if not adopted and aerr then
            runtime.started = false
            return nil, "adaptive_limit: limiter \"" .. limiter.cfg.name ..
                "\": adopt shared state failed: " .. tostring(aerr)
        end
        -- first heartbeat before the scheduler's first tick, so worker
        -- liveness is correct from the moment start() returns
        local wid = ngx.worker.id()
        if wid ~= nil then
            limiter:heartbeat(wid)
        end
        -- The scheduler assumes it ticks at least once per window and
        -- well within the heartbeat TTL. A slower cadence is legal (tests
        -- drive ticks by hand) but in production it silently drops
        -- windows and flaps workers_active — say so.
        local fi = runtime.flush_interval
        if fi > limiter.cfg.sample_window or fi >= limiter_mod.HB_TTL then
            ngx.log(ngx.WARN, "adaptive_limit[", limiter.cfg.name,
                "] flush_interval ", fi, " exceeds sample_window ",
                limiter.cfg.sample_window, " or the heartbeat TTL ",
                limiter_mod.HB_TTL, ": controller windows will be skipped ",
                "and worker liveness will flap")
        end
    end

    local sok, serr = scheduler.start()
    if not sok then
        runtime.started = false
        return nil, serr
    end
    runtime.started = true
    return true
end

--- Stop the per-worker scheduler (testing and controlled shutdown).
function _M.stop()
    runtime.started = false
    return scheduler.stop()
end

--- Reconcile worker-local state into the shared counter. Call once from
-- exit_worker_by_lua* (see README): slots this worker still holds are
-- subtracted so a graceful shutdown or reload drain cannot leak them.
function _M.exit()
    local order = runtime.order
    for i = 1, #order do
        order[i]:exit_worker()
    end
    return true
end

--- Look up a limiter registered in this worker by name. An unknown name
-- is a programming error (a typo in an nginx block), so it raises with
-- a clear message instead of returning nil + err.
function _M.get(name)
    return runtime.registry[name]
        or error("adaptive_limit: no limiter named \"" .. tostring(name) .. "\"", 2)
end

--- Names of the limiters registered in this worker.
function _M.limiters()
    local order = runtime.order
    local names = {}
    for i = 1, #order do
        names[i] = order[i].cfg.name
    end
    return names
end

return _M
