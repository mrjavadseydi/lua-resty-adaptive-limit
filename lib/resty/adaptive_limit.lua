-- lua-resty-adaptive-limit
--
-- Adaptive concurrency limiting and load shedding for OpenResty: the
-- allowed number of concurrent requests adjusts automatically from
-- observed completion latency and explicit overload signals.
--
-- See design.md for the architecture, README.md for usage, and
-- spec/ + t/ for the proof. Entry points:
--
--   local adaptive = require "resty.adaptive_limit"
--   local limiter  = assert(adaptive.new({ name = "payments",
--       shared_dict = "adaptive_limit" }))
--   -- init_worker_by_lua:  adaptive.start()
--   -- access_by_lua:       limiter:access()
--   -- log_by_lua:          limiter:log()

local errors = require("resty.adaptive_limit.errors")
local limiter_mod = require("resty.adaptive_limit.limiter")
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
        if type(opts.flush_interval) ~= "number" or opts.flush_interval <= 0 then
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
                    "\": " .. err
            end
            return nil, "adaptive_limit: limiter \"" .. limiter.cfg.name ..
                "\": schema check failed: " .. tostring(err)
        end
        limiter:adopt_shared_state()
    end

    runtime.started = true

    -- The scheduler itself (timer creation, tick loop, controller
    -- wiring) is installed here by the phases that follow; admission
    -- already works once `started` is set.
    if runtime.start_scheduler then
        return runtime.start_scheduler()
    end

    return true
end

--- Stop the per-worker scheduler (testing and controlled shutdown).
function _M.stop()
    runtime.started = false
    if runtime.stop_scheduler then
        return runtime.stop_scheduler()
    end
    return true
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
