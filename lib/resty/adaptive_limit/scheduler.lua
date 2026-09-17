-- Per-worker scheduler: exactly one ngx.timer.every per worker serves
-- every registered limiter (heartbeat + stats flush + controller work).
--
-- Timer rules (design.md §8): the callback is fully pcall-wrapped per
-- limiter so one limiter's failure cannot kill the tick for the others;
-- ngx.timer.every re-arms itself (no recursive timer creation); on
-- worker shutdown nginx passes premature=true and the timer dies with
-- the worker — there is nothing to leak.

local runtime = require("resty.adaptive_limit.runtime")

local ngx = ngx
local ngx_ERR = ngx.ERR

local _M = {
    running = false,
    timer_created = false,
}

local function tick(premature)
    if premature or not _M.running then
        _M.running = false
        return
    end

    local now = ngx.now()
    local worker_id = ngx.worker.id()
    local order = runtime.order

    for i = 1, #order do
        local limiter = order[i]
        local ok, err = pcall(limiter.tick, limiter, now, worker_id)
        if not ok then
            limiter.timer_failures = (limiter.timer_failures or 0) + 1
            pcall(limiter.rate_limited_log, limiter, "tick", ngx_ERR,
                "scheduler tick failed: ", tostring(err))
        end
    end
end

function _M.start()
    if _M.running then
        return true
    end
    if _M.timer_created then
        _M.running = true
        return true
    end
    local ok, err = ngx.timer.every(runtime.flush_interval, tick)
    if not ok then
        return nil, "adaptive_limit scheduler: " .. tostring(err)
    end
    _M.timer_created = true
    _M.running = true
    return true
end

-- ngx.timer.every timers cannot be cancelled; stop() only pauses ticks,
-- and mirrors the shutdown latch.
function _M.stop()
    _M.running = false
    return true
end

function _M.reset()
    _M.running = false
    _M.timer_created = false
end

return _M
