-- Micro-benchmark: per-operation cost of the admission/release path
-- against a real shared dict, measured inside OpenResty.
--
--   resty --main-conf "lua_shared_dict bench 10m;" -I lib benchmark/microbench.lua
--
-- Reports wall-clock per operation in microseconds:
--   raw dict get+incr     — the shared-dictionary floor
--   try_acquire (admit)   — full admission including the limit read
--   release               — decrement + observation bookkeeping
--   try_acquire (reject)  — increment + rollback path

local adaptive = require("resty.adaptive_limit")

local shared = ngx.shared.bench
local N = 200000

local function bench(name, f)
    -- warmup
    for _ = 1, 1000 do f() end
    ngx.update_time()
    local t0 = ngx.now()
    for _ = 1, N do f() end
    ngx.update_time()
    local dt = ngx.now() - t0
    print(string.format("%-24s %8.3f us/op  (%d ops in %.2fs)",
        name, dt / N * 1e6, N, dt))
end

-- raw shared-dict floor: get(limit) + incr(inflight)
shared:set("limit", 1000000)
shared:set("inflight", 0)
bench("raw dict get+incr", function()
    local limit = shared:get("limit")
    local n = shared:incr("inflight", 1, 0)
    if n > limit then
        shared:incr("inflight", -1)
    end
end)

-- adaptive admission at a huge limit: always admits
-- (limiters must be registered before adaptive.start(); the scheduler
-- and startup seeding then cover all of them)
local huge = assert(adaptive.new({ name = "bench_huge",
    shared_dict = "bench", initial_limit = 1000000,
    min_limit = 1, max_limit = 1000000 }))
-- rejection path: limit 1, one slot pre-held so every acquire rejects
-- and rolls back
local tiny = assert(adaptive.new({ name = "bench_tiny",
    shared_dict = "bench", initial_limit = 1, min_limit = 1, max_limit = 1 }))
assert(adaptive.start())
bench("try_acquire (admit)", function()
    assert(huge:try_acquire())
end)

-- release path
bench("release", function()
    assert(huge:release(0.020, "success"))
end)

assert(tiny:try_acquire()) -- hold the only slot
bench("try_acquire (reject)", function()
    assert(not tiny:try_acquire())
end)
