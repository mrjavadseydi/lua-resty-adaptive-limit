-- Limiter definitions, typically required from init_by_lua (early
-- config validation) and from init_worker_by_lua / your locations.
--
--   init_by_lua_block     { require("app.limiters") }
--   init_worker_by_lua_block {
--       local limiters = require("app.limiters")
--       local ok, err = limiters.start()
--       if not ok then error(err) end
--   }
--
-- Everything the library needs is a dedicated lua_shared_dict zone and
-- these definitions. See examples/nginx.conf for the full nginx side.

local adaptive = require "resty.adaptive_limit"

local M = {}

-- The protected upstream API: one pool, Gradient2 controller.
M.payments = assert(adaptive.new({
    name = "payments",
    shared_dict = "adaptive_limit",

    algorithm = "gradient2",

    initial_limit = 50,
    min_limit = 5,
    max_limit = 2000,

    sample_window = 1.0,
    min_samples = 20,

    failure_mode = "fail_open",
}))

-- A second, independent pool (search backend) sharing the same zone.
M.search = assert(adaptive.new({
    name = "search",
    shared_dict = "adaptive_limit",

    algorithm = "gradient2",
    profile = "responsive",   -- spiky workload: tolerate bursts, react fast

    initial_limit = 30,
    min_limit = 2,
    max_limit = 500,
}))

-- Observability: a cheap snapshot accessor for a status endpoint or
-- an external scraper. See prometheus.lua for metrics integration.
M.snapshots = function()
    return {
        payments = M.payments:state(),
        search = M.search:state(),
    }
end

function M.start()
    return adaptive.start()
end

function M.exit()
    return adaptive.exit()
end

return M
