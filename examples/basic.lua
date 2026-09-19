-- Limiter definitions. Require this module from init_by_lua (early
-- config validation); workers inherit the registered limiters and look
-- them up by name:
--
--   init_by_lua_block        { require("app.limiters") }
--   init_worker_by_lua_block { assert(require("resty.adaptive_limit").start()) }
--   exit_worker_by_lua_block { require("resty.adaptive_limit").exit() }
--   access_by_lua_block      { require("resty.adaptive_limit").get("payments"):guard() }
--   log_by_lua_block         { require("resty.adaptive_limit").get("payments"):log() }
--
-- Everything the library needs is a dedicated lua_shared_dict zone and
-- these definitions. See examples/nginx.conf for the full nginx side.

local adaptive = require "resty.adaptive_limit"

-- The protected upstream API: one pool, Gradient2 controller.
assert(adaptive.new({
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
assert(adaptive.new({
    name = "search",
    shared_dict = "adaptive_limit",

    algorithm = "gradient2",
    profile = "responsive",   -- spiky workload: tolerate bursts, react fast

    initial_limit = 30,
    min_limit = 2,
    max_limit = 500,
}))

-- Observability: a cheap snapshot accessor for a status endpoint or
-- an external scraper. See prometheus_example.lua for metrics integration.
return {
    snapshots = function()
        local out = {}
        for _, name in ipairs(adaptive.limiters()) do
            out[name] = adaptive.get(name):state()
        end
        return out
    end,
}
