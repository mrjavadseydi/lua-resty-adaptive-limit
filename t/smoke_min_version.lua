-- Minimum-version smoke test: run the library's core paths on the
-- oldest supported OpenResty.
local adaptive = require("resty.adaptive_limit")
local g2 = require("resty.adaptive_limit.controller.gradient2")
local common = require("resty.adaptive_limit.controller.common")
local ut = require("resty.adaptive_limit.upstream_time")

-- pure controller math
local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
local ns = assert(common.safe_update(g2, state,
    { sample_count = 1000, mean_rtt = 0.020, completions = 1000,
      rejected_count = 1 },
    { min_samples = 20, sample_alpha = 0.5, baseline_alpha = 0.05,
      rtt_tolerance = 2.0, min_gradient = 0.5, smoothing = 0.5,
      headroom_min = 1, headroom_max = 50, min_limit = 1, max_limit = 2000,
      overload_min_samples = 20, overload_failure_ratio = 0.10,
      overload_backoff = 0.80 }))
assert(math.abs(ns.limit - 105.0) < 1e-9, "gradient2 math changed")
assert(ut.parse("0.005, 0.010", "last") == 0.010, "parser changed")

-- admission against a real shared dict
local L = assert(adaptive.new({ name = "smoke", shared_dict = "smoke_dict",
    initial_limit = 2, min_limit = 1, max_limit = 2 }))
assert(adaptive.start())
assert(L:try_acquire())
assert(L:try_acquire())
local ok, err = L:try_acquire()
assert(not ok and err == "rejected", "admission semantics changed: " .. tostring(err))
assert(L:release(0.001, "success"))
assert(L:try_acquire())
assert(L:release(ngx.now() - ngx.req.start_time() or 0.001, "aborted"))
local s = L:state()
assert(s.limit == 2 and s.inflight == 1, "state snapshot broken: limit=" .. tostring(s.limit) .. " inflight=" .. tostring(s.inflight))
print("SMOKE OK on OpenResty " .. ngx.config.nginx_version ..
      " / ngx_lua " .. ngx.config.ngx_lua_version)
