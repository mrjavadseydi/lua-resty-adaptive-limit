-- Optional nginx-lua-prometheus integration (no mandatory dependency:
-- the core library never requires this file).
--
--   luarocks install nginx-lua-prometheus
--
-- Metric names follow spec §35. Labels: the limiter name only — no
-- high-cardinality labels anywhere.
--
--   http {
--       lua_shared_dict prometheus_metrics 10M;
--       init_worker_by_lua_block { require("prometheus_example").init() }
--       location /metrics {
--           content_by_lua_block { require("prometheus_example").collect() }
--       }
--   }

local prometheus = require "prometheus"

local adaptive = require "resty.adaptive_limit"

local metric_limit = prometheus:gauge(
    "adaptive_limit_limit", "Current adaptive concurrency limit", { "limiter" })
local metric_inflight = prometheus:gauge(
    "adaptive_limit_inflight", "Currently in-flight admitted requests", { "limiter" })
local metric_admitted = prometheus:counter(
    "adaptive_limit_admitted_total", "Admitted requests", { "limiter" })
local metric_rejected = prometheus:counter(
    "adaptive_limit_rejected_total", "Rejected requests", { "limiter" })
local metric_short_rtt = prometheus:gauge(
    "adaptive_limit_sample_rtt_seconds", "Smoothed observed RTT", { "limiter" })
local metric_long_rtt = prometheus:gauge(
    "adaptive_limit_baseline_rtt_seconds", "Long-term RTT baseline", { "limiter" })
local metric_gradient = prometheus:gauge(
    "adaptive_limit_gradient", "Last controller gradient", { "limiter" })
local metric_updates = prometheus:counter(
    "adaptive_limit_controller_updates_total",
    "Controller window updates", { "limiter" })
local metric_skips = prometheus:counter(
    "adaptive_limit_controller_skipped_total",
    "Controller windows skipped/held", { "limiter" })
local metric_internal = prometheus:counter(
    "adaptive_limit_internal_errors_total", "Limiter internal errors", { "limiter" })
local metric_anomalies = prometheus:counter(
    "adaptive_limit_counter_anomalies_total", "Counter anomalies", { "limiter" })

local limiters = {}

local M = {}

function M.register(limiter)
    limiters[#limiters + 1] = limiter
end

function M.init()
    M.register(assert(adaptive.new({
        name = "payments", shared_dict = "adaptive_limit",
    })))
    assert(adaptive.start())
    -- Gauges read the shared state at scrape time via limiter:state()
    -- (a handful of dict reads per scrape — never on the request path),
    -- so no controller hook is required for this integration.
end

function M.collect()
    prometheus:collect()
    for i = 1, #limiters do
        local lim = limiters[i]
        local s = lim:state() -- a handful of dict reads per scrape: fine
        local labels = { lim.cfg.name }
        metric_limit:set(s.limit or 0, labels)
        metric_inflight:set(s.inflight or 0, labels)
        metric_admitted:inc(s.admitted_total, labels)
        metric_rejected:inc(s.rejected_total, labels)
        if s.short_rtt then metric_short_rtt:set(s.short_rtt, labels) end
        if s.long_rtt then metric_long_rtt:set(s.long_rtt, labels) end
        if s.gradient then metric_gradient:set(s.gradient, labels) end
        metric_updates:inc(s.controller_updates, labels)
        metric_skips:inc(s.controller_skips, labels)
        metric_internal:inc(s.internal_errors, labels)
        metric_anomalies:inc(s.counter_anomalies, labels)
    end
end

return M
