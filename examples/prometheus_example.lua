-- Optional nginx-lua-prometheus integration (no mandatory dependency:
-- the core library never requires this file).
--
--   luarocks install nginx-lua-prometheus
--
-- Metric names follow design.md §13. Labels: the limiter name only — no
-- high-cardinality labels anywhere.
--
--   http {
--       lua_shared_dict prometheus_metrics 10M;
--       init_worker_by_lua_block { require("prometheus_example").init() }
--       location /metrics {
--           content_by_lua_block { require("prometheus_example").collect() }
--       }
--   }

local adaptive = require "resty.adaptive_limit"

local prometheus
local metric_limit
local metric_inflight
local metric_admitted
local metric_rejected
local metric_short_rtt
local metric_long_rtt
local metric_gradient
local metric_updates
local metric_skips
local metric_internal
local metric_anomalies

local limiters = {}
local prev_counts = {}

local function inc_delta(metric, name, field, current, labels)
    current = current or 0
    -- Keyed per worker: /metrics can be served by any worker process, and
    -- each worker's lim:state() counters are worker-local (see
    -- limiter.lua's state() doc). Without the worker id, a scrape landing
    -- on a different worker than the previous one looks like a counter
    -- reset and its whole total gets re-added on top of what was already
    -- counted. Keying by worker keeps each worker's own series monotonic;
    -- the metric still sums correctly across workers over time.
    local key = name .. ":" .. field .. ":" .. tostring(ngx.worker.id())
    local prev = prev_counts[key] or 0
    if current >= prev then
        local delta = current - prev
        if delta > 0 then
            metric:inc(delta, labels)
        end
    else
        -- counter reset (e.g. process reload/restart)
        metric:inc(current, labels)
    end
    prev_counts[key] = current
end

local M = {}

function M.register(limiter)
    limiters[#limiters + 1] = limiter
end

function M.init()
    prometheus = require("prometheus").init("prometheus_metrics")
    metric_limit = prometheus:gauge(
        "adaptive_limit_limit", "Current adaptive concurrency limit", { "limiter" })
    metric_inflight = prometheus:gauge(
        "adaptive_limit_inflight", "Currently in-flight admitted requests", { "limiter" })
    metric_admitted = prometheus:counter(
        "adaptive_limit_admitted_total", "Admitted requests", { "limiter" })
    metric_rejected = prometheus:counter(
        "adaptive_limit_rejected_total", "Rejected requests", { "limiter" })
    metric_short_rtt = prometheus:gauge(
        "adaptive_limit_sample_rtt_seconds", "Smoothed observed RTT", { "limiter" })
    metric_long_rtt = prometheus:gauge(
        "adaptive_limit_baseline_rtt_seconds", "Long-term RTT baseline", { "limiter" })
    metric_gradient = prometheus:gauge(
        "adaptive_limit_gradient", "Last controller gradient", { "limiter" })
    metric_updates = prometheus:counter(
        "adaptive_limit_controller_updates_total",
        "Controller window updates", { "limiter" })
    metric_skips = prometheus:counter(
        "adaptive_limit_controller_skipped_total",
        "Controller windows skipped/held", { "limiter" })
    metric_internal = prometheus:counter(
        "adaptive_limit_internal_errors_total", "Limiter internal errors", { "limiter" })
    metric_anomalies = prometheus:counter(
        "adaptive_limit_counter_anomalies_total", "Counter anomalies", { "limiter" })
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
        local name = lim.cfg.name
        local labels = { name }
        metric_limit:set(s.limit or 0, labels)
        metric_inflight:set(s.inflight or 0, labels)
        -- Note: worker-local cumulative stats reflect the worker(s) serving
        -- /metrics; increment by deltas to avoid polynomial over-counting
        inc_delta(metric_admitted, name, "admitted", s.admitted_total, labels)
        inc_delta(metric_rejected, name, "rejected", s.rejected_total, labels)
        if s.short_rtt then metric_short_rtt:set(s.short_rtt, labels) end
        if s.long_rtt then metric_long_rtt:set(s.long_rtt, labels) end
        if s.gradient then metric_gradient:set(s.gradient, labels) end
        inc_delta(metric_updates, name, "updates", s.controller_updates, labels)
        inc_delta(metric_skips, name, "skips", s.controller_skips, labels)
        inc_delta(metric_internal, name, "internal", s.internal_errors, labels)
        inc_delta(metric_anomalies, name, "anomalies", s.counter_anomalies, labels)
    end
end

return M
