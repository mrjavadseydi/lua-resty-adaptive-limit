# Observability: state() snapshot, stuck diagnostics, anomaly hooks.
use Test::Nginx::Socket 'no_plan';

repeat_each(1);
workers(1);
timeout(30);

run_tests();

__DATA__

=== TEST 1: state() snapshot reflects shared and worker-local state
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 10 }))
    assert(adaptive.start())
}
--- config
location /work {
    access_by_lua_block { PAY:access() }
    content_by_lua_block { ngx.say("ok") }
    log_by_lua_block { PAY:log() }
}
location /state {
    content_by_lua_block {
        local s = PAY:state()
        ngx.say("name=", s.name, " algorithm=", s.algorithm)
        ngx.say("limit=", s.limit, " inflight=", s.inflight,
            " local_inflight=", s.local_inflight)
        ngx.say("admitted_total=", s.admitted_total,
            " rejected_total=", s.rejected_total)
        ngx.say("last_sample_count=", tonumber(s.last_sample_count) or 0)
        ngx.say("workers_active=", s.workers_active,
            " expected=", s.workers_expected)
        ngx.say("stalled=", tostring(s.controller_stalled))
        ngx.say("updates=", s.controller_updates >= 0,
            " skips=", s.controller_skips >= 0)
        ngx.say("anomalies=", s.counter_anomalies == 0)
    }
}
--- request eval
["GET /work", "GET /state"]
--- response_body_like eval
[
    qr/^ok$/,
    qr/^name=pay algorithm=gradient2
limit=10 inflight=0 local_inflight=0
admitted_total=1 rejected_total=0
last_sample_count=\d+
workers_active=1 expected=1
stalled=false
updates=true skips=true
anomalies=true$/
]
--- no_error_log
[error]

=== TEST 2: stuck diagnostics fire when the pool hangs
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 10,
        stale_threshold = 1 }))
    assert(adaptive.start())
}
--- config
location /hang {
    content_by_lua_block {
        -- occupy every slot and never complete: the hang scenario (§26)
        for i = 1, 10 do
            assert(PAY:try_acquire())
        end
        ngx.sleep(1.5)
        local s = PAY:state()
        ngx.say("stalled=", tostring(s.controller_stalled),
            " inflight=", s.inflight, " limit=", s.limit)
    }
}
--- request
GET /hang
--- response_body_like
^stalled=true inflight=10 limit=10$
--- no_error_log
[error]

=== TEST 3: on_update hook fires on controller publication
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    UPDATES = 0
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 100,
        sample_window = 0.2, aggregation_grace = 0.1,
        on_update = function(snap) UPDATES = UPDATES + 1 end }))
    assert(adaptive.start({ flush_interval = 10 }))
}
--- config
location /run {
    content_by_lua_block {
        for i = 1, 30 do
            assert(PAY:try_acquire())
            assert(PAY:release(0.020, "success"))
        end
        PAY:flush(100.0)
        PAY:control(100.5)
        ngx.say("updates=", UPDATES)
        ngx.say("limit=", ngx.shared.adaptive_limit:get("al:1:pay:limit"))
    }
}
--- request
GET /run
--- response_body_like
^updates=1
limit=([1-9]\d+)$
--- no_error_log
[error]

=== TEST 4: on_anomaly hook fires on malformed latency
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    ANOMALIES = {}
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 10,
        on_anomaly = function(kind, detail) ANOMALIES[kind] = true end }))
    assert(adaptive.start())
}
--- config
location /work {
    content_by_lua_block {
        assert(PAY:try_acquire())
        assert(PAY:release(-3, "success")) -- malformed latency
        ngx.say("bad_latency=", tostring(ANOMALIES.bad_latency == true))
    }
}
--- request
GET /work
--- response_body
bad_latency=true
--- no_error_log
[error]
