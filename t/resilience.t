# Worker-exit reconciliation and heartbeat wiring.
#
# The straggler scenario proves the documented claim that reconciliation
# is safe under either phase ordering: if a log phase released a slot
# AFTER exit_worker already reconciled it, the shared counter floors at
# zero and the anomaly surfaces instead of corrupting.
use Test::Nginx::Socket 'no_plan';

repeat_each(1);
workers(1);
timeout(30);

run_tests();

__DATA__

=== TEST 1: exit_worker reconciles held slots; late releases stay safe
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
location /acquire {
    content_by_lua_block {
        for i = 1, 3 do
            assert(PAY:try_acquire())
        end
        ngx.say("inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"))
    }
}
location /reconcile {
    content_by_lua_block {
        assert(PAY:exit_worker())
        ngx.say("inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"))
        ngx.say("held=", PAY._inflight)
    }
}
location /straggler {
    content_by_lua_block {
        -- late log phases for requests torn down with the worker
        for i = 1, 3 do
            assert(PAY:release(0.001, "success"))
        end
        ngx.say("inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"))
        ngx.say("negative_anomalies=", (PAY.anomalies.negative_inflight or 0))
    }
}
--- request eval
["GET /acquire", "GET /reconcile", "GET /straggler"]
--- response_body eval
[
"inflight=3
",
"inflight=0
held=0
",
"inflight=0
negative_anomalies=3
"
]
--- no_error_log
[error]

=== TEST 2: scheduler heartbeats land in the shared dict
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 10,
        flush_interval = 0.1 }))
    assert(adaptive.start())
}
--- config
location /hb {
    content_by_lua_block {
        ngx.sleep(0.5) -- several scheduler ticks
        ngx.say("hb=", ngx.shared.adaptive_limit:get("al:1:pay:hb:0") ~= nil)
    }
}
--- request
GET /hb
--- response_body
hb=true
--- no_error_log
[error]
