# Statistics pipeline integration: request observations land in
# per-window shared accumulators via flush(), with window rollover and
# bucket separation. Flushes are called explicitly from the test
# endpoints (the scheduler drives them automatically in production).
use Test::Nginx::Socket 'no_plan';

repeat_each(1);
workers(1);
timeout(30);

run_tests();

__DATA__

=== TEST 1: observations land in the current window's accumulators
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 10,
        sample_window = 0.5 }))
    assert(adaptive.start())
}
--- config
location /work {
    content_by_lua_block {
        for i = 1, 3 do
            assert(PAY:try_acquire())
            assert(PAY:release(0.010, "success"))
        end
        local win = PAY:flush(ngx.now())
        local d = ngx.shared.adaptive_limit
        ngx.say("win=", win)
        ngx.say("c=", d:get("al:1:pay:w:" .. win .. ":c"))
        ngx.say("s_summed=", math.floor(d:get("al:1:pay:w:" .. win .. ":s") * 1000 + 0.5))
    }
}
--- request
GET /work
--- response_body_like
^win=\d+
c=3
s_summed=30$
--- no_error_log
[error]

=== TEST 2: window rollover splits samples into separate accumulators
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 10,
        sample_window = 0.5 }))
    assert(adaptive.start())
}
--- config
location /work {
    content_by_lua_block {
        assert(PAY:try_acquire())
        assert(PAY:release(0.010, "success"))
        local win = PAY:flush(ngx.now())
        ngx.say("win=", win, " c=", ngx.shared.adaptive_limit:get(
            "al:1:pay:w:" .. win .. ":c"))
    }
}
location /wait {
    content_by_lua_block { ngx.sleep(0.7) ngx.say("waited") }
}
--- request eval
["GET /work", "GET /wait", "GET /work"]
--- response_body_like eval
[
    qr/^win=\d+ c=1$/,
    qr/^waited$/,
    qr/^win=\d+ c=1$/
]
--- no_error_log
[error]
