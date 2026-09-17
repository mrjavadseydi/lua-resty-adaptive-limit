# Configuration validation (design.md §3) and error semantics (design.md §11).
# Invalid configuration must be rejected immediately with clear errors;
# nothing nonsensical may reach the controller.
use Test::Nginx::Socket 'no_plan';

repeat_each(1);
workers(1);

run_tests();

__DATA__

=== TEST 1: exact validation message shape
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
--- config
location /t {
    content_by_lua_block {
        local adaptive = require("resty.adaptive_limit")
        local lim, err = adaptive.new({ name = "x",
            shared_dict = "adaptive_limit", min_limit = 0 })
        if not lim then
            ngx.say("ERR: ", err)
        else
            ngx.say("created?!")
        end
    }
}
--- request
GET /t
--- response_body
ERR: adaptive_limit config: min_limit must be an integer in [1, 2147483648]
--- no_error_log
[error]

=== TEST 2: each remaining invalid case (one request, sequential checks)
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
--- config
location /t {
    content_by_lua_block {
        local adaptive = require("resty.adaptive_limit")
        local cases = {
            { "initial_below_min", { name = "x", shared_dict = "adaptive_limit",
                min_limit = 10, max_limit = 100, initial_limit = 5 } },
            { "initial_above_max", { name = "x", shared_dict = "adaptive_limit",
                min_limit = 10, max_limit = 100, initial_limit = 500 } },
            { "max_lt_min", { name = "x", shared_dict = "adaptive_limit",
                min_limit = 100, max_limit = 10 } },
            { "window_zero", { name = "x", shared_dict = "adaptive_limit",
                sample_window = 0 } },
            { "smoothing_zero", { name = "x", shared_dict = "adaptive_limit",
                smoothing = 0 } },
            { "smoothing_gt_1", { name = "x", shared_dict = "adaptive_limit",
                smoothing = 1.5 } },
            { "backoff_zero", { name = "x", shared_dict = "adaptive_limit",
                overload_backoff = 0 } },
            { "backoff_one", { name = "x", shared_dict = "adaptive_limit",
                overload_backoff = 1 } },
            { "tolerance_one", { name = "x", shared_dict = "adaptive_limit",
                rtt_tolerance = 1 } },
            { "gradient_one", { name = "x", shared_dict = "adaptive_limit",
                min_gradient = 1 } },
            { "bad_algorithm", { name = "x", shared_dict = "adaptive_limit",
                algorithm = "tokyo_tower" } },
            { "bad_failure_mode", { name = "x", shared_dict = "adaptive_limit",
                failure_mode = "fail_silent" } },
            { "bad_latency_source", { name = "x", shared_dict = "adaptive_limit",
                latency_source = "vibes" } },
            { "bad_name_numeric", { name = "9x",
                shared_dict = "adaptive_limit" } },
            { "bad_name_case", { name = "Payments",
                shared_dict = "adaptive_limit" } },
            { "bad_name_space", { name = "pay ments",
                shared_dict = "adaptive_limit" } },
            { "bad_name_long", { name = string.rep("x", 64),
                shared_dict = "adaptive_limit" } },
            { "no_name", { shared_dict = "adaptive_limit" } },
            { "no_dict", { name = "x" } },
            { "empty_dict_name", { name = "x", shared_dict = "" } },
            { "missing_dict_zone", { name = "x", shared_dict = "no_such_zone" } },
        }
        local bad = 0
        for i = 1, #cases do
            local lim, err = adaptive.new(cases[i][2])
            if lim then
                ngx.say("unexpectedly created: ", cases[i][1])
            elseif not err then
                ngx.say("no error for: ", cases[i][1])
            else
                bad = bad + 1
            end
        end
        ngx.say("rejected=", bad, "/", #cases)
    }
}
--- request
GET /t
--- response_body
rejected=21/21
--- no_error_log
[error]

=== TEST 3: duplicate limiter name in one worker
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
--- config
location /t {
    content_by_lua_block {
        local adaptive = require("resty.adaptive_limit")
        local a, erra = adaptive.new({ name = "dup", shared_dict = "adaptive_limit" })
        local b, errb = adaptive.new({ name = "dup", shared_dict = "adaptive_limit" })
        ngx.say("first=", tostring(a ~= nil))
        if not b then
            ngx.say("second=ERR ", errb)
        else
            ngx.say("second=created?!")
        end
    }
}
--- request
GET /t
--- response_body
first=true
second=ERR adaptive_limit: duplicate limiter name "dup"
--- no_error_log
[error]

=== TEST 4: admission without start() reports not_started
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit" }))
    -- deliberately no adaptive.start()
}
--- config
location /t {
    content_by_lua_block {
        local ok, err = PAY:try_acquire()
        ngx.say("acquire=", tostring(ok), " err=", tostring(err))
        local ok2, err2 = PAY:release(0.001)
        ngx.say("release=", tostring(ok2), " err=", tostring(err2))
    }
}
--- request
GET /t
--- response_body
acquire=nil err=not_started
release=nil err=not_started
--- no_error_log
[error]

=== TEST 5: admission refuses a foreign shared-state schema
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    ngx.shared.adaptive_limit:set("adaptive_limit:schema", "9")
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit" }))
    local ok, err = adaptive.start()
    if not ok then
        START_ERR = err
    end
}
--- config
location /t {
    content_by_lua_block {
        ngx.say("start_err=", tostring(START_ERR))
        local ok, err = PAY:try_acquire()
        ngx.say("acquire=", tostring(ok), " err=", tostring(err))
    }
}
--- request
GET /t
--- response_body_like
^start_err=adaptive_limit: limiter "pay": incompatible shared state schema \(expected version 1\)
acquire=nil err=not_started
--- no_error_log
[alert]
