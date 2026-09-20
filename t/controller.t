# Full control-loop integration: stats flush -> window close -> grace ->
# lease -> controller update -> published limit. The scheduler from
# adaptive.start() runs for real; endpoints poll with generous margins so
# the assertions are timing-robust.
use Test::Nginx::Socket 'no_plan';

repeat_each(1);
workers(1);
timeout(60);

run_tests();

__DATA__

=== TEST 1: healthy traffic grows the published limit
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 100,
        sample_window = 0.2, aggregation_grace = 0.1 }))
    assert(adaptive.start())
}
--- config
location /work {
    content_by_lua_block {
        for _ = 1, 5 do
            for i = 1, 10 do assert(PAY:try_acquire()) end
            assert(not PAY:try_acquire()) -- prove demand reached the cap
            for i = 1, 10 do assert(PAY:release(0.020, "success")) end
        end
        ngx.say("done")
    }
}
location /limit {
    content_by_lua_block {
        local t0 = ngx.now()
        local limit = tonumber(ngx.shared.adaptive_limit:get("al:1:pay:limit"))
        while limit <= 10 and ngx.now() - t0 < 5 do
            ngx.sleep(0.2)
            limit = tonumber(ngx.shared.adaptive_limit:get("al:1:pay:limit"))
        end
        ngx.say("limit=", limit)
    }
}
--- request eval
["GET /work", "GET /limit", "GET /limit"]
--- response_body_like eval
[
    qr/^done$/,
    qr/^limit=(1[1-9]|[2-9]\d|100)$/,
    qr/^limit=(1[1-9]|[2-9]\d|100)$/
]
--- no_error_log
[error]

=== TEST 2: insufficient samples hold the limit exactly
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 100,
        sample_window = 0.2, aggregation_grace = 0.1 }))
    assert(adaptive.start())
}
--- config
location /limit {
    content_by_lua_block {
        ngx.sleep(1.5) -- several windows close with no traffic at all
        ngx.say("limit=", ngx.shared.adaptive_limit:get("al:1:pay:limit"))
        ngx.say("updates=", tonumber(PAY.controller_updates) or 0)
    }
}
--- request
GET /limit
--- response_body
limit=10
updates=0
--- no_error_log
[error]

=== TEST 3: strong overload signals shed the limit
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 50, min_limit = 1, max_limit = 200,
        sample_window = 0.2, aggregation_grace = 0.1 }))
    assert(adaptive.start())
}
--- config
location /work {
    content_by_lua_block {
        -- 60% timeouts: far above the 10% backoff gate
        for i = 1, 60 do
            assert(PAY:try_acquire())
            if i % 5 < 3 then
                assert(PAY:release(0.020, "timeout"))
            else
                assert(PAY:release(0.020, "success"))
            end
        end
        ngx.say("done")
    }
}
location /limit {
    content_by_lua_block {
        local t0 = ngx.now()
        local limit = tonumber(ngx.shared.adaptive_limit:get("al:1:pay:limit"))
        while limit >= 50 and ngx.now() - t0 < 5 do
            ngx.sleep(0.2)
            limit = tonumber(ngx.shared.adaptive_limit:get("al:1:pay:limit"))
        end
        ngx.say("limit=", limit)
    }
}
location /recover {
    content_by_lua_block {
        -- healthy saturated traffic (one rejection per burst proves the
        -- demand reached the cap) must grow the limit back above the
        -- shed value within the deadline
        local d = ngx.shared.adaptive_limit
        local shed = tonumber(d:get("al:1:pay:limit"))
        local t0 = ngx.now()
        local limit = shed
        while limit <= shed and ngx.now() - t0 < 5 do
            local held = 0
            while PAY:try_acquire() do held = held + 1 end
            for _ = 1, held do assert(PAY:release(0.020, "success")) end
            ngx.sleep(0.02)
            limit = tonumber(d:get("al:1:pay:limit"))
        end
        ngx.say(limit > shed and "recovered" or "stuck at " .. limit)
    }
}
--- request eval
["GET /work", "GET /work", "GET /limit", "GET /recover"]
--- response_body_like eval
[
    qr/^done$/,
    qr/^done$/,
    qr/^limit=([1-9]|[1-4]\d)$/,
    qr/^recovered$/,
]
--- no_error_log
[error]

=== TEST 4: window processing is idempotent (last_window + lease)
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 100,
        sample_window = 0.2, aggregation_grace = 0.1 }))
    -- keep the background scheduler out of the way: its first tick is
    -- 10s out, long after this test finished driving windows manually
    assert(adaptive.start({ flush_interval = 10 }))
}
--- config
location /control {
    content_by_lua_block {
        -- synthetic timeline: samples flushed into window 500
        -- ([100.0, 100.2)), control runs at 100.5 where that window is
        -- closed and past the grace period; three identical passes must
        -- process it exactly once
        for i = 1, 3 do
            for j = 1, 3 do
                -- fill to the cap: the rejection allows growth
                local held = 0
                while PAY:try_acquire() do held = held + 1 end
                for k = 1, held do assert(PAY:release(0.020, "success")) end
            end
            PAY:flush(100.0)
            PAY:control(100.5)
        end
        -- one update: 10 * 0.5 + (10 * 1.0 + sqrt(10)) * 0.5 = 11.58 -> 11;
        -- a second pass would have published 13
        ngx.say("updates=", tonumber(PAY.controller_updates) or 0)
        ngx.say("limit=", ngx.shared.adaptive_limit:get("al:1:pay:limit"))
    }
}
--- request
GET /control
--- response_body
updates=1
limit=11
--- no_error_log
[error]

=== TEST 5: one scheduler serves multiple limiters
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 100,
        sample_window = 0.2, aggregation_grace = 0.1 }))
    SEARCH = assert(adaptive.new({ name = "search",
        shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 100,
        sample_window = 0.2, aggregation_grace = 0.1 }))
    assert(adaptive.start())
}
--- config
location /work {
    content_by_lua_block {
        for _ = 1, 5 do
            for i = 1, 10 do
                assert(PAY:try_acquire())
                assert(SEARCH:try_acquire())
            end
            assert(not PAY:try_acquire())
            assert(not SEARCH:try_acquire())
            for i = 1, 10 do
                assert(PAY:release(0.020, "success"))
                assert(SEARCH:release(0.020, "success"))
            end
        end
        ngx.say("done")
    }
}
location /limit {
    content_by_lua_block {
        local t0 = ngx.now()
        local d = ngx.shared.adaptive_limit
        local pay = tonumber(d:get("al:1:pay:limit"))
        local search = tonumber(d:get("al:1:search:limit"))
        while (pay <= 10 or search <= 10) and ngx.now() - t0 < 5 do
            ngx.sleep(0.2)
            pay = tonumber(d:get("al:1:pay:limit"))
            search = tonumber(d:get("al:1:search:limit"))
        end
        ngx.say("pay=", pay, " search=", search)
    }
}
--- request eval
["GET /work", "GET /limit"]
--- response_body_like eval
[
    qr/^done$/,
    qr/^pay=(1[1-9]|[2-9]\d|100) search=(1[1-9]|[2-9]\d|100)$/
]
--- no_error_log
[error]
