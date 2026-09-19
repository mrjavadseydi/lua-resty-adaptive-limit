# Request-lifecycle integration tests (design.md §2): access/log ordering,
# idempotence, internal redirects, bypass, failure modes, rejection
# responses, and outcome classification over real nginx phases.
# Client aborts (499) are covered at the classifier level in
# spec/lifecycle_spec.lua; nginx cannot deliver a 499 to Test::Nginx.
use Test::Nginx::Socket 'no_plan';

repeat_each(1);
workers(1);
timeout(30);

run_tests();

__DATA__

=== TEST 1: normal request — admitted, sampled, released
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
location /t {
    access_by_lua_block { PAY:access() }
    content_by_lua_block { ngx.say("hello") }
    log_by_lua_block { PAY:log() }
}
location /stats {
    content_by_lua_block {
        local s = PAY.stats
        ngx.say("sample_count=", s.sample_count,
            " success=", s.success,
            " admitted=", s.admitted_total,
            " inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"))
    }
}
--- request eval
["GET /t", "GET /stats"]
--- response_body eval
[
"hello
",
"sample_count=1 success=1 admitted=1 inflight=0
"
]
--- no_error_log
[error]

=== TEST 2: double log() is a clean no-op
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
location /t {
    access_by_lua_block { PAY:access() }
    content_by_lua_block { ngx.say("hello") }
    log_by_lua_block {
        PAY:log()
        PAY:log()
        PAY:log()
    }
}
location /stats {
    content_by_lua_block {
        ngx.say("inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"),
            " anomalies=", (PAY.anomalies.negative_inflight or 0))
    }
}
--- request eval
["GET /t", "GET /stats"]
--- response_body eval
[
"hello
",
"inflight=0 anomalies=0
"
]
--- no_error_log
[error]

=== TEST 3: log() without access() releases nothing
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
location /t {
    content_by_lua_block { ngx.say("hello") }
    log_by_lua_block { PAY:log() }
}
location /stats {
    content_by_lua_block {
        ngx.say("inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"),
            " admitted=", PAY.stats.admitted_total)
    }
}
--- request eval
["GET /t", "GET /stats"]
--- response_body eval
[
"hello
",
"inflight=0 admitted=0
"
]
--- no_error_log
[error]

=== TEST 4: exception after acquire — log() still releases (invariant 10)
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
location /t {
    access_by_lua_block { PAY:access() }
    content_by_lua_block { error("boom after acquire") }
    log_by_lua_block { PAY:log() }
}
location /stats {
    content_by_lua_block {
        local s = PAY.stats
        ngx.say("inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"),
            " error=", s.error, " samples=", s.sample_count)
    }
}
--- request eval
["GET /t", "GET /stats"]
--- error_code eval
[500, 200]
--- response_body_like eval
[
    qr/500 Internal Server Error|boomed|error/i,
    qr/^inflight=0 error=1 samples=1$/
]
--- no_error_log
[alert]

=== TEST 5: rejection path — 503 with Retry-After, no slot leaked
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 10 }))
    assert(adaptive.start())
    -- occupy every slot before any request arrives
    for i = 1, 10 do
        assert(PAY:try_acquire())
    end
}
--- config
location /t {
    access_by_lua_block {
        local ok, err = PAY:access()
        if not ok then
            PAY:enforce(err)
            return
        end
    }
    content_by_lua_block { ngx.say("should not happen") }
    log_by_lua_block { PAY:log() }
}
location /drain {
    content_by_lua_block {
        for i = 1, 10 do
            PAY:release(0.001, "success")
        end
        ngx.say("inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"))
    }
}
--- request
GET /t
--- error_code: 503
--- response_headers
Retry-After: 1
--- response_body_like: 503 Service
--- no_error_log
[error]

=== TEST 5b: the rejection consumed no slot (drain finds all 10 held)
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 10 }))
    assert(adaptive.start())
    -- occupy every slot before any request arrives
    for i = 1, 10 do
        assert(PAY:try_acquire())
    end
}
--- config
location /drain {
    content_by_lua_block {
        -- simulate the rejected request from TEST 5: admission refused
        local ok, err = PAY:access()
        ngx.say("rejected_cleanly=", tostring(ok == nil and err == "rejected"))
        for i = 1, 10 do
            PAY:release(0.001, "success")
        end
        ngx.say("inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"))
    }
}
--- request
GET /drain
--- response_body
rejected_cleanly=true
inflight=0
--- no_error_log
[error]

=== TEST 5c: guard() + get() — the one-line form of TEST 5
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    local pay = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 10 }))
    assert(adaptive.start())
    for i = 1, 10 do
        assert(pay:try_acquire())
    end
}
--- config
location /t {
    access_by_lua_block { require("resty.adaptive_limit").get("pay"):guard() }
    content_by_lua_block { ngx.say("should not happen") }
    log_by_lua_block { require("resty.adaptive_limit").get("pay"):log() }
}
--- request
GET /t
--- error_code: 503
--- response_headers
Retry-After: 1
--- response_body_like: 503 Service
--- no_error_log
[error]

=== TEST 6: ngx.exec fronting — admission at the internal target
-- Verified platform behavior: ngx.exec resets ngx.ctx, the target runs
-- with ngx.req.is_internal() == true, and the log phase runs once, for
-- the final location. The balanced lifecycle pattern for exec chains is
-- therefore: no access() at the exec-ing entry, access() at the target
-- with allow_internal = true. (Subrequest targets of such a location
-- must not be captured — their log phase never runs.)
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 10, min_limit = 1, max_limit = 10,
        allow_internal = true }))
    assert(adaptive.start())
}
--- config
location /start {
    content_by_lua_block { ngx.exec("/target") }
}
location /target {
    access_by_lua_block { PAY:access() }
    content_by_lua_block {
        ngx.say("inflight_during=",
            ngx.shared.adaptive_limit:get("al:1:pay:inflight"))
    }
    log_by_lua_block { PAY:log() }
}
location /stats {
    content_by_lua_block {
        ngx.say("inflight_after=",
            ngx.shared.adaptive_limit:get("al:1:pay:inflight"),
            " admitted=", PAY.stats.admitted_total)
    }
}
--- request eval
["GET /start", "GET /stats"]
--- response_body eval
[
"inflight_during=1
",
"inflight_after=0 admitted=1
"
]
--- no_error_log
[error]

=== TEST 6b: internal targets bypass admission by default
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
location /start {
    content_by_lua_block { ngx.exec("/target") }
}
location /target {
    access_by_lua_block { PAY:access() }
    content_by_lua_block {
        ngx.say("inflight_during=",
            ngx.shared.adaptive_limit:get("al:1:pay:inflight"))
    }
    log_by_lua_block { PAY:log() }
}
location /stats {
    content_by_lua_block {
        ngx.say("inflight_after=",
            ngx.shared.adaptive_limit:get("al:1:pay:inflight"),
            " admitted=", PAY.stats.admitted_total)
    }
}
--- request eval
["GET /start", "GET /stats"]
--- response_body eval
[
"inflight_during=0
",
"inflight_after=0 admitted=0
"
]
--- no_error_log
[error]

=== TEST 7: bypass — no admission, no observation
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
location /t {
    access_by_lua_block { PAY:access({ bypass = true }) }
    content_by_lua_block { ngx.say("hello") }
    log_by_lua_block { PAY:log() }
}
location /stats {
    content_by_lua_block {
        ngx.say("inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"),
            " admitted=", PAY.stats.admitted_total,
            " samples=", PAY.stats.sample_count)
    }
}
--- request eval
["GET /t", "GET /stats"]
--- response_body eval
[
"hello
",
"inflight=0 admitted=0 samples=0
"
]
--- no_error_log
[error]

=== TEST 8: upstream connect failure classifies as connect_error
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
location /t {
    access_by_lua_block { PAY:access() }
    proxy_pass http://127.0.0.1:1;
    log_by_lua_block { PAY:log() }
}
location /stats {
    content_by_lua_block {
        ngx.say("inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"),
            " connect_error=", PAY.stats.connect_error)
    }
}
--- request eval
["GET /t", "GET /stats"]
--- error_code eval
[502, 200]
--- response_body_like eval
[
    qr/502 Bad Gateway/,
    qr/^inflight=0 connect_error=1$/
]
--- no_error_log
[alert]

=== TEST 9: upstream read timeout classifies as timeout
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
location /t {
    access_by_lua_block { PAY:access() }
    proxy_pass http://127.0.0.1:$TEST_NGINX_SERVER_PORT/slow;
    proxy_read_timeout 50ms;
    log_by_lua_block { PAY:log() }
}
location /slow {
    content_by_lua_block { ngx.sleep(1) }
}
location /stats {
    content_by_lua_block {
        ngx.say("inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"),
            " timeout=", PAY.stats.timeout)
    }
}
--- request eval
["GET /t", "GET /stats"]
--- error_code eval
[504, 200]
--- response_body_like eval
[
    qr/504 Gateway Time-out/,
    qr/^inflight=0 timeout=1$/
]
--- no_error_log
[alert]

=== TEST 10: error_page fallback — slot released when fallback explicitly releases
# In Nginx, internal redirection (error_page) resets ngx.ctx and skips the
# admitting location's log phase. If an admitting location redirects via
# error_page, the fallback location must explicitly release the slot
# (e.g. via PAY:release(nil, outcome)) to prevent leaking concurrency.
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
location /api {
    access_by_lua_block { PAY:access() }
    proxy_pass http://127.0.0.1:1;
    error_page 502 = /fallback;
}
location /fallback {
    content_by_lua_block {
        ngx.say("fallback response")
    }
    log_by_lua_block {
        PAY:release(nil, "connect_error")
    }
}
location /stats {
    content_by_lua_block {
        ngx.say("inflight=", ngx.shared.adaptive_limit:get("al:1:pay:inflight"),
            " admitted=", PAY.stats.admitted_total,
            " connect_error=", PAY.stats.connect_error)
    }
}
--- request eval
["GET /api", "GET /stats"]
--- response_body eval
[
"fallback response
",
"inflight=0 admitted=1 connect_error=1
"
]
--- no_error_log
[alert]

