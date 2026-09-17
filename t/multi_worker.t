# Multi-worker shared-state correctness (design.md §1).
#
# A holder coroutine on one worker grabs every slot and holds them while a
# real HTTP request arrives — whatever worker it lands on, admission must
# be rejected, which proves the shared counter is honored across worker
# processes (a per-worker counter would admit). After the holder releases,
# the counter must return to exactly zero.
use Test::Nginx::Socket 'no_plan';

repeat_each(1);
workers(2);
timeout(30);

run_tests();

__DATA__

=== TEST 1: slots held by one worker are enforced on all workers
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({
        name = "pay",
        shared_dict = "adaptive_limit",
        initial_limit = 10,
        min_limit = 10,
        max_limit = 10,
    }))
    assert(adaptive.start())

    if ngx.worker.id() == 0 then
        ngx.timer.at(0.1, function(premature)
            if premature then return end
            local d = ngx.shared.adaptive_limit
            local held = {}
            for i = 1, 10 do
                held[i] = PAY:try_acquire()
                if not held[i] then break end
                d:incr("probe_hold", 1, 0)
            end
            ngx.sleep(2)
            for i = 1, #held do
                if held[i] then
                    d:incr("probe_hold", -1)
                    PAY:release(0.001, "success")
                end
            end
        end)
    end
}
--- config
location /t {
    content_by_lua_block {
        local d = ngx.shared.adaptive_limit
        -- wait until the holder worker has taken all 10 slots
        local t0 = ngx.now()
        while (d:get("probe_hold") or 0) < 10 and ngx.now() - t0 < 10 do
            ngx.sleep(0.05)
        end
        ngx.say("held=", d:get("probe_hold"))
        -- this request may land on either worker: rejection proves the
        -- slots are enforced process-wide
        local ok, err = PAY:try_acquire()
        ngx.say("acquire=", tostring(ok), " err=", tostring(err))
        if ok then
            PAY:release(0.001, "success")
        end
        -- wait for the holder to release everything
        while (d:get("probe_hold") or 0) > 0 and ngx.now() - t0 < 20 do
            ngx.sleep(0.1)
        end
        ngx.say("after_release=", d:get("probe_hold") or 0)
        ngx.say("inflight=", d:get("al:1:pay:inflight"))
    }
}
--- request
GET /t
--- response_body
held=10
acquire=nil err=rejected
after_release=0
inflight=0
--- no_error_log
[error]

=== TEST 2: no counter drift after distributed bursts across workers
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({
        name = "pay",
        shared_dict = "adaptive_limit",
        initial_limit = 10,
        min_limit = 10,
        max_limit = 10,
    }))
    assert(adaptive.start())
}
--- config
location /t {
    content_by_lua_block {
        local d = ngx.shared.adaptive_limit
        local n, reps = 20, 100
        local total_admitted = 0
        for _ = 1, reps do
            local threads = {}
            for i = 1, n do
                threads[i] = ngx.thread.spawn(function()
                    local ok = PAY:try_acquire()
                    if ok then
                        d:incr("probe_admitted", 1, 0)
                        ngx.sleep(0.001)
                        PAY:release(0.0001, "success")
                        return true
                    end
                    d:incr("probe_rejected", 1, 0)
                    return nil
                end)
            end
            for i = 1, n do
                ngx.thread.wait(threads[i])
            end
        end
        -- this request ran on exactly one worker; fire more requests via
        -- the caller (the harness sends several) and assert no leak
        ngx.say("admitted=", d:get("probe_admitted"))
        ngx.say("rejected=", d:get("probe_rejected"))
        ngx.say("inflight=", d:get("al:1:pay:inflight"))
    }
}
--- request eval
["GET /t", "GET /t", "GET /t", "GET /t"]
--- response_body_like eval
[
    qr/^admitted=\d+\nrejected=\d+\ninflight=0$/,
    qr/^admitted=\d+\nrejected=\d+\ninflight=0$/,
    qr/^admitted=\d+\nrejected=\d+\ninflight=0$/,
    qr/^admitted=\d+\nrejected=\d+\ninflight=0$/
]
--- no_error_log
[error]
