# Admission invariants against a stable limit (design.md §1), single worker.
#
# The light-thread bursts exercise the full admission/release machinery
# under cooperative interleaving with thousands of repetitions. True
# cross-process concurrency is exercised with 4 real workers in
# multi_worker.t and under wrk load in the benchmark/soak harness.
use Test::Nginx::Socket 'no_plan';

repeat_each(1);
workers(1);
timeout(60);

run_tests();

__DATA__

=== TEST 1: sequential admission honors the stable limit
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
        local acquired = 0
        for i = 1, 10 do
            if PAY:try_acquire() then
                acquired = acquired + 1
            end
        end
        local eleventh, err = PAY:try_acquire()
        ngx.say("acquired=", acquired)
        ngx.say("eleventh=", tostring(eleventh), " err=", tostring(err))
        for i = 1, acquired do
            PAY:release(0.001, "success")
        end
        ngx.say("after_release=", tostring(PAY:try_acquire()))
        PAY:release(0.001, "success")
        local d = ngx.shared.adaptive_limit
        ngx.say("inflight=", d:get("al:1:pay:inflight"))
        ngx.say("rejected_total=", PAY.stats.rejected_total)
    }
}
--- request
GET /t
--- response_body
acquired=10
eleventh=nil err=rejected
after_release=true
inflight=0
rejected_total=1
--- no_error_log
[error]

=== TEST 2: deterministic burst — cap, then exact release accounting
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
        d:delete("probe_current")
        d:delete("probe_peak")
        d:delete("probe_admitted")
        d:delete("probe_rejected")
        local n, reps = 20, 300
        for _ = 1, reps do
            local threads = {}
            for i = 1, n do
                threads[i] = ngx.thread.spawn(function()
                    local ok = PAY:try_acquire()
                    if ok then
                        d:incr("probe_admitted", 1, 0)
                        local cur = d:incr("probe_current", 1, 0)
                        local peak = d:get("probe_peak") or 0
                        if cur > peak then
                            d:set("probe_peak", cur)
                        end
                        ngx.sleep(0.003)
                        d:incr("probe_current", -1)
                        PAY:release(0.0001, "success")
                    else
                        d:incr("probe_rejected", 1, 0)
                    end
                end)
            end
            for i = 1, n do
                ngx.thread.wait(threads[i])
            end
        end
        ngx.say("admitted=", d:get("probe_admitted"))
        ngx.say("peak=", d:get("probe_peak"))
        ngx.say("current=", d:get("probe_current") or 0)
        ngx.say("inflight=", d:get("al:1:pay:inflight"))
    }
}
--- request
GET /t
--- response_body
admitted=3000
peak=10
current=0
inflight=0
--- no_error_log
[error]

=== TEST 3: interleaved stress — staggered acquire/release overlap
-- Threads sleep before acquiring so acquire/compare/rollback/release
-- paths interleave across light threads; only the invariants (peak <=
-- limit, no leaks, conservation of decisions) are asserted.
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
        d:delete("probe_current")
        d:delete("probe_peak")
        d:delete("probe_admitted")
        d:delete("probe_rejected")
        local n, reps = 50, 200
        for _ = 1, reps do
            local threads = {}
            for i = 1, n do
                threads[i] = ngx.thread.spawn(function()
                    ngx.sleep((i % 7) * 0.001)
                    local ok = PAY:try_acquire()
                    if ok then
                        d:incr("probe_admitted", 1, 0)
                        local cur = d:incr("probe_current", 1, 0)
                        local peak = d:get("probe_peak") or 0
                        if cur > peak then
                            d:set("probe_peak", cur)
                        end
                        ngx.sleep(0.002)
                        d:incr("probe_current", -1)
                        PAY:release(0.0001, "success")
                    else
                        d:incr("probe_rejected", 1, 0)
                    end
                end)
            end
            for i = 1, n do
                ngx.thread.wait(threads[i])
            end
        end
        local admitted = d:get("probe_admitted") or 0
        local rejected = d:get("probe_rejected") or 0
        ngx.say("conserved=", admitted + rejected == n * reps)
        ngx.say("peak=", d:get("probe_peak"))
        ngx.say("current=", d:get("probe_current") or 0)
        ngx.say("inflight=", d:get("al:1:pay:inflight"))
    }
}
--- request
GET /t
--- response_body_like
^conserved=true
peak=10
current=0
inflight=0
--- no_error_log
[error]
