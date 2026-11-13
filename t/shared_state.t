# Recovery and publication regressions against real shared memory/workers.
use Test::Nginx::Socket 'no_plan';

repeat_each(1);
workers(2);
master_process_enabled('on');
timeout(30);

run_tests();

__DATA__

=== TEST 1: missing inflight never forgets sibling holdings under fail_closed
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 2, min_limit = 2, max_limit = 2,
        failure_mode = "fail_closed" }))
    assert(adaptive.start({ flush_interval = 10 }))
    ngx.timer.at(0.05, function(premature)
        if premature then return end
        assert(PAY:try_acquire())
        ngx.shared.adaptive_limit:incr("holding_workers", 1, 0)
    end)
}
--- config
location /run {
    content_by_lua_block {
        local d = ngx.shared.adaptive_limit
        local t0 = ngx.now()
        while (d:get("holding_workers") or 0) < 2 and ngx.now() - t0 < 5 do
            ngx.sleep(0.01)
        end
        assert(d:get("holding_workers") == 2)
        d:delete(PAY.K_inflight)
        for _ = 1, 2 do
            local ok, err = PAY:try_acquire()
            ngx.say("acquire=", tostring(ok), " err=", tostring(err))
        end
        local ok, err = PAY:access()
        ngx.say("access=", tostring(ok), " err=", tostring(err))
        ok, err = PAY:release(0.01)
        ngx.say("release=", tostring(ok), " err=", tostring(err))
        ngx.say("counter=", tostring(d:get(PAY.K_inflight)), " local_held=", PAY._inflight)
    }
}
--- request
GET /run
--- response_body
acquire=nil err=internal_error
acquire=nil err=internal_error
access=nil err=internal_error
release=nil err=internal_error
counter=nil local_held=1
--- error_log
inflight counter missing
--- no_error_log
[alert]

=== TEST 2: limiter-wide lease orders adjacent startup windows across workers
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 100, min_limit = 1, max_limit = 200,
        probe_interval = 0 }))
    assert(adaptive.start({ flush_interval = 10 }))
    local d = ngx.shared.adaptive_limit
    if ngx.worker.id() == 0 then
        PAY.st:add_window(998, "tmo", 20, 20)
        PAY.st:add_window(998, "cmp", 20, 20)
        local algorithm = PAY.algorithm
        local paused = false
        PAY.algorithm = { update = function(state, m, cfg)
            if not paused then
                paused = true
                d:set("a_paused", true)
                -- Model process preemption after A reads state for 997.
                ngx.sleep(0.2)
            end
            return algorithm.update(state, m, cfg)
        end }
        ngx.timer.at(0.05, function(premature)
            if premature then return end
            PAY:control(1000.3)
            d:set("a_done", true)
        end)
    else
        ngx.timer.at(0.05, function(premature)
            if premature then return end
            local t0 = ngx.now()
            while not d:get("a_paused") and ngx.now() - t0 < 5 do ngx.sleep(0.01) end
            assert(d:get("a_paused"))
            PAY:control(1000.4)
            d:set("b_last_while_a_paused", d:get(PAY.st.K.last_window))
            while not d:get("a_done") and ngx.now() - t0 < 5 do ngx.sleep(0.01) end
            assert(d:get("a_done"))
            PAY:control(1000.4)
            d:set("b_done", true)
        end)
    end
}
--- config
location /run {
    content_by_lua_block {
        local d = ngx.shared.adaptive_limit
        local t0 = ngx.now()
        while not d:get("b_done") and ngx.now() - t0 < 5 do ngx.sleep(0.01) end
        assert(d:get("b_done"))
        ngx.say("during_pause=", d:get("b_last_while_a_paused"))
        ngx.say("last_window=", d:get(PAY.st.K.last_window), " limit=", d:get(PAY.K_limit))
        ngx.say("lease=", tostring(d:get(PAY.st.K.controller_lease)))
    }
}
--- request
GET /run
--- response_body
during_pause=0
last_window=999 limit=90
lease=nil
--- no_error_log
[error]
[alert]

=== TEST 3: changing window size refuses adoption and preserves existing progress
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit" }))
    assert(adaptive.start({ flush_interval = 10 }))
}
--- config
location /run {
    content_by_lua_block {
        local adaptive = require("resty.adaptive_limit")
        local runtime = require("resty.adaptive_limit.runtime")
        assert(PAY.st:publish_controller_state(80, nil, nil, nil, nil, 999, 1000))
        runtime.registry = {}
        runtime.order = {}
        assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit", sample_window = 2 }))
        local ok, err = adaptive.start()
        ngx.say("start=", tostring(ok), " mismatch=", tostring(err:find("sample_window changed", 1, true) ~= nil))
        ngx.say("started=", tostring(runtime.started))
        local d = ngx.shared.adaptive_limit
        ngx.say("last_window=", d:get(PAY.st.K.last_window), " limit=", d:get(PAY.K_limit))
    }
}
--- request
GET /run
--- response_body
start=nil mismatch=true
started=false
last_window=999 limit=80
--- no_error_log
[error]
[alert]

=== TEST 4: timeout-only probes back off both controllers at the reduced limit
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    LIMITERS = {}
    for _, name in ipairs({ "gradient2", "aimd" }) do
        LIMITERS[#LIMITERS + 1] = assert(adaptive.new({ name = name,
            shared_dict = "adaptive_limit", algorithm = name,
            initial_limit = 100, min_limit = 1, max_limit = 200 }))
    end
    assert(adaptive.start({ flush_interval = 10 }))
}
--- config
location /run {
    content_by_lua_block {
        local d = ngx.shared.adaptive_limit
        for _, l in ipairs(LIMITERS) do
            assert(l.st:publish_controller_state(50, 0.02, 0.02, nil, 100, 31, 32))
            for _ = 1, 20 do
                assert(l:try_acquire())
                assert(l:release(nil, "timeout"))
            end
            l:flush(32.5)
            assert(l:control_window(32, 33.5))
            ngx.say(l.cfg.algorithm, "=", d:get(l.K_limit),
                " restore=", tostring(d:get(l.st.K.probe_restore)))
        end
    }
}
--- request
GET /run
--- response_body
gradient2=45 restore=nil
aimd=40 restore=nil
--- no_error_log
[error]
[alert]

=== TEST 5: real slab allocation failure prevents startup
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict tiny 128k;
--- config
location /run {
    content_by_lua_block {
        local d = ngx.shared.tiny
        -- A long limiter name needs a larger slab class; evicting 30
        -- short keys cannot free a page for it.
        for i = 1, 10000 do assert(d:set("fill" .. i, i)) end
        local adaptive = require("resty.adaptive_limit")
        assert(adaptive.new({ name = "x" .. string.rep("a", 62), shared_dict = "tiny" }))
        local ok, err = adaptive.start()
        ngx.say("start=", tostring(ok), " no_memory=",
            tostring(err ~= nil and err:find("no memory", 1, true) ~= nil))
        ngx.say("started=", tostring(require("resty.adaptive_limit.runtime").started))
    }
}
--- request
GET /run
--- response_body
start=nil no_memory=true
started=false
--- no_error_log
[alert]

=== TEST 6: corrupt inflight needs a full restart to discard stale worker mirrors
--- http_config
lua_package_path '/work/lib/?.lua;;';
lua_shared_dict adaptive_limit 1m;
init_worker_by_lua_block {
    local adaptive = require("resty.adaptive_limit")
    PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 2, max_limit = 2, failure_mode = "fail_closed" }))
    assert(adaptive.start({ flush_interval = 10 }))
    function WAIT_FOR(key, count)
        local d = ngx.shared.adaptive_limit
        local t0 = ngx.now()
        count = count or 1
        while (d:get(key) or 0) < count and ngx.now() - t0 < 5 do
            ngx.sleep(0.01)
        end
        assert((d:get(key) or 0) >= count, key)
    end
    ngx.timer.at(0.05, function(premature)
        if premature then return end
        local d = ngx.shared.adaptive_limit
        assert(PAY:try_acquire())
        d:incr("ready", 1, 0)
        WAIT_FOR("corrupt")
        local ok, err = PAY:release(0.02)
        assert(not ok and err == "internal_error")
        assert(PAY._inflight == 1)
        d:incr("pending_mirrors", PAY._inflight, 0)
        d:incr("released", 1, 0)
        WAIT_FOR("reset")
        if ngx.worker.id() == 0 then
            WAIT_FOR("sibling_full")
            assert(PAY:exit_worker())
            d:set("worker_exited", 1)
        else
            assert(PAY:try_acquire())
            assert(PAY:try_acquire())
            d:set("sibling_full", 1)
            WAIT_FOR("worker_exited")
            d:set("overadmitted", PAY:try_acquire() and 1 or -1)
        end
    end)
}
--- config
location /run {
    content_by_lua_block {
        local d = ngx.shared.adaptive_limit
        WAIT_FOR("ready", 2)
        d:set(PAY.K_inflight, "corrupt")
        d:set("corrupt", 1)
        WAIT_FOR("released", 2)
        ngx.say("pending_mirrors=", d:get("pending_mirrors"))
        ngx.say("counter_before_reset=", d:get(PAY.K_inflight))
        -- Demonstrate why resetting only the shared key after quiescing is unsafe.
        d:set(PAY.K_inflight, 0)
        d:set("reset", 1)
        WAIT_FOR("overadmitted")
        ngx.say("unsafe_reset_overadmitted=", d:get("overadmitted") == 1)
        ngx.say("counter=", d:get(PAY.K_inflight))
    }
}
--- request
GET /run
--- response_body
pending_mirrors=2
counter_before_reset=corrupt
unsafe_reset_overadmitted=true
counter=2
--- error_log
fully restart nginx once traffic is quiesced
--- no_error_log
[alert]
