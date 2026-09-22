-- Controller wiring regressions (limiter.control_window against the ngx
-- mock): measurement validation with real flushed accumulators, and the
-- hold/repair path when the shared controller state fails validation.

local ngx = require "spec.mock_ngx"
local adaptive = require "resty.adaptive_limit"
local runtime = require "resty.adaptive_limit.runtime"

local function fresh_limiter(cfg_overrides)
    runtime.started = false
    runtime.registry = {}
    runtime.order = {}
    ngx.reset()
    ngx.shared.adaptive_limit = ngx.make_dict()
    local cfg = {
        name = "pay",
        shared_dict = "adaptive_limit",
        initial_limit = 10,
        min_limit = 1,
        max_limit = 10,
        sample_window = 10,
    }
    if cfg_overrides then
        for k, v in pairs(cfg_overrides) do
            cfg[k] = v
        end
    end
    local limiter = assert(adaptive.new(cfg))
    assert(adaptive.start())
    return limiter
end

local function win_key(n, f)
    return "al:1:pay:w:" .. n .. ":" .. f
end

describe("control_window measurement validation", function()
    it("processes a legitimate abort-heavy window (no false corruption)",
        function()
            local limiter = fresh_limiter()
            -- 40 sampled successes + 60 client aborts (never sampled):
            -- class counts exceed sample_count, which is legitimate
            for _ = 1, 40 do
                assert.True(limiter:try_acquire())
                assert.True(limiter:release(0.010, "success"))
            end
            for _ = 1, 60 do
                assert.True(limiter:try_acquire())
                assert.True(limiter:release(nil, "aborted"))
            end
            limiter:flush(1005) -- window 100
            local d = ngx.shared.adaptive_limit._data
            assert.are.equal(40, d[win_key(100, "c")])
            assert.are.equal(60, d[win_key(100, "abt")])
            assert.are.equal(100, d[win_key(100, "cmp")])

            assert.True(limiter:control_window(100, 1015))
            -- not rejected as corrupt: no internal error, no skip, the
            -- window was processed and published
            assert.are.equal(0, limiter.internal_errors)
            assert.are.equal(0, limiter.controller_skips or 0)
            assert.are.equal(1, limiter.controller_updates or 0)
            assert.are.equal(100,
                ngx.shared.adaptive_limit._data["al:1:pay:last_window"])
    end)

    it("backs off on explicit failures without latency samples", function()
        local limiter = fresh_limiter({
            initial_limit = 50,
            min_limit = 1,
            max_limit = 100,
        })
        for _ = 1, 20 do
            assert.True(limiter:try_acquire())
            assert.True(limiter:release(nil, "connect_error"))
        end
        limiter:flush(1005)

        assert.True(limiter:control_window(100, 1015))
        assert.are.equal(45,
            ngx.shared.adaptive_limit:get("al:1:pay:limit_f"))
        assert.are.equal(1, limiter.controller_updates)
    end)

    it("repairs out-of-policy limit when max_limit was lowered on reload",
        function()
            -- Simulate pre-reload state with limit=10
            local limiter = fresh_limiter({
                initial_limit = 3,
                min_limit = 1,
                max_limit = 3,
                min_samples = 5,
            })
            local dict = ngx.shared.adaptive_limit
            dict:set("al:1:pay:limit_f", 10)
            dict:set("al:1:pay:limit", 10)

            -- Window 100 has some samples
            for _ = 1, 5 do
                assert.True(limiter:try_acquire())
                assert.True(limiter:release(0.010, "success"))
            end
            limiter:flush(1005)

            -- Window 100 control: 10 exceeds max_limit 3, safe_update rejects it.
            -- Hold path must repair limit to max_limit (3) before publishing.
            assert.True(limiter:control_window(100, 1015))
            assert.are.equal(1, limiter.controller_skips)
            assert.are.equal(1, limiter.internal_errors)
            assert.are.equal(3, dict:get("al:1:pay:limit_f"))
            assert.are.equal(3, dict:get("al:1:pay:limit"))

            -- Window 101: shared limit is now 3 (valid!). Next window adapts normally.
            for _ = 1, 5 do
                assert.True(limiter:try_acquire())
                assert.True(limiter:release(0.010, "success"))
            end
            limiter:flush(1015)
            assert.True(limiter:control_window(101, 1025))
            assert.are.equal(1, limiter.controller_updates)
            -- internal errors did not increase
            assert.are.equal(1, limiter.internal_errors)
        end)

    it("repairs non-numeric shared limit to initial_limit without throwing",
        function()
            local limiter = fresh_limiter({
                initial_limit = 5,
                min_limit = 1,
                max_limit = 10,
            })
            local dict = ngx.shared.adaptive_limit
            dict:set("al:1:pay:limit_f", "corrupt_string")
            dict:set("al:1:pay:limit", "corrupt_string")

            for _ = 1, 5 do
                assert.True(limiter:try_acquire())
                assert.True(limiter:release(0.010, "success"))
            end
            limiter:flush(1005)

            -- control_window must not throw in math.floor
            assert.True(limiter:control_window(100, 1015))
            assert.are.equal(1, limiter.controller_skips)
            assert.are.equal(5, dict:get("al:1:pay:limit_f"))
            assert.are.equal(5, dict:get("al:1:pay:limit"))
        end)

    it("drops a corrupt shared long_rtt instead of re-rejecting it every window",
        function()
            local limiter = fresh_limiter()
            local dict = ngx.shared.adaptive_limit
            dict:set("al:1:pay:long_rtt", "corrupt_string")

            for _ = 1, 25 do
                assert.True(limiter:try_acquire())
                assert.True(limiter:release(0.010, "success"))
            end
            limiter:flush(1005)
            assert.True(limiter:control_window(100, 1015))
            assert.are.equal(1, limiter.internal_errors)
            -- the repair published nil: the corrupt key is gone
            assert.is_nil(dict:get("al:1:pay:long_rtt"))

            -- the next window is clean: no second internal error
            for _ = 1, 25 do
                assert.True(limiter:try_acquire())
                assert.True(limiter:release(0.010, "success"))
            end
            limiter:flush(1015)
            assert.True(limiter:control_window(101, 1025))
            assert.are.equal(1, limiter.internal_errors)
            assert.is_near(0.010, dict:get("al:1:pay:long_rtt"), 1e-9)
        end)

    it("abandons a window a sibling already superseded after the lease", function()
        local limiter = fresh_limiter({ max_limit = 100 })
        local dict = ngx.shared.adaptive_limit
        for _ = 1, 25 do
            assert.True(limiter:try_acquire())
            assert.True(limiter:release(0.010, "success"))
        end
        limiter:flush(1005)

        -- this worker takes the lease on window 100, then stalls past
        -- LEASE_TTL; a sibling re-leases, processes 100..102 and publishes
        -- limit 42 at last_window 102 before we re-read the state
        local st = limiter.st
        local read_window = st.read_window
        st.read_window = function(self, n)
            st.read_window = read_window
            local acc = read_window(self, n)
            assert(st:publish_controller_state(42, 0.010, 0.010, 1.0, nil, 102, 1020))
            st:delete_window(100)
            return acc
        end
        assert.falsy(limiter:control_window(100, 1015))

        -- nothing moved backwards: the sibling's publication stands
        assert.are.equal(102, dict:get("al:1:pay:last_window"))
        assert.are.equal(42, dict:get("al:1:pay:limit"))
        assert.are.equal(1, limiter.controller_skips)
        assert.are.equal(1, limiter.anomalies.stale_controller_window)
        assert.are.equal(0, limiter.controller_updates or 0)
        assert.are.equal(0, limiter.internal_errors)
    end)

    it("keeps the window when publication fails so it is retried", function()
        local limiter = fresh_limiter()
        local dict = ngx.shared.adaptive_limit
        for _ = 1, 25 do
            assert.True(limiter:try_acquire())
            assert.True(limiter:release(0.010, "success"))
        end
        limiter:flush(1005)

        local set = dict.set
        dict.set = function(d, key, ...)
            if key == "al:1:pay:limit" then
                return nil, "no memory"
            end
            return set(d, key, ...)
        end
        assert.falsy(limiter:control_window(100, 1015))
        dict.set = set
        assert.are.equal(1, limiter.internal_errors)
        -- a failed publication is not an update
        assert.are.equal(0, limiter.controller_updates or 0)
        -- last_window did not advance and the accumulators survived
        assert.are.equal(0, dict:get("al:1:pay:last_window"))
        assert.are.equal(25, dict:get(win_key(100, "c")))
    end)

    it("adopt_shared_state clamps adopted limit into [min_limit, max_limit]",
        function()
            fresh_limiter()
            local dict = ngx.shared.adaptive_limit
            dict:set("al:1:pay2:limit_f", 50)
            dict:set("al:1:pay2:limit", 50)

            local l2 = assert(adaptive.new({
                name = "pay2",
                shared_dict = "adaptive_limit",
                initial_limit = 5,
                min_limit = 2,
                max_limit = 15,
            }))
            local adopted = assert(l2:adopt_shared_state())
            assert.are.equal(15, adopted)
            assert.are.equal(15, l2._last_limit)
        end)

    it("cache manager / loader processes with nil worker_id start and tick safely",
        function()
            runtime.started = false
            runtime.registry = {}
            runtime.order = {}
            ngx.reset()
            ngx.shared.adaptive_limit = ngx.make_dict()

            local orig_id = ngx.worker.id
            ngx.worker.id = function() return nil end

            local l = assert(adaptive.new({
                name = "pay",
                shared_dict = "adaptive_limit",
                initial_limit = 10,
            }))
            assert.True(adaptive.start())

            -- tick with nil worker_id must not throw or increment timer_failures
            l:tick(1000, nil)
            assert.are.equal(0, l.timer_failures or 0)

            ngx.worker.id = orig_id
        end)

    it("refuses to start when the shared dict cannot expire keys", function()
        runtime.started = false
        runtime.registry = {}
        runtime.order = {}
        ngx.reset()
        local dict = ngx.make_dict()
        dict.expire = nil
        ngx.shared.adaptive_limit = dict
        assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit" }))
        local ok, err = adaptive.start()
        assert.falsy(ok)
        assert.True(tostring(err):find("expire", 1, true) ~= nil)
    end)
end)
