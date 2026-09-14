-- Statistics pipeline: worker-local struct -> per-window shared
-- accumulators via monotonic delta flushing.

local ngx = require "spec.mock_ngx"
local adaptive = require "resty.adaptive_limit"
local runtime = require "resty.adaptive_limit.runtime"

-- sample_window = 10s and a clock starting at 1000: window ids are
-- whole numbers of the clock (100..101..102), easy to assert on.
local function fresh_limiter()
    runtime.started = false
    runtime.registry = {}
    runtime.order = {}
    ngx.reset()
    ngx.shared.adaptive_limit = ngx.make_dict()
    local limiter = assert(adaptive.new({ name = "pay",
        shared_dict = "adaptive_limit", initial_limit = 10,
        min_limit = 1, max_limit = 10, sample_window = 10 }))
    assert(adaptive.start())
    return limiter
end

local function win_key(n, f)
    return "al:1:pay:w:" .. n .. ":" .. f
end

describe("stats flush", function()
    it("writes deltas into the accumulator of the current window", function()
        local limiter = fresh_limiter()
        assert.True(limiter:try_acquire())
        assert.True(limiter:release(0.020, "success"))
        limiter:flush(1005) -- window floor(1005/10) = 100
        local d = ngx.shared.adaptive_limit._data
        assert.are.equal(1, d[win_key(100, "c")])
        assert.True(math.abs(d[win_key(100, "s")] - 0.020) < 1e-12)
        assert.Nil(d[win_key(100, "ovl")])
    end)

    it("flushes nothing when idle (no keys are created)", function()
        local limiter = fresh_limiter()
        limiter:flush(1005)
        local d = ngx.shared.adaptive_limit._data
        assert.Nil(d[win_key(100, "c")])
    end)

    it("accumulates across flushes within one window", function()
        local limiter = fresh_limiter()
        for i = 1, 3 do
            assert.True(limiter:try_acquire())
            assert.True(limiter:release(0.010, "success"))
        end
        limiter:flush(1002)
        limiter:flush(1008)
        local d = ngx.shared.adaptive_limit._data
        assert.are.equal(3, d[win_key(100, "c")])
        assert.True(math.abs(d[win_key(100, "s")] - 0.030) < 1e-12)
    end)

    it("splits samples across window rollovers", function()
        local limiter = fresh_limiter()
        assert.True(limiter:try_acquire())
        assert.True(limiter:release(0.010, "success"))
        limiter:flush(1005) -- window 100
        ngx._now = 1010
        assert.True(limiter:try_acquire())
        assert.True(limiter:release(0.030, "timeout"))
        limiter:flush(1012) -- window 101
        local d = ngx.shared.adaptive_limit._data
        assert.are.equal(1, d[win_key(100, "c")])
        assert.are.equal(1, d[win_key(101, "c")])
        assert.are.equal(1, d[win_key(101, "tmo")])
    end)

    it("keeps unflushed deltas when the dict write fails", function()
        local limiter = fresh_limiter()
        assert.True(limiter:try_acquire())
        assert.True(limiter:release(0.010, "success"))

        -- break the dict's incr to simulate a shared-memory failure
        local real_incr = limiter.st.dict.incr
        limiter.st.dict.incr = function() return nil, "no memory" end
        limiter:flush(1005)
        assert.are.equal(1, limiter.internal_errors)

        -- recover: the same delta is retried and lands correctly
        limiter.st.dict.incr = real_incr
        limiter:flush(1006)
        local d = ngx.shared.adaptive_limit._data
        assert.are.equal(1, d[win_key(100, "c")])
        assert.are.equal(1, limiter.stats.sample_count) -- monotonic source
    end)

    it("accounts aborted, rejected and error outcomes in their buckets", function()
        local limiter = fresh_limiter()
        assert.True(limiter:try_acquire())
        assert.True(limiter:release(0.010, "aborted"))
        assert.True(limiter:try_acquire())
        assert.True(limiter:release(0.010, "error"))
        -- force a rejection
        ngx.shared.adaptive_limit._data["al:1:pay:inflight"] = 10
        limiter:try_acquire()
        limiter:flush(1005)
        local d = ngx.shared.adaptive_limit._data
        -- aborted samples release but are not latency samples
        assert.are.equal(1, d[win_key(100, "c")])
        assert.are.equal(1, d[win_key(100, "abt")])
        assert.are.equal(1, d[win_key(100, "err")])
        assert.are.equal(1, d[win_key(100, "rej")])
    end)
end)

describe("strong-signal classification (controllers)", function()
    it("treats connect errors as a strong signal", function()
        local g2 = require("resty.adaptive_limit.controller.gradient2")
        local common = require("resty.adaptive_limit.controller.common")
        local cfg = {
            min_samples = 20, sample_alpha = 0.5, baseline_alpha = 0.05,
            rtt_tolerance = 2.0, min_gradient = 0.5, smoothing = 0.5,
            headroom_min = 1, headroom_max = 50, min_limit = 1,
            max_limit = 2000, overload_min_samples = 20,
            overload_failure_ratio = 0.10, overload_backoff = 0.80,
        }
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local m = { sample_count = 100, mean_rtt = 0.020,
            connect_error_count = 20 } -- ratio 0.2 > 0.10
        local next_state = assert(common.safe_update(g2, state, m, cfg))
        -- backoff: candidate = min(110, 80) = 80 -> limit 90
        assert.are.equal(90.0, next_state.limit)
        -- and the baseline is frozen
        assert.are.equal(0.020, next_state.long_rtt)
    end)

    it("does not treat application errors as a strong signal", function()
        local g2 = require("resty.adaptive_limit.controller.gradient2")
        local common = require("resty.adaptive_limit.controller.common")
        local cfg = {
            min_samples = 20, sample_alpha = 0.5, baseline_alpha = 0.05,
            rtt_tolerance = 2.0, min_gradient = 0.5, smoothing = 0.5,
            headroom_min = 1, headroom_max = 50, min_limit = 1,
            max_limit = 2000, overload_min_samples = 20,
            overload_failure_ratio = 0.10, overload_backoff = 0.80,
        }
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local m = { sample_count = 100, mean_rtt = 0.020, error_count = 50 }
        local next_state = assert(common.safe_update(g2, state, m, cfg))
        -- no backoff: the healthy-window candidate wins -> 105. (The
        -- baseline-freeze behavior itself is covered by the backoff
        -- test above, where long_rtt provably stays put.)
        assert.are.equal(105.0, next_state.limit)
    end)
end)
