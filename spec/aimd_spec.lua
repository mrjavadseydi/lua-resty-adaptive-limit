-- AIMD controller unit tests (hand-computed values).

local aimd = require "resty.adaptive_limit.controller.aimd"
local common = require "resty.adaptive_limit.controller.common"

local function base_cfg()
    return {
        min_samples = 20,
        sample_alpha = 0.5,
        baseline_alpha = 0.05,
        rtt_tolerance = 2.0,
        min_gradient = 0.5,
        smoothing = 0.5,
        headroom_min = 1,
        headroom_max = 50,
        min_limit = 1,
        max_limit = 2000,
        overload_min_samples = 20,
        overload_failure_ratio = 0.10,
        overload_backoff = 0.80,
        aimd_increment = 1,
        aimd_decrease = 0.8,
    }
end

local function window(sc, mean)
    return { sample_count = sc, mean_rtt = mean }
end

describe("aimd", function()
    it("adds the increment on a healthy window", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local next_state = assert(common.safe_update(aimd, state,
            window(100, 0.020), base_cfg()))
        assert.are.equal(101, next_state.limit)
        assert.are.equal(1, next_state.gradient)
    end)

    it("multiplies down when latency congestion is detected", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.060 }
        -- short' = 0.060 > rtt_tolerance * long' (= 2 * 0.022)
        local next_state = assert(common.safe_update(aimd, state,
            window(100, 0.060), base_cfg()))
        assert.are.equal(80, next_state.limit)
        assert.are.equal(0, next_state.gradient)
    end)

    it("multiplies down on strong failure signals and freezes the baseline", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local m = window(100, 0.060)
        m.timeout_count = 20 -- ratio 0.2 > 0.10
        local next_state = assert(common.safe_update(aimd, state, m, base_cfg()))
        assert.are.equal(80, next_state.limit)
        assert.are.equal(0.020, next_state.long_rtt) -- frozen
    end)

    it("holds when samples are insufficient", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local next_state = assert(common.safe_update(aimd, state,
            window(19, 0.5), base_cfg()))
        assert.True(next_state.held)
        assert.are.equal(100, next_state.limit)
    end)

    it("clamps at max_limit", function()
        local state = { limit = 2000, long_rtt = 0.020, short_rtt = 0.020 }
        local next_state = assert(common.safe_update(aimd, state,
            window(100, 0.020), base_cfg()))
        assert.are.equal(2000, next_state.limit)
    end)

    it("clamps at min_limit", function()
        local state = { limit = 1, long_rtt = 0.020, short_rtt = 0.060 }
        local next_state = assert(common.safe_update(aimd, state,
            window(100, 0.060), base_cfg()))
        assert.are.equal(1, next_state.limit)
    end)

    it("does not count client aborts as congestion", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local m = window(100, 0.030)
        m.aborted_count = 100
        local next_state = assert(common.safe_update(aimd, state, m, base_cfg()))
        assert.are.equal(101, next_state.limit)
    end)
end)
