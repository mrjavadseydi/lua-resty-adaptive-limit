-- Gradient2 unit tests. Every numeric expectation is hand-computed from
-- the equations documented in lib/resty/adaptive_limit/controller/
-- gradient2.lua; epsilon is 1e-9 (differences below double-precision
-- noise for these inputs).

local g2 = require "resty.adaptive_limit.controller.gradient2"
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
    }
end

local function healthy_window(sc, mean)
    sc = sc or 1000
    return { sample_count = sc, mean_rtt = mean or 0.020,
        completions = sc, rejected_count = 1 }
end

local function near(a, b, eps)
    return math.abs(a - b) < (eps or 1e-6)
end

-- the controller's output as the next window's input (what the wiring does)
local function state_of(next_state)
    return { limit = next_state.limit, long_rtt = next_state.long_rtt,
        short_rtt = next_state.short_rtt, gradient = next_state.gradient,
        probe_restore = next_state.probe_restore }
end

describe("gradient2", function()
    it("grows by smoothing * headroom on a healthy window", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local next_state = assert(common.safe_update(g2, state,
            healthy_window(1000, 0.020), base_cfg()))
        -- short' = 0.020; long' = 0.020; gradient = clamp(2.0) = 1.0
        -- headroom = sqrt(100) = 10; candidate = 110
        -- limit' = 100*0.5 + 110*0.5 = 105
        assert.are.equal(105.0, next_state.limit)
        assert.are.equal(1.0, next_state.gradient)
        assert.True(near(next_state.long_rtt, 0.020))
        assert.True(near(next_state.short_rtt, 0.020))
    end)

    it("does not shed within rtt_tolerance", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        -- mean 1.5x baseline: short' = 0.025, long' = 0.02025
        -- gradient = clamp(2 * 0.02025 / 0.025 = 1.62, 0.5, 1) = 1.0
        local next_state = assert(common.safe_update(g2, state,
            healthy_window(1000, 0.030), base_cfg()))
        assert.are.equal(1.0, next_state.gradient)
        assert.are.equal(105.0, next_state.limit)
    end)

    it("sheds when queueing exceeds rtt_tolerance", function()
        -- State already tracking a 3x RTT episode (short EWMA caught up).
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.060 }
        -- short' = 0.060; long' = 0.020 (frozen: the window is saturated)
        -- gradient = clamp(2*0.020/0.060 = 0.6666.., 0.5, 1) = 0.6666..
        -- candidate = 66.666.. + 10 = 76.666..; limit' = 50 + 38.333.. = 88.333..
        local next_state = assert(common.safe_update(g2, state,
            healthy_window(1000, 0.060), base_cfg()))
        assert.True(near(next_state.gradient, 0.6666666666666666))
        assert.True(near(next_state.limit, 88.33333333333333))
        assert.True(near(next_state.long_rtt, 0.020))
    end)

    it("learns the baseline only from app-limited windows", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local m = healthy_window(1000, 0.060)
        m.rejected_count = 0
        -- short' = 0.040; long' = 0.05*0.040 + 0.95*0.020 = 0.021
        local next_state = assert(common.safe_update(g2, state, m, base_cfg()))
        assert.True(near(next_state.long_rtt, 0.021))
        -- the same window under saturation: the limit shapes the latency,
        -- learning it would ratchet (simulation I)
        m.rejected_count = 1
        next_state = assert(common.safe_update(g2, state, m, base_cfg()))
        assert.True(near(next_state.long_rtt, 0.020))
    end)

    it("probes the baseline under saturation every probe_interval windows", function()
        local cfg = base_cfg()
        cfg.probe_interval = 30
        cfg.probe_fraction = 0.5
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.044 }
        -- window 29: normal update, no probe
        local m = healthy_window(1000, 0.044)
        m.window = 29
        local s1 = assert(common.safe_update(g2, state, m, cfg))
        assert.is_nil(s1.probe_restore)
        -- window 30: normal update (limit' = 50 + (100*0.9090.. + 10)*0.5
        -- = 100.4545..), then published at half for the probe
        m.window = 30
        local s2 = assert(common.safe_update(g2, state, m, cfg))
        assert.True(near(s2.probe_restore, 100.45454545454545))
        assert.True(near(s2.limit, 50.22727272727273))
        -- window 31 is polluted by pre-probe admissions: held at the
        -- probe limit, nothing learned
        m.window = 31
        m.mean_rtt = 0.030
        local s3 = assert(common.safe_update(g2, state_of(s2), m, cfg))
        assert.True(s3.held)
        assert.True(near(s3.limit, 50.22727272727273))
        assert.True(near(s3.long_rtt, 0.020))
        assert.True(near(s3.short_rtt, 0.044))
        -- window 32 ran entirely at the probe limit: it re-seeds the
        -- baseline (up or down) and restores the limit
        m.window = 32
        m.mean_rtt = 0.025
        local s4 = assert(common.safe_update(g2, state_of(s3), m, cfg))
        assert.is_nil(s4.probe_restore)
        assert.True(near(s4.limit, 100.45454545454545))
        assert.True(near(s4.long_rtt, 0.025))
        -- app-limited windows never probe: the baseline learns directly
        m.window = 60
        m.rejected_count = 0
        assert.is_nil(common.safe_update(g2, state, m, cfg).probe_restore)
        m.rejected_count = 1
        -- an overloaded probe window learns nothing but still restores
        m.window = 32
        m.overload_count = 500
        local s5 = assert(common.safe_update(g2, state_of(s3), m, cfg))
        assert.is_nil(s5.probe_restore)
        assert.True(near(s5.long_rtt, 0.020))
    end)

    it("backs off faster on explicit overload windows and freezes the baseline", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local m = healthy_window(100, 0.060)
        m.overload_count = 20 -- ratio 0.2 > 0.10
        -- overloaded: long frozen at 0.020; short' = 0.040
        -- gradient = clamp(2*0.020/0.040 = 1.0) = 1.0; candidate = 110
        -- backoff: candidate = min(110, 100*0.8) = 80; limit' = 50+40 = 90
        local next_state = assert(common.safe_update(g2, state, m, base_cfg()))
        assert.are.equal(90.0, next_state.limit)
        assert.True(near(next_state.long_rtt, 0.020)) -- frozen, not poisoned
    end)

    it("gates backoff on overload.min_samples", function()
        local cfg = base_cfg()
        cfg.overload_min_samples = 30
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local m = healthy_window(25, 0.060)
        m.overload_count = 20 -- ratio 0.8 > 0.10 but sc < overload_min_samples
        -- backoff must not fire; baseline is still frozen (strong signals)
        -- gradient = 1.0; candidate = 110; limit' = 105
        local next_state = assert(common.safe_update(g2, state, m, cfg))
        assert.are.equal(105.0, next_state.limit)
        assert.True(near(next_state.long_rtt, 0.020))
    end)

    it("holds exactly when samples are insufficient", function()
        local state = { limit = 137.5, long_rtt = 0.021, short_rtt = 0.023 }
        local next_state = assert(common.safe_update(g2, state,
            healthy_window(19, 0.5), base_cfg()))
        assert.True(next_state.held)
        assert.are.equal(137.5, next_state.limit)
        assert.True(near(next_state.long_rtt, 0.021))
        assert.True(near(next_state.short_rtt, 0.023))
    end)

    it("holds exactly on a zero-sample window", function()
        local state = { limit = 137.5, long_rtt = 0.021, short_rtt = 0.023 }
        local next_state = assert(common.safe_update(g2, state,
            { sample_count = 0 }, base_cfg()))
        assert.True(next_state.held)
        assert.are.equal(137.5, next_state.limit)
    end)

    it("backs off on explicit failures even without latency samples", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local next_state = assert(common.safe_update(g2, state, {
            sample_count = 0,
            completions = 100,
            connect_error_count = 100,
        }, base_cfg()))
        assert.are.equal(90, next_state.limit)
        assert.are.equal(0.020, next_state.long_rtt)
        assert.are.equal(0.020, next_state.short_rtt)
    end)

    it("does not grow when demand has not reached the current limit", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local m = healthy_window(100, 0.020)
        m.rejected_count = 0
        local next_state = assert(common.safe_update(g2, state, m, base_cfg()))
        assert.are.equal(100, next_state.limit)
    end)

    it("seeds RTT state from the first sufficient window", function()
        local state = { limit = 50 } -- no RTT state yet (cold start)
        -- short = 0.020 (seed); long = 0.020 (seed); gradient = 1.0
        -- headroom = sqrt(50) = 7.0710678..; candidate = 57.0710678..
        -- limit' = 25 + 28.5355339.. = 53.5355339..
        local next_state = assert(common.safe_update(g2, state,
            healthy_window(100, 0.020), base_cfg()))
        assert.True(near(next_state.limit, 53.53553390593274))
        assert.True(near(next_state.long_rtt, 0.020))
        assert.True(near(next_state.short_rtt, 0.020))
    end)

    it("treats zero RTT as healthy without dividing by zero", function()
        local state = { limit = 100, long_rtt = 0, short_rtt = 0 }
        local next_state = assert(common.safe_update(g2, state,
            healthy_window(100, 0), base_cfg()))
        assert.are.equal(1.0, next_state.gradient)
        assert.are.equal(105.0, next_state.limit)
    end)

    it("never exceeds max_limit", function()
        local state = { limit = 1998, long_rtt = 0.020, short_rtt = 0.020 }
        -- candidate = 1998 + sqrt(1998) = 2042.69..; limit' = 2020.34..
        -- clamped to 2000
        local next_state = assert(common.safe_update(g2, state,
            healthy_window(100, 0.020), base_cfg()))
        assert.are.equal(2000, next_state.limit)
    end)

    it("never drops below min_limit", function()
        local cfg = base_cfg()
        cfg.min_limit = 2.5
        cfg.overload_backoff = 0.5
        local state = { limit = 3.0, long_rtt = 0.020, short_rtt = 0.200 }
        local m = healthy_window(1000, 0.200)
        m.overload_count = 200 -- ratio 0.2 > 0.10: overloaded
        -- baseline frozen at 0.020; short' = 0.200
        -- gradient = clamp(2*0.020/0.200 = 0.2, 0.5, 1) = 0.5
        -- headroom = clamp(sqrt(3.0) = 1.7320508, 1, 50)
        -- candidate = min(3.0*0.5 + 1.7320508, 3.0*0.5) = 1.5
        -- limit' = 3.0*0.5 + 1.5*0.5 = 2.25 -> clamped to 2.5
        local next_state = assert(common.safe_update(g2, state, m, cfg))
        assert.are.equal(2.5, next_state.limit)
    end)

    it("clamps the gradient at min_gradient under extreme queueing", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 1.0 }
        -- gradient = clamp(2*0.024/1.0 = 0.048, 0.5, 1) = 0.5
        -- candidate = 50 + 10 = 60; limit' = 80
        local next_state = assert(common.safe_update(g2, state,
            healthy_window(1000, 1.0), base_cfg()))
        assert.are.equal(0.5, next_state.gradient)
        assert.True(near(next_state.limit, 80.0))
    end)

    it("does not let aborts poison the baseline", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local m = healthy_window(100, 0.030)
        m.aborted_count = 50 -- client aborts are not strong signals
        m.rejected_count = 0
        -- baseline updates normally: long' = 0.020 + 0.05*(0.025-0.020)
        --                          = 0.02025
        local next_state = assert(common.safe_update(g2, state, m, base_cfg()))
        assert.True(near(next_state.long_rtt, 0.02025))
    end)
end)
