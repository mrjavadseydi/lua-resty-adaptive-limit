-- util/clamp and util/ewma unit tests, plus the controller/common
-- validation boundary.

local clamp = require "resty.adaptive_limit.util.clamp"
local ewma = require "resty.adaptive_limit.util.ewma"
local common = require "resty.adaptive_limit.controller.common"
local g2 = require "resty.adaptive_limit.controller.gradient2"

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

describe("clamp", function()
    it("clamps low and high", function()
        assert.are.equal(1, clamp(-5, 1, 10))
        assert.are.equal(10, clamp(50, 1, 10))
        assert.are.equal(7, clamp(7, 1, 10))
        assert.are.equal(1, clamp(1, 1, 10))
        assert.are.equal(10, clamp(10, 1, 10))
    end)
end)

describe("ewma", function()
    it("seeds from a nil current", function()
        assert.are.equal(5, ewma(nil, 5, 0.3))
    end)

    it("computes the documented update", function()
        assert.are.equal(15, ewma(10, 20, 0.5))
        assert.are.equal(10.5, ewma(10, 20, 0.05))
    end)

    it("alpha 1 replaces the value", function()
        assert.are.equal(20, ewma(10, 20, 1))
    end)
end)

describe("controller.common validation", function()
    local cfg = base_cfg()

    it("accepts a valid measurement and normalizes defaults", function()
        local m = assert(common.validate_measurement(
            { sample_count = 10, mean_rtt = 0.5 }, cfg))
        assert.are.equal(10, m.sample_count)
        assert.are.equal(0, m.overload_count)
        assert.are.equal(0.5, m.mean_rtt)
    end)

    it("rejects NaN and infinite mean_rtt", function()
        assert.falsy(common.validate_measurement(
            { sample_count = 10, mean_rtt = 0 / 0 }, cfg))
        assert.falsy(common.validate_measurement(
            { sample_count = 10, mean_rtt = 1 / 0 }, cfg))
    end)

    it("rejects negative latency and fractional/negative counts", function()
        assert.falsy(common.validate_measurement(
            { sample_count = 10, mean_rtt = -0.5 }, cfg))
        assert.falsy(common.validate_measurement(
            { sample_count = 10, overload_count = 1.5 }, cfg))
        assert.falsy(common.validate_measurement(
            { sample_count = 10, error_count = -1 }, cfg))
    end)

    it("rejects class counts exceeding sample_count (corruption)", function()
        assert.falsy(common.validate_measurement(
            { sample_count = 10, overload_count = 11 }, cfg))
        assert.falsy(common.validate_measurement(
            { sample_count = 10, overload_count = 6, timeout_count = 6 }, cfg))
    end)

    it("rejects invalid state (NaN limit, out of range, bad RTT)", function()
        assert.falsy(common.validate_state({ limit = 0 / 0 }, cfg))
        assert.falsy(common.validate_state({ limit = 0 }, cfg))
        assert.falsy(common.validate_state({ limit = 5000 }, cfg))
        assert.falsy(common.validate_state(
            { limit = 100, long_rtt = 0 / 0 }, cfg))
        assert.falsy(common.validate_state(
            { limit = 100, short_rtt = -1 }, cfg))
        assert.True(common.validate_state({ limit = 100 }, cfg))
    end)

    it("safe_update rejects corrupted input instead of corrupting state", function()
        local state = { limit = 100, long_rtt = 0.020, short_rtt = 0.020 }
        local next_state, err = common.safe_update(g2, state,
            { sample_count = 100, mean_rtt = 0 / 0 }, cfg)
        assert.falsy(next_state)
        assert.True(type(err) == "string" and #err > 0)
        assert.are.equal(100, state.limit) -- untouched
    end)

    it("safe_update rejects an invalid initial state", function()
        local next_state, err = common.safe_update(g2,
            { limit = "corrupted" }, { sample_count = 0 }, cfg)
        assert.falsy(next_state)
        assert.match("state", err, 1, true)
    end)
end)
