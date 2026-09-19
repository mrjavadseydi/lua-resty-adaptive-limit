-- Configuration validation (design.md §3). Guards against silently accepted
-- nonsense — including the multi-value-return trap where a validator's
-- error string is assigned into a single local and reads as nil.

local config = require "resty.adaptive_limit.config"

local function valid_base()
    return {
        name = "payments",
        shared_dict = "adaptive_limit",
    }
end

describe("config.build", function()
    it("accepts a minimal valid configuration and applies defaults", function()
        local cfg = assert(config.build(valid_base()))
        assert.are.equal("payments", cfg.name)
        assert.are.equal("gradient2", cfg.algorithm)
        assert.are.equal(50, cfg.initial_limit)
        assert.are.equal(1, cfg.min_limit)
        assert.are.equal(2000, cfg.max_limit)
        assert.are.equal(1.0, cfg.sample_window)
        assert.are.equal(20, cfg.min_samples)
        assert.are.equal("fail_open", cfg.failure_mode)
    end)

    it("applies profiles but lets explicit options win", function()
        local cfg = assert(config.build({
            name = "x", shared_dict = "d", profile = "conservative",
        }))
        assert.are.equal(1.5, cfg.rtt_tolerance)
        local cfg2 = assert(config.build({
            name = "x", shared_dict = "d", profile = "conservative",
            rtt_tolerance = 3.0,
        }))
        assert.are.equal(3.0, cfg2.rtt_tolerance)
    end)

    it("rejects an unknown profile explicitly", function()
        assert.falsy(config.build({ name = "x", shared_dict = "d",
            profile = "aggressive" }))
    end)

    it("rejects every invalid configuration case", function()
        local cases = {
            { name = "x", shared_dict = "d", min_limit = 0 },
            { name = "x", shared_dict = "d", min_limit = 100, max_limit = 10 },
            { name = "x", shared_dict = "d", min_limit = 10, max_limit = 100,
                initial_limit = 5 },
            { name = "x", shared_dict = "d", min_limit = 10, max_limit = 100,
                initial_limit = 500 },
            { name = "x", shared_dict = "d", sample_window = 0 },
            { name = "x", shared_dict = "d", sample_window = -1 },
            { name = "x", shared_dict = "d", min_samples = 0 },
            { name = "x", shared_dict = "d", smoothing = 0 },
            { name = "x", shared_dict = "d", smoothing = 1.5 },
            { name = "x", shared_dict = "d", overload_backoff = 0 },
            { name = "x", shared_dict = "d", overload_backoff = 1 },
            { name = "x", shared_dict = "d", overload_failure_ratio = 0 },
            { name = "x", shared_dict = "d", rtt_tolerance = 1 },
            { name = "x", shared_dict = "d", min_gradient = 1 },
            { name = "x", shared_dict = "d", min_gradient = 0 },
            { name = "x", shared_dict = "d", baseline_alpha = 0 },
            { name = "x", shared_dict = "d", baseline_alpha = 1 },
            { name = "x", shared_dict = "d", sample_alpha = 0 },
            { name = "x", shared_dict = "d", headroom_max = 5,
                headroom_min = 10 },
            { name = "x", shared_dict = "d", algorithm = "wild" },
            { name = "x", shared_dict = "d", failure_mode = "fail_silent" },
            { name = "x", shared_dict = "d", latency_source = "vibes" },
            { name = "x", shared_dict = "d", upstream_time_choice = "first" },
            { name = "9x", shared_dict = "d" },
            { name = "Payments", shared_dict = "d" },
            { name = "pay ments", shared_dict = "d" },
            { name = "pay/me", shared_dict = "d" },
            { name = string.rep("x", 64), shared_dict = "d" },
            { shared_dict = "d" },
            { name = "x" },
            { name = "x", shared_dict = "" },
        }
        for i = 1, #cases do
            local cfg, err = config.build(cases[i])
            assert.falsy(cfg, "case " .. i .. " was accepted")
            assert.True(type(err) == "string" and #err > 0,
                "case " .. i .. " produced no message")
            assert.match("adaptive_limit config:", err, 1, true)
        end
    end)

    it("rejects a non-table options argument", function()
        assert.falsy(config.build(nil))
        assert.falsy(config.build("name=payments"))
    end)

    it("rejects non-finite numeric options", function()
        local fields = {
            "retry_after", "stale_threshold", "aggregation_grace",
            "rtt_tolerance", "min_gradient", "smoothing", "headroom_min",
            "headroom_max", "baseline_alpha", "sample_alpha",
            "overload_failure_ratio", "overload_backoff", "aimd_increment",
            "aimd_decrease",
        }
        for _, field in ipairs(fields) do
            for _, value in ipairs({ 0 / 0, math.huge, -math.huge }) do
                local opts = valid_base()
                opts[field] = value
                assert.falsy(config.build(opts), field .. " accepted non-finite value")
            end
        end
    end)
end)
