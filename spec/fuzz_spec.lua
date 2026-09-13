-- Property/fuzz tests (spec §54) over generated measurement sequences.
--
-- Deterministic: a pure-Lua LCG, fixed seed. Properties:
--   - published limit always finite, positive, min_limit <= limit <= max_limit
--   - RTT state always finite and non-negative
--   - zero-sample windows cannot move anything
--   - invalid measurements are rejected and cannot corrupt persistent state
--   - identical seeds produce identical trajectories (determinism)

local g2 = require "resty.adaptive_limit.controller.gradient2"
local aimd = require "resty.adaptive_limit.controller.aimd"
local common = require "resty.adaptive_limit.controller.common"

local MIN, MAX = 5, 500

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
        min_limit = MIN,
        max_limit = MAX,
        overload_min_samples = 20,
        overload_failure_ratio = 0.10,
        overload_backoff = 0.80,
        aimd_increment = 1,
        aimd_decrease = 0.8,
    }
end

-- Deterministic LCG (glibc constants); no dependence on math.random.
local function lcg(seed)
    local s = seed
    return function(max)
        s = (s * 1103515245 + 12345) % 2147483648
        return s % max
    end
end

local function gen_measurement(rand)
    local r = rand(100)
    if r < 10 then
        -- corrupt measurements: NaN, inf, negative, impossible counts
        local corrupt = {
            { sample_count = 100, mean_rtt = 0 / 0 },
            { sample_count = 100, mean_rtt = 1 / 0 },
            { sample_count = -5, mean_rtt = 0.1 },
            { sample_count = 100, mean_rtt = -0.1 },
            { sample_count = 10, overload_count = 11 },
            { sample_count = "many", mean_rtt = 0.1 },
        }
        return corrupt[rand(6) + 1], true
    end
    local m = {
        sample_count = rand(1000),
        mean_rtt = rand(500) / 10000, -- 0..0.05s
    }
    if rand(4) == 0 then
        m.overload_count = rand(math.min(m.sample_count or 0, 100) + 1)
    end
    if rand(4) == 0 then
        m.timeout_count = rand(math.min(m.sample_count or 0, 100) + 1)
    end
    if rand(10) == 0 then
        m.aborted_count = rand(math.min(m.sample_count or 0, 100) + 1)
    end
    return m, false
end

local function run_trajectory(algorithm, seed, steps)
    local rand = lcg(seed)
    local cfg = base_cfg()
    local state = { limit = 50 + rand(200) }
    local holds = 0

    for _ = 1, steps do
        local m, corrupt = gen_measurement(rand)
        local next_state, err = common.safe_update(algorithm, state, m, cfg)

        if corrupt or next_state == nil then
            -- rejected input must leave state untouched
            assert.falsy(next_state, "corrupt input produced state: "
                .. tostring(err))
        else
            state = next_state
            local limit = state.limit
            assert.True(type(limit) == "number" and limit == limit,
                "limit is NaN")
            assert.True(limit ~= 1 / 0 and limit ~= -1 / 0, "limit is inf")
            assert.True(limit >= MIN, "limit below min: " .. tostring(limit))
            assert.True(limit <= MAX, "limit above max: " .. tostring(limit))
            if state.long_rtt then
                assert.True(state.long_rtt == state.long_rtt
                    and state.long_rtt >= 0, "long_rtt corrupted")
            end
            if state.short_rtt then
                assert.True(state.short_rtt == state.short_rtt
                    and state.short_rtt >= 0, "short_rtt corrupted")
            end
            if next_state.held then
                holds = holds + 1
            end
        end
    end

    return state, holds
end

describe("fuzz properties", function()
    for _, algorithm in ipairs({ g2, aimd }) do
        describe(algorithm.name, function()
            it("keeps state valid over 500 generated windows", function()
                run_trajectory(algorithm, 20260919, 500)
            end)

            it("is deterministic for a given seed", function()
                local a = run_trajectory(algorithm, 42, 200)
                local b = run_trajectory(algorithm, 42, 200)
                assert.are.equal(a.limit, b.limit)
                assert.are.equal(a.long_rtt, b.long_rtt)
                assert.are.equal(a.short_rtt, b.short_rtt)
            end)

            it("never moves on zero-sample windows", function()
                local rand = lcg(7)
                local cfg = base_cfg()
                local state = { limit = 123, long_rtt = 0.021,
                                short_rtt = 0.023 }
                for _ = 1, 50 do
                    local before = state.limit
                    local next_state = assert(common.safe_update(
                        algorithm, state, { sample_count = 0 }, cfg))
                    state = next_state
                    assert.True(next_state.held)
                    assert.are.equal(before, state.limit)
                    assert.are.equal(0.021, state.long_rtt)
                    assert.are.equal(0.023, state.short_rtt)
                    rand(1) -- keep the generator exercised
                end
            end)
        end)
    end
end)
