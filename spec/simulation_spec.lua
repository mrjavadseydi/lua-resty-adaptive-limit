-- Deterministic controller simulations (design.md §14, scenarios A–I).
--
-- Backend model: fixed capacity C and base RTT R with a queueing knee —
--   rtt(A)   = R * (1 + 8 * max(0, A - C) / C)          (A = admitted concurrency)
--   timeouts ramp once admitted concurrency exceeds 1.3 * C
--   completions per 1s window = A * window / rtt (throughput, not concurrency)
--   RTT jitter +-5% from a seeded LCG; everything is deterministic.
--
-- The limiter observes only completions (latency samples, outcome
-- counts) — exactly what the real controller sees through the window
-- accumulators. Defaults in these scenarios are the library defaults;
-- they are the justification (design.md §4) for those defaults.

local g2 = require "resty.adaptive_limit.controller.gradient2"
local aimd = require "resty.adaptive_limit.controller.aimd"
local common = require "resty.adaptive_limit.controller.common"

local function lcg(seed)
    local s = seed
    return function(max)
        s = (s * 1103515245 + 12345) % 2147483648
        return s % max
    end
end

local function sim_cfg()
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
        probe_interval = 30,
        probe_fraction = 0.5,
        aimd_increment = 1,
        aimd_decrease = 0.8,
    }
end

local sim = {}

sim.__index = sim

function sim.new(opts)
    local self = setmetatable({}, sim)
    self.rand = lcg(opts.seed or 1)
    self.cfg = sim_cfg()
    self.alg = opts.algorithm or g2
    self.capacity = opts.capacity
    self.base = opts.base_rtt or 0.020
    self.sw = 1.0
    self.limit = opts.initial_limit
    self.float_limit = opts.initial_limit
    self.demand = opts.demand
    self.no_timeouts = opts.no_timeouts
    self.window = 0
    self.long_rtt = nil
    self.short_rtt = nil
    self.history = {}
    return self
end

function sim:rtt(a)
    local over = math.max(0, a - self.capacity) / self.capacity
    return self.base * (1 + 8 * over)
end

function sim:step()
    local admitted = math.min(self.demand, self.limit)
    local rtt = self:rtt(admitted)
    local completions = math.floor(admitted * self.sw / rtt)

    -- queue formation degrades into timeouts past 1.3x capacity
    local tmo_ratio = 0
    if not self.no_timeouts and admitted > self.capacity * 1.3 then
        tmo_ratio = math.min(0.4, admitted / self.capacity - 1.3)
    end
    local timeouts = math.floor(completions * tmo_ratio)
    local ok_completions = completions - timeouts

    -- jitter on the observed mean
    local mean = rtt * (1 + (self.rand(11) - 5) / 100)

    self.window = self.window + 1
    local m = {
        sample_count = ok_completions,
        mean_rtt = mean,
        timeout_count = timeouts,
        completions = completions,
        rejected_count = self.demand > self.limit and 1 or 0,
        window = self.window,
    }
    local state = {
        limit = self.float_limit,
        long_rtt = self.long_rtt,
        short_rtt = self.short_rtt,
        probe_restore = self.probe_restore,
    }
    local next_state = assert(common.safe_update(self.alg, state, m, self.cfg),
        "simulation produced an invalid controller update")
    self.float_limit = next_state.limit
    self.long_rtt = next_state.long_rtt
    self.short_rtt = next_state.short_rtt
    self.probe_restore = next_state.probe_restore
    self.limit = math.floor(next_state.limit)
    -- assertions look at the limit the controller is steering, not at
    -- the two-window probe dips (unit-tested in gradient2_spec)
    self.eff = math.floor(next_state.probe_restore or next_state.limit)
    self.history[#self.history + 1] = self.eff
    return self.eff
end

function sim:run(windows)
    for _ = 1, windows do
        self:step()
    end
    return self.eff
end

local function assert_gradual(history, max_jump_frac)
    for i = 2, #history do
        local a, b = history[i - 1], history[i]
        assert.True(b <= a * (1 + max_jump_frac) + 1,
            "limit jumped from " .. a .. " to " .. b)
    end
end

describe("simulation A: stable capacity", function()
    it("grows toward the useful region and stabilizes without oscillation", function()
        local s = sim.new({ capacity = 100, demand = 20, initial_limit = 20,
            seed = 11 })
        -- demand ramps: 20 -> 220 over 60 windows
        for i = 1, 60 do
            s.demand = 20 + i * 3.3
            s:step()
        end
        local final = s:run(20)
        assert.True(final >= 40, "limit failed to grow: " .. final)
        assert.True(final <= 200, "limit overshot capacity: " .. final)
        -- per-window growth is gradual (headroom-bounded)
        assert_gradual(s.history, 0.35)
        -- and stable once demand is saturated: the last 15 windows stay
        -- within a sane band around the capacity knee
        for i = #s.history - 14, #s.history do
            assert.True(s.history[i] >= 30 and s.history[i] <= 220,
                "oscillation: window " .. i .. " at " .. s.history[i])
        end
    end)
end)

describe("simulation B: capacity suddenly halves", function()
    it("detects congestion and sheds fast enough to recover", function()
        local s = sim.new({ capacity = 100, demand = 200, initial_limit = 120,
            seed = 22 })
        s:run(30) -- settle
        local before = s.eff
        s.capacity = 50
        local shed_at
        for i = 1, 30 do
            s:step()
            if not shed_at and s.eff <= before * 0.75 then
                shed_at = i
            end
        end
        -- observed dynamics (defaults): the limit sheds within a handful
        -- of windows and then cycles mildly around the timeout knee at
        -- ~1.3x capacity — the equilibrium a completion-observing
        -- Gradient2 controller holds under saturated demand
        assert.True(shed_at ~= nil and shed_at <= 10,
            "shedding took " .. tostring(shed_at) .. " windows")
        for i = #s.history - 9, #s.history do
            local l = s.history[i]
            assert.True(l >= 30 and l <= 90,
                "post-shed limit out of band: window " .. i .. " at " .. l)
            assert.True(s:rtt(l) <= s.base * 5,
                "post-shed rtt out of band at limit " .. l)
        end
    end)
end)

describe("simulation C: capacity doubles", function()
    it("grows gradually and uses the new capacity", function()
        local s = sim.new({ capacity = 50, demand = 150, initial_limit = 55,
            seed = 33 })
        s:run(20)
        s.capacity = 100
        local reached
        for i = 1, 60 do
            s:step()
            if not reached and s.eff >= 90 then
                reached = i
            end
        end
        assert.True(reached ~= nil, "never used the new capacity")
        assert_gradual(s.history, 0.4) -- no instant 100 -> 1000 jumps
    end)
end)

describe("simulation D: single latency outlier", function()
    it("does not collapse the limit because of one slow request", function()
        local s = sim.new({ capacity = 100, demand = 120, initial_limit = 100,
            seed = 44 })
        s:run(20)
        local before = s.eff
        -- one 5s outlier among thousands of healthy completions
        local m = {
            sample_count = 5000,
            mean_rtt = (5000 * 0.020 + 5.0) / 5001,
            completions = 5000,
            rejected_count = 1,
        }
        local state = { limit = s.float_limit, long_rtt = s.long_rtt,
            short_rtt = s.short_rtt }
        local next_state = assert(common.safe_update(s.alg, state, m, s.cfg))
        local after = math.floor(next_state.limit)
        assert.True(math.abs(after - before) <= math.max(3, before * 0.06),
            "single outlier moved the limit from " .. before .. " to " .. after)
    end)
end)

describe("simulation E: timeout storm", function()
    it("sheds until the timeout zone is exited", function()
        local s = sim.new({ capacity = 100, demand = 250, initial_limit = 200,
            seed = 55 })
        s:run(10)
        local before = s.eff
        -- the storm: demand spikes deep into the timeout zone
        s.demand = 800
        local min_seen = before
        for i = 1, 5 do
            s:step()
            min_seen = math.min(min_seen, s.eff)
        end
        -- backoff + gradient shedding must pull the limit down (the
        -- exact backoff arithmetic itself is unit-tested in
        -- gradient2_spec); once below the knee the controller stops
        -- shedding, so a sustained collapse is NOT the expected shape
        assert.True(min_seen < before * 0.95,
            "storm did not shed: " .. before .. " -> " .. s.eff)
        assert.True(s:rtt(s.eff) <= s.base * 5,
            "post-storm rtt out of band: " .. s:rtt(s.eff) / s.base .. "x base")
    end)
end)

describe("simulation F: low traffic", function()
    it("holds the limit when samples are insufficient", function()
        -- 2 req/s at 20ms => ~0.04 concurrent => ~2 completions/window
        local s = sim.new({ capacity = 100, demand = 0.04, initial_limit = 80,
            seed = 66 })
        s:run(50)
        assert.are.equal(80, s.eff)
    end)
end)

describe("simulation G: idle then burst", function()
    it("holds state while idle and never jumps to max on the burst", function()
        local s = sim.new({ capacity = 100, demand = 100, initial_limit = 90,
            seed = 77 })
        s:run(20)
        local settled = s.eff
        s.demand = 0
        s:run(300) -- five minutes idle
        assert.are.equal(settled, s.eff) -- nothing drifts while idle
        s.demand = 1000
        for i = 1, 10 do
            s:step()
            -- even with heavy demand, growth stays headroom-bounded
            assert.True(s.eff <= settled * 2,
                "uncontrolled jump to " .. s.eff)
        end
    end)
end)

describe("simulation H: permanently slower backend", function()
    it("adapts instead of permanently shrinking to minimum", function()
        local s = sim.new({ capacity = 100, demand = 150, initial_limit = 130,
            seed = 88 })
        s:run(20)
        s.base = 0.035 -- the service legitimately becomes slower
        s:run(120)
        assert.True(s.eff >= 40,
            "collapsed to minimum: " .. s.eff)
        assert.True(s.eff <= 300,
            "ran away: " .. s.eff)
        -- the baseline must have learned the new normal
        assert.True(s.long_rtt ~= nil and s.long_rtt > 0.025,
            "baseline never adapted: " .. tostring(s.long_rtt))
    end)
end)

describe("simulation I: sustained congestion without failures", function()
    -- The backend queues but never fails: no timeouts, no 503s. Latency
    -- is the only congestion signal. A baseline that learns from
    -- saturated windows normalizes the queue and ratchets the limit to
    -- max_limit (2000 by window 180, ~3 s latency, with the pre-probe
    -- controller); the promise "shed before failures" rests on this
    -- scenario.
    local function run(alg)
        local s = sim.new({ algorithm = alg, capacity = 100, demand = 5000,
            initial_limit = 20, no_timeouts = true, seed = 111 })
        s:run(300)
        return s
    end

    it("gradient2 stops growing near the capacity knee", function()
        local s = run(g2)
        for i = #s.history - 59, #s.history do
            local l = s.history[i]
            assert.True(l <= 150, "window " .. i .. " ratcheted to " .. l)
            assert.True(s:rtt(l) <= s.base * 5,
                "window " .. i .. " latency " .. s:rtt(l) / s.base .. "x base")
        end
        assert.True(s.eff >= 80, "collapsed to " .. s.eff)
        assert.True(s.long_rtt < s.base * 1.2,
            "baseline normalized the queue: " .. s.long_rtt)
    end)

    it("aimd stops growing near the capacity knee", function()
        local s = run(aimd)
        for i = #s.history - 59, #s.history do
            assert.True(s.history[i] <= 150,
                "window " .. i .. " ratcheted to " .. s.history[i])
        end
        assert.True(s.eff >= 80, "collapsed to " .. s.eff)
    end)
end)

describe("simulation: AIMD comparison on scenario B", function()
    it("recovers too, using the same harness", function()
        local s = sim.new({ algorithm = aimd, capacity = 100, demand = 200,
            initial_limit = 120, seed = 99 })
        s:run(30)
        s.capacity = 50
        -- initial 120 > capacity seeded a congested baseline; the probe
        -- at window 60 re-measures it, then AIMD sheds below the knee
        s:run(35)
        assert.True(s:rtt(s.eff) <= s.base * 3,
            "AIMD failed to shed below the knee: " .. s.eff)
    end)
end)
