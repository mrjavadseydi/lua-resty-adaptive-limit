local ngx = require "spec.mock_ngx"
local adaptive = require "resty.adaptive_limit"
local runtime = require "resty.adaptive_limit.runtime"
local scheduler = require "resty.adaptive_limit.scheduler"
local common = require "resty.adaptive_limit.controller.common"

local function new_worker(overrides, dict)
    runtime.started = false
    runtime.registry = {}
    runtime.order = {}
    scheduler.reset()
    if not dict then
        ngx.reset()
        dict = ngx.make_dict()
    end
    ngx.shared.adaptive_limit = dict
    local cfg = { name = "pay", shared_dict = "adaptive_limit",
        initial_limit = 100, min_limit = 1, max_limit = 200,
        probe_interval = 0 }
    for k, v in pairs(overrides or {}) do cfg[k] = v end
    return assert(adaptive.new(cfg)), dict
end

describe("shared-state startup", function()
    for _, field in ipairs({ "schema", "sample_window", "limit_f", "inflight",
        "last_window", "last_update", "limit" }) do
        it("refuses a failed " .. field .. " write and can retry", function()
            local l, dict = new_worker()
            local key = field == "schema" and "adaptive_limit:schema" or l.st.K[field]
            local add = dict.add
            dict.add = function(d, k, ...)
                if k == key then return false, "no memory" end
                return add(d, k, ...)
            end
            local ok, err = adaptive.start()
            assert.is_nil(ok)
            assert.match("no memory", err, 1, true)
            assert.False(runtime.started)
            assert.are.equal(0, ngx._timer_calls)
            dict.add = add
            assert.True(adaptive.start())
            assert.are.equal(100, dict:get(l.K_limit))
            assert.are.equal(0, dict:get(l.K_inflight))
        end)
    end

    for _, field in ipairs({ "limit", "limit_f" }) do
        it("refuses a failed adopted " .. field .. " clamp", function()
            local old, dict = new_worker()
            assert(adaptive.start())
            dict:set(old.st.K.last_window, 999)
            local l = new_worker({ initial_limit = 5, max_limit = 10 }, dict)
            local set = dict.set
            dict.set = function(d, k, ...)
                if k == l.st.K[field] then return false, "no memory" end
                return set(d, k, ...)
            end
            local ok, err = adaptive.start()
            assert.is_nil(ok)
            assert.match("no memory", err, 1, true)
            assert.False(runtime.started)
            assert.are.equal(999, dict:get(l.st.K.last_window))
            dict.set = set
            assert(adaptive.start())
            assert.are.equal(10, dict:get(l.K_limit))
            assert.are.equal(10, dict:get(l.st.K.limit_f))
        end)
    end

    it("does not seed a missing counter on reload, even if limit is missing", function()
        for _, lose_limit in ipairs({ false, true }) do
            local old, dict = new_worker()
            assert(adaptive.start())
            assert(old:try_acquire())
            dict:delete(old.K_inflight)
            if lose_limit then dict:delete(old.K_limit) end
            new_worker(nil, dict)
            local ok, err = adaptive.start()
            assert.is_nil(ok)
            assert.match("inflight counter missing", err, 1, true)
            assert.False(runtime.started)
            assert.is_nil(dict:get(old.K_inflight))
            assert.are.equal(1, old._inflight)
        end
    end)

    it("preserves sibling publications while finishing concurrent initialization", function()
        local a, dict = new_worker()
        local add = dict.add
        local injected = false
        dict.add = function(d, key, ...)
            if key == a.K_limit and not injected then
                injected = true
                local b = new_worker(nil, dict)
                assert(adaptive.start())
                assert(b:try_acquire())
                assert(b.st:publish_controller_state(80, nil, nil, nil, nil, 999, 1000))
            end
            return add(d, key, ...)
        end
        assert(adaptive.start())
        assert.are.equal(80, dict:get(a.K_limit))
        assert.are.equal(80, dict:get(a.st.K.limit_f))
        assert.are.equal(999, dict:get(a.st.K.last_window))
        assert.are.equal(1, dict:get(a.K_inflight))
    end)

    it("recovers a missing integer limit from learned state without resetting progress", function()
        local old, dict = new_worker()
        assert(adaptive.start())
        assert(old.st:publish_controller_state(8.35, 0.02, 0.02, 1, nil, 999, 1000))
        assert(old:try_acquire())
        dict:delete(old.K_limit)
        local l = new_worker(nil, dict)
        assert(adaptive.start())
        assert.are.equal(8, dict:get(l.K_limit))
        assert.are.equal(8.35, dict:get(l.st.K.limit_f))
        assert.are.equal(999, dict:get(l.st.K.last_window))
        assert.are.equal(1, dict:get(l.K_inflight))
    end)

    it("rejects changed window sizes before modifying learned state", function()
        for _, sw in ipairs({ 0.5, 2 }) do
            local old, dict = new_worker()
            assert(adaptive.start())
            assert(old.st:publish_controller_state(80, 0.02, 0.02, 1, nil, 999, 1000))
            new_worker({ sample_window = sw }, dict)
            local ok, err = adaptive.start()
            assert.is_nil(ok)
            assert.match("sample_window changed from 1 to " .. sw, err, 1, true)
            assert.False(runtime.started)
            assert.are.equal(80, dict:get(old.K_limit))
            assert.are.equal(999, dict:get(old.st.K.last_window))
            assert.are.equal(1, dict:get(old.st.K.sample_window))
        end
        new_worker({ sample_window = 2 })
        assert.True(adaptive.start())
    end)

    it("refuses unmarked legacy state instead of guessing its window size", function()
        local old, dict = new_worker()
        assert(adaptive.start())
        dict:delete(old.st.K.sample_window)
        new_worker(nil, dict)
        local ok, err = adaptive.start()
        assert.is_nil(ok)
        assert.match("no sample_window metadata", err, 1, true)
        assert.are.equal(100, dict:get(old.K_limit))
        assert.is_nil(dict:get(old.st.K.sample_window))
    end)
end)

describe("controller publication ordering", function()
    it("serializes adjacent startup windows across two workers", function()
        local a, dict = new_worker()
        assert(adaptive.start())
        local b = new_worker(nil, dict)
        assert(adaptive.start())
        assert(b.st:add_window(998, "tmo", 20, 20))
        assert(b.st:add_window(998, "cmp", 20, 20))
        local publish = a.st.publish_controller_state
        local injected = false
        a.st.publish_controller_state = function(self, ...)
            if not injected then
                injected = true
                b:control(1000.4)
                -- A still owns the lease for 997; B cannot publish 998/999.
                assert.are.equal(0, dict:get(self.K.last_window))
                assert.are.equal(0, b.controller_updates or 0)
            end
            return publish(self, ...)
        end
        a:control(1000.3)
        b:control(1000.4)
        assert.are.equal(999, dict:get(a.st.K.last_window))
        assert.are.equal(90, dict:get(a.K_limit))
        assert.is_nil(dict:get(a.st.K.controller_lease))
    end)

    it("abandons a replaced lease and leaves the successor's lease intact", function()
        local l, dict = new_worker()
        assert(adaptive.start())
        l.algorithm = { update = function()
            dict:set(l.st.K.controller_lease, "successor")
            return { limit = 120 }
        end }
        assert.False(l:control_window(999, 1000.5))
        assert.are.equal(100, dict:get(l.K_limit))
        assert.are.equal(0, dict:get(l.st.K.last_window))
        assert.are.equal("successor", dict:get(l.st.K.controller_lease))
        assert.are.equal(1, l.anomalies.stale_controller_window)
    end)

    it("releases the controller lease when publication fails or an algorithm throws", function()
        for _, throws in ipairs({ false, true }) do
            local l, dict = new_worker()
            assert(adaptive.start())
            if throws then
                l.algorithm = { update = function() error("algorithm failed") end }
            else
                l.st.publish_controller_state = function() return nil, "no memory" end
            end
            local ok, result = pcall(l.control_window, l, 999, 1000.5)
            if throws then
                assert.False(ok)
                assert.match("algorithm failed", result, 1, true)
            else
                assert.True(ok)
                assert.False(result)
            end
            assert.is_nil(dict:get(l.st.K.controller_lease))
            assert.are.equal(0, dict:get(l.st.K.last_window))
        end
    end)
end)

describe("probe failure handling", function()
    for _, name in ipairs({ "gradient2", "aimd" }) do
        local algorithm = require("resty.adaptive_limit.controller." .. name)
        it(name .. " backs off timeout-only probes, including skipped cycles", function()
            local l = new_worker({ probe_interval = 30 })
            for _, n in ipairs({ 31, 32, 60 }) do
                local ns = assert(common.safe_update(algorithm,
                    { limit = 50, probe_restore = 100, long_rtt = 0.02, short_rtt = 0.02 },
                    { window = n, sample_count = 0, completions = 100, timeout_count = 100 },
                    l.cfg))
                assert.True(ns.limit < 50)
                assert.is_nil(ns.probe_restore)
                assert.are.equal(0.02, ns.long_rtt)
            end
        end)

        it(name .. " restores only with enough healthy positive latency samples", function()
            local l = new_worker({ probe_interval = 30 })
            local state = { limit = 50, probe_restore = 100, long_rtt = 0.02, short_rtt = 0.02 }
            for _, m in ipairs({
                { window = 32, sample_count = 0, completions = 0 },
                { window = 32, sample_count = 19, completions = 19, mean_rtt = 0.02 },
                { window = 32, sample_count = 100, completions = 100, mean_rtt = 0 },
            }) do
                state = assert(common.safe_update(algorithm, state, m, l.cfg))
                assert.are.equal(50, state.limit)
                assert.are.equal(100, state.probe_restore)
            end
            local restored = assert(common.safe_update(algorithm, state,
                { window = 32, sample_count = 100, completions = 100, mean_rtt = 0.02 }, l.cfg))
            assert.are.equal(100, restored.limit)
            assert.is_nil(restored.probe_restore)
        end)
    end
end)
