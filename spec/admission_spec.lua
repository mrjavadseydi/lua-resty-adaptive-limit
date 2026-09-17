-- Admission/release semantics against a faithful mock of ngx.shared.DICT
-- (including nil + "not found" for missing keys and a deterministic
-- clock). The multi-worker race properties are additionally proven with
-- real workers in t/admission.t and t/multi_worker.t.

local mock = require "spec.mock_ngx"
local adaptive = require "resty.adaptive_limit"
local errors = adaptive.errors
local runtime = require "resty.adaptive_limit.runtime"

local function fresh_env(cfg_overrides)
    runtime.started = false
    runtime.registry = {}
    runtime.order = {}
    ngx.reset()
    ngx.shared.adaptive_limit = ngx.make_dict()

    local cfg = {
        name = "payments",
        shared_dict = "adaptive_limit",
        initial_limit = 10,
        min_limit = 1,
        max_limit = 10, -- stable limit for admission testing
    }
    if cfg_overrides then
        for k, v in pairs(cfg_overrides) do
            cfg[k] = v
        end
    end
    local limiter = assert(adaptive.new(cfg))
    assert(adaptive.start())
    return limiter, mock.shared.adaptive_limit
end

describe("admission", function()
    it("refuses to operate before start()", function()
        runtime.started = false
        runtime.registry = {}
        runtime.order = {}
        ngx.reset()
        ngx.shared.adaptive_limit = ngx.make_dict()
        local limiter = assert(adaptive.new({ name = "x",
            shared_dict = "adaptive_limit" }))
        local ok, err = limiter:try_acquire()
        assert.falsy(ok)
        assert.are.equal(errors.NOT_STARTED, err)
        local ok2, err2 = limiter:release(0.1)
        assert.falsy(ok2)
        assert.are.equal(errors.NOT_STARTED, err2)
    end)

    it("fails on a missing shared dict at new()", function()
        ngx.reset()
        local limiter, err = adaptive.new({ name = "x",
            shared_dict = "does_not_exist" })
        assert.falsy(limiter)
        assert.match("does_not_exist", err, 1, true)
    end)

    it("rejects duplicate names in one worker", function()
        fresh_env()
        local dup, err = adaptive.new({ name = "payments",
            shared_dict = "adaptive_limit" })
        assert.falsy(dup)
        assert.match("duplicate", err, 1, true)
    end)

    it("seeds shared state on first start", function()
        local limiter, dict = fresh_env()
        assert.are.equal("1", dict._data["adaptive_limit:schema"])
        assert.are.equal(10, dict._data["al:1:payments:limit"])
        assert.are.equal(0, dict._data["al:1:payments:inflight"])
    end)

    it("seeds every limiter sharing one zone, not just the first", function()
        runtime.started = false
        runtime.registry = {}
        runtime.order = {}
        ngx.reset()
        ngx.shared.adaptive_limit = ngx.make_dict()
        local a = assert(adaptive.new({ name = "one",
            shared_dict = "adaptive_limit", initial_limit = 10,
            min_limit = 1, max_limit = 100 }))
        local b = assert(adaptive.new({ name = "two",
            shared_dict = "adaptive_limit", initial_limit = 20,
            min_limit = 1, max_limit = 100 }))
        assert(adaptive.start())
        local d = ngx.shared.adaptive_limit._data
        assert.are.equal(10, d["al:1:one:limit"])
        assert.are.equal(20, d["al:1:two:limit"])
        -- and neither limiter raised a missing-limit anomaly afterwards
        assert.Nil(a.anomalies.limit_missing)
        assert.Nil(b.anomalies.limit_missing)
    end)

    it("caps simultaneous admissions at the limit (invariant 1)", function()
        local limiter, dict = fresh_env()
        for _ = 1, 10 do
            assert.True(limiter:try_acquire())
        end
        local ok, err = limiter:try_acquire()
        assert.falsy(ok)
        assert.are.equal(errors.REJECTED, err)
        assert.are.equal(10, dict._data["al:1:payments:inflight"])
        assert.are.equal(10, limiter._inflight)
    end)

    it("never lets a rejected request raise inflight (invariant 3)", function()
        local limiter, dict = fresh_env()
        assert.True(limiter:try_acquire())
        -- force the counter over the limit
        dict._data["al:1:payments:inflight"] = 10
        local before = dict._data["al:1:payments:inflight"]
        local ok, err = limiter:try_acquire()
        assert.falsy(ok)
        assert.are.equal(errors.REJECTED, err)
        -- the rollback must undo the reservation exactly
        assert.are.equal(before, dict._data["al:1:payments:inflight"])
    end)

    it("releases slots for future admissions (invariant 2)", function()
        local limiter, dict = fresh_env()
        for _ = 1, 10 do
            assert.True(limiter:try_acquire())
        end
        assert.falsy(limiter:try_acquire())
        assert.True(limiter:release(0.020, "success"))
        assert.True(limiter:try_acquire())
        assert.are.equal(10, dict._data["al:1:payments:inflight"])
    end)

    it("snaps a double release to zero and counts the anomaly", function()
        local limiter, dict = fresh_env()
        assert.True(limiter:try_acquire())
        assert.True(limiter:release(0.010))
        -- simulate the double release: inflight is now 0
        assert.True(limiter:release(0.010))
        assert.are.equal(0, dict._data["al:1:payments:inflight"])
        assert.are.equal(1, limiter.anomalies.negative_inflight)
    end)

    it("re-seeds from the last observed limit when the key disappears", function()
        local limiter, dict = fresh_env()
        assert.True(limiter:try_acquire())
        local last = limiter._last_limit
        dict._data["al:1:payments:limit"] = nil
        -- next admission must re-seed, not admit unbounded
        for i = 1, last do
            if i > 1 then
                assert.True(limiter:try_acquire())
            end
        end
        -- inflight was 1, now filled to `last` again
        local ok, err = limiter:try_acquire()
        assert.falsy(ok)
        assert.are.equal(errors.REJECTED, err)
        assert.are.equal(1, limiter.anomalies.limit_missing)
        assert.are.equal(last, dict._data["al:1:payments:limit"])
    end)

    it("records observations into the fixed-size stats struct", function()
        local limiter, _ = fresh_env()
        assert.True(limiter:try_acquire())
        assert.True(limiter:release(0.020, "success"))
        local s = limiter.stats
        assert.are.equal(1, s.sample_count)
        assert.True(math.abs(s.latency_sum - 0.020) < 1e-12)
        assert.are.equal(1, s.success)
        assert.are.equal(1000.0, s.last_completion)
        assert.are.equal(1, s.admitted_total)
    end)

    it("does not sample latency for client aborts but still releases", function()
        local limiter, dict = fresh_env()
        assert.True(limiter:try_acquire())
        assert.True(limiter:release(9.999, "aborted"))
        local s = limiter.stats
        assert.are.equal(0, s.sample_count) -- truncated duration: no sample
        assert.are.equal(1, s.aborted)
        assert.are.equal(0, dict._data["al:1:payments:inflight"])
    end)

    it("ignores 'ignored' outcomes entirely", function()
        local limiter, dict = fresh_env()
        assert.True(limiter:try_acquire())
        assert.True(limiter:release(0.5, "ignored"))
        local s = limiter.stats
        assert.are.equal(0, s.sample_count)
        assert.are.equal(0, s.success)
        assert.are.equal(0, dict._data["al:1:payments:inflight"])
    end)

    it("rejects unknown outcomes with a distinct error", function()
        local limiter = fresh_env()
        assert.True(limiter:try_acquire())
        local ok, err = limiter:release(0.1, "totally_broken")
        assert.falsy(ok)
        assert.are.equal(errors.INVALID_STATE, err)
        -- nothing was released
        assert.are.equal(1, limiter._inflight)
    end)

    it("does not sample malformed latency but still releases", function()
        local limiter = fresh_env()
        assert.True(limiter:try_acquire())
        assert.True(limiter:release(-5, "success"))
        assert.are.equal(0, limiter.stats.sample_count)
        assert.are.equal(1, limiter.anomalies.bad_latency)

        assert.True(limiter:try_acquire())
        assert.True(limiter:release(0 / 0, "success"))
        assert.are.equal(0, limiter.stats.sample_count)
        assert.are.equal(2, limiter.anomalies.bad_latency)
    end)

    it("preserves a learned limit across a simulated reload", function()
        local limiter, dict = fresh_env()
        -- simulate controller-published state (float + integer)
        dict._data["al:1:payments:limit"] = 7
        dict._data["al:1:payments:limit_f"] = 7.35
        -- fresh worker VM: reset registries, keep the dict
        runtime.started = false
        runtime.registry = {}
        runtime.order = {}
        local limiter2 = assert(adaptive.new({ name = "payments",
            shared_dict = "adaptive_limit", initial_limit = 10,
            min_limit = 1, max_limit = 10 }))
        assert(adaptive.start())
        assert.True(math.abs(limiter2._last_limit - 7.35) < 1e-9)
        -- and the schema marker was not recreated
        assert.are.equal("1", dict._data["adaptive_limit:schema"])
    end)

    it("refuses incompatible shared state schema", function()
        ngx.reset()
        local dict = ngx.make_dict()
        ngx.shared.adaptive_limit = dict
        dict._data["adaptive_limit:schema"] = "9"
        runtime.registry = {}
        runtime.order = {}
        local limiter = assert(adaptive.new({ name = "payments",
            shared_dict = "adaptive_limit" }))
        local ok, err = adaptive.start()
        assert.falsy(ok)
        assert.match("schema", err, 1, true)
        -- no state was reseeded under the foreign schema
        assert.Nil(dict._data["al:1:payments:limit"])
    end)
end)
