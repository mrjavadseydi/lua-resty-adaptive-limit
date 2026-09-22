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

    it("does not repair another worker's negative hole", function()
        local limiter, dict = fresh_env()
        -- four unrepaired excess releases already sit in the counter
        dict._data["al:1:payments:inflight"] = -4
        assert.True(limiter:release(0.010))
        -- this call subtracted 1 and adds 1 back; the pre-existing hole stays
        assert.are.equal(-4, dict._data["al:1:payments:inflight"])
        assert.are.equal(1, limiter.anomalies.negative_inflight)
    end)

    it("repairs a negative rollback by one, not by the whole hole", function()
        local limiter, dict = fresh_env({ initial_limit = 1, max_limit = 1,
            min_limit = 1 })
        assert.True(limiter:try_acquire())
        local incr = dict.incr
        dict.incr = function(d, key, delta, init, ttl)
            local n, err = incr(d, key, delta, init, ttl)
            if key == "al:1:payments:inflight" and delta == 1 and n == 2 then
                -- concurrent releases drain the counter before rollback
                incr(d, key, -3)
            end
            return n, err
        end
        local ok, err = limiter:try_acquire()
        dict.incr = incr
        assert.falsy(ok)
        assert.are.equal(errors.REJECTED, err)
        -- rollback observed -2 and added back its own 1, leaving -1
        assert.are.equal(-1, dict._data["al:1:payments:inflight"])
        assert.are.equal(1, limiter.anomalies.negative_inflight)
    end)

    it("negative-inflight repair keeps a sibling's concurrent admission", function()
        local limiter, dict = fresh_env()
        -- inflight is 0; a double release drives it to -1, and another
        -- worker admits (incr +1) before the repair runs
        local incr = dict.incr
        local injected = false
        dict.incr = function(d, key, delta, init)
            local n, err = incr(d, key, delta, init)
            if not injected and key == "al:1:payments:inflight" and n == -1 then
                injected = true
                incr(d, key, 1) -- the sibling's admission
            end
            return n, err
        end
        assert.True(limiter:release(0.010))
        dict.incr = incr
        -- set(0) would have erased the sibling's slot; incr(+1) keeps it
        assert.are.equal(1, dict._data["al:1:payments:inflight"])
        assert.are.equal(1, limiter.anomalies.negative_inflight)
    end)

    it("rebuilds an evicted inflight counter from held slots instead of zero", function()
        -- ngx.shared may evict unexpired keys under memory pressure: two
        -- slots held at limit 2, the counter vanishes, a third acquire
        -- must not become a third held slot on a counter reading 1
        local limiter, dict = fresh_env({ initial_limit = 2, max_limit = 2 })
        assert.True(limiter:try_acquire())
        assert.True(limiter:try_acquire())
        dict._data["al:1:payments:inflight"] = nil

        local ok, err = limiter:try_acquire()
        assert.falsy(ok)
        assert.are.equal(errors.INTERNAL_ERROR, err)
        assert.are.equal(1, limiter.anomalies.inflight_missing)
        assert.are.equal(1, limiter.internal_errors)
        assert.are.equal(2, limiter._inflight)
        assert.are.equal(2, dict._data["al:1:payments:inflight"])
        -- the rebuilt counter enforces the cap again
        local rok, rerr = limiter:try_acquire()
        assert.falsy(rok)
        assert.are.equal(errors.REJECTED, rerr)
        assert.True(limiter:release(0.010))
        assert.True(limiter:try_acquire())
        assert.are.equal(2, dict._data["al:1:payments:inflight"])

        -- eviction discovered by a release: rebuilt, then released
        dict._data["al:1:payments:inflight"] = nil
        assert.True(limiter:release(0.010))
        assert.are.equal(1, dict._data["al:1:payments:inflight"])
        assert.are.equal(1, limiter._inflight)
        assert.are.equal(2, limiter.anomalies.inflight_missing)
        assert.are.equal(1, limiter.internal_errors)
    end)

    it("surfaces a non-finite inflight counter as an internal error, never admits, never resets",
        function()
            for _, garbage in ipairs({ -math.huge, 0 / 0, math.huge }) do
                local limiter, dict = fresh_env()
                dict._data["al:1:payments:inflight"] = garbage
                -- -inf + 1 == -inf would otherwise admit forever
                local ok, err = limiter:try_acquire()
                assert.falsy(ok, tostring(garbage))
                assert.are.equal(errors.INTERNAL_ERROR, err)
                assert.are.equal(1, limiter.anomalies.inflight_corrupted)
                assert.are.equal(1, limiter.internal_errors)
                assert.are.equal(0, limiter._inflight)
                -- the key is left for the operator: no unsynchronized set
                local v = dict._data["al:1:payments:inflight"]
                assert.True(v ~= v or v == garbage)

                local rok, rerr = limiter:release(0.01)
                assert.falsy(rok)
                assert.are.equal(errors.INTERNAL_ERROR, rerr)
                assert.are.equal(2, limiter.anomalies.inflight_corrupted)
            end
        end)

    it("surfaces a non-numeric inflight counter and follows failure_mode",
        function()
            local limiter, dict = fresh_env()
            dict._data["al:1:payments:inflight"] = "garbage"
            local ok, err = limiter:try_acquire()
            assert.falsy(ok)
            assert.are.equal(errors.INTERNAL_ERROR, err)
            assert.are.equal(1, limiter.anomalies.inflight_corrupted)
            assert.are.equal("garbage", dict._data["al:1:payments:inflight"])

            -- lifecycle: fail_open admits without a slot, fail_closed sheds
            ngx.ctx = {}
            assert.True(limiter:access())
            assert.are.equal(3, ngx.ctx["alim:payments"])
            local closed = fresh_env({ failure_mode = "fail_closed" })
            ngx.shared.adaptive_limit._data["al:1:payments:inflight"] = "garbage"
            ngx.ctx = {}
            local cok, cerr = closed:access()
            assert.falsy(cok)
            assert.are.equal(errors.INTERNAL_ERROR, cerr)
        end)

    it("treats a limit above max_limit (or inf) as corrupted", function()
        local limiter, dict = fresh_env()
        dict._data["al:1:payments:limit"] = math.huge
        for _ = 1, 10 do
            assert.True(limiter:try_acquire())
        end
        local ok, err = limiter:try_acquire()
        assert.falsy(ok)
        assert.are.equal(errors.REJECTED, err)
        assert.are.equal(1, limiter.anomalies.limit_corrupted)
        assert.are.equal(10, dict._data["al:1:payments:limit"])
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

    it("releases the slot on an unknown outcome and drops the observation",
        function()
            local limiter, dict = fresh_env()
            assert.True(limiter:try_acquire())
            assert.True(limiter:release(0.1, "totally_broken"))
            -- the slot never leaks over a typo; the sample is not recorded
            assert.are.equal(0, limiter._inflight)
            assert.are.equal(0, dict._data["al:1:payments:inflight"])
            assert.are.equal(0, limiter.stats.completions)
            assert.are.equal(1, limiter.anomalies.bad_outcome)
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
