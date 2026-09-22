-- Request lifecycle helpers (access/log) against the ngx mock: covers
-- design.md §2 semantics that do not need real nginx (double log, release
-- without acquire, internal redirect, bypass, fail-open/closed,
-- classification). The full HTTP-level lifecycle runs in t/lifecycle.t.

local ngx = require "spec.mock_ngx"
local adaptive = require "resty.adaptive_limit"
local errors = adaptive.errors
local runtime = require "resty.adaptive_limit.runtime"
local http = require "resty.adaptive_limit.http"
local scheduler = require "resty.adaptive_limit.scheduler"

local function fresh_limiter(cfg_overrides)
    runtime.started = false
    runtime.registry = {}
    runtime.order = {}
    scheduler.reset()
    ngx.reset()
    ngx.shared.adaptive_limit = ngx.make_dict()

    local cfg = {
        name = "pay",
        shared_dict = "adaptive_limit",
        initial_limit = 10,
        min_limit = 1,
        max_limit = 10,
    }
    if cfg_overrides then
        for k, v in pairs(cfg_overrides) do
            cfg[k] = v
        end
    end
    local limiter = assert(adaptive.new(cfg))
    assert(adaptive.start())
    return limiter
end

describe("access/log lifecycle", function()
    it("admits through access() and releases through log()", function()
        local limiter = fresh_limiter()
        assert.True(limiter:access())
        assert.are.equal(1, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
        assert.True(limiter:log())
        assert.are.equal(0, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
    end)

    it("is idempotent: a second log() cannot corrupt the counter", function()
        local limiter = fresh_limiter()
        assert.True(limiter:access())
        assert.True(limiter:log())
        assert.True(limiter:log())
        assert.True(limiter:log())
        assert.are.equal(0, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
        -- no anomaly was counted: the double log was a clean no-op
        assert.Nil(limiter.anomalies.negative_inflight)
    end)

    it("releases the slot before the outcome classifier runs", function()
        local seen
        local limiter = fresh_limiter({
            outcome_classifier = function()
                seen = ngx.shared.adaptive_limit._data["al:1:pay:inflight"]
                return "success"
            end,
        })
        assert.True(limiter:access())
        assert.True(limiter:log())
        assert.are.equal(0, seen)
        assert.are.equal(0, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
    end)

    it("log() without access() is a harmless no-op", function()
        local limiter = fresh_limiter()
        assert.True(limiter:log())
        local d = ngx.shared.adaptive_limit
        assert.are.equal(0, d._data["al:1:pay:inflight"])
    end)

    it("internal redirect: access() twice holds exactly one slot", function()
        local limiter = fresh_limiter()
        assert.True(limiter:access())
        assert.True(limiter:access()) -- simulated redirect re-entry
        assert.are.equal(1, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
        assert.True(limiter:log())
        assert.are.equal(0, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
    end)

    it("internal re-entries bypass admission by default", function()
        local limiter = fresh_limiter()
        ngx._internal = true -- subrequest / exec target / X-Accel target
        assert.True(limiter:access())
        assert.are.equal(3, ngx.ctx[limiter._ctx_key])
        assert.are.equal(0, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
        assert.True(limiter:log()) -- nothing to release
        assert.are.equal(0, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
    end)

    it("allow_internal re-enables admission for exec-fronted locations", function()
        local limiter = fresh_limiter({ allow_internal = true })
        ngx._internal = true
        assert.True(limiter:access())
        assert.are.equal(1, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
        assert.True(limiter:log())
        assert.are.equal(0, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
    end)

    it("bypass admits nothing and log() releases nothing", function()
        local limiter = fresh_limiter()
        assert.True(limiter:access({ bypass = true }))
        assert.are.equal(0, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
        assert.True(limiter:log())
        assert.are.equal(0, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
    end)

    it("fail_open admits without a slot on internal limiter failure", function()
        local limiter = fresh_limiter({ failure_mode = "fail_open" })
        -- simulate a broken shared dict underneath the limiter
        limiter.st.dict = {
            get = function() return nil end,
            incr = function() return nil, "no memory" end,
            set = function() return true end,
        }
        assert.True(limiter:access()) -- fail_open: request proceeds
        assert.are.equal(3, ngx.ctx[limiter._ctx_key])
        assert.True(limiter:log())    -- and nothing is released
        -- restore: the counter was never touched
        limiter.st.dict = ngx.shared.adaptive_limit
        assert.are.equal(0, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
    end)

    it("fail_closed reports internal errors instead of admitting", function()
        local limiter = fresh_limiter({ failure_mode = "fail_closed" })
        limiter.st.dict = {
            get = function() return nil end,
            incr = function() return nil, "no memory" end,
            set = function() return true end,
        }
        local ok, err = limiter:access()
        assert.falsy(ok)
        assert.are.equal(errors.INTERNAL_ERROR, err)
    end)

    it("not_started is never masked by fail_open", function()
        local limiter = fresh_limiter({ failure_mode = "fail_open" })
        runtime.started = false
        local ok, err = limiter:access()
        assert.falsy(ok)
        assert.are.equal(errors.NOT_STARTED, err)
    end)

    it("rejection propagates as errors.REJECTED", function()
        local limiter = fresh_limiter({ initial_limit = 1 })
        assert.True(limiter:access())
        local ok, err = limiter:access() -- a *new* request context
        -- same ctx here: clear and retry as a fresh request would
        ngx.ctx = {}
        ok, err = limiter:try_acquire()
        assert.falsy(ok)
        assert.are.equal(errors.REJECTED, err)
    end)
end)

describe("default observation classifier", function()
    local cases = {
        { 200, "success" },
        { 201, "success" },
        { 302, "success" },
        { 400, "success" }, -- client errors are not capacity signals
        { 404, "success" },
        { 429, "success" },
        { 499, "aborted" }, -- client abort: no latency sampled
        { 500, "error" },
        { 501, "error" },
        { 502, "connect_error" },
        { 503, "overload" },
        { 504, "timeout" },
    }

    for _, c in ipairs(cases) do
        it(string.format("classifies HTTP %d", c[1]), function()
            local limiter = fresh_limiter()
            ngx.status = c[1]
            local latency, outcome = limiter.default_observation(limiter)
            if outcome == "aborted" then
                -- truncated duration: never sampled
                assert.Nil(latency)
            else
                assert.True(math.abs(latency - 0.02) < 1e-9)
            end
            assert.are.equal(c[2], outcome)
        end)
    end

    it("lets a custom classifier override outcomes", function()
        local limiter = fresh_limiter({
            outcome_classifier = function(status)
                if status == 500 then return "overload" end
            end,
        })
        ngx.status = 500
        local _, outcome = limiter.default_observation(limiter)
        assert.are.equal("overload", outcome)
    end)

    it("falls back to the default when the classifier errors", function()
        local limiter = fresh_limiter({
            outcome_classifier = function() error("boom") end,
        })
        ngx.status = 500
        local _, outcome = limiter.default_observation(limiter)
        assert.are.equal("error", outcome)
        assert.are.equal(1, limiter.anomalies.classifier_error)
    end)

    it("ignores unknown classifier return values", function()
        local limiter = fresh_limiter({
            outcome_classifier = function() return "not_an_outcome" end,
        })
        ngx.status = 200
        local _, outcome = limiter.default_observation(limiter)
        assert.are.equal("success", outcome)
    end)

    it("samples no latency for manual mode at the classifier level", function()
        local limiter = fresh_limiter({ latency_source = "manual" })
        ngx.status = 200
        local latency, outcome = limiter.default_observation(limiter)
        assert.Nil(latency)
        assert.are.equal("success", outcome)
    end)

    it("parses upstream_response_time when configured", function()
        local limiter = fresh_limiter({
            latency_source = "upstream_response_time",
        })
        ngx.status = 200
        ngx.var.upstream_response_time = "0.005, 0.010"
        local latency = limiter.default_observation(limiter)
        assert.True(math.abs(latency - 0.010) < 1e-9)

        ngx.var.upstream_response_time = "-"
        assert.Nil(limiter.default_observation(limiter))
    end)
end)

describe("http.reject helper", function()
    it("maps rejections to the configured status with Retry-After", function()
        local limiter = fresh_limiter({ rejection_status = 503,
            retry_after = 2 })
        http.reject(limiter.cfg, errors.REJECTED)
        assert.are.equal(503, ngx._last_exit)
        assert.are.equal("2", ngx.header["Retry-After"])
    end)

    it("maps internal errors to 500 without Retry-After header", function()
        local limiter = fresh_limiter()
        ngx.header["Retry-After"] = nil
        http.reject(limiter.cfg, errors.INTERNAL_ERROR)
        assert.are.equal(500, ngx._last_exit)
        assert.is_nil(ngx.header["Retry-After"])
    end)
end)

describe("guard() and get()", function()
    it("guard() admits like access() and emits nothing", function()
        local limiter = fresh_limiter()
        assert.True(limiter:guard())
        assert.is_nil(ngx._last_exit)
        assert.are.equal(1, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
        assert.True(limiter:log())
        assert.are.equal(0, ngx.shared.adaptive_limit
            ._data["al:1:pay:inflight"])
    end)

    it("guard() emits the rejection response when the pool is full", function()
        local limiter = fresh_limiter({ initial_limit = 1, max_limit = 1 })
        assert.True(limiter:try_acquire())
        ngx.ctx = {}
        local ok, err = limiter:guard()
        assert.is_nil(ok)
        assert.are.equal(errors.REJECTED, err)
        assert.are.equal(503, ngx._last_exit)
        assert.are.equal("1", ngx.header["Retry-After"])
    end)

    it("get() returns the registered limiter and raises on a typo", function()
        local limiter = fresh_limiter()
        assert.are.equal(limiter, adaptive.get("pay"))
        assert.error_matches(function() adaptive.get("pya") end,
            'adaptive_limit: no limiter named "pya"', 1, true)
    end)
end)

describe("scheduler timer management", function()
    it("reuses recurring timer across start/stop/start cycles", function()
        fresh_limiter()
        assert.are.equal(1, ngx._timer_calls)
        assert.True(adaptive.stop())
        assert.False(scheduler.running)
        assert.True(adaptive.start())
        assert.True(scheduler.running)
        -- Still exactly 1 timer call (no duplicate timer created)
        assert.are.equal(1, ngx._timer_calls)
    end)
end)
