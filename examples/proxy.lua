-- Low-level API usage: maximum performance, explicit accounting.
--
-- The low-level API never touches ngx.ctx, never builds strings and
-- never allocates: it is two shared-dict operations per admitted request
-- and one on rejection. Use it when the lifecycle helpers do not fit
-- (e.g. non-HTTP traffic, custom completion signals) or when you want
-- full control of the observation.

local adaptive = require "resty.adaptive_limit"

local payments = assert(adaptive.new({
    name = "payments",
    shared_dict = "adaptive_limit",
    initial_limit = 50,
    min_limit = 5,
    max_limit = 2000,
    failure_mode = "fail_open",
}))

-- access_by_lua ---------------------------------------------------------
local function access()
    local ok, err = payments:try_acquire()

    if ok then
        return true
    end

    if err == "rejected" then
        -- The limiter is healthy and the pool is full: the application
        -- picks the semantics. 503 is the natural choice for upstream
        -- capacity exhaustion; 429 fits client-facing quota policies.
        ngx.header["Retry-After"] = "1"
        return ngx.exit(503)
    end

    -- Limiter-internal failure (shared dict trouble, ...). The error is
    -- already counted; under fail_open you usually proceed WITHOUT a
    -- slot (nothing to release), under fail_closed you reject here.
    ngx.log(ngx.ERR, "adaptive limiter internal error: ", err)
    return ngx.exit(503)
end

-- content/proxy phase ---------------------------------------------------
-- proxy_pass as usual. Latency and outcome are captured in the log
-- phase, or passed explicitly when the request spans multiple upstream
-- attempts.

-- log_by_lua -----------------------------------------------------------
local function log()
    -- release() decrements first, records the observation second;
    -- it never yields. Outcomes: success, timeout, connect_error,
    -- overload, error, aborted, ignored.
    local ok, err = payments:release(ngx.now() - ngx.req.start_time(),
        ngx.status >= 500 and "error" or "success")
    if not ok then
        -- the slot could not be released: surfaced in the limiter's
        -- internal_errors; a single retry of release() is safe
        ngx.log(ngx.ERR, "adaptive limiter release failed: ", err)
    end
end

-- Multi-attempt upstream timing, parsed explicitly:
--   local upstream_time = require("resty.adaptive_limit.upstream_time")
--   local seconds = upstream_time.parse(ngx.var.upstream_response_time,
--       "last")  -- final attempt; or "max"/"sum"
--   payments:release(seconds, outcome)

return {
    access = access,
    log = log,
}
