-- The limiter instance: admission, release, lifecycle helpers,
-- statistics and controller wiring.
--
-- Concurrency notes that the whole file depends on:
--
--  * An OpenResty worker runs Lua cooperatively in a single VM. A
--    sequence of Lua statements with no yield point is effectively
--    atomic within the worker; every yield-capable call here is in the
--    shared dictionary, and none of our read-modify-write pairs on
--    worker-local state contain one.
--  * Cross-worker state lives exclusively in the shared dictionary and
--    moves only through its atomic operations. The admission
--    linearization point is dict:incr(inflight, 1): every successful
--    increment receives a distinct resulting value, so more than `limit`
--    requests can never hold slots at a stable limit.

local errors = require("resty.adaptive_limit.errors")
local config_mod = require("resty.adaptive_limit.config")
local state_mod = require("resty.adaptive_limit.state")
local upstream_time = require("resty.adaptive_limit.upstream_time")
local http_mod = require("resty.adaptive_limit.http")
local runtime = require("resty.adaptive_limit.runtime")

local ngx = ngx
local ngx_now = ngx.now
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_WARN = ngx.WARN
local ngx_shared = ngx.shared

local floor = math.floor

-- Stats fields flushed into shared window accumulators, and their
-- accumulator names. All flushed fields are monotonic: flush() writes
-- deltas (current - flushed-marker) so a request racing the flush can
-- only leave its increment for the next tick, never lose it.
local FLUSH_FIELDS = {
    "sample_count", "latency_sum", "overload", "timeout",
    "connect_error", "error", "aborted", "rejected_total", "completions",
}
local FIELD_TO_WINDOW = {
    sample_count  = "c",
    latency_sum   = "s",
    overload      = "ovl",
    timeout       = "tmo",
    connect_error = "cer",
    error         = "err",
    aborted       = "abt",
    rejected_total = "rej",
    completions   = "cmp",
}

-- Outcomes accepted by release(). "ignored" excludes the sample from
-- controller statistics entirely (health checks, synthetic probes).
local OUTCOMES = {
    success = 1, timeout = 1, connect_error = 1, overload = 1,
    error = 1, aborted = 1, ignored = 1,
}

-- Sanity clamp for externally supplied latency values (design.md §10: a
-- malformed latency must not corrupt the stats). Values outside
-- [0, LATENCY_CAP] are counted as anomalies and not sampled.
local LATENCY_CAP = 3600

-- Rate-limited logging: at most one message per kind per LOG_INTERVAL
-- seconds per worker (surface violations, never spam).
local LOG_INTERVAL = 1.0

local _M = {}
local mt = { __index = _M }

-- rate_limited_log(self, kind, level, ...)
-- Returns true if the message was emitted within the budget.
local function rate_limited_log(self, kind, level, ...)
    local now = ngx_now()
    local last = self._log_last[kind]
    if last and now - last < LOG_INTERVAL then
        return false
    end
    self._log_last[kind] = now
    ngx_log(level, "adaptive_limit[", self.cfg.name, "] ", ...)
    return true
end

-- Every anomaly counter goes through here so the on_anomaly hook sees
-- all of them (README: "on anomalies/internal errors"). n defaults to 1.
local function count_anomaly(self, kind, n)
    self.anomalies[kind] = (self.anomalies[kind] or 0) + (n or 1)
    local hook = self.cfg.on_anomaly
    if hook then
        -- pcall: user code must never break accounting (invariant 10)
        pcall(hook, kind, self.anomalies[kind])
    end
end

-- Classify a raw shared-dict error into a limiter-internal failure.
local function internal_error(self, where, err)
    self.internal_errors = self.internal_errors + 1
    rate_limited_log(self, "internal", ngx_ERR,
        where, " failed: ", err or "unknown")
    local hook = self.cfg.on_anomaly
    if hook then
        pcall(hook, "internal_error", self.internal_errors)
    end
    return nil, errors.INTERNAL_ERROR
end

-- A non-numeric inflight counter (foreign write into the zone) would turn
-- every incr into an internal error — permanent fail-open. Snap it to 0
-- and surface, like the limit key.
local function repair_inflight(self, dict, err)
    if err ~= "not a number" then
        return false
    end
    count_anomaly(self, "inflight_corrupted")
    dict:set(self.K_inflight, 0)
    rate_limited_log(self, "inflight_corrupted", ngx_ERR,
        "inflight key corrupted (non-numeric); re-seeded to 0")
    return true
end

function _M.new(user_cfg)
    local cfg, err = config_mod.build(user_cfg)
    if not cfg then
        return nil, err
    end

    local dict = ngx_shared and ngx_shared[cfg.shared_dict] or nil
    local st, serr = state_mod.new(dict, cfg.name)
    if not st then
        return nil, "adaptive_limit: shared dict \"" .. cfg.shared_dict
            .. "\": " .. tostring(serr)
    end

    local algorithm = require("resty.adaptive_limit.controller."
        .. (cfg.algorithm == "aimd" and "aimd" or "gradient2"))

    if runtime.registry[cfg.name] then
        return nil, "adaptive_limit: duplicate limiter name \""
            .. cfg.name .. "\""
    end

    -- Precomputed, hot-path-only fields come first; the fast path reads
    -- nothing it does not need.
    local limiter = {
        cfg = cfg,
        st = st,
        algorithm = algorithm,

        -- shared-dict key handles
        K_limit = st.K.limit,
        K_inflight = st.K.inflight,

        -- worker-local inflight mirror (see top comment: exact without
        -- synchronization because no yield sits between the paired
        -- updates)
        _inflight = 0,
        _last_limit = nil,

        -- fixed-size worker-local statistics. Nothing here is ever
        -- indexed by request; no structure grows with traffic.
        stats = {
            sample_count = 0,
            latency_sum = 0,
            success = 0,
            timeout = 0,
            connect_error = 0,
            overload = 0,
            error = 0,
            aborted = 0,
            completions = 0,
            admitted_total = 0,
            rejected_total = 0,
            last_completion = nil,
        },

        internal_errors = 0,
        anomalies = {},

        _log_last = {},
        -- ngx.ctx key, precomputed once ("alim:" .. name)
        _ctx_key = "alim:" .. cfg.name,

        -- flush bookkeeping (scheduler); _flush_pending is the window a
        -- partially failed flush must finish before moving on
        _flush_window = nil,
        _flush_pending = nil,
        -- last stats.last_completion value published to the shared dict
        _lc_published = nil,
        -- flushed-marker mirror of the monotonic stats fields: flush()
        -- writes deltas against these into the shared window
        -- accumulators and advances them by exactly the written delta
        _flushed = {
            sample_count = 0, latency_sum = 0, overload = 0, timeout = 0,
            connect_error = 0, error = 0, aborted = 0, rejected_total = 0,
            completions = 0,
        },
    }

    runtime.registry[cfg.name] = limiter
    runtime.order[#runtime.order + 1] = limiter

    return setmetatable(limiter, mt)
end

function _M:reseed_shared_state()
    local st = self.st
    local cfg = self.cfg

    st:write_schema()

    -- A fresh limiter starts at initial_limit. A restart with existing
    -- state adopts it (checked by the caller before calling this).
    self.st.dict:set(self.K_limit, cfg.initial_limit)
    self.st.dict:set(st.K.limit_f, cfg.initial_limit)
    -- add, not set: a sibling worker may already be serving requests by
    -- the time this worker runs its init; zeroing a live counter would
    -- over-admit until the next negative-inflight snap
    self.st.dict:add(st.K.inflight, 0)
    self.st.dict:set(st.K.last_window, 0)
    self.st.dict:set(st.K.last_update, ngx_now())
    self._last_limit = cfg.initial_limit
end

-- Validate the shared state schema marker and this limiter's own keys;
-- reseed what is missing, refuse on mismatch. Called from
-- adaptive.start() (init_worker).
--
-- The schema marker is dict-global but the controller keys are
-- per-limiter: without step 2 below, only the FIRST limiter in a zone
-- would get seeded at startup and every later limiter would lazily
-- create its limit key on the first admission (surfacing a spurious
-- anomaly per deployment).
function _M:check_schema()
    local st = self.st
    local dict = st.dict
    local v, err = st:read_schema()
    if v == nil then
        if err and err ~= "not found" then
            return nil, errors.INTERNAL_ERROR
        end
        -- no marker: first worker here, or the dict was flushed
        st:write_schema()
    elseif v ~= tostring(state_mod.SCHEMA_VERSION) then
        rate_limited_log(self, "schema", ngx_ERR,
            "shared state schema version ", tostring(v),
            " is not supported (expected ", tostring(state_mod.SCHEMA_VERSION),
            "); refusing to reuse incompatible state")
        return nil, errors.INVALID_STATE
    end

    -- per-limiter key existence (second+ limiters in a shared zone)
    local limit, lerr = dict:get(self.K_limit)
    if limit == nil then
        if lerr and lerr ~= "not found" then
            return nil, errors.INTERNAL_ERROR
        end
        self:reseed_shared_state()
    end
    return true
end

-- Adopt existing shared controller state after a reload (graceful HUP):
-- the learned limit must survive. Returns the adopted limit.
function _M:adopt_shared_state()
    local st = self.st
    local cs, err = st:read_controller_state()
    if not cs then
        -- genuine dict error: let the caller surface it; the learned
        -- state is unknown, not absent
        return nil, errors.INTERNAL_ERROR
    end
    -- prefer the float controller state; fall back to the integer limit
    local adopted = cs.limit_f or cs.limit
    if adopted == nil or type(adopted) ~= "number" or adopted ~= adopted then
        -- absent or corrupt: fall back to a reseed
        self:reseed_shared_state()
        return self.cfg.initial_limit
    end
    local clamped = adopted
    if adopted < self.cfg.min_limit then
        clamped = self.cfg.min_limit
    elseif adopted > self.cfg.max_limit then
        clamped = self.cfg.max_limit
    end
    if clamped ~= adopted then
        -- Out-of-policy adopted state (e.g. max_limit lowered across a
        -- reload): publish the clamp immediately so admission is bounded
        -- from the first request, not just from the next controller
        -- window.
        st.dict:set(self.K_limit, floor(clamped))
        st.dict:set(st.K.limit_f, clamped)
    end
    self._last_limit = clamped
    return clamped
end

------------------------------------------------------------------------
-- Admission (fast path)
------------------------------------------------------------------------

--- Try to acquire one concurrency slot.
-- Returns true, or nil + one of errors.REJECTED / errors.INTERNAL_ERROR /
-- errors.NOT_STARTED / errors.INVALID_STATE.
function _M:try_acquire()
    if not runtime.started then
        return nil, errors.NOT_STARTED
    end

    local dict = self.st.dict

    local limit, err = dict:get(self.K_limit)
    if limit == nil then
        if err and err ~= "not found" then
            return internal_error(self, "get(limit)", err)
        end
        -- Critical key missing (eviction, flushed dict, or first boot).
        -- Re-seed from this worker's last observed limit (fresh to within
        -- one window) and surface the anomaly instead of admitting
        -- unbounded.
        count_anomaly(self, "limit_missing")
        limit = self._last_limit or self.cfg.initial_limit
        dict:set(self.K_limit, limit)
        rate_limited_log(self, "limit_missing", ngx_WARN,
            "limit key missing; re-seeded from last observed value")
    elseif type(limit) ~= "number" or limit ~= limit
        or limit < self.cfg.min_limit or limit > self.cfg.max_limit then
        -- Corrupted shared value (non-numeric, NaN, or outside the
        -- configured policy — every legitimate publication is clamped
        -- into [min_limit, max_limit], so +inf or 10^9 can only be a
        -- foreign write): never trust it and never crash the request
        -- comparing against it. Replace with the last observed limit
        -- and surface.
        count_anomaly(self, "limit_corrupted")
        limit = self._last_limit or self.cfg.initial_limit
        dict:set(self.K_limit, limit)
        rate_limited_log(self, "limit_corrupted", ngx_ERR,
            "limit key corrupted (non-numeric or outside ",
            "[min_limit, max_limit]); re-seeded")
    end
    self._last_limit = limit

    -- Admission linearization point (see top-of-file comment).
    local n, ierr = dict:incr(self.K_inflight, 1, 0)
    if not n and repair_inflight(self, dict, ierr) then
        n, ierr = dict:incr(self.K_inflight, 1, 0)
    end
    if not n then
        return internal_error(self, "incr(inflight)", ierr)
    end

    if n <= limit then
        self._inflight = self._inflight + 1
        self.stats.admitted_total = self.stats.admitted_total + 1
        return true
    end

    -- Over the limit: roll the reservation back unconditionally.
    local _, rerr = dict:incr(self.K_inflight, -1)
    if rerr then
        self.internal_errors = self.internal_errors + 1
        count_anomaly(self, "rollback_failed")
        rate_limited_log(self, "rollback", ngx_ERR,
            "rollback incr failed: ", rerr)
    end
    self.stats.rejected_total = self.stats.rejected_total + 1
    return nil, errors.REJECTED
end

------------------------------------------------------------------------
-- Release (fast path)
------------------------------------------------------------------------

-- Validate and normalize a latency sample; returns usable number or nil.
local function sanitize_latency(self, latency)
    if latency == nil then
        return nil
    end
    if type(latency) ~= "number" or latency ~= latency
        or latency < 0 or latency > LATENCY_CAP then
        count_anomaly(self, "bad_latency")
        return nil
    end
    return latency
end

--- Release one slot and record the completion observation.
-- latency: seconds (number) or nil; outcome: see design.md §10.
-- Accounting happens before observation; observation failures can never
-- prevent the release (invariant 10).
function _M:release(latency, outcome)
    if not runtime.started then
        return nil, errors.NOT_STARTED
    end

    local dict = self.st.dict

    -- 1. Release the slot. This must succeed for accounting to hold.
    local n, err = dict:incr(self.K_inflight, -1)
    if not n and repair_inflight(self, dict, err) then
        -- the counter was garbage: the slot no longer exists to release
        n = 0
    end
    if not n then
        -- The slot leaks; surfaced via internal_errors and the stuck
        -- diagnostics. The caller may retry the release once.
        return internal_error(self, "incr(inflight, -1)", err)
    end
    if n < 0 then
        -- Double release (or a lost admission): undo our own excess
        -- decrement with an atomic incr rather than set(0) — another
        -- worker may have admitted between the two operations, and a
        -- set would erase that legitimate slot. Surface it.
        count_anomaly(self, "negative_inflight")
        dict:incr(self.K_inflight, -n)
        rate_limited_log(self, "negative", ngx_WARN,
            "inflight went negative; double release suspected")
    end
    if self._inflight > 0 then
        self._inflight = self._inflight - 1
    end

    -- 2. Validate the observation. The slot is already released: an
    --    outcome typo in one code path must never leak concurrency, so
    --    it is dropped and surfaced instead of refused.
    if outcome == nil then
        outcome = "success"
    elseif not OUTCOMES[outcome] then
        count_anomaly(self, "bad_outcome")
        rate_limited_log(self, "bad_outcome", ngx_ERR,
            "release(): unknown outcome ", tostring(outcome),
            "; observation dropped")
        return true
    end

    -- 3. Record the observation (never blocks the release: fixed-size
    --    struct updates only). sample_count counts usable latency
    --    observations; outcome counters count every completed outcome;
    --    completions counts every non-ignored outcome (the denominator
    --    the controller's corruption check validates against — aborted
    --    outcomes and unusable latencies are completions but never
    --    samples).
    if outcome ~= "ignored" then
        local s = self.stats
        s.completions = s.completions + 1
        if outcome ~= "aborted" then
            -- Client aborts release the slot but their (truncated)
            -- duration is not a capacity signal; malformed latency is
            -- dropped by sanitize_latency so mean_rtt remains valid.
            local latency_n = sanitize_latency(self, latency)
            if latency_n then
                s.sample_count = s.sample_count + 1
                s.latency_sum = s.latency_sum + latency_n
            end
        end
        if outcome == "success" then
            s.success = s.success + 1
        elseif outcome == "timeout" then
            s.timeout = s.timeout + 1
        elseif outcome == "connect_error" then
            s.connect_error = s.connect_error + 1
        elseif outcome == "overload" then
            s.overload = s.overload + 1
        elseif outcome == "error" then
            s.error = s.error + 1
        elseif outcome == "aborted" then
            s.aborted = s.aborted + 1
        end
        s.last_completion = ngx_now()
    end

    return true
end

------------------------------------------------------------------------
-- Statistics flush (control path — called by the scheduler only)
------------------------------------------------------------------------

--- Flush this worker's stats deltas into the shared accumulator of the
-- window that covers `now`. Non-yielding; safe inside timer callbacks.
-- Returns the window id the flush targeted.
-- The whole body contains no yield point, so a light thread can never
-- interleave mid-flush; the delta/flushed-marker pattern additionally
-- keeps the math exact even if a future dict op were to yield.
function _M:flush(now)
    local cfg = self.cfg
    -- A flush that failed part-way keeps targeting the SAME window until
    -- every field has landed: the fields of one completion must never be
    -- split across two windows (c/s in N, cmp in N+1 fails validation in
    -- both). A stale target past its grace is at worst lost, never
    -- corrupting.
    local win = self._flush_pending or floor(now / cfg.sample_window)
    local s = self.stats
    local flushed = self._flushed
    local ttl = cfg.sample_window + cfg.aggregation_grace + 15

    for i = 1, #FLUSH_FIELDS do
        local f = FLUSH_FIELDS[i]
        local delta = s[f] - flushed[f]
        if delta ~= 0 then
            local ok, err = self.st:add_window(win, FIELD_TO_WINDOW[f],
                delta, ttl)
            if not ok then
                -- do not advance the marker: the delta is retried on
                -- the next tick; surface the dict failure
                self.internal_errors = self.internal_errors + 1
                rate_limited_log(self, "flush", ngx_ERR,
                    "window flush failed: ", err or "unknown")
                self._flush_pending = win
                return win
            end
            flushed[f] = flushed[f] + delta
        end
    end

    self._flush_pending = nil
    self._flush_window = win
    return win
end

------------------------------------------------------------------------
-- Request lifecycle helpers (access / log)
------------------------------------------------------------------------
--
-- ngx.ctx values stored under the limiter's precomputed key:
--   1 = admitted by this limiter (a slot is held)
--   2 = released (log() already ran: idempotence latch)
--   3 = no slot held (explicit bypass, or fail-open admission after a
--       limiter-internal error): log() releases and records nothing
--
-- Within one location's phase chain, the ctx flag makes access() a no-op
-- for a limiter that already admitted this request: one request holds at
-- most one slot per limiter. ngx.ctx does NOT survive an internal
-- redirect (ngx.exec or error_page): both reset it, so a redirected
-- request re-enters access() with a clean ctx. Subrequests and internal
-- redirects are guarded separately (see access() below) because
-- subrequests never get a log phase and a redirected request would
-- otherwise acquire a second slot with no matching release in the
-- original location.

-- Default outcome classifier (design.md §10). 499 is a client abort, not
-- a capacity signal; 502/503/504 from the protected upstream are strong
-- overload signals; other 5xx are application errors, counted but not
-- treated as overload.
local STATUS_OUTCOMES = {
    [499] = "aborted",
    [502] = "connect_error",
    [503] = "overload",
    [504] = "timeout",
}

local function default_observation(self)
    local status = ngx.status
    -- 499 is a client abort, not a capacity signal; 502/503/504 from the
    -- protected upstream are strong overload signals; other 5xx are
    -- application errors, counted but not treated as overload
    local outcome = STATUS_OUTCOMES[status]
        or (status >= 500 and "error" or "success")

    local classifier = self.cfg.outcome_classifier
    if classifier then
        -- user code: never allowed to break the release
        local ok, custom = pcall(classifier, status)
        if ok and custom and OUTCOMES[custom] then
            outcome = custom
        elseif not ok then
            count_anomaly(self, "classifier_error")
            rate_limited_log(self, "classifier", ngx_ERR,
                "outcome_classifier failed: ", tostring(custom))
        end
    end

    if outcome == "aborted" or outcome == "ignored" then
        return nil, outcome
    end

    local source = self.cfg.latency_source
    if source == "manual" then
        return nil, outcome
    end
    if source == "upstream_response_time" then
        return upstream_time.parse(ngx.var.upstream_response_time,
            self.cfg.upstream_time_choice), outcome
    end
    -- "request_time": full request duration; trade-offs in README
    return ngx_now() - ngx.req.start_time(), outcome
end

--- Request-lifecycle admission. Call from access_by_lua*.
-- opts.bypass: admit nothing, track nothing (health checks, streams).
-- Returns true, or nil + errors.REJECTED (caller chooses the response)
-- or a limiter-internal error (masked to `true` under fail_open).
function _M:access(opts)
    local ctx = ngx.ctx
    if type(opts) == "table" and opts.bypass then
        if ctx[self._ctx_key] == nil then
            ctx[self._ctx_key] = 3
        end
        return true
    end

    if ctx[self._ctx_key] ~= nil then
        -- admitted, released, or bypassed earlier in this request
        -- (internal redirect): never acquire a second slot
        return true
    end

    -- Internal re-entries (subrequests, error_page targets, ngx.exec
    -- targets) share the parent request's admission by default: the log
    -- phase never runs for subrequests and runs only once (for the final
    -- location) in exec chains, so acquiring here too would leak a slot
    -- per re-entry. All of this is verified empirically (see t/lifecycle.t
    -- TEST 6/6b/10): ngx.ctx is reset by internal redirects (ngx.exec and
    -- error_page), subrequests get no log phase, and $request_id changes at
    -- every redirect — so there is no stable cross-ctx request identity.
    --
    -- allow_internal = true re-enables admission for internal requests,
    -- for limiters protecting locations that are ONLY reached through
    -- ngx.exec / X-Accel-Redirect (whose log phase runs there). Under
    -- that mode, subrequests into the location would acquire without a
    -- matching log phase and leak — do not capture such locations.
    if not self.cfg.allow_internal and ngx.req.is_internal() then
        ctx[self._ctx_key] = 3
        return true
    end

    local ok, err = self:try_acquire()
    if ok then
        ctx[self._ctx_key] = 1
        return true
    end

    if err == errors.NOT_STARTED or err == errors.INVALID_STATE then
        -- configuration-level problems: fail_open masks genuine limiter
        -- failures, not a misconfigured deployment
        return nil, err
    end

    if err ~= errors.REJECTED and self.cfg.failure_mode == "fail_open" then
        ctx[self._ctx_key] = 3
        return true
    end
    return nil, err
end

--- Request-lifecycle release. Call from log_by_lua*.
-- Idempotent; releases only what access() admitted; accounting happens
-- before any observation work; observability failures never block the
-- release (invariant 10). Yields nothing.
function _M:log()
    local ctx = ngx.ctx
    local state = ctx[self._ctx_key]
    if state == nil then
        -- nothing acquired through the lifecycle API (low-level usage or
        -- a request that never reached access())
        return true
    end
    if state == 2 then
        -- double log(): no-op, counters untouched
        return true
    end

    if state == 3 then
        ctx[self._ctx_key] = 2
        return true
    end

    -- admitted: compute the observation (cheap, non-yielding), then
    -- release() performs the accounting first and the recording second
    local latency, outcome = default_observation(self)
    ctx[self._ctx_key] = 2
    return self:release(latency, outcome)
end

--- Convenience rejection response (optional, outside the core).
-- "rejected" -> configured status + Retry-After; internal errors -> 500.
function _M:enforce(err)
    return http_mod.reject(self.cfg, err)
end

--- access() + enforce() in one call: admit, or emit the rejection/500
-- response. Same opts as access(). Returns true when admitted.
function _M:guard(opts)
    local ok, err = self:access(opts)
    if ok then
        return true
    end
    http_mod.reject(self.cfg, err)
    return nil, err
end

------------------------------------------------------------------------
-- Controller wiring (control path — called by the scheduler only)
------------------------------------------------------------------------

local LEASE_TTL = 5      -- seconds; per-window lease outlives any tick
local HB_TTL = 5         -- seconds; worker heartbeat key lifetime
-- At most this many closed windows are processed in one tick; older
-- backlog (timer pause, SIGSTOP, laptop sleep) is skipped and counted.
local MAX_WINDOWS_PER_TICK = 2

local common_ctrl = require("resty.adaptive_limit.controller.common")

--- One scheduler tick for this limiter: heartbeat, stats flush,
-- controller work. Non-yielding throughout.
function _M:tick(now, worker_id)
    -- heartbeat first: liveness stays visible even if the rest fails
    if worker_id ~= nil then
        self.st:heartbeat(worker_id, now, HB_TTL)
    end
    self:flush(now)
    -- Publish this worker's newest completion time so the stuck
    -- diagnostics in state() see the whole instance, not one worker
    -- (an idle worker next to saturated siblings would otherwise report
    -- a false stall). Monotonic max; once per tick, never per request.
    local lc = self.stats.last_completion
    if lc and lc ~= self._lc_published then
        self.st:publish_last_completion(lc)
        self._lc_published = lc
    end
    self:control(now)
end

--- Initial heartbeat, written synchronously in adaptive.start() so a
-- worker is visible in state() before its first scheduler tick.
function _M:heartbeat(worker_id)
    if worker_id ~= nil then
        self.st:heartbeat(worker_id, ngx_now(), HB_TTL)
    end
end

--- Run controller updates for every closed window past the grace period
-- that this limiter has not processed yet.
function _M:control(now)
    local cfg = self.cfg
    local sw = cfg.sample_window
    local st = self.st

    local cs, err = st:read_controller_state()
    if not cs then
        self.internal_errors = self.internal_errors + 1
        rate_limited_log(self, "control", ngx_ERR,
            "controller state read failed: ", err or "unknown")
        return
    end

    -- a non-numeric last_window (foreign write) must not throw on every
    -- tick: treat it as "never processed"; the next publish repairs it
    local last = tonumber(cs.last_window) or 0
    local n_ready = floor((now - cfg.aggregation_grace) / sw) - 1
    if n_ready <= last then
        return
    end

    -- stale backlog: never replay old windows after a long pause. On a
    -- fresh boot `last` is 0 and epoch-based window ids make n_ready
    -- astronomically large; the clamp below applies either way, but only
    -- a genuine pause (last > 0) counts as "skipped" — boot is not a
    -- pause.
    local from = last + 1
    if n_ready - from + 1 > MAX_WINDOWS_PER_TICK then
        -- Jumping ahead while another worker still holds the lease on
        -- last+1 would publish a later window before an earlier one
        -- (last_window regresses, the newer limit is overwritten). Its
        -- publish advances last_window; wait for it.
        if last > 0 and st:lease_held(from) then
            return
        end
        from = n_ready - MAX_WINDOWS_PER_TICK + 1
        if last > 0 then
            local skipped = from - last - 1
            self.controller_skips = (self.controller_skips or 0) + skipped
            rate_limited_log(self, "stale", ngx_WARN,
                "skipping ", tostring(skipped),
                " stale controller windows after a pause")
        end
    end

    for n = from, n_ready do
        if not self:control_window(n, now) then
            break -- lease lost or dict failure: stop for this tick
        end
    end
end

--- Process exactly one closed window. Returns false when the caller
-- should stop (lease lost, dict failure); true otherwise (including
-- "held: insufficient samples").
function _M:control_window(n, now)
    local cfg = self.cfg
    local st = self.st
    local dict = st.dict

    -- Controller lease: add wins. TTL-only expiry — we never delete a
    -- lease, so a successor lease for a later window is never disturbed,
    -- and last_window makes re-processing impossible anyway.
    local ok = st:try_lease(n,
        tostring(ngx.worker and ngx.worker.pid() or 0) .. ":" .. tostring(n),
        LEASE_TTL)
    if not ok then
        self.controller_skips = (self.controller_skips or 0) + 1
        return false
    end

    local acc, aerr = st:read_window(n)
    if not acc then
        self.internal_errors = self.internal_errors + 1
        rate_limited_log(self, "control", ngx_ERR,
            "window read failed: ", aerr or "unknown")
        return false
    end

    local cs, err = st:read_controller_state()
    if not cs then
        self.internal_errors = self.internal_errors + 1
        rate_limited_log(self, "control", ngx_ERR,
            "controller state read failed: ", err or "unknown")
        return false
    end

    local measurement = {
        sample_count = acc.c,
        mean_rtt = acc.c > 0 and acc.s / acc.c or 0,
        overload_count = acc.ovl,
        timeout_count = acc.tmo,
        connect_error_count = acc.cer,
        error_count = acc.err,
        aborted_count = acc.abt,
        rejected_count = acc.rej,
        completions = acc.cmp,
    }

    local state = {
        limit = cs.limit_f or cs.limit or cfg.initial_limit,
        long_rtt = cs.long_rtt,
        short_rtt = cs.short_rtt,
    }

    local next_state, uerr = common_ctrl.safe_update(self.algorithm,
        state, measurement, cfg)
    local held = false
    if not next_state then
        -- invalid input or invalid algorithm output: hold the previous
        -- valid state; never publish anything unvalidated (invariant 9)
        held = true
        self.controller_skips = (self.controller_skips or 0) + 1
        self.internal_errors = self.internal_errors + 1
        rate_limited_log(self, "controller", ngx_ERR,
            "controller update rejected, holding limit: ", uerr or "?")

        -- Repair state before publishing so out-of-policy limits or corrupt
        -- RTT values do not permanently wedge the controller across reloads
        local repaired_state, repaired = common_ctrl.repair_state(state, cfg)
        if repaired then
            -- own kind: the "controller" ERR just above would otherwise
            -- always suppress this line
            rate_limited_log(self, "repair", ngx_WARN,
                "repaired invalid shared controller state during hold: limit=",
                tostring(repaired_state.limit))
        end

        next_state = {
            limit = repaired_state.limit,
            long_rtt = repaired_state.long_rtt,
            short_rtt = repaired_state.short_rtt,
            gradient = nil,
        }
    elseif next_state.held then
        held = true
        self.controller_skips = (self.controller_skips or 0) + 1
    else
        self.controller_updates = (self.controller_updates or 0) + 1
    end

    local pok, perr = st:publish_controller_state(next_state.limit,
        next_state.long_rtt, next_state.short_rtt, next_state.gradient, n, now)
    if not pok then
        -- Partial publication (no memory, ...): last_window is written
        -- last, so it did not advance and the window's accumulators are
        -- kept — the next tick re-processes it instead of losing it.
        self.internal_errors = self.internal_errors + 1
        rate_limited_log(self, "publish", ngx_ERR,
            "controller state publish failed: ", perr or "unknown")
        return false
    end
    self._last_limit = math.floor(next_state.limit)
    st:delete_window(n)

    -- The hook contract is "after each controller publication": held
    -- windows advance bookkeeping only and fire nothing.
    if cfg.on_update and not held then
        pcall(cfg.on_update, {
            name = cfg.name,
            window = n,
            limit = self._last_limit,
            float_limit = next_state.limit,
            long_rtt = next_state.long_rtt,
            short_rtt = next_state.short_rtt,
            gradient = next_state.gradient,
            samples = measurement.sample_count,
            held = false,
        })
    end
    return true
end

------------------------------------------------------------------------
-- Worker exit reconciliation (control path — exit_worker_by_lua only)
------------------------------------------------------------------------

--- Reconcile this worker's still-held slots into the shared counter.
-- Call via adaptive.exit() from exit_worker_by_lua*. During graceful
-- shutdown the requests held here are torn down and will never reach
-- log_by_lua, so their slots would leak forever without this. If a
-- straggler log phase still fires afterwards, its release floors the
-- shared counter at 0 and raises the negative-inflight anomaly —
-- visible, never corrupting. Abrupt death (SIGKILL) runs nothing at
-- all: that leak is a documented limitation, surfaced through heartbeat
-- expiry and the stuck diagnostics in state().
function _M:exit_worker()
    local held = self._inflight
    if held <= 0 then
        return true
    end
    local dict = self.st.dict
    local n = dict:incr(self.K_inflight, -held)
    if not n then
        self.internal_errors = self.internal_errors + 1
        return nil, errors.INTERNAL_ERROR
    end
    if n < 0 then
        dict:incr(self.K_inflight, -n) -- see release(): atomic, not set(0)
    end
    self._inflight = 0
    count_anomaly(self, "exit_with_inflight", held)
    rate_limited_log(self, "exit", ngx_WARN,
        "worker exited holding ", tostring(held),
        " slots; reconciled shared counter")
    return true
end

------------------------------------------------------------------------
-- Observability (NOT on the request hot path)
------------------------------------------------------------------------

--- Observability snapshot. Performs several shared-dict reads and a
-- fixed number of heartbeat probes; never call per request. Numeric
-- counters under `stats`, `anomalies` and the flat counters are
-- worker-local (since this worker's start); shared state (`limit`,
-- `inflight`, RTTs, window bookkeeping) is cluster-wide for this
-- nginx instance. No JSON is produced anywhere.
function _M:state()
    local st = self.st
    local dict = st.dict
    local K = st.K
    local cfg = self.cfg
    local now = ngx_now()
    local s = self.stats

    -- numbers only: a foreign non-numeric value must not make the
    -- comparisons below throw
    local function getnum(key)
        local v = dict:get(key)
        if type(v) ~= "number" then
            return nil
        end
        return v
    end

    local limit = getnum(K.limit)
    local inflight = getnum(K.inflight)
    -- newest completion across the instance: the shared value is
    -- published once per tick (see tick()), this worker's own may be a
    -- tick fresher
    local last_completion = getnum(K.last_completion)
    if s.last_completion
        and (last_completion == nil or s.last_completion > last_completion) then
        last_completion = s.last_completion
    end
    local since_completion = last_completion and (now - last_completion)

    -- Stuck diagnostics (design.md §9): the pool is exhausted and no
    -- worker has seen a completion for stale_threshold seconds. A
    -- completion-based controller cannot observe a fully hung backend;
    -- backpressure still holds, and this flag makes it visible.
    local stalled = inflight ~= nil and limit ~= nil
        and inflight >= limit
        and (since_completion == nil
             or since_completion > cfg.stale_threshold)

    -- worker liveness: heartbeat slots are indexed by worker id
    local expected = ngx.worker.count() or 1
    local active = 0
    for i = 0, expected - 1 do
        if st:worker_alive(i) then
            active = active + 1
        end
    end

    -- last closed window's raw accumulators (shared view)
    local win = floor(now / cfg.sample_window) - 1
    local acc = st:read_window(win)

    local capacity_ok, capacity = pcall(dict.capacity, dict)
    if not capacity_ok then capacity = nil end
    local free_ok, free = pcall(dict.free_space, dict)
    if not free_ok then free = nil end

    local anomalies = 0
    for _, v in pairs(self.anomalies) do
        anomalies = anomalies + v
    end

    return {
        name = cfg.name,
        algorithm = cfg.algorithm,

        limit = limit,
        float_limit = getnum(K.limit_f),
        inflight = inflight,
        local_inflight = self._inflight,

        short_rtt = getnum(K.short_rtt),
        long_rtt = getnum(K.long_rtt),
        gradient = getnum(K.gradient),

        window = floor(now / cfg.sample_window),
        last_window = getnum(K.last_window),
        last_update = getnum(K.last_update),
        last_sample_count = acc and acc.c or nil,
        last_overload_count = acc and (acc.ovl + acc.tmo + acc.cer) or nil,
        last_rejected = acc and acc.rej or nil,

        admitted_total = s.admitted_total,
        rejected_total = s.rejected_total,
        sample_count = s.sample_count,

        controller_updates = self.controller_updates or 0,
        controller_skips = self.controller_skips or 0,
        internal_errors = self.internal_errors,
        counter_anomalies = anomalies,
        anomalies = self.anomalies,
        timer_failures = self.timer_failures or 0,

        last_completion_age = since_completion,
        controller_stalled = stalled,
        workers_active = active,
        workers_expected = expected,

        shared_dict_capacity = capacity,
        shared_dict_free = free,
    }
end

-- Exposed for tests and the scheduler.
_M.HB_TTL = HB_TTL
_M.rate_limited_log = rate_limited_log
_M.count_anomaly = count_anomaly
_M.default_observation = default_observation
_M.OUTCOMES = OUTCOMES

return _M
