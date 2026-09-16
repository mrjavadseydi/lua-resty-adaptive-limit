-- The limiter instance: admission, release, and (from later phases)
-- lifecycle helpers, statistics and controller wiring.
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
    "connect_error", "error", "aborted", "rejected_total",
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
}

-- Outcomes accepted by release(). "ignored" excludes the sample from
-- controller statistics entirely (health checks, synthetic probes).
local OUTCOMES = {
    success = 1, timeout = 1, connect_error = 1, overload = 1,
    error = 1, aborted = 1, ignored = 1,
}

-- Sanity clamp for externally supplied latency values (spec §31: a
-- malformed latency must not corrupt the stats). Values outside
-- [0, LATENCY_CAP] are counted as anomalies and not sampled.
local LATENCY_CAP = 3600

-- Rate-limited logging: at most one message per kind per LOG_INTERVAL
-- seconds per worker (spec §11: surface violations, never spam).
local LOG_INTERVAL = 1.0

local _M = {}
local mt = { __index = _M }

-- rate_limited_log(self, kind, level, ...)
-- Returns true if the message was emitted within the budget.
local function rate_limited_log(self, kind, level, msg)
    local now = ngx_now()
    local last = self._log_last[kind]
    if last and now - last < LOG_INTERVAL then
        return false
    end
    self._log_last[kind] = now
    ngx_log(level, "adaptive_limit[", self.cfg.name, "] ", msg)
    return true
end

local function count_anomaly(self, kind)
    self.anomalies[kind] = (self.anomalies[kind] or 0) + 1
    local hook = self.cfg.on_anomaly
    if hook then
        hook(kind, nil)
    end
end

-- Classify a raw shared-dict error into a limiter-internal failure.
local function internal_error(self, where, err)
    self.internal_errors = self.internal_errors + 1
    rate_limited_log(self, "internal", ngx_ERR,
        where, " failed: ", err or "unknown")
    return nil, errors.INTERNAL_ERROR
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
            admitted_total = 0,
            rejected_total = 0,
            last_completion = nil,
        },

        internal_errors = 0,
        anomalies = {},

        _log_last = {},
        -- ngx.ctx key, precomputed once ("alim:" .. name)
        _ctx_key = "alim:" .. cfg.name,

        -- flush bookkeeping (scheduler)
        _flush_window = nil,
        -- flushed-marker mirror of the monotonic stats fields: flush()
        -- writes deltas against these into the shared window
        -- accumulators and advances them by exactly the written delta
        _flushed = {
            sample_count = 0, latency_sum = 0, overload = 0, timeout = 0,
            connect_error = 0, error = 0, aborted = 0, rejected_total = 0,
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
    self.st.dict:set(st.K.inflight, 0)
    self.st.dict:set(st.K.last_window, 0)
    self.st.dict:set(st.K.last_update, ngx_now())
    self._last_limit = cfg.initial_limit
end

-- Validate the shared state schema marker; reseed if absent, refuse on
-- mismatch. Called from adaptive.start() (init_worker) and after schema
-- errors detected at runtime.
function _M:check_schema()
    local st = self.st
    local v, err = st:read_schema()
    if v == nil then
        if err and err ~= "not found" then
            return nil, errors.INTERNAL_ERROR
        end
        -- no marker: first worker here, or the dict was flushed
        self:reseed_shared_state()
        return true
    end
    if v ~= tostring(state_mod.SCHEMA_VERSION) then
        rate_limited_log(self, "schema", ngx_ERR,
            "shared state schema version ", tostring(v),
            " is not supported (expected ", tostring(state_mod.SCHEMA_VERSION),
            "); refusing to reuse incompatible state")
        return nil, errors.INVALID_STATE
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
    self._last_limit = adopted
    return adopted
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
        self.anomalies.limit_missing = (self.anomalies.limit_missing or 0) + 1
        limit = self._last_limit or self.cfg.initial_limit
        dict:set(self.K_limit, limit)
        rate_limited_log(self, "limit_missing", ngx_WARN,
            "limit key missing; re-seeded from last observed value")
    elseif type(limit) ~= "number" or limit ~= limit or limit < 0 then
        -- Corrupted shared value: never trust it and never crash the
        -- request comparing against it (spec §31/§46). Replace with the
        -- last observed limit and surface.
        self.anomalies.limit_corrupted =
            (self.anomalies.limit_corrupted or 0) + 1
        limit = self._last_limit or self.cfg.initial_limit
        dict:set(self.K_limit, limit)
        rate_limited_log(self, "limit_corrupted", ngx_ERR,
            "limit key corrupted (non-numeric/negative); re-seeded")
    end
    self._last_limit = limit

    -- Admission linearization point (see top-of-file comment).
    local n, ierr = dict:incr(self.K_inflight, 1, 0)
    if not n then
        return internal_error(self, "incr(inflight)", ierr)
    end

    if n <= limit then
        self._inflight = self._inflight + 1
        self.stats.admitted_total = self.stats.admitted_total + 1
        return true
    end

    -- Over the limit: roll the reservation back unconditionally.
    dict:incr(self.K_inflight, -1)
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
    if outcome ~= nil and not OUTCOMES[outcome] then
        return nil, errors.INVALID_STATE
    end
    outcome = outcome or "success"

    local dict = self.st.dict

    -- 1. Release the slot. This must succeed for accounting to hold.
    local n, err = dict:incr(self.K_inflight, -1)
    if not n then
        -- The slot leaks; surfaced via internal_errors and the stuck
        -- diagnostics. The caller may retry the release once.
        return internal_error(self, "incr(inflight, -1)", err)
    end
    if n < 0 then
        -- Double release (or a lost admission): snap to zero and surface.
        self.anomalies.negative_inflight =
            (self.anomalies.negative_inflight or 0) + 1
        dict:set(self.K_inflight, 0)
        rate_limited_log(self, "negative", ngx_WARN,
            "inflight went negative; double release suspected")
    end
    if self._inflight > 0 then
        self._inflight = self._inflight - 1
    end

    -- 2. Record the observation (never blocks the release: fixed-size
    --    struct updates only). sample_count counts usable latency
    --    observations; outcome counters count every completed outcome.
    if outcome ~= "ignored" then
        local s = self.stats
        if outcome ~= "aborted" then
            -- Client aborts release the slot but their (truncated)
            -- duration is not a capacity signal; malformed latency is
            -- dropped by sanitize_latency so mean_rtt stays honest.
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
    local win = floor(now / cfg.sample_window)
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
                return win
            end
            flushed[f] = flushed[f] + delta
        end
    end

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
-- Within one location's phase chain (and across error_page redirects,
-- where ngx.ctx survives), the ctx flag makes access() a no-op for a
-- limiter that already admitted this request: one request holds at most
-- one slot per limiter. ngx.exec targets and subrequests are guarded
-- separately (see access() below) because ngx.exec resets ngx.ctx and
-- subrequests never get a log phase.

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
    -- TEST 6/6b): ngx.ctx survives error_page redirects but is reset by
    -- ngx.exec, subrequests get no log phase, and $request_id changes at
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
    self.st:heartbeat(worker_id, now, HB_TTL)
    self:flush(now)
    self:control(now)
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

    local last = cs.last_window or 0
    local n_ready = floor((now - cfg.aggregation_grace) / sw) - 1
    if n_ready <= last then
        return
    end

    -- stale backlog: never replay old windows after a long pause
    local from = last + 1
    if n_ready - from + 1 > MAX_WINDOWS_PER_TICK then
        local skipped = n_ready - MAX_WINDOWS_PER_TICK - last
        self.controller_skips = (self.controller_skips or 0) + skipped
        rate_limited_log(self, "stale", ngx_WARN,
            "skipping ", tostring(skipped),
            " stale controller windows after a pause")
        from = n_ready - MAX_WINDOWS_PER_TICK + 1
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
        next_state = state
    elseif next_state.held then
        held = true
        self.controller_skips = (self.controller_skips or 0) + 1
    else
        self.controller_updates = (self.controller_updates or 0) + 1
    end

    st:publish_controller_state(next_state.limit, next_state.long_rtt,
        next_state.short_rtt, next_state.gradient, n, now)
    self._last_limit = math.floor(next_state.limit)
    st:delete_window(n)

    if cfg.on_update then
        pcall(cfg.on_update, {
            name = cfg.name,
            window = n,
            limit = self._last_limit,
            float_limit = next_state.limit,
            long_rtt = next_state.long_rtt,
            short_rtt = next_state.short_rtt,
            gradient = next_state.gradient,
            samples = measurement.sample_count,
            held = held,
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
        dict:set(self.K_inflight, 0)
    end
    self._inflight = 0
    self.anomalies.exit_with_inflight =
        (self.anomalies.exit_with_inflight or 0) + held
    rate_limited_log(self, "exit", ngx_WARN,
        "worker exited holding ", tostring(held),
        " slots; reconciled shared counter")
    return true
end

-- Exposed for tests and the scheduler.
_M.rate_limited_log = rate_limited_log
_M.count_anomaly = count_anomaly
_M.default_observation = default_observation
_M.OUTCOMES = OUTCOMES

return _M
