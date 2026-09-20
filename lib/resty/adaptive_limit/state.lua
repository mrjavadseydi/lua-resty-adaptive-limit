-- Shared-dictionary state layer.
--
-- Owns every key the module reads or writes, all of them interned at
-- construction time. The request path performs zero string construction:
-- admission touches exactly K_limit and K_inflight.
--
-- Key layout (prefix = "al:<schema>:<name>:"):
--   limit, inflight, long_rtt, short_rtt, gradient,
--   last_window, last_update   — permanent controller state
--   lc:<worker_id>             — that worker's newest completion time
--   w:<n>:c|s|ovl|tmo|err|abt|rej|cmp — per-window aggregate accumulators;
--       every writer uses atomic incr (init 0), so flushes from any
--       number of workers are race-free by construction. Keys carry an
--       exptime (set once per window per worker, on rollover) so an
--       interrupted controller cannot leak window keys; the controller
--       additionally deletes them after processing.
--   lease:<n>                  — controller lease for window n (TTL only)
--   hb:<worker_id>             — worker heartbeat (TTL only)
--
-- All methods return library-level results; raw dict error strings are
-- propagated to the caller (limiter.lua classifies them).

local WINDOW_FIELDS = { "c", "s", "ovl", "tmo", "cer", "err", "abt", "rej",
    "cmp" }

local floor = math.floor

local _M = {}

_M.SCHEMA_VERSION = 1

-- The schema marker lives OUTSIDE the versioned per-limiter namespace:
-- if it sat at "al:<version>:..." an upgraded library would simply use a
-- different prefix and old/new workers would split-brain into two
-- independent counters inside the same zone without ever detecting each
-- other. One dict-global marker makes an incompatible shared state a
-- loud startup error instead.
_M.SCHEMA_KEY = "adaptive_limit:schema"

function _M.new(dict, name)
    if not dict then
        return nil, "shared dict not found"
    end
    local prefix = "al:" .. _M.SCHEMA_VERSION .. ":" .. name .. ":"

    local K = {
        limit       = prefix .. "limit",       -- published integer limit
        limit_f     = prefix .. "limit_f",     -- controller float state
        inflight    = prefix .. "inflight",
        long_rtt    = prefix .. "long_rtt",
        short_rtt   = prefix .. "short_rtt",
        gradient    = prefix .. "gradient",
        last_window = prefix .. "last_window",
        last_update = prefix .. "last_update",
    }

    local state = {
        dict = dict,
        K = K,
        prefix = prefix,
        -- per-window key cache, rebuilt on window rollover by the
        -- scheduler (control path); never touched by the request path
        _win = { n = nil },
    }

    return setmetatable(state, { __index = _M })
end

-- Window accumulator key cache: control path only.
function _M:window_keys(n)
    local cache = self._win
    if cache.n ~= n then
        local p = self.prefix .. "w:" .. n .. ":"
        cache.n = n
        for i = 1, #WINDOW_FIELDS do
            local f = WINDOW_FIELDS[i]
            cache[f] = p .. f
        end
    end
    return cache
end

-- Atomic add of a partial aggregate into window n. init 0 makes the
-- first writer create the key; concurrent flushers serialize inside the
-- shared dictionary. window_ttl is applied on EVERY write (not just the
-- first): a field whose first write lands after a worker's first flush
-- of the window must still get the backstop expiry, and this way no
-- per-worker rollover state is needed. One extra expire per written
-- field per tick — control-path cost only.
function _M:add_window(n, field, delta, window_ttl)
    local keys = self:window_keys(n)
    local key = keys[field]
    local value, err = self.dict:incr(key, delta, 0)
    if not value then
        return nil, err
    end
    self.dict:expire(key, window_ttl)
    return value
end

-- Read all accumulators of window n; missing keys read as 0.
-- (dict:get returns nil, "not found" for absent keys — that is a normal
-- empty accumulator here, only genuine errors are propagated.)
function _M:read_window(n)
    local keys = self:window_keys(n)
    local out = {}
    local dict = self.dict
    for i = 1, #WINDOW_FIELDS do
        local f = WINDOW_FIELDS[i]
        local v, err = dict:get(keys[f])
        if v == nil and err and err ~= "not found" then
            return nil, err
        end
        -- a foreign non-numeric value reads as an empty accumulator
        out[f] = type(v) == "number" and v or 0
    end
    return out
end

function _M:delete_window(n)
    local keys = self:window_keys(n)
    for i = 1, #WINDOW_FIELDS do
        self.dict:delete(keys[WINDOW_FIELDS[i]])
    end
end

-- Controller lease: add wins; TTL-only expiry (the lease is never
-- explicitly deleted — a successor lease for window n+1 must never be
-- disturbed by our cleanup, and window n is never processed again
-- anyway thanks to the last_window guard).
function _M:try_lease(n, owner, ttl)
    return self.dict:add(self.prefix .. "lease:" .. n, owner, ttl)
end

-- True while some worker holds (or recently held) the lease on window n.
function _M:lease_held(n)
    return self.dict:get(self.prefix .. "lease:" .. n) ~= nil
end

function _M:heartbeat(worker_id, now, ttl)
    return self.dict:set(self.prefix .. "hb:" .. worker_id, now, ttl)
end

function _M:worker_alive(worker_id, now)
    local v = self.dict:get(self.prefix .. "hb:" .. worker_id)
    return v ~= nil, v
end

function _M:read_schema()
    return self.dict:get(self.SCHEMA_KEY)
end

function _M:write_schema()
    -- no TTL; add() so a concurrent first-worker race is benign
    return self.dict:add(self.SCHEMA_KEY, tostring(self.SCHEMA_VERSION))
end

-- Controller state read/write (control path only). Missing keys come
-- back as nil fields; the return value is nil only on genuine dict
-- errors.
function _M:read_controller_state()
    local dict = self.dict
    local K = self.K
    local cs = {}
    local fields = { "limit", "limit_f", "long_rtt", "short_rtt",
        "last_window" }
    local keys = { K.limit, K.limit_f, K.long_rtt, K.short_rtt,
        K.last_window }
    for i = 1, #fields do
        local v, err = dict:get(keys[i])
        if v == nil and err and err ~= "not found" then
            return nil, err
        end
        cs[fields[i]] = v
    end
    return cs
end

-- Returns true, or nil + the first dict error. last_window is written
-- LAST: if a write fails (no memory) or the worker dies mid-publish,
-- last_window lags and the window is simply processed again — at worst
-- one extra smoothing step on values that are clamped and finite either
-- way. (Writing last_window first instead would skip the window; both
-- failure orders are bounded, the repeated-update order keeps more
-- signal.)
function _M:publish_controller_state(limit_f, long_rtt, short_rtt, gradient,
                                     last_window, now)
    local dict = self.dict
    local K = self.K
    -- nil RTT state means "no valid value" (not seeded yet, or dropped by
    -- repair_state): the key must go, or a corrupt foreign value would
    -- be read back and rejected again on every window.
    local function put(key, value)
        if value == nil then
            return dict:delete(key)
        end
        return dict:set(key, value)
    end
    if gradient ~= nil then
        -- diagnostics only; kept across held windows (which carry none)
        dict:set(K.gradient, gradient)
    end
    local ok, err = put(K.limit_f, limit_f)
    if ok then ok, err = put(K.limit, math.floor(limit_f)) end
    if ok then ok, err = put(K.long_rtt, long_rtt) end
    if ok then ok, err = put(K.short_rtt, short_rtt) end
    if ok then ok, err = put(K.last_update, now) end
    if ok then ok, err = put(K.last_window, last_window) end
    if not ok then
        return nil, err
    end
    return true
end

-- Per-worker newest completion time (no TTL: a dead worker's last
-- completion is still a completion). One key per worker, so no
-- cross-worker read-modify-write exists; state() takes the max.
function _M:publish_last_completion(worker_id, t)
    return self.dict:set(self.prefix .. "lc:" .. worker_id, t)
end

function _M:worker_last_completion(worker_id)
    local v = self.dict:get(self.prefix .. "lc:" .. worker_id)
    return type(v) == "number" and v or nil
end

return _M
