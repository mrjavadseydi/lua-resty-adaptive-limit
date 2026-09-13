-- ngx + ngx.shared.DICT stub for pure-Lua busted specs.
--
-- Implements exactly the surface the library uses, with the documented
-- ngx.shared semantics (including nil + "not found" for missing keys) and
-- a deterministic clock. Requiring this file is idempotent: all specs in
-- one busted run share the same stub, because the library captures
-- ngx.shared at module-load time.
--
-- Usage:
--   local mock = require "spec.mock_ngx"
--   local dict = mock.make_dict()      -- fresh empty zone
--   ngx.shared.adaptive_limit = dict
--   ngx._now = 1234.5                  -- advance the clock

if not (ngx and ngx.__is_test_stub) then
    local stub = {
        __is_test_stub = true,
        _now = 1000.0,
        WARN = ngx and ngx.WARN or 4,
        ERR = ngx and ngx.ERR or 5,
    }

    stub.now = function()
        return stub._now
    end

    stub.log = function() end

    stub.get_phase = function()
        return "init_worker"
    end

    stub.shared = {}
    ngx = stub
end

local function make_dict()
    local data = {}

    local dict = {}

    function dict:get(key)
        local v = data[key]
        if v == nil then
            return nil, "not found"
        end
        return v
    end

    function dict:set(key, value, exptime)
        data[key] = value
        return true
    end

    function dict:delete(key)
        data[key] = nil
        return true
    end

    function dict:incr(key, delta, init)
        local v = data[key]
        if v == nil then
            if init == nil then
                return nil, "not found"
            end
            v = init
        end
        if type(v) ~= "number" then
            return nil, "not a number"
        end
        v = v + delta
        data[key] = v
        return v
    end

    function dict:add(key, value, exptime)
        if data[key] ~= nil then
            return false, "exists"
        end
        data[key] = value
        return true
    end

    function dict:expire(key, ttl)
        return data[key] ~= nil
    end

    dict._data = data
    return dict
end

ngx.make_dict = make_dict

-- Clear shared zones in place and rewind the clock. Zones must never be
-- replaced wholesale: the library captures ngx.shared at module-load
-- time, so only in-place mutation stays visible to it.
function ngx.reset()
    for k in pairs(ngx.shared) do
        ngx.shared[k] = nil
    end
    ngx._now = 1000.0
end

return ngx
