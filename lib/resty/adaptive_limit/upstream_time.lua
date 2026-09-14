-- Parser for NGINX upstream timing variables ($upstream_response_time,
-- $upstream_header_time, $upstream_connect_time).
--
-- With upstream retries these variables hold multiple values separated
-- by commas and/or spaces, e.g. "0.005, 0.010" or "0.005 , 0.010";
-- "-" means that attempt produced no timing (e.g. connect failure).
-- Calling tonumber() on the raw value would either yield nil or, worse,
-- silently parse only the first token depending on LuaJIT's conversion
-- semantics — so a compound value is parsed here, explicitly:
--
--   choice = "last"  the final attempt's duration (default: the attempt
--                    whose response the client actually saw)
--   choice = "max"   the slowest attempt
--   choice = "sum"   total time spent across attempts
--
-- Any token that is not a finite non-negative number makes the whole
-- value unusable: the parser returns nil and no observation is recorded.
-- An honest absence of data beats a wrong number.

local tonumber = tonumber
local math_huge = math.huge

local _M = {}

function _M.parse(raw, choice)
    if raw == nil or raw == "" or raw == "-" then
        return nil
    end

    local total = 0
    local max_v
    local last_v
    local count = 0

    for token in raw:gmatch("[^,%s]+") do
        local v = tonumber(token)
        if v == nil or v ~= v or v < 0 or v == math_huge then
            return nil
        end
        total = total + v
        if max_v == nil or v > max_v then
            max_v = v
        end
        last_v = v
        count = count + 1
    end

    if count == 0 then
        return nil
    end
    if choice == "sum" then
        return total
    end
    if choice == "max" then
        return max_v
    end
    return last_v
end

return _M
