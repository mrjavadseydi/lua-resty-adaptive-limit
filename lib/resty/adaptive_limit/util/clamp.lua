-- Clamp a number into [lo, hi].
--
-- Returned as a bare function: this sits on the controller path where the
-- call is compiled to a direct Lua call with no table indirection.
--
-- NaN propagates untouched by comparison semantics (comparisons with NaN
-- are false), so callers that must reject NaN do so before clamping; see
-- controller/common.lua.

return function(v, lo, hi)
    if v < lo then
        return lo
    end
    if v > hi then
        return hi
    end
    return v
end
