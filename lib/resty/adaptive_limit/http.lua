-- Optional HTTP-facing helpers. Deliberately outside the core limiter:
-- the low-level API returns decisions and never touches nginx response
-- state itself.

local errors = require("resty.adaptive_limit.errors")

local ngx = ngx
local ngx_exit = ngx.exit
local tostring = tostring

local _M = {}

--- Produce the configured rejection response.
-- err must be one of the limiter's error constants. "rejected" maps to
-- the configured rejection_status (default 503 — backend capacity
-- exhaustion; 429 fits client-facing quota policies instead, see README)
-- with a Retry-After header; limiter-internal failures map to 500.
-- Must be called from a content/access phase that is allowed to produce
-- a response.
function _M.reject(cfg, err)
    -- ngx.header is per-request: it must be read at call time, never
    -- cached at module load
    ngx.header["Retry-After"] = tostring(cfg.retry_after)
    if err == errors.REJECTED then
        ngx_exit(cfg.rejection_status)
        return
    end
    ngx_exit(500)
end

return _M
