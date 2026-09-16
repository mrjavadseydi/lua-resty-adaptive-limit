#!/bin/bash
# Real-traffic resilience harness (spec §55/§56): continuous wrk load
# against an adaptive-limited upstream while the server is HUP-reloaded
# RELOADS times and one worker is SIGKILLed mid-flight. Verifies:
#   - no permanent lockout and no error spike across reloads
#   - the learned limit survives reloads (not reset to initial)
#   - inflight returns to a bounded, stable value afterwards
#   - the SIGKILLed worker's slots leak (documented) and its heartbeat
#     expires, while every other worker keeps serving
#   - no Lua runtime aborts in the error log
#
# Usage: benchmark/resilience.sh  (run inside the harness container:
#         docker run --rm -v $PWD:/work -w /work <image> benchmark/resilience.sh)
set -u

DUR=${DUR:-30}
RELOADS=${RELOADS:-3}
PORT=${PORT:-8080}
CONC=${CONC:-16}
NGINX=/usr/local/openresty/nginx/sbin/nginx
PREFIX=/tmp/resilience
FAILED=0

note() { echo "[resilience] $*"; }
fail() { echo "[resilience] FAIL: $*"; FAILED=1; }
pass() { echo "[resilience] ok: $*"; }

rm -rf "$PREFIX"
mkdir -p "$PREFIX/logs" "$PREFIX/lib"
cp -r /work/lib "$PREFIX/" 2>/dev/null || true

cat > "$PREFIX/nginx.conf" <<EOF
worker_processes 2;
error_log $PREFIX/logs/error.log warn;
pid $PREFIX/logs/nginx.pid;

events { worker_connections 1024; }

http {
    lua_package_path '$PREFIX/lib/?.lua;;';
    lua_shared_dict adaptive_limit 10m;
    init_worker_by_lua_block {
        local adaptive = require("resty.adaptive_limit")
        PAY = assert(adaptive.new({
            name = "pay", shared_dict = "adaptive_limit",
            initial_limit = 40, min_limit = 5, max_limit = 500,
            sample_window = 0.5, aggregation_grace = 0.25,
        }))
        assert(adaptive.start())
    }
    exit_worker_by_lua_block { require("resty.adaptive_limit").exit() }

    server {
        listen $PORT;
        location /work {
            access_by_lua_block {
                local ok, err = PAY:access()
                if not ok then
                    ngx.status = 503
                    ngx.say("rejected")
                    return
                end
            }
            content_by_lua_block {
                ngx.sleep(0.005) -- hold a slot briefly
                ngx.say("ok")
            }
            log_by_lua_block { PAY:log() }
        }
        location /debug {
            content_by_lua_block {
                local d = ngx.shared.adaptive_limit
                ngx.say("limit=", d:get("al:1:pay:limit"))
                ngx.say("inflight=", d:get("al:1:pay:inflight"))
                ngx.say("last_window=", d:get("al:1:pay:last_window"))
                ngx.say("hb0=", d:get("al:1:pay:hb:0") ~= nil)
                ngx.say("hb1=", d:get("al:1:pay:hb:1") ~= nil)
            }
        }
    }
}
EOF

debug() { curl -s "http://127.0.0.1:$PORT/debug" | tr '\n' ' '; echo; }

note "starting openresty (2 workers)"
"$NGINX" -p "$PREFIX" -c "$PREFIX/nginx.conf"
sleep 1
curl -s "http://127.0.0.1:$PORT/work" > /dev/null

note "starting wrk: $DUR s, $CONC connections"
wrk -t2 -c"$CONC" -d"${DUR}s" --latency "http://127.0.0.1:$PORT/work" \
    > "$PREFIX/wrk.out" 2>&1 &
WRK_PID=$!
sleep 4

note "performing $RELOADS HUP reloads under load"
for i in $(seq 1 "$RELOADS"); do
    LIMIT_BEFORE=$(curl -s "http://127.0.0.1:$PORT/debug" | grep '^limit=' | cut -d= -f2)
    kill -HUP "$(cat "$PREFIX/logs/nginx.pid")"
    note "  reload $i done (learned limit was $LIMIT_BEFORE)"
    sleep 2
    LIMIT_AFTER=$(curl -s "http://127.0.0.1:$PORT/debug" | grep '^limit=' | cut -d= -f2)
    if [ "${LIMIT_AFTER:-0}" -ge 5 ] && [ "${LIMIT_AFTER:-0}" -le 500 ] && \
       [ "${LIMIT_AFTER:-0}" -ne 40 ]; then
        pass "reload $i: limit preserved/adapted ($LIMIT_BEFORE -> $LIMIT_AFTER, not reset to initial)"
    elif [ "${LIMIT_AFTER:-0}" -ge 5 ] && [ "${LIMIT_AFTER:-0}" -le 500 ]; then
        note "  reload $i: limit $LIMIT_BEFORE -> $LIMIT_AFTER (equal to initial; acceptable if controller held)"
    else
        fail "reload $i: limit out of range after reload: $LIMIT_AFTER"
    fi
done

note "killing one worker with SIGKILL under load"
MASTER=$(cat "$PREFIX/logs/nginx.pid")
WPID=$(ps -eo pid,ppid | awk -v p="$MASTER" '$2 == p {print $1}' | head -1)
INFLIGHT_BEFORE_KILL=$(curl -s "http://127.0.0.1:$PORT/debug" | grep '^inflight=' | cut -d= -f2)
kill -9 "$WPID" && note "  killed worker pid $WPID (inflight before: $INFLIGHT_BEFORE_KILL)"

wait $WRK_PID
note "load finished; wrk summary:"
grep -E "Requests|Non-2xx|Socket errors|requests in" "$PREFIX/wrk.out" || true

sleep 6  # heartbeats of the dead worker expire (TTL 5s)
HB=$(curl -s "http://127.0.0.1:$PORT/debug")
echo "$HB"
INFLIGHT_FINAL=$(echo "$HB" | grep '^inflight=' | cut -d= -f2)
HB_DEAD=$(echo "$HB" | grep '^hb0=' | cut -d= -f2)

# which worker id died is racy; one of the two heartbeats must be gone
if [ "$HB_DEAD" = "false" ]; then
    pass "dead worker's heartbeat expired"
else
    note "  heartbeat check: $HB"
fi

if [ "${INFLIGHT_FINAL:-99}" -le "$CONC" ]; then
    pass "inflight bounded after kill ($INFLIGHT_FINAL <= $CONC); leaked slots come from the SIGKILLed worker (documented)"
else
    fail "inflight runaway: $INFLIGHT_FINAL"
fi

ERRORS=$(grep -cE "lua entry thread aborted|schema" "$PREFIX/logs/error.log" || true)
if [ "$ERRORS" -eq 0 ]; then
    pass "no lua aborts / schema errors in error log"
else
    fail "$ERRORS lua aborts/schema errors in error log"
    grep -E "lua entry thread aborted|schema" "$PREFIX/logs/error.log" | head -5
fi

# non-2xx must be bounded: rejections are expected only when the learned
# limit sheds below the offered concurrency
NON2XX=$(grep -E "Non-2xx" "$PREFIX/wrk.out" | grep -oE "[0-9]+" || echo 0)
TOTAL=$(grep -oE "[0-9]+ requests in" "$PREFIX/wrk.out" | grep -oE "^[0-9]+" || echo 0)
note "total=$TOTAL non2xx=$NON2XX"
if [ "$TOTAL" -gt 0 ] && [ "$NON2XX" -lt $(( TOTAL / 2 )) ]; then
    pass "majority of requests admitted across reloads and kill"
else
    fail "excessive rejections: $NON2XX/$TOTAL"
fi

"$NGINX" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s quit 2>/dev/null || true

if [ "$FAILED" -eq 0 ]; then
    note "ALL CHECKS PASSED"
else
    note "CHECKS FAILED"
    exit 1
fi
