#!/bin/bash
# Soak harness (spec §57): sustained traffic with periodic sampling of
# worker RSS, Lua GC size, shared-dict usage and inflight. Memory must
# stabilize — no structure may grow with the number of requests served.
#
#   SOAK_SECONDS=600 make soak
#
# Samples land in benchmark/results/soak-<ts>.csv and are checked for
# monotonic growth at the end.

set -u
DUR=${SOAK_SECONDS:-600}
SAMPLE_EVERY=10
PORT=${PORT:-8080}
NGINX=/usr/local/openresty/nginx/sbin/nginx
PREFIX=/tmp/soak
OUT=benchmark/results/soak-$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"

note() { echo "[soak] $*"; }

rm -rf "$PREFIX"
mkdir -p "$PREFIX/logs"
cp -r /work/lib "$PREFIX/"

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
        PAY = assert(adaptive.new({ name = "pay", shared_dict = "adaptive_limit",
            initial_limit = 100, min_limit = 5, max_limit = 1000,
            sample_window = 0.5, aggregation_grace = 0.25 }))
        assert(adaptive.start())
    }
    exit_worker_by_lua_block { require("resty.adaptive_limit").exit() }

    server {
        listen $PORT;
        location /work {
            access_by_lua_block {
                local ok = PAY:access()
                if not ok then
                    ngx.status = 503
                    ngx.say("rejected")
                    return
                end
            }
            content_by_lua_block {
                ngx.sleep(0.005)
                ngx.say("ok")
            }
            log_by_lua_block { PAY:log() }
        }
        location /gc {
            content_by_lua_block {
                ngx.say("gc_kb=", math.floor(collectgarbage("count")))
            }
        }
        location /debug {
            content_by_lua_block {
                local d = ngx.shared.adaptive_limit
                ngx.say("inflight=", d:get("al:1:pay:inflight"))
                ngx.say("limit=", d:get("al:1:pay:limit"))
            }
        }
    }
}
EOF

note "starting openresty, $DUR s of wrk load (2 threads, 32 conn)"
"$NGINX" -p "$PREFIX" -c "$PREFIX/nginx.conf"
sleep 1

wrk -t2 -c32 -d"${DUR}s" "http://127.0.0.1:$PORT/work" > "$OUT/wrk.txt" 2>&1 &
WRK_PID=$!

CSV="$OUT/samples.csv"
echo "t_sec,rss_kb_total,gc_kb,inflight,limit" > "$CSV"
T0=$(date +%s)
while true; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - T0))
    [ "$ELAPSED" -ge "$DUR" ] && break
    sleep "$SAMPLE_EVERY"
    RSS=$(ps -eo rss,comm | awk '/nginx/ {s+=$1} END {print s+0}')
    GC=$(curl -s "http://127.0.0.1:$PORT/gc" | grep -oE '[0-9]+')
    DBG=$(curl -s "http://127.0.0.1:$PORT/debug")
    INFLIGHT=$(echo "$DBG" | grep '^inflight=' | cut -d= -f2)
    LIMIT=$(echo "$DBG" | grep '^limit=' | cut -d= -f2)
    note "t=${ELAPSED}s rss=${RSS}kB gc=${GC}kB inflight=${INFLIGHT} limit=${LIMIT}"
    echo "$ELAPSED,$RSS,$GC,${INFLIGHT:-0},${LIMIT:-0}" >> "$CSV"
done
wait $WRK_PID

note "wrk summary:"
grep -E "Requests/sec|Socket errors|Non-2xx" "$OUT/wrk.txt" || true

# memory-stability check: last quarter of samples vs first quarter
awk -F, '
NR > 1 {
    c++
    gc[c] = $3 + 0
    rss[c] = $2 + 0
}
END {
    if (c < 4) { print "[soak] FAIL: not enough samples"; exit 1 }
    q = int(c / 4); if (q < 1) q = 1
    g1 = 0; g2 = 0; r1 = 0; r2 = 0
    for (i = 1; i <= q; i++)      { g1 += gc[i]; r1 += rss[i] }
    for (i = c - q + 1; i <= c; i++) { g2 += gc[i]; r2 += rss[i] }
    gg = g2 / q - g1 / q
    rg = r2 / q - r1 / q
    ming = 9999999; maxg = 0; minr = 9999999; maxr = 0
    for (i = 1; i <= c; i++) {
        if (gc[i] < ming) ming = gc[i]
        if (gc[i] > maxg) maxg = gc[i]
        if (rss[i] < minr) minr = rss[i]
        if (rss[i] > maxr) maxr = rss[i]
    }
    printf "[soak] gc:  range %d..%d kB, quarter-growth %+.0f kBn", ming, maxg, gg
    printf "[soak] rss: range %d..%d kB, quarter-growth %+.0f kBn", minr, maxr, rg
    if (gg < 512 && rg < 20480) {
        print "[soak] PASS: memory stable"
    } else {
        print "[soak] FAIL: memory growing"
        exit 1
    }
}' "$CSV"
STATUS=$?
"$NGINX" -p "$PREFIX" -c "$PREFIX/nginx.conf" -s quit 2>/dev/null || true
grep -cE "lua entry thread aborted" "$PREFIX/logs/error.log" | grep -q '^0$' \
    || { note "FAIL: lua aborts in error log"; STATUS=1; }

note "samples: $CSV"
exit $STATUS
