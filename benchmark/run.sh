#!/bin/bash
# Benchmark harness (design.md §14): always against a no-limiter baseline.
#
# Runs inside the harness container:
#   docker run --rm -v $PWD:/work -w /work <image> benchmark/run.sh
#
# Scenarios (wrk -t2 -c64, 3 repetitions of 15s each; the MEDIAN rps is
# reported — single runs on a shared VM swing by more than 10%):
#   baseline          no limiter
#   fixed             fixed shared-dict counter (resty.limit.conn shape)
#   adaptive-huge     full adaptive limiter, limit 100000 (pure admission cost)
#   adaptive-tiny     limit 2 (heavy rejection path)
#   adaptive-ctrl     limit 40 with 1ms upstream, controller active
#   adaptive-ctrl5ms  limit 40 with 5ms upstream, controller active
#   adaptive-many16   1 limiter on the request path, scheduler serving 16
# Worker counts: 1, 2, 4. Raw wrk output lands in benchmark/results/.

set -u
PORT=8080
CONC=64
DUR=15s
REPS=3
NGINX=/usr/local/openresty/nginx/sbin/nginx
OUT=benchmark/results/$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"

note() { echo "[bench] $*"; }

machine_details() {
    echo "machine: $(uname -srm)"
    echo "cpu:     $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2)"
    echo "cores:   $(nproc)"
    echo "memory:  $(free -h 2>/dev/null | awk '/Mem/{print $2}')"
    echo "openresty: $($NGINX -v 2>&1)"
    echo "wrk:     $(wrk --version 2>&1)"
    echo "date:    $(date -u)"
    echo "load:    wrk -t2 -c$CONC -d$DUR, median of $REPS repetitions"
}

prep_prefix() { # $1 prefix
    rm -rf "$1"
    mkdir -p "$1/logs"
    cp -r /work/lib "$1/"
}

wait_port_free() {
    for _ in $(seq 1 40); do
        if ! curl -s -o /dev/null "http://127.0.0.1:$PORT/" 2>/dev/null; then
            return
        fi
        sleep 0.25
    done
    note "port $PORT still occupied; continuing anyway"
}

start_nginx() { # $1 conf $2 prefix [$3 env assignments]
    wait_port_free
    # shellcheck disable=SC2086
    env ${3:-} "$NGINX" -p "$2" -c "$1" 2>&1 \
        || { note "NGINX FAILED TO START ($1)"; exit 1; }
    sleep 1
    curl -s -o /dev/null "http://127.0.0.1:$PORT/work" \
        || { note "NGINX NOT SERVING ($1)"; exit 1; }
}

stop_nginx() { # $1 conf $2 prefix
    "$NGINX" -p "$2" -c "$1" -s quit 2>/dev/null
    sleep 0.5
    # TERM is the fast shutdown: workers are killed outright instead of
    # draining; between scenarios there is nothing to drain and a
    # graceful drain on a saturated single-core VM can stall the next
    # scenario's bind
    pkill -f "nginx.*$2" 2>/dev/null || true
    sleep 1
}

run_wrk() { # $1 name
    local name=$1 rpss=""
    for k in $(seq 1 "$REPS"); do
        # --timeout: never let a wedged connection block the harness for
        # an hour; timeout(1) is the harness backstop (fail the rep)
        if timeout -k 5 40 wrk -t2 -c"$CONC" -d"$DUR" --timeout 5s --latency \
            "http://127.0.0.1:$PORT/work" > "$OUT/$name-r$k.txt" 2>&1; then
            local rps=$(grep -oE "Requests/sec:[ ]+[0-9.]+" "$OUT/$name-r$k.txt" \
                | grep -oE "[0-9.]+")
            rpss="$rpss $rps"
        else
            note "REP FAILED for $name-r$k (see $OUT/$name-r$k.txt)"
            rpss="$rpss FAIL"
        fi
    done
    local median=$(echo $rpss | tr ' ' '\n' | grep -v FAIL | sort -n \
        | awk '{a[NR]=$1} END {if (NR>0) print a[int((NR+1)/2)]}')
    echo "$name: median=${median:-FAIL} rps (reps:$rpss)" | tee -a "$OUT/summary.txt"
}

machine_details > "$OUT/machine.txt"
note "results in $OUT"

# ---------------------------------------------------------------- fixed shape
for conf in baseline fixed; do
    for workers in 1 2 4; do
        prefix=/tmp/bench-$conf
        prep_prefix "$prefix"
        sed "s/^worker_processes 1;/worker_processes $workers;/" \
            "/work/benchmark/nginx-$conf.conf" > "$prefix/nginx.conf"
        start_nginx "$prefix/nginx.conf" "$prefix"
        run_wrk "$conf-w$workers"
        stop_nginx "$prefix/nginx.conf" "$prefix"
    done
done

# ---------------------------------------------------------------- adaptive profiles
for profile in huge tiny ctrl many16; do
    for workers in 1 2 4; do
        prefix=/tmp/bench-adaptive
        prep_prefix "$prefix"
        sed "s/^worker_processes 1;/worker_processes $workers;/" \
            /work/benchmark/nginx-adaptive.conf > "$prefix/nginx.conf"
        start_nginx "$prefix/nginx.conf" "$prefix" "BENCH_PROFILE=$profile BENCH_SLEEP_MS=1"
        run_wrk "adaptive-$profile-w$workers"
        stop_nginx "$prefix/nginx.conf" "$prefix"
    done
done

# ---------------------------------------------------------------- slower ctrl
note "ctrl profile with a 5ms upstream (controller shedding under load)"
prefix=/tmp/bench-adaptive
prep_prefix "$prefix"
sed "s/^worker_processes 1;/worker_processes 4;/" \
    /work/benchmark/nginx-adaptive.conf > "$prefix/nginx.conf"
start_nginx "$prefix/nginx.conf" "$prefix" "BENCH_PROFILE=ctrl BENCH_SLEEP_MS=5"
run_wrk "adaptive-ctrl5ms-w4"
stop_nginx "$prefix/nginx.conf" "$prefix"

# ---------------------------------------------------------------- micro
note "micro-benchmark (per-op admission cost)"
resty --http-conf "lua_shared_dict bench 10m;" -I /work/lib \
    /work/benchmark/microbench.lua 2>&1 | tee "$OUT/microbench.txt"

note "done: $OUT"
