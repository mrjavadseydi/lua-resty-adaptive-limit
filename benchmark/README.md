# Benchmarks

All harnesses run inside the Docker test image (see `docker/Dockerfile.test`)
so local runs and CI use identical tooling. Everything here exists to keep
the performance section of the README honest: measured truth over
marketing numbers, baseline comparison over absolute figures.

## Methodology

- **Load generator**: wrk 4.2.0 (`-t2 -c64`), 3 repetitions of 15 s per
  scenario, **median** reported. Single runs on a shared VM swing by more
  than 10%; medians tame that without hiding it (all raw repetitions are
  in `results/<run-id>/`).
- **Every scenario is compared against a no-limiter baseline** with an
  identical request shape (`access`-shaped phase chain, 1 ms simulated
  upstream).
- `fixed` is a minimal fixed shared-dict concurrency counter — the same
  admission shape as `resty.limit.conn` — isolating the cost of the
  adaptive machinery above the shared-dict floor (the §69 comparison).
- Micro-benchmark (`microbench.lua`) measures per-operation cost inside
  OpenResty over 200k operations, including the raw `get`+`incr` dict
  floor for reference.
- Absolute numbers are Docker-VM-relative (Linuxkit on the host machine
  documented per run in `machine.txt`); only the relative
  baseline-vs-limiter comparison is treated as meaningful.

## Scenarios

| name | limiter | limit | upstream | what it measures |
|---|---|---|---|---|
| `baseline-wN` | none | — | 1 ms | no-limiter baseline |
| `fixed-wN` | fixed counter | 10^6 | 1 ms | shared-dict admission floor |
| `adaptive-huge-wN` | adaptive | 10^5 | 1 ms | pure admission cost, no rejections |
| `adaptive-tiny-wN` | adaptive | 2 | 1 ms | heavy-rejection path (incr+rollback) |
| `adaptive-ctrl-wN` | adaptive | 20→40 | 1 ms | controller active + shedding |
| `adaptive-ctrl5ms-w4` | adaptive | 20→40 | 5 ms | controller shedding a slower upstream |
| `adaptive-many16-wN` | adaptive ×16 | 10^5 | 1 ms | scheduler cost with 16 limiters |

## Running

```bash
make image
make bench          # writes benchmark/results/<run-id>/
SOAK_SECONDS=600 make soak       # memory/GC stability under sustained load
make resilience     # HUP reloads + worker SIGKILL under real traffic
```

## Results

Published numbers live in `results/` (git-tracked summaries, raw wrk
output per run) and in the README performance table. When re-running on
your hardware, note the VM/CPU details from `machine.txt` before comparing.
