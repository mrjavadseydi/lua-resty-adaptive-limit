# lua-resty-adaptive-limit — Makefile
#
# All tests, simulations and benchmarks run inside the Docker harness image
# (see docker/Dockerfile.test) so local runs and CI use identical tooling.

IMAGE        ?= lua-resty-adaptive-limit/harness:latest
OPENRESTY_IMAGE ?= openresty/openresty:1.31.1.1-3-jammy

# Test::Nginx scratch lives outside the repo mount so repeated runs stay clean.
TEST_ENV := TEST_NGINX_SERVROOT=/tmp/adlim-servroot TEST_NGINX_RANDOM_DELAY=off

.PHONY: image test test-unit test-integration sim bench soak shell clean

image:
	docker build -t $(IMAGE) --build-arg OPENRESTY_IMAGE=$(OPENRESTY_IMAGE) -f docker/Dockerfile.test .

# Pure-Lua unit tests + fuzz properties (busted)
test-unit:
	docker run --rm -v $(CURDIR):/work -w /work $(IMAGE) busted spec/

# OpenResty integration tests (Test::Nginx)
test-integration:
	docker run --rm -v $(CURDIR):/work -w /work $(IMAGE) \
		sh -c '$(TEST_ENV) prove -I. -r t/'

test: test-unit test-integration

# Deterministic controller simulations (scenarios A–H)
sim:
	docker run --rm -v $(CURDIR):/work -w /work $(IMAGE) busted spec/simulation_spec.lua

# HTTP benchmarks (wrk). Duration/PROFILE tunables live in benchmark/run.sh.
bench:
	docker run --rm -v $(CURDIR):/work -w /work $(IMAGE) benchmark/run.sh

# Bounded soak (default 10 minutes; override SOAK_SECONDS for longer runs)
soak:
	docker run --rm -v $(CURDIR):/work -w /work $(IMAGE) benchmark/soak.sh

shell:
	docker run --rm -it -v $(CURDIR):/work -w /work $(IMAGE)

clean:
	rm -rf t/servroot* benchmark/tmp
