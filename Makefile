# lua-resty-adaptive-limit — Makefile
#
# Integration tests, simulations and benchmarks run inside the Docker
# harness image (see docker/Dockerfile.test) so local runs and CI use
# identical tooling. The image is built on first use. Pure-Lua specs run
# on a local busted when one is installed (see CONTRIBUTING.md).

IMAGE        ?= lua-resty-adaptive-limit/harness:latest
OPENRESTY_IMAGE ?= openresty/openresty:1.31.1.1-3-jammy

# Narrow a run to one file: make test-unit SPEC=spec/gradient2_spec.lua
SPEC ?= spec/
T    ?= t/

BUSTED := $(shell command -v busted 2>/dev/null)
RUN    := docker run --rm -v $(CURDIR):/work -w /work $(IMAGE)

# Test::Nginx scratch lives outside the repo mount so repeated runs stay clean.
TEST_ENV := TEST_NGINX_SERVROOT=/tmp/adlim-servroot TEST_NGINX_RANDOM_DELAY=off

.PHONY: image image-if-missing test test-unit test-integration sim bench soak resilience shell clean

image:
	docker build -t $(IMAGE) --build-arg OPENRESTY_IMAGE=$(OPENRESTY_IMAGE) -f docker/Dockerfile.test .

image-if-missing:
	@docker image inspect $(IMAGE) >/dev/null 2>&1 || $(MAKE) image

# Pure-Lua unit tests + fuzz properties (busted): local when available
test-unit:
ifdef BUSTED
	busted $(SPEC)
else
	@$(MAKE) image-if-missing
	$(RUN) busted $(SPEC)
endif

# OpenResty integration tests (Test::Nginx)
test-integration: | image-if-missing
	$(RUN) sh -c '$(TEST_ENV) prove -I. -r $(T)'

test: test-unit test-integration

# Deterministic controller simulations (scenarios A–H)
sim: SPEC = spec/simulation_spec.lua
sim: test-unit

# HTTP benchmarks (wrk). Duration/PROFILE tunables live in benchmark/run.sh.
bench: | image-if-missing
	$(RUN) benchmark/run.sh

# Bounded soak (default 10 minutes; override SOAK_SECONDS for longer runs)
soak: | image-if-missing
	$(RUN) benchmark/soak.sh

# HUP reloads + worker SIGKILL under real wrk traffic
resilience: | image-if-missing
	$(RUN) benchmark/resilience.sh

shell: | image-if-missing
	docker run --rm -it -v $(CURDIR):/work -w /work $(IMAGE)

clean:
	rm -rf t/servroot* benchmark/tmp
