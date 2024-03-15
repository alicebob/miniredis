.PHONY: help
help:	### This screen. Keep it first target to be default
ifeq ($(UNAME), Linux)
	@grep -P '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
else
	@# this is not tested, but prepared in advance for you, Mac drivers
	@awk -F ':.*###' '$$0 ~ FS {printf "%15s%s\n", $$1 ":", $$2}' \
		$(MAKEFILE_LIST) | grep -v '@awk' | sort
endif

.PHONY: test
test: ### Run unit tests
	go test ./...

.PHONY: testrace
testrace: ### Run unit tests with race detector
	go test -race ./...

.PHONY: conformance
conformance: conformance/redis_src/redis-server ### Run conformance tests (compare miniredis implementation with original redis)
	MINIREDIS_CONFORMANCE=1 go test ./conformance/...

conformance/redis_src/redis-server: conformance/get_redis.sh ### Download and build redis if not available or in wrong version
	./conformance/get_redis.sh

.PHONY: clean
clean: ### Cleanup conformance test files (including redis binary)
	rm -rf \
		conformance/redis_src \
		conformance/dump.rdb \
		conformance/nodes.conf
