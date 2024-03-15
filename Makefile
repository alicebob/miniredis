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

.PHONY: int
int: integration/redis_src/redis-server ### Run integration tests
	INT=1 go test ./integration/...

integration/redis_src/redis-server: integration/get_redis.sh ### Download and build redis if not available or in wrong version
	./integration/get_redis.sh

.PHONY: clean
clean: ### Cleanup integration test files (including built redis binary)
	rm -rf \
		integration/redis_src \
		integration/dump.rdb \
		integration/nodes.conf
