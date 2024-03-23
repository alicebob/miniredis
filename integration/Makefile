.PHONY: int
int:
	INT=1 go test

redis_src/redis-server: get_redis.sh
	./get_redis.sh

.PHONY: clean
clean:
	rm -rf \
		redis_src \
		dump.rdb \
		nodes.conf
