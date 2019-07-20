// +build int

package main

import (
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestSubscribe(t *testing.T) {
	testCommands(t,
		fail("SUBSCRIBE"),

		succ("SUBSCRIBE", "foo"),
		succ("UNSUBSCRIBE"),

		succ("SUBSCRIBE", "foo"),
		succ("UNSUBSCRIBE", "foo"),

		succ("SUBSCRIBE", "foo", "bar"),
		succ("UNSUBSCRIBE", "foo", "bar"),

		succ("SUBSCRIBE", -1),
		succ("UNSUBSCRIBE", -1),
	)
}

func TestPsubscribe(t *testing.T) {
	testClients2(t, func(c1, c2 chan<- command) {
		c1 <- fail("PSUBSCRIBE")

		c1 <- succ("PSUBSCRIBE", "foo")
		c2 <- succ("PUBLISH", "foo", "hi")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE")

		c1 <- succ("PSUBSCRIBE", "foo")
		c2 <- succ("PUBLISH", "foo", "hi")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "foo")

		c1 <- succ("PSUBSCRIBE", "foo", "bar")
		c2 <- succ("PUBLISH", "foo", "hi")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "foo", "bar")

		c1 <- succ("PSUBSCRIBE", "f?o")
		c2 <- succ("PUBLISH", "foo", "hi")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "f?o")

		c1 <- succ("PSUBSCRIBE", "f*o")
		c2 <- succ("PUBLISH", "foo", "hi")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "f*o")

		c1 <- succ("PSUBSCRIBE", "f[oO]o")
		c2 <- succ("PUBLISH", "foo", "hi")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "f[oO]o")

		c1 <- succ("PSUBSCRIBE", "f\\?o")
		c2 <- succ("PUBLISH", "f?o", "hi")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "f\\?o")

		c1 <- succ("PSUBSCRIBE", "f\\*o")
		c2 <- succ("PUBLISH", "f*o", "hi")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "f\\*o")

		c1 <- succ("PSUBSCRIBE", "f\\[oO]o")
		c2 <- succ("PUBLISH", "f[oO]o", "hi")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "f\\[oO]o")

		c1 <- succ("PSUBSCRIBE", "f\\\\oo")
		c2 <- succ("PUBLISH", "f\\\\oo", "hi")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "f\\\\oo")

		c1 <- succ("PSUBSCRIBE", -1)
		c2 <- succ("PUBLISH", "foo", "hi")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", -1)
	})

	testClients2(t, func(c1, c2 chan<- command) {
		c1 <- succ("PSUBSCRIBE", "news*")
		c2 <- succ("PUBLISH", "news", "fire!")
		c1 <- receive()
	})

	testClients2(t, func(c1, c2 chan<- command) {
		c1 <- succ("PSUBSCRIBE", "news") // no pattern
		c2 <- succ("PUBLISH", "news", "fire!")
		c1 <- receive()
	})
}

func TestPublish(t *testing.T) {
	testCommands(t,
		fail("PUBLISH"),
		fail("PUBLISH", "foo"),
		succ("PUBLISH", "foo", "bar"),
		fail("PUBLISH", "foo", "bar", "deadbeef"),
		succ("PUBLISH", -1, -2),
	)
}

func TestPubSub(t *testing.T) {
	testCommands(t,
		fail("PUBSUB"),
		fail("PUBSUB", "FOO"),

		succ("PUBSUB", "CHANNELS"),
		succ("PUBSUB", "CHANNELS", "foo"),
		fail("PUBSUB", "CHANNELS", "foo", "bar"),
		succ("PUBSUB", "CHANNELS", "f?o"),
		succ("PUBSUB", "CHANNELS", "f*o"),
		succ("PUBSUB", "CHANNELS", "f[oO]o"),
		succ("PUBSUB", "CHANNELS", "f\\?o"),
		succ("PUBSUB", "CHANNELS", "f\\*o"),
		succ("PUBSUB", "CHANNELS", "f\\[oO]o"),
		succ("PUBSUB", "CHANNELS", "f\\\\oo"),
		succ("PUBSUB", "CHANNELS", -1),

		succ("PUBSUB", "NUMSUB"),
		succ("PUBSUB", "NUMSUB", "foo"),
		succ("PUBSUB", "NUMSUB", "foo", "bar"),
		succ("PUBSUB", "NUMSUB", -1),

		succ("PUBSUB", "NUMPAT"),
		fail("PUBSUB", "NUMPAT", "foo"),
	)
}

func TestPubsubFull(t *testing.T) {
	testClients2(t, func(c1, c2 chan<- command) {
		c1 <- succ("SUBSCRIBE", "news", "sport")
		c1 <- receive()
		c2 <- succ("PUBLISH", "news", "revolution!")
		c2 <- succ("PUBLISH", "news", "alien invasion!")
		c2 <- succ("PUBLISH", "sport", "lady biked too fast")
		c2 <- succ("PUBLISH", "gossip", "man bites dog")
		c1 <- receive()
		c1 <- receive()
		c1 <- receive()
		c1 <- succ("UNSUBSCRIBE", "news", "sport")
		c1 <- receive()
	})
}

func TestPubsubMulti(t *testing.T) {
	var wg1 sync.WaitGroup
	wg1.Add(2)
	testMultiCommands(t,
		func(r chan<- command, _ *miniredis.Miniredis) {
			r <- succ("SUBSCRIBE", "news", "sport")
			r <- receive()
			wg1.Done()
			r <- receive()
			r <- receive()
			r <- receive()
			r <- succ("UNSUBSCRIBE", "news", "sport")
			r <- receive()
		},
		func(r chan<- command, _ *miniredis.Miniredis) {
			r <- succ("SUBSCRIBE", "sport")
			wg1.Done()
			r <- receive()
			r <- succ("UNSUBSCRIBE", "sport")
		},
		func(r chan<- command, _ *miniredis.Miniredis) {
			wg1.Wait()
			r <- succ("PUBLISH", "news", "revolution!")
			r <- succ("PUBLISH", "news", "alien invasion!")
			r <- succ("PUBLISH", "sport", "lady biked too fast")
		},
	)
}

func TestPubsubSelect(t *testing.T) {
	testClients2(t, func(r1, r2 chan<- command) {
		r1 <- succ("SUBSCRIBE", "news", "sport")
		r1 <- receive()
		r2 <- succ("SELECT", 3)
		r2 <- succ("PUBLISH", "news", "revolution!")
		r1 <- receive()
	})
}

func TestPubsubMode(t *testing.T) {
	// most commands aren't allowed in publish mode
	testCommands(t,
		succ("SUBSCRIBE", "news", "sport"),
		receive(),
		succ("PING"),
		succ("PING", "foo"),
		fail("ECHO", "foo"),
		fail("HGET", "foo", "bar"),
		fail("SET", "foo", "bar"),
		succ("QUIT"),
	)

	e := "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context"
	cbs := []command{
		succ("SUBSCRIBE", "news"),
		// failWith(e, "PING"),
		// failWith(e, "PSUBSCRIBE"),
		// failWith(e, "PUNSUBSCRIBE"),
		// failWith(e, "QUIT"),
		// failWith(e, "SUBSCRIBE"),
		// failWith(e, "UNSUBSCRIBE"),

		failWith(e, "APPEND", "foo", "foo"),
		failWith(e, "AUTH", "foo"),
		failWith(e, "BITCOUNT", "foo"),
		failWith(e, "BITOP", "OR", "foo", "bar"),
		failWith(e, "BITPOS", "foo", 0),
		failWith(e, "BLPOP", "key", 1),
		failWith(e, "BRPOP", "key", 1),
		failWith(e, "BRPOPLPUSH", "foo", "bar", 1),
		failWith(e, "DBSIZE"),
		failWith(e, "DECR", "foo"),
		failWith(e, "DECRBY", "foo", 3),
		failWith(e, "DEL", "foo"),
		failWith(e, "DISCARD"),
		failWith(e, "ECHO", "foo"),
		failWith(e, "EVAL", "foo", "{}"),
		failWith(e, "EVALSHA", "foo", "{}"),
		failWith(e, "EXEC"),
		failWith(e, "EXISTS", "foo"),
		failWith(e, "EXPIRE", "foo", 12),
		failWith(e, "EXPIREAT", "foo", 12),
		failWith(e, "FLUSHALL"),
		failWith(e, "FLUSHDB"),
		failWith(e, "GET", "foo"),
		failWith(e, "GETBIT", "foo", 12),
		failWith(e, "GETRANGE", "foo", 12, 12),
		failWith(e, "GETSET", "foo", "bar"),
		failWith(e, "HDEL", "foo", "bar"),
		failWith(e, "HEXISTS", "foo", "bar"),
		failWith(e, "HGET", "foo", "bar"),
		failWith(e, "HGETALL", "foo"),
		failWith(e, "HINCRBY", "foo", "bar", 12),
		failWith(e, "HINCRBYFLOAT", "foo", "bar", 12.34),
		failWith(e, "HKEYS", "foo"),
		failWith(e, "HLEN", "foo"),
		failWith(e, "HMGET", "foo", "bar"),
		failWith(e, "HMSET", "foo", "bar", "baz"),
		failWith(e, "HSCAN", "foo", 0),
		failWith(e, "HSET", "foo", "bar", "baz"),
		failWith(e, "HSETNX", "foo", "bar", "baz"),
		failWith(e, "HVALS", "foo"),
		failWith(e, "INCR", "foo"),
		failWith(e, "INCRBY", "foo", 12),
		failWith(e, "INCRBYFLOAT", "foo", 12.34),
		failWith(e, "KEYS", "*"),
		failWith(e, "LINDEX", "foo", 0),
		failWith(e, "LINSERT", "foo", "after", "bar", 0),
		failWith(e, "LLEN", "foo"),
		failWith(e, "LPOP", "foo"),
		failWith(e, "LPUSH", "foo", "bar"),
		failWith(e, "LPUSHX", "foo", "bar"),
		failWith(e, "LRANGE", "foo", 1, 1),
		failWith(e, "LREM", "foo", 0, "bar"),
		failWith(e, "LSET", "foo", 0, "bar"),
		failWith(e, "LTRIM", "foo", 0, 0),
		failWith(e, "MGET", "foo", "bar"),
		failWith(e, "MOVE", "foo", "bar"),
		failWith(e, "MSET", "foo", "bar"),
		failWith(e, "MSETNX", "foo", "bar"),
		failWith(e, "MULTI"),
		failWith(e, "PERSIST", "foo"),
		failWith(e, "PEXPIRE", "foo", 12),
		failWith(e, "PEXPIREAT", "foo", 12),
		failWith(e, "PSETEX", "foo", 12, "bar"),
		failWith(e, "PTTL", "foo"),
		failWith(e, "PUBLISH", "foo", "bar"),
		failWith(e, "PUBSUB", "CHANNELS"),
		failWith(e, "RANDOMKEY"),
		failWith(e, "RENAME", "foo", "bar"),
		failWith(e, "RENAMENX", "foo", "bar"),
		failWith(e, "RPOP", "foo"),
		failWith(e, "RPOPLPUSH", "foo", "bar"),
		failWith(e, "RPUSH", "foo", "bar"),
		failWith(e, "RPUSHX", "foo", "bar"),
		failWith(e, "SADD", "foo", "bar"),
		failWith(e, "SCAN", 0),
		failWith(e, "SCARD", "foo"),
		failWith(e, "SCRIPT", "FLUSH"),
		failWith(e, "SDIFF", "foo"),
		failWith(e, "SDIFFSTORE", "foo", "bar"),
		failWith(e, "SELECT", 12),
		failWith(e, "SET", "foo", "bar"),
		failWith(e, "SETBIT", "foo", 0, 1),
		failWith(e, "SETEX", "foo", 12, "bar"),
		failWith(e, "SETNX", "foo", "bar"),
		failWith(e, "SETRANGE", "foo", 0, "bar"),
		failWith(e, "SINTER", "foo", "bar"),
		failWith(e, "SINTERSTORE", "foo", "bar", "baz"),
		failWith(e, "SISMEMBER", "foo", "bar"),
		failWith(e, "SMEMBERS", "foo"),
		failWith(e, "SMOVE", "foo", "bar", "baz"),
		failWith(e, "SPOP", "foo"),
		failWith(e, "SRANDMEMBER", "foo"),
		failWith(e, "SREM", "foo", "bar", "baz"),
		failWith(e, "SSCAN", "foo", 0),
		failWith(e, "STRLEN", "foo"),
		failWith(e, "SUNION", "foo", "bar"),
		failWith(e, "SUNIONSTORE", "foo", "bar", "baz"),
		failWith(e, "TIME"),
		failWith(e, "TTL", "foo"),
		failWith(e, "TYPE", "foo"),
		failWith(e, "UNWATCH"),
		failWith(e, "WATCH", "foo"),
		failWith(e, "ZADD", "foo", "INCR", 1, "bar"),
		failWith(e, "ZCARD", "foo"),
		failWith(e, "ZCOUNT", "foo", 0, 1),
		failWith(e, "ZINCRBY", "foo", "bar", 12),
		failWith(e, "ZINTERSTORE", "foo", 1, "bar"),
		failWith(e, "ZLEXCOUNT", "foo", "-", "+"),
		failWith(e, "ZRANGE", "foo", 0, -1),
		failWith(e, "ZRANGEBYLEX", "foo", "-", "+"),
		failWith(e, "ZRANGEBYSCORE", "foo", 0, 1),
		failWith(e, "ZRANK", "foo", "bar"),
		failWith(e, "ZREM", "foo", "bar"),
		failWith(e, "ZREMRANGEBYLEX", "foo", "-", "+"),
		failWith(e, "ZREMRANGEBYRANK", "foo", 0, 1),
		failWith(e, "ZREMRANGEBYSCORE", "foo", 0, 1),
		failWith(e, "ZREVRANGE", "foo", 0, -1),
		failWith(e, "ZREVRANGEBYLEX", "foo", "+", "-"),
		failWith(e, "ZREVRANGEBYSCORE", "foo", 0, 1),
		failWith(e, "ZREVRANK", "foo", "bar"),
		failWith(e, "ZSCAN", "foo", 0),
		failWith(e, "ZSCORE", "foo", "bar"),
		failWith(e, "ZUNIONSTORE", "foo", 1, "bar"),
	}
	testCommands(t, cbs...)
}

func TestSubscriptions(t *testing.T) {
	testClients2(t, func(r1, r2 chan<- command) {
		r1 <- succ("SUBSCRIBE", "foo", "bar", "foo")
		r2 <- succ("PUBSUB", "NUMSUB")
		r1 <- succ("UNSUBSCRIBE", "bar", "bar", "bar")
		r2 <- succ("PUBSUB", "NUMSUB")
	})
}

func TestPubsubUnsub(t *testing.T) {
	testClients2(t, func(c1, c2 chan<- command) {
		c1 <- succ("SUBSCRIBE", "news", "sport")
		c1 <- receive()
		c2 <- succSorted("PUBSUB", "CHANNELS")
		c1 <- succ("QUIT")
		c2 <- succSorted("PUBSUB", "CHANNELS")
	})
}

func TestPubsubTx(t *testing.T) {
	// publish is in a tx
	testClients2(t, func(c1, c2 chan<- command) {
		c1 <- succ("SUBSCRIBE", "foo")
		c2 <- succ("MULTI")
		c2 <- succ("PUBSUB", "CHANNELS")
		c2 <- succ("PUBLISH", "foo", "hello one")
		c2 <- fail("GET")
		c2 <- succ("PUBLISH", "foo", "hello two")
		c2 <- fail("EXEC")

		c2 <- succ("PUBLISH", "foo", "post tx")
		c1 <- receive()
	})

	// SUBSCRIBE is in a tx
	testClients2(t, func(c1, c2 chan<- command) {
		c1 <- succ("MULTI")
		c1 <- succ("SUBSCRIBE", "foo")
		c2 <- succ("PUBSUB", "CHANNELS")
		c1 <- succ("EXEC")
		c2 <- succ("PUBSUB", "CHANNELS")

		c1 <- fail("MULTI") // we're in SUBSCRIBE mode
	})

	// DISCARDing a tx prevents from entering publish mode
	testCommands(t,
		succ("MULTI"),
		succ("SUBSCRIBE", "foo"),
		succ("DISCARD"),
		succ("PUBSUB", "CHANNELS"),
	)

	// UNSUBSCRIBE is in a tx
	testClients2(t, func(c1, c2 chan<- command) {
		c1 <- succ("MULTI")
		c1 <- succ("SUBSCRIBE", "foo")
		c1 <- succ("UNSUBSCRIBE", "foo")
		c2 <- succ("PUBSUB", "CHANNELS")
		c1 <- succ("EXEC")
		c2 <- succ("PUBSUB", "CHANNELS")
		c1 <- succ("PUBSUB", "CHANNELS")
	})

	// PSUBSCRIBE is in a tx
	testClients2(t, func(c1, c2 chan<- command) {
		c1 <- succ("MULTI")
		c1 <- succ("PSUBSCRIBE", "foo")
		c2 <- succ("PUBSUB", "NUMPAT")
		c1 <- succ("EXEC")
		c2 <- succ("PUBSUB", "NUMPAT")

		c1 <- fail("MULTI") // we're in SUBSCRIBE mode
	})

	// PUNSUBSCRIBE is in a tx
	testClients2(t, func(c1, c2 chan<- command) {
		c1 <- succ("MULTI")
		c1 <- succ("PSUBSCRIBE", "foo")
		c1 <- succ("PUNSUBSCRIBE", "foo")
		c2 <- succ("PUBSUB", "NUMPAT")
		c1 <- succ("EXEC")
		c2 <- succ("PUBSUB", "NUMPAT")
		c1 <- succ("PUBSUB", "NUMPAT")
	})
}
