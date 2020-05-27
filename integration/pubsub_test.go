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
		c2 <- succ("PUBLISH", "foo", "hi2")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "foo")

		c1 <- succ("PSUBSCRIBE", "foo", "bar")
		c1 <- receive()
		c2 <- succ("PUBLISH", "foo", "hi3")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "foo", "bar")
		c1 <- receive()

		c1 <- succ("PSUBSCRIBE", "f?o")
		c2 <- succ("PUBLISH", "foo", "hi4")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "f?o")

		c1 <- succ("PSUBSCRIBE", "f*o")
		c2 <- succ("PUBLISH", "foo", "hi5")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "f*o")

		c1 <- succ("PSUBSCRIBE", "f[oO]o")
		c2 <- succ("PUBLISH", "foo", "hi6")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "f[oO]o")

		c1 <- succ("PSUBSCRIBE", `f\?o`)
		c2 <- succ("PUBLISH", "f?o", "hi7")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", `f\?o`)

		c1 <- succ("PSUBSCRIBE", `f\*o`)
		c2 <- succ("PUBLISH", "f*o", "hi8")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", `f\*o`)

		c1 <- succ("PSUBSCRIBE", "f\\[oO]o")
		c2 <- succ("PUBLISH", "f[oO]o", "hi9")
		c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", "f\\[oO]o")

		c1 <- succ("PSUBSCRIBE", `f\\oo`)
		c2 <- succ("PUBLISH", `f\\oo`, "hi10")
		// c1 <- receive()
		c1 <- succ("PUNSUBSCRIBE", `f\\oo`)

		c1 <- succ("PSUBSCRIBE", -1)
		c2 <- succ("PUBLISH", "foo", "hi11")
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

	cbs := []command{
		succ("SUBSCRIBE", "news"),
		// failWith(e, "PING"),
		// failWith(e, "PSUBSCRIBE"),
		// failWith(e, "PUNSUBSCRIBE"),
		// failWith(e, "QUIT"),
		// failWith(e, "SUBSCRIBE"),
		// failWith(e, "UNSUBSCRIBE"),

		fail("APPEND", "foo", "foo"),
		fail("AUTH", "foo"),
		fail("BITCOUNT", "foo"),
		fail("BITOP", "OR", "foo", "bar"),
		fail("BITPOS", "foo", 0),
		fail("BLPOP", "key", 1),
		fail("BRPOP", "key", 1),
		fail("BRPOPLPUSH", "foo", "bar", 1),
		fail("DBSIZE"),
		fail("DECR", "foo"),
		fail("DECRBY", "foo", 3),
		fail("DEL", "foo"),
		fail("DISCARD"),
		fail("ECHO", "foo"),
		fail("EVAL", "foo", "{}"),
		fail("EVALSHA", "foo", "{}"),
		fail("EXEC"),
		fail("EXISTS", "foo"),
		fail("EXPIRE", "foo", 12),
		fail("EXPIREAT", "foo", 12),
		fail("FLUSHALL"),
		fail("FLUSHDB"),
		fail("GET", "foo"),
		fail("GETBIT", "foo", 12),
		fail("GETRANGE", "foo", 12, 12),
		fail("GETSET", "foo", "bar"),
		fail("HDEL", "foo", "bar"),
		fail("HEXISTS", "foo", "bar"),
		fail("HGET", "foo", "bar"),
		fail("HGETALL", "foo"),
		fail("HINCRBY", "foo", "bar", 12),
		fail("HINCRBYFLOAT", "foo", "bar", 12.34),
		fail("HKEYS", "foo"),
		fail("HLEN", "foo"),
		fail("HMGET", "foo", "bar"),
		fail("HMSET", "foo", "bar", "baz"),
		fail("HSCAN", "foo", 0),
		fail("HSET", "foo", "bar", "baz"),
		fail("HSETNX", "foo", "bar", "baz"),
		fail("HVALS", "foo"),
		fail("INCR", "foo"),
		fail("INCRBY", "foo", 12),
		fail("INCRBYFLOAT", "foo", 12.34),
		fail("KEYS", "*"),
		fail("LINDEX", "foo", 0),
		fail("LINSERT", "foo", "after", "bar", 0),
		fail("LLEN", "foo"),
		fail("LPOP", "foo"),
		fail("LPUSH", "foo", "bar"),
		fail("LPUSHX", "foo", "bar"),
		fail("LRANGE", "foo", 1, 1),
		fail("LREM", "foo", 0, "bar"),
		fail("LSET", "foo", 0, "bar"),
		fail("LTRIM", "foo", 0, 0),
		fail("MGET", "foo", "bar"),
		fail("MOVE", "foo", "bar"),
		fail("MSET", "foo", "bar"),
		fail("MSETNX", "foo", "bar"),
		fail("MULTI"),
		fail("PERSIST", "foo"),
		fail("PEXPIRE", "foo", 12),
		fail("PEXPIREAT", "foo", 12),
		fail("PSETEX", "foo", 12, "bar"),
		fail("PTTL", "foo"),
		fail("PUBLISH", "foo", "bar"),
		fail("PUBSUB", "CHANNELS"),
		fail("RANDOMKEY"),
		fail("RENAME", "foo", "bar"),
		fail("RENAMENX", "foo", "bar"),
		fail("RPOP", "foo"),
		fail("RPOPLPUSH", "foo", "bar"),
		fail("RPUSH", "foo", "bar"),
		fail("RPUSHX", "foo", "bar"),
		fail("SADD", "foo", "bar"),
		fail("SCAN", 0),
		fail("SCARD", "foo"),
		fail("SCRIPT", "FLUSH"),
		fail("SDIFF", "foo"),
		fail("SDIFFSTORE", "foo", "bar"),
		fail("SELECT", 12),
		fail("SET", "foo", "bar"),
		fail("SETBIT", "foo", 0, 1),
		fail("SETEX", "foo", 12, "bar"),
		fail("SETNX", "foo", "bar"),
		fail("SETRANGE", "foo", 0, "bar"),
		fail("SINTER", "foo", "bar"),
		fail("SINTERSTORE", "foo", "bar", "baz"),
		fail("SISMEMBER", "foo", "bar"),
		fail("SMEMBERS", "foo"),
		fail("SMOVE", "foo", "bar", "baz"),
		fail("SPOP", "foo"),
		fail("SRANDMEMBER", "foo"),
		fail("SREM", "foo", "bar", "baz"),
		fail("SSCAN", "foo", 0),
		fail("STRLEN", "foo"),
		fail("SUNION", "foo", "bar"),
		fail("SUNIONSTORE", "foo", "bar", "baz"),
		fail("TIME"),
		fail("TTL", "foo"),
		fail("TYPE", "foo"),
		fail("UNWATCH"),
		fail("WATCH", "foo"),
		fail("ZADD", "foo", "INCR", 1, "bar"),
		fail("ZCARD", "foo"),
		fail("ZCOUNT", "foo", 0, 1),
		fail("ZINCRBY", "foo", "bar", 12),
		fail("ZINTERSTORE", "foo", 1, "bar"),
		fail("ZLEXCOUNT", "foo", "-", "+"),
		fail("ZRANGE", "foo", 0, -1),
		fail("ZRANGEBYLEX", "foo", "-", "+"),
		fail("ZRANGEBYSCORE", "foo", 0, 1),
		fail("ZRANK", "foo", "bar"),
		fail("ZREM", "foo", "bar"),
		fail("ZREMRANGEBYLEX", "foo", "-", "+"),
		fail("ZREMRANGEBYRANK", "foo", 0, 1),
		fail("ZREMRANGEBYSCORE", "foo", 0, 1),
		fail("ZREVRANGE", "foo", 0, -1),
		fail("ZREVRANGEBYLEX", "foo", "+", "-"),
		fail("ZREVRANGEBYSCORE", "foo", 0, 1),
		fail("ZREVRANK", "foo", "bar"),
		fail("ZSCAN", "foo", 0),
		fail("ZSCORE", "foo", "bar"),
		fail("ZUNIONSTORE", "foo", 1, "bar"),
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
