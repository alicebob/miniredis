// +build int

package main

import (
	"sync"
	"testing"
)

func TestSubscribe(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SUBSCRIBE")

		c.Do("SUBSCRIBE", "foo")
		c.Do("UNSUBSCRIBE")

		c.Do("SUBSCRIBE", "foo")
		c.Do("UNSUBSCRIBE", "foo")

		c.Do("SUBSCRIBE", "foo", "bar")
		c.Receive()
		c.Do("UNSUBSCRIBE", "foo", "bar")
		c.Receive()

		c.Do("SUBSCRIBE", "-1")
		c.Do("UNSUBSCRIBE", "-1")

		c.Do("UNSUBSCRIBE")
	})
}

func TestPsubscribe(t *testing.T) {
	testRaw2(t, func(c1, c2 *client) {
		c1.Do("PSUBSCRIBE")

		c1.Do("PSUBSCRIBE", "foo")
		c2.Do("PUBLISH", "foo", "hi")
		c1.Receive()
		c1.Do("PUNSUBSCRIBE")

		c1.Do("PSUBSCRIBE", "foo")
		c2.Do("PUBLISH", "foo", "hi2")
		c1.Receive()
		c1.Do("PUNSUBSCRIBE", "foo")

		c1.Do("PSUBSCRIBE", "foo", "bar")
		c1.Receive()
		c2.Do("PUBLISH", "foo", "hi3")
		c1.Receive()
		c1.Do("PUNSUBSCRIBE", "foo", "bar")
		c1.Receive()

		c1.Do("PSUBSCRIBE", "f?o")
		c2.Do("PUBLISH", "foo", "hi4")
		c1.Receive()
		c1.Do("PUNSUBSCRIBE", "f?o")

		c1.Do("PSUBSCRIBE", "f*o")
		c2.Do("PUBLISH", "foo", "hi5")
		c1.Receive()
		c1.Do("PUNSUBSCRIBE", "f*o")

		c1.Do("PSUBSCRIBE", "f[oO]o")
		c2.Do("PUBLISH", "foo", "hi6")
		c1.Receive()
		c1.Do("PUNSUBSCRIBE", "f[oO]o")

		c1.Do("PSUBSCRIBE", `f\?o`)
		c2.Do("PUBLISH", "f?o", "hi7")
		c1.Receive()
		c1.Do("PUNSUBSCRIBE", `f\?o`)

		c1.Do("PSUBSCRIBE", `f\*o`)
		c2.Do("PUBLISH", "f*o", "hi8")
		c1.Receive()
		c1.Do("PUNSUBSCRIBE", `f\*o`)

		c1.Do("PSUBSCRIBE", "f\\[oO]o")
		c2.Do("PUBLISH", "f[oO]o", "hi9")
		c1.Receive()
		c1.Do("PUNSUBSCRIBE", "f\\[oO]o")

		c1.Do("PSUBSCRIBE", `f\\oo`)
		c2.Do("PUBLISH", `f\\oo`, "hi10")
		c1.Do("PUNSUBSCRIBE", `f\\oo`)

		c1.Do("PSUBSCRIBE", "-1")
		c2.Do("PUBLISH", "foo", "hi11")
		c1.Do("PUNSUBSCRIBE", "-1")
	})

	testRaw2(t, func(c1, c2 *client) {
		c1.Do("PSUBSCRIBE", "news*")
		c2.Do("PUBLISH", "news", "fire!")
		c1.Receive()
	})

	testRaw2(t, func(c1, c2 *client) {
		c1.Do("PSUBSCRIBE", "news") // no pattern
		c2.Do("PUBLISH", "news", "fire!")
		c1.Receive()
	})

	testRaw(t, func(c *client) {
		c.Do("PUNSUBSCRIBE")
		c.Do("PUNSUBSCRIBE")
	})
}

func TestPublish(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("PUBLISH")
		c.Do("PUBLISH", "foo")
		c.Do("PUBLISH", "foo", "bar")
		c.Do("PUBLISH", "foo", "bar", "deadbeef")
		c.Do("PUBLISH", "-1", "-2")
	})
}

func TestPubSub(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("PUBSUB")
		c.Do("PUBSUB", "FOO")

		c.Do("PUBSUB", "CHANNELS")
		c.Do("PUBSUB", "CHANNELS", "foo")
		c.Do("PUBSUB", "CHANNELS", "foo", "bar")
		c.Do("PUBSUB", "CHANNELS", "f?o")
		c.Do("PUBSUB", "CHANNELS", "f*o")
		c.Do("PUBSUB", "CHANNELS", "f[oO]o")
		c.Do("PUBSUB", "CHANNELS", "f\\?o")
		c.Do("PUBSUB", "CHANNELS", "f\\*o")
		c.Do("PUBSUB", "CHANNELS", "f\\[oO]o")
		c.Do("PUBSUB", "CHANNELS", "f\\\\oo")
		c.Do("PUBSUB", "CHANNELS", "-1")

		c.Do("PUBSUB", "NUMSUB")
		c.Do("PUBSUB", "NUMSUB", "foo")
		c.Do("PUBSUB", "NUMSUB", "foo", "bar")
		c.Do("PUBSUB", "NUMSUB", "-1")

		c.Do("PUBSUB", "NUMPAT")
		c.Do("PUBSUB", "NUMPAT", "foo")
	})
}

func TestPubsubFull(t *testing.T) {
	testRaw2(t, func(c1, c2 *client) {
		c1.Do("SUBSCRIBE", "news", "sport")
		c1.Receive()
		c2.Do("PUBLISH", "news", "revolution!")
		c2.Do("PUBLISH", "news", "alien invasion!")
		c2.Do("PUBLISH", "sport", "lady biked too fast")
		c2.Do("PUBLISH", "gossip", "man bites dog")
		c1.Receive()
		c1.Receive()
		c1.Receive()
		c1.Do("UNSUBSCRIBE", "news", "sport")
		c1.Receive()
	})

	testRESP3Pair(t, func(c1, c2 *client) {
		c1.Do("SUBSCRIBE", "news", "sport")
		c1.Receive()
		c2.Do("PUBLISH", "news", "fire!")
		c1.Receive()
		c1.Do("UNSUBSCRIBE", "news", "sport")
		c1.Receive()
	})
}

func TestPubsubMulti(t *testing.T) {
	var wg1 sync.WaitGroup
	wg1.Add(2)
	testMulti(t,
		func(c *client) {
			c.Do("SUBSCRIBE", "news", "sport")
			c.Receive()
			wg1.Done()
			c.Receive()
			c.Receive()
			c.Receive()
			c.Do("UNSUBSCRIBE", "news", "sport")
			c.Receive()
		},
		func(c *client) {
			c.Do("SUBSCRIBE", "sport")
			wg1.Done()
			c.Receive()
			c.Do("UNSUBSCRIBE", "sport")
		},
		func(c *client) {
			wg1.Wait()
			c.Do("PUBLISH", "news", "revolution!")
			c.Do("PUBLISH", "news", "alien invasion!")
			c.Do("PUBLISH", "sport", "lady biked too fast")
		},
	)
}

func TestPubsubSelect(t *testing.T) {
	testRaw2(t, func(c1, c2 *client) {
		c1.Do("SUBSCRIBE", "news", "sport")
		c1.Receive()
		c2.Do("SELECT", "3")
		c2.Do("PUBLISH", "news", "revolution!")
		c1.Receive()
	})
}

func TestPubsubMode(t *testing.T) {
	// most commands aren't allowed in publish mode
	testRaw(t, func(c *client) {
		c.Do("SUBSCRIBE", "news", "sport")
		c.Receive()
		c.Do("PING")
		c.Do("PING", "foo")
		c.Do("ECHO", "foo")
		c.Do("HGET", "foo", "bar")
		c.Do("SET", "foo", "bar")
		c.Do("QUIT")
	})

	testRaw(t, func(c *client) {
		c.Do("SUBSCRIBE", "news")
		// failWith(e, "PING"),
		// failWith(e, "PSUBSCRIBE"),
		// failWith(e, "PUNSUBSCRIBE"),
		// failWith(e, "QUIT"),
		// failWith(e, "SUBSCRIBE"),
		// failWith(e, "UNSUBSCRIBE"),

		c.Do("APPEND", "foo", "foo")
		c.Do("AUTH", "foo")
		c.Do("BITCOUNT", "foo")
		c.Do("BITOP", "OR", "foo", "bar")
		c.Do("BITPOS", "foo", "0")
		c.Do("BLPOP", "key", "1")
		c.Do("BRPOP", "key", "1")
		c.Do("BRPOPLPUSH", "foo", "bar", "1")
		c.Do("DBSIZE")
		c.Do("DECR", "foo")
		c.Do("DECRBY", "foo", "3")
		c.Do("DEL", "foo")
		c.Do("DISCARD")
		c.Do("ECHO", "foo")
		c.Do("EVAL", "foo", "{}")
		c.Do("EVALSHA", "foo", "{}")
		c.Do("EXEC")
		c.Do("EXISTS", "foo")
		c.Do("EXPIRE", "foo", "12")
		c.Do("EXPIREAT", "foo", "12")
		c.Do("FLUSHALL")
		c.Do("FLUSHDB")
		c.Do("GET", "foo")
		c.Do("GETBIT", "foo", "12")
		c.Do("GETRANGE", "foo", "12", "12")
		c.Do("GETSET", "foo", "bar")
		c.Do("HDEL", "foo", "bar")
		c.Do("HEXISTS", "foo", "bar")
		c.Do("HGET", "foo", "bar")
		c.Do("HGETALL", "foo")
		c.Do("HINCRBY", "foo", "bar", "12")
		c.Do("HINCRBYFLOAT", "foo", "bar", "12.34")
		c.Do("HKEYS", "foo")
		c.Do("HLEN", "foo")
		c.Do("HMGET", "foo", "bar")
		c.Do("HMSET", "foo", "bar", "baz")
		c.Do("HSCAN", "foo", "0")
		c.Do("HSET", "foo", "bar", "baz")
		c.Do("HSETNX", "foo", "bar", "baz")
		c.Do("HVALS", "foo")
		c.Do("INCR", "foo")
		c.Do("INCRBY", "foo", "12")
		c.Do("INCRBYFLOAT", "foo", "12.34")
		c.Do("KEYS", "*")
		c.Do("LINDEX", "foo", "0")
		c.Do("LINSERT", "foo", "after", "bar", "0")
		c.Do("LLEN", "foo")
		c.Do("LPOP", "foo")
		c.Do("LPUSH", "foo", "bar")
		c.Do("LPUSHX", "foo", "bar")
		c.Do("LRANGE", "foo", "1", "1")
		c.Do("LREM", "foo", "0", "bar")
		c.Do("LSET", "foo", "0", "bar")
		c.Do("LTRIM", "foo", "0", "0")
		c.Do("MGET", "foo", "bar")
		c.Do("MOVE", "foo", "bar")
		c.Do("MSET", "foo", "bar")
		c.Do("MSETNX", "foo", "bar")
		c.Do("MULTI")
		c.Do("PERSIST", "foo")
		c.Do("PEXPIRE", "foo", "12")
		c.Do("PEXPIREAT", "foo", "12")
		c.Do("PSETEX", "foo", "12", "bar")
		c.Do("PTTL", "foo")
		c.Do("PUBLISH", "foo", "bar")
		c.Do("PUBSUB", "CHANNELS")
		c.Do("RANDOMKEY")
		c.Do("RENAME", "foo", "bar")
		c.Do("RENAMENX", "foo", "bar")
		c.Do("RPOP", "foo")
		c.Do("RPOPLPUSH", "foo", "bar")
		c.Do("RPUSH", "foo", "bar")
		c.Do("RPUSHX", "foo", "bar")
		c.Do("SADD", "foo", "bar")
		c.Do("SCAN", "0")
		c.Do("SCARD", "foo")
		c.Do("SCRIPT", "FLUSH")
		c.Do("SDIFF", "foo")
		c.Do("SDIFFSTORE", "foo", "bar")
		c.Do("SELECT", "12")
		c.Do("SET", "foo", "bar")
		c.Do("SETBIT", "foo", "0", "1")
		c.Do("SETEX", "foo", "12", "bar")
		c.Do("SETNX", "foo", "bar")
		c.Do("SETRANGE", "foo", "0", "bar")
		c.Do("SINTER", "foo", "bar")
		c.Do("SINTERSTORE", "foo", "bar", "baz")
		c.Do("SISMEMBER", "foo", "bar")
		c.Do("SMEMBERS", "foo")
		c.Do("SMOVE", "foo", "bar", "baz")
		c.Do("SPOP", "foo")
		c.Do("SRANDMEMBER", "foo")
		c.Do("SREM", "foo", "bar", "baz")
		c.Do("SSCAN", "foo", "0")
		c.Do("STRLEN", "foo")
		c.Do("SUNION", "foo", "bar")
		c.Do("SUNIONSTORE", "foo", "bar", "baz")
		c.Do("TIME")
		c.Do("TTL", "foo")
		c.Do("TYPE", "foo")
		c.Do("UNWATCH")
		c.Do("WATCH", "foo")
		c.Do("ZADD", "foo", "INCR", "1", "bar")
		c.Do("ZCARD", "foo")
		c.Do("ZCOUNT", "foo", "0", "1")
		c.Do("ZINCRBY", "foo", "bar", "12")
		c.Do("ZINTERSTORE", "foo", "1", "bar")
		c.Do("ZLEXCOUNT", "foo", "-", "+")
		c.Do("ZRANGE", "foo", "0", "-1")
		c.Do("ZRANGEBYLEX", "foo", "-", "+")
		c.Do("ZRANGEBYSCORE", "foo", "0", "1")
		c.Do("ZRANK", "foo", "bar")
		c.Do("ZREM", "foo", "bar")
		c.Do("ZREMRANGEBYLEX", "foo", "-", "+")
		c.Do("ZREMRANGEBYRANK", "foo", "0", "1")
		c.Do("ZREMRANGEBYSCORE", "foo", "0", "1")
		c.Do("ZREVRANGE", "foo", "0", "-1")
		c.Do("ZREVRANGEBYLEX", "foo", "+", "-")
		c.Do("ZREVRANGEBYSCORE", "foo", "0", "1")
		c.Do("ZREVRANK", "foo", "bar")
		c.Do("ZSCAN", "foo", "0")
		c.Do("ZSCORE", "foo", "bar")
		c.Do("ZUNIONSTORE", "foo", "1", "bar")
	})
}

func TestSubscriptions(t *testing.T) {
	testRaw2(t, func(c1, c2 *client) {
		c1.Do("SUBSCRIBE", "foo", "bar", "foo")
		c2.Do("PUBSUB", "NUMSUB")
		c1.Do("UNSUBSCRIBE", "bar", "bar", "bar")
		c2.Do("PUBSUB", "NUMSUB")
	})
}

func TestPubsubUnsub(t *testing.T) {
	testRaw2(t, func(c1, c2 *client) {
		c1.Do("SUBSCRIBE", "news", "sport")
		c1.Receive()
		c2.DoSorted("PUBSUB", "CHANNELS")
		c1.Do("QUIT")
		c2.DoSorted("PUBSUB", "CHANNELS")
	})
}

func TestPubsubTx(t *testing.T) {
	// publish is in a tx
	testRaw2(t, func(c1, c2 *client) {
		c1.Do("SUBSCRIBE", "foo")
		c2.Do("MULTI")
		c2.Do("PUBSUB", "CHANNELS")
		c2.Do("PUBLISH", "foo", "hello one")
		c2.Do("GET")
		c2.Do("PUBLISH", "foo", "hello two")
		c2.Do("EXEC")

		c2.Do("PUBLISH", "foo", "post tx")
		c1.Receive()
	})

	// SUBSCRIBE is in a tx
	testRaw2(t, func(c1, c2 *client) {
		c1.Do("MULTI")
		c1.Do("SUBSCRIBE", "foo")
		c2.Do("PUBSUB", "CHANNELS")
		c1.Do("EXEC")
		c2.Do("PUBSUB", "CHANNELS")

		c1.Do("MULTI") // we're in SUBSCRIBE mode
	})

	// DISCARDing a tx prevents from entering publish mode
	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("SUBSCRIBE", "foo")
		c.Do("DISCARD")
		c.Do("PUBSUB", "CHANNELS")
	})

	// UNSUBSCRIBE is in a tx
	testRaw2(t, func(c1, c2 *client) {
		c1.Do("MULTI")
		c1.Do("SUBSCRIBE", "foo")
		c1.Do("UNSUBSCRIBE", "foo")
		c2.Do("PUBSUB", "CHANNELS")
		c1.Do("EXEC")
		c2.Do("PUBSUB", "CHANNELS")
		c1.Do("PUBSUB", "CHANNELS")
	})

	// PSUBSCRIBE is in a tx
	testRaw2(t, func(c1, c2 *client) {
		c1.Do("MULTI")
		c1.Do("PSUBSCRIBE", "foo")
		c2.Do("PUBSUB", "NUMPAT")
		c1.Do("EXEC")
		c2.Do("PUBSUB", "NUMPAT")

		c1.Do("MULTI") // we're in SUBSCRIBE mode
	})

	// PUNSUBSCRIBE is in a tx
	testRaw2(t, func(c1, c2 *client) {
		c1.Do("MULTI")
		c1.Do("PSUBSCRIBE", "foo")
		c1.Do("PUNSUBSCRIBE", "foo")
		c2.Do("PUBSUB", "NUMPAT")
		c1.Do("EXEC")
		c2.Do("PUBSUB", "NUMPAT")
		c1.Do("PUBSUB", "NUMPAT")
	})
}
