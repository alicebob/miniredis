// +build int

package main

// List keys.

import (
	"sync"
	"testing"
	"time"
)

func TestLPush(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("LPUSH", "l", "aap", "noot", "mies")
		c.Do("TYPE", "l")
		c.Do("LPUSH", "l", "more", "keys")
		c.Do("LRANGE", "l", "0", "-1")
		c.Do("LRANGE", "l", "0", "6")
		c.Do("LRANGE", "l", "2", "6")
		c.Do("LRANGE", "l", "-100", "-100")
		c.Do("LRANGE", "nosuch", "2", "6")
		c.Do("LPOP", "l")
		c.Do("LPOP", "l")
		c.Do("LPOP", "l")
		c.Do("LPOP", "l")
		c.Do("LPOP", "l")
		c.Do("LPOP", "l")
		c.Do("EXISTS", "l")
		c.Do("LPOP", "nosuch")

		// failure cases
		c.Do("LPUSH")
		c.Do("LPUSH", "l")
		c.Do("SET", "str", "I am a string")
		c.Do("LPUSH", "str", "noot", "mies")
		c.Do("LRANGE")
		c.Do("LRANGE", "key")
		c.Do("LRANGE", "key", "2")
		c.Do("LRANGE", "key", "2", "6", "toomany")
		c.Do("LRANGE", "key", "noint", "6")
		c.Do("LRANGE", "key", "2", "noint")
		c.Do("LPOP")
		c.Do("LPOP", "key", "args")
	})
}

func TestLPushx(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("LPUSHX", "l", "aap")
		c.Do("EXISTS", "l")
		c.Do("LRANGE", "l", "0", "-1")
		c.Do("LPUSH", "l", "noot")
		c.Do("LPUSHX", "l", "mies")
		c.Do("EXISTS", "l")
		c.Do("LRANGE", "l", "0", "-1")
		c.Do("LPUSHX", "l", "even", "more", "arguments")

		// failure cases
		c.Do("LPUSHX")
		c.Do("LPUSHX", "l")
		c.Do("SET", "str", "I am a string")
		c.Do("LPUSHX", "str", "mies")
	})
}

func TestRPush(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("RPUSH", "l", "aap", "noot", "mies")
		c.Do("TYPE", "l")
		c.Do("RPUSH", "l", "more", "keys")
		c.Do("LRANGE", "l", "0", "-1")
		c.Do("LRANGE", "l", "0", "6")
		c.Do("LRANGE", "l", "2", "6")
		c.Do("RPOP", "l")
		c.Do("RPOP", "l")
		c.Do("RPOP", "l")
		c.Do("RPOP", "l")
		c.Do("RPOP", "l")
		c.Do("RPOP", "l")
		c.Do("EXISTS", "l")
		c.Do("RPOP", "nosuch")

		// failure cases
		c.Do("RPUSH")
		c.Do("RPUSH", "l")
		c.Do("SET", "str", "I am a string")
		c.Do("RPUSH", "str", "noot", "mies")
		c.Do("RPOP")
		c.Do("RPOP", "key", "args")
	})
}

func TestLinxed(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("RPUSH", "l", "aap", "noot", "mies")
		c.Do("LINDEX", "l", "0")
		c.Do("LINDEX", "l", "1")
		c.Do("LINDEX", "l", "2")
		c.Do("LINDEX", "l", "3")
		c.Do("LINDEX", "l", "4")
		c.Do("LINDEX", "l", "44444")
		c.Do("LINDEX", "l", "-0")
		c.Do("LINDEX", "l", "-1")
		c.Do("LINDEX", "l", "-2")
		c.Do("LINDEX", "l", "-3")
		c.Do("LINDEX", "l", "-4")
		c.Do("LINDEX", "l", "-4000")

		// failure cases
		c.Do("LINDEX")
		c.Do("LINDEX", "l")
		c.Do("SET", "str", "I am a string")
		c.Do("LINDEX", "str", "1")
		c.Do("LINDEX", "l", "noint")
		c.Do("LINDEX", "l", "1", "too many")
	})
}

func TestLlen(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("RPUSH", "l", "aap", "noot", "mies")
		c.Do("LLEN", "l")
		c.Do("LLEN", "nosuch")

		// failure cases
		c.Do("SET", "str", "I am a string")
		c.Do("LLEN", "str")
		c.Do("LLEN")
		c.Do("LLEN", "l", "too many")
	})
}

func TestLtrim(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("RPUSH", "l", "aap", "noot", "mies")
		c.Do("LTRIM", "l", "0", "1")
		c.Do("LRANGE", "l", "0", "-1")
		c.Do("RPUSH", "l2", "aap", "noot", "mies", "vuur")
		c.Do("LTRIM", "l2", "-2", "-1")
		c.Do("LRANGE", "l2", "0", "-1")
		c.Do("RPUSH", "l3", "aap", "noot", "mies", "vuur")
		c.Do("LTRIM", "l3", "-2", "-1000")
		c.Do("LRANGE", "l3", "0", "-1")

		// remove the list
		c.Do("RPUSH", "l4", "aap")
		c.Do("LTRIM", "l4", "0", "-999")
		c.Do("EXISTS", "l4")

		// failure cases
		c.Do("SET", "str", "I am a string")
		c.Do("LTRIM", "str", "0", "1")
		c.Do("LTRIM", "l", "0", "1", "toomany")
		c.Do("LTRIM", "l", "noint", "1")
		c.Do("LTRIM", "l", "0", "noint")
		c.Do("LTRIM", "l", "0")
		c.Do("LTRIM", "l")
		c.Do("LTRIM")
	})
}

func TestLrem(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("RPUSH", "l", "aap", "noot", "mies", "mies", "mies")
		c.Do("LREM", "l", "1", "mies")
		c.Do("LRANGE", "l", "0", "-1")
		c.Do("RPUSH", "l2", "aap", "noot", "mies", "mies", "mies")
		c.Do("LREM", "l2", "-2", "mies")
		c.Do("LRANGE", "l2", "0", "-1")
		c.Do("RPUSH", "l3", "aap", "noot", "mies", "mies", "mies")
		c.Do("LREM", "l3", "0", "mies")
		c.Do("LRANGE", "l3", "0", "-1")

		// remove the list
		c.Do("RPUSH", "l4", "aap")
		c.Do("LREM", "l4", "999", "aap")
		c.Do("EXISTS", "l4")

		// failure cases
		c.Do("SET", "str", "I am a string")
		c.Do("LREM", "str", "0", "aap")
		c.Do("LREM", "l", "0", "aap", "toomany")
		c.Do("LREM", "l", "noint", "aap")
		c.Do("LREM", "l", "0")
		c.Do("LREM", "l")
		c.Do("LREM")
	})
}

func TestLset(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("RPUSH", "l", "aap", "noot", "mies", "mies", "mies")
		c.Do("LSET", "l", "1", "[cencored]")
		c.Do("LRANGE", "l", "0", "-1")
		c.Do("LSET", "l", "-1", "[cencored]")
		c.Do("LRANGE", "l", "0", "-1")
		c.Do("LSET", "l", "1000", "new")
		c.Do("LSET", "l", "-7000", "new")
		c.Do("LSET", "nosuch", "1", "new")

		// failure cases
		c.Do("LSET")
		c.Do("LSET", "l")
		c.Do("LSET", "l", "0")
		c.Do("LSET", "l", "noint", "aap")
		c.Do("LSET", "l", "0", "aap", "toomany")
		c.Do("SET", "str", "I am a string")
		c.Do("LSET", "str", "0", "aap")
	})
}

func TestLinsert(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("RPUSH", "l", "aap", "noot", "mies", "mies", "mies!")
		c.Do("LINSERT", "l", "before", "aap", "1")
		c.Do("LINSERT", "l", "before", "noot", "2")
		c.Do("LINSERT", "l", "after", "mies!", "3")
		c.Do("LINSERT", "l", "after", "mies", "4")
		c.Do("LINSERT", "l", "after", "nosuch", "0")
		c.Do("LINSERT", "nosuch", "after", "nosuch", "0")
		c.Do("LRANGE", "l", "0", "-1")
		c.Do("LINSERT", "l", "AfTeR", "mies", "4")
		c.Do("LRANGE", "l", "0", "-1")

		// failure cases
		c.Do("LINSERT")
		c.Do("LINSERT", "l")
		c.Do("LINSERT", "l", "before")
		c.Do("LINSERT", "l", "before", "aap")
		c.Do("LINSERT", "l", "before", "aap", "too", "many")
		c.Do("LINSERT", "l", "What?", "aap", "noot")
		c.Do("SET", "str", "I am a string")
		c.Do("LINSERT", "str", "before", "aap", "noot")
	})
}

func TestRpoplpush(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("RPUSH", "l", "aap", "noot", "mies")
		c.Do("RPOPLPUSH", "l", "l2")
		c.Do("LRANGE", "l", "0", "-1")
		c.Do("LRANGE", "2l", "0", "-1")
		c.Do("RPOPLPUSH", "l", "l2")
		c.Do("RPOPLPUSH", "l", "l2")
		c.Do("RPOPLPUSH", "l", "l2") // now empty
		c.Do("EXISTS", "l")
		c.Do("LRANGE", "2l", "0", "-1")

		c.Do("RPUSH", "round", "aap", "noot", "mies")
		c.Do("RPOPLPUSH", "round", "round")
		c.Do("LRANGE", "round", "0", "-1")
		c.Do("RPOPLPUSH", "round", "round")
		c.Do("RPOPLPUSH", "round", "round")
		c.Do("RPOPLPUSH", "round", "round")
		c.Do("RPOPLPUSH", "round", "round")
		c.Do("LRANGE", "round", "0", "-1")

		// failure cases
		c.Do("RPUSH", "chk", "aap", "noot", "mies")
		c.Do("RPOPLPUSH")
		c.Do("RPOPLPUSH", "chk")
		c.Do("RPOPLPUSH", "chk", "too", "many")
		c.Do("SET", "str", "I am a string")
		c.Do("RPOPLPUSH", "chk", "str")
		c.Do("RPOPLPUSH", "str", "chk")
		c.Do("LRANGE", "chk", "0", "-1")
	})
}

func TestRpushx(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("RPUSHX", "l", "aap")
		c.Do("EXISTS", "l")
		c.Do("RPUSH", "l", "noot", "mies")
		c.Do("RPUSHX", "l", "vuur")
		c.Do("EXISTS", "l")
		c.Do("LRANGE", "l", "0", "-1")
		c.Do("RPUSHX", "l", "more", "arguments")

		// failure cases
		c.Do("RPUSH", "chk", "noot", "mies")
		c.Do("RPUSHX")
		c.Do("RPUSHX", "chk")
		c.Do("LRANGE", "chk", "0", "-1")
		c.Do("SET", "str", "I am a string")
		c.Do("RPUSHX", "str", "value")
	})
}

func TestBrpop(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("LPUSH", "l", "one")
		c.Do("BRPOP", "l", "1")
		c.Do("EXISTS", "l")

		// transaction
		c.Do("MULTI")
		c.Do("BRPOP", "nosuch", "10")
		c.Do("EXEC")

		// failure cases
		c.Do("BRPOP")
		c.Do("BRPOP", "l")
		c.Do("BRPOP", "l", "X")
		c.Do("BRPOP", "l", "")
		c.Do("BRPOP", "1")
		c.Do("BRPOP", "key", "-1")
	})
}

func TestBrpopMulti(t *testing.T) {
	testMulti(t,
		func(c *client) {
			c.Do("BRPOP", "key", "1")
			c.Do("BRPOP", "key", "1")
			c.Do("BRPOP", "key", "1")
			c.Do("BRPOP", "key", "1")
			c.Do("BRPOP", "key", "1") // will timeout
		},
		func(c *client) {
			c.Do("LPUSH", "key", "aap", "noot", "mies")
			time.Sleep(50 * time.Millisecond)
			c.Do("LPUSH", "key", "toon")
		},
	)
}

func TestBrpopTrans(t *testing.T) {
	testMulti(t,
		func(c *client) {
			c.Do("BRPOP", "key", "1")
		},
		func(c *client) {
			c.Do("MULTI")
			c.Do("LPUSH", "key", "toon")
			c.Do("EXEC")
		},
	)
}

func TestBlpop(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("LPUSH", "l", "one")
		c.Do("BLPOP", "l", "1")
		c.Do("EXISTS", "l")

		// failure cases
		c.Do("BLPOP")
		c.Do("BLPOP", "l")
		c.Do("BLPOP", "l", "X")
		c.Do("BLPOP", "l", "")
		c.Do("BLPOP", "1")
		c.Do("BLPOP", "key", "-1")
	})

	testMulti(t,
		func(c *client) {
			c.Do("BLPOP", "key", "1")
			c.Do("BLPOP", "key", "1")
			c.Do("BLPOP", "key", "1")
			c.Do("BLPOP", "key", "1")
			c.Do("BLPOP", "key", "1") // will timeout
		},
		func(c *client) {
			c.Do("LPUSH", "key", "aap", "noot", "mies")
			time.Sleep(10 * time.Millisecond)
			c.Do("LPUSH", "key", "toon")
		},
	)
}

func TestBrpoplpush(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("LPUSH", "l", "one")
		c.Do("BRPOPLPUSH", "l", "l2", "1")
		c.Do("EXISTS", "l")
		c.Do("EXISTS", "l2")
		c.Do("LRANGE", "l", "0", "-1")
		c.Do("LRANGE", "l2", "0", "-1")

		// failure cases
		c.Do("BRPOPLPUSH")
		c.Do("BRPOPLPUSH", "l")
		c.Do("BRPOPLPUSH", "l", "x")
		c.Do("BRPOPLPUSH", "1")
		c.Do("BRPOPLPUSH", "from", "to", "-1")
		c.Do("BRPOPLPUSH", "from", "to", "-1", "xxx")
	})

	wg := &sync.WaitGroup{}
	wg.Add(1)
	testMulti(t,
		func(c *client) {
			c.Do("BRPOPLPUSH", "from", "to", "1")
			c.Do("BRPOPLPUSH", "from", "to", "1")
			c.Do("BRPOPLPUSH", "from", "to", "1")
			c.Do("BRPOPLPUSH", "from", "to", "1")
			c.Do("BRPOPLPUSH", "from", "to", "1") // will timeout
			wg.Done()
		},
		func(c *client) {
			c.Do("LPUSH", "from", "aap", "noot", "mies")
			time.Sleep(20 * time.Millisecond)
			c.Do("LPUSH", "from", "toon")
			wg.Wait()
			c.Do("LRANGE", "from", "0", "-1")
			c.Do("LRANGE", "to", "0", "-1")
		},
	)
}
