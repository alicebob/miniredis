// +build int

package main

// Set keys.

import (
	"testing"
)

func TestSet(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SADD", "s", "aap", "noot", "mies")
		c.Do("SADD", "s", "vuur", "noot")
		c.Do("TYPE", "s")
		c.Do("EXISTS", "s")
		c.Do("SCARD", "s")
		c.DoSorted("SMEMBERS", "s")
		c.DoSorted("SMEMBERS", "nosuch")
		c.Do("SISMEMBER", "s", "aap")
		c.Do("SISMEMBER", "s", "nosuch")

		c.Do("SCARD", "nosuch")
		c.Do("SISMEMBER", "nosuch", "nosuch")

		// failure cases
		c.Do("SADD")
		c.Do("SADD", "s")
		c.Do("SMEMBERS")
		c.Do("SMEMBERS", "too", "many")
		c.Do("SCARD")
		c.Do("SCARD", "too", "many")
		c.Do("SISMEMBER")
		c.Do("SISMEMBER", "few")
		c.Do("SISMEMBER", "too", "many", "arguments")
		// Wrong type
		c.Do("SET", "str", "I am a string")
		c.Do("SADD", "str", "noot", "mies")
		c.Do("SMEMBERS", "str")
		c.Do("SISMEMBER", "str", "noot")
		c.Do("SCARD", "str")
	})
}

func TestSetMove(t *testing.T) {
	// Move a set around
	testRaw(t, func(c *client) {
		c.Do("SADD", "s", "aap", "noot", "mies")
		c.Do("RENAME", "s", "others")
		c.DoSorted("SMEMBERS", "s")
		c.DoSorted("SMEMBERS", "others")
		c.Do("MOVE", "others", "2")
		c.DoSorted("SMEMBERS", "others")
		c.Do("SELECT", "2")
		c.DoSorted("SMEMBERS", "others")
	})
}

func TestSetDel(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SADD", "s", "aap", "noot", "mies")
		c.Do("SREM", "s", "noot", "nosuch")
		c.Do("SCARD", "s")
		c.DoSorted("SMEMBERS", "s")

		// failure cases
		c.Do("SREM")
		c.Do("SREM", "s")
		// Wrong type
		c.Do("SET", "str", "I am a string")
		c.Do("SREM", "str", "noot")
	})
}

func TestSetSMove(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SADD", "s", "aap", "noot", "mies")
		c.Do("SMOVE", "s", "s2", "aap")
		c.Do("SCARD", "s")
		c.Do("SCARD", "s2")
		c.Do("SMOVE", "s", "s2", "nosuch")
		c.Do("SCARD", "s")
		c.Do("SCARD", "s2")
		c.Do("SMOVE", "s", "nosuch", "noot")
		c.Do("SCARD", "s")
		c.Do("SCARD", "s2")

		c.Do("SMOVE", "s", "s2", "mies")
		c.Do("SCARD", "s")
		c.Do("EXISTS", "s")
		c.Do("SCARD", "s2")
		c.Do("EXISTS", "s2")

		c.Do("SMOVE", "s2", "s2", "mies")

		c.Do("SADD", "s5", "aap")
		c.Do("SADD", "s6", "aap")
		c.Do("SMOVE", "s5", "s6", "aap")

		// failure cases
		c.Do("SMOVE")
		c.Do("SMOVE", "s")
		c.Do("SMOVE", "s", "s2")
		c.Do("SMOVE", "s", "s2", "too", "many")
		// Wrong type
		c.Do("SET", "str", "I am a string")
		c.Do("SMOVE", "str", "s2", "noot")
		c.Do("SMOVE", "s2", "str", "noot")
	})
}

func TestSetSpop(t *testing.T) {
	testRaw(t, func(c *client) {
		// Without count argument
		c.Do("SADD", "s", "aap")
		c.Do("SPOP", "s")
		c.Do("EXISTS", "s")

		c.Do("SPOP", "nosuch")

		c.Do("SADD", "s", "aap")
		c.Do("SADD", "s", "noot")
		c.Do("SADD", "s", "mies")
		c.Do("SADD", "s", "noot")
		c.Do("SCARD", "s")
		c.DoLoosely("SMEMBERS", "s")

		// failure cases
		c.Do("SPOP")
		c.Do("SADD", "s", "aap")
		c.Do("SPOP", "s", "s2")
		c.Do("SPOP", "nosuch", "s2")
		// Wrong type
		c.Do("SET", "str", "I am a string")
		c.Do("SPOP", "str")
	})

	testRaw(t, func(c *client) {
		// With count argument
		c.Do("SADD", "s", "aap")
		c.Do("SADD", "s", "noot")
		c.Do("SADD", "s", "mies")
		c.Do("SADD", "s", "vuur")
		c.DoLoosely("SPOP", "s", "2")
		c.Do("EXISTS", "s")
		c.Do("SCARD", "s")

		c.DoLoosely("SPOP", "s", "200")
		c.Do("SPOP", "s", "1")
		c.Do("SCARD", "s")

		c.Do("SPOP", "nosuch", "1")
		c.Do("SPOP", "nosuch", "0")

		// failure cases
		c.Do("SPOP", "foo", "one")
		c.Do("SPOP", "foo", "-4")
	})
}

func TestSetSrandmember(t *testing.T) {
	testRaw(t, func(c *client) {
		// Set with a single member...
		c.Do("SADD", "s", "aap")
		c.Do("SRANDMEMBER", "s")
		c.Do("SRANDMEMBER", "s", "1")
		c.Do("SRANDMEMBER", "s", "5")
		c.Do("SRANDMEMBER", "s", "-1")
		c.Do("SRANDMEMBER", "s", "-5")

		c.Do("SRANDMEMBER", "s", "0")
		c.Do("SPOP", "nosuch")

		// failure cases
		c.Do("SRANDMEMBER")
		c.Do("SRANDMEMBER", "s", "noint")
		c.Do("SRANDMEMBER", "s", "1", "toomany")
		// Wrong type
		c.Do("SET", "str", "I am a string")
		c.Do("SRANDMEMBER", "str")
	})
}

func TestSetSdiff(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SDIFF", "s1", "aap", "noot", "mies")
		c.Do("SDIFF", "s2", "noot", "mies", "vuur")
		c.Do("SDIFF", "s3", "mies", "wim")
		c.Do("SDIFF", "s1")
		c.Do("SDIFF", "s1", "s2")
		c.Do("SDIFF", "s1", "s2", "s3")
		c.Do("SDIFF", "nosuch")
		c.Do("SDIFF", "s1", "nosuch", "s2", "nosuch", "s3")
		c.Do("SDIFF", "s1", "s1")

		c.Do("SDIFFSTORE", "res", "s3", "nosuch", "s1")
		c.Do("SMEMBERS", "res")

		// failure cases
		c.Do("SDIFF")
		c.Do("SDIFFSTORE")
		c.Do("SDIFFSTORE", "key")
		// Wrong type
		c.Do("SET", "str", "I am a string")
		c.Do("SDIFF", "s1", "str")
		c.Do("SDIFF", "nosuch", "str")
		c.Do("SDIFF", "str", "s1")
		c.Do("SDIFFSTORE", "res", "str", "s1")
		c.Do("SDIFFSTORE", "res", "s1", "str")
	})
}

func TestSetSinter(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SADD", "s1", "aap", "noot", "mies")
		c.Do("SADD", "s2", "noot", "mies", "vuur")
		c.Do("SADD", "s3", "mies", "wim")
		c.DoSorted("SINTER", "s1")
		c.DoSorted("SINTER", "s1", "s2")
		c.DoSorted("SINTER", "s1", "s2", "s3")
		c.Do("SINTER", "nosuch")
		c.Do("SINTER", "s1", "nosuch", "s2", "nosuch", "s3")
		c.DoSorted("SINTER", "s1", "s1")

		c.Do("SINTERSTORE", "res", "s3", "nosuch", "s1")
		c.Do("SMEMBERS", "res")

		// failure cases
		c.Do("SINTER")
		c.Do("SINTERSTORE")
		c.Do("SINTERSTORE", "key")
		// Wrong type
		c.Do("SET", "str", "I am a string")
		c.Do("SINTER", "s1", "str")
		c.Do("SINTER", "nosuch", "str") // SINTER succeeds if an input type is wrong as long as the preceding inputs result in an empty set
		c.Do("SINTER", "str", "nosuch")
		c.Do("SINTER", "str", "s1")
		c.Do("SINTERSTORE", "res", "str", "s1")
		c.Do("SINTERSTORE", "res", "s1", "str")
	})
}

func TestSetSunion(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SUNION", "s1", "aap", "noot", "mies")
		c.Do("SUNION", "s2", "noot", "mies", "vuur")
		c.Do("SUNION", "s3", "mies", "wim")
		c.Do("SUNION", "s1")
		c.Do("SUNION", "s1", "s2")
		c.Do("SUNION", "s1", "s2", "s3")
		c.Do("SUNION", "nosuch")
		c.Do("SUNION", "s1", "nosuch", "s2", "nosuch", "s3")
		c.Do("SUNION", "s1", "s1")

		c.Do("SUNIONSTORE", "res", "s3", "nosuch", "s1")
		c.Do("SMEMBERS", "res")

		// failure cases
		c.Do("SUNION")
		c.Do("SUNIONSTORE")
		c.Do("SUNIONSTORE", "key")
		// Wrong type
		c.Do("SET", "str", "I am a string")
		c.Do("SUNION", "s1", "str")
		c.Do("SUNION", "nosuch", "str")
		c.Do("SUNION", "str", "s1")
		c.Do("SUNIONSTORE", "res", "str", "s1")
		c.Do("SUNIONSTORE", "res", "s1", "str")
	})
}

func TestSscan(t *testing.T) {
	testRaw(t, func(c *client) {
		// No set yet
		c.Do("SSCAN", "set", "0")

		c.Do("SADD", "set", "key1")
		c.Do("SSCAN", "set", "0")
		c.Do("SSCAN", "set", "0", "COUNT", "12")
		c.Do("SSCAN", "set", "0", "cOuNt", "12")

		c.Do("SADD", "set", "anotherkey")
		c.Do("SSCAN", "set", "0", "MATCH", "anoth*")
		c.Do("SSCAN", "set", "0", "MATCH", "anoth*", "COUNT", "100")
		c.Do("SSCAN", "set", "0", "COUNT", "100", "MATCH", "anoth*")

		// Can't really test multiple keys.
		// c.Do("SET", "key2", "value2")
		// c.Do("SCAN", "0")

		// Error cases
		c.Do("SSCAN")
		c.Do("SSCAN", "noint")
		c.Do("SSCAN", "set", "0", "COUNT", "noint")
		c.Do("SSCAN", "set", "0", "COUNT")
		c.Do("SSCAN", "set", "0", "MATCH")
		c.Do("SSCAN", "set", "0", "garbage")
		c.Do("SSCAN", "set", "0", "COUNT", "12", "MATCH", "foo", "garbage")
		c.Do("SET", "str", "1")
		c.Do("SSCAN", "str", "0")
	})
}

func TestSetNoAuth(t *testing.T) {
	testAuth(t,
		"supersecret",
		func(c *client) {
			c.Do("SET", "foo", "bar")
			c.Do("AUTH", "supersecret")
			c.Do("SET", "foo", "bar")
		},
	)
}
