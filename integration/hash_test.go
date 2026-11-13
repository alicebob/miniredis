package main

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestHash(t *testing.T) {
	skip(t)
	t.Run("basics", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("HSET", "aap", "noot", "mies")
			c.Do("HGET", "aap", "noot")
			c.Do("HMGET", "aap", "noot")
			c.Do("HLEN", "aap")
			c.Do("HKEYS", "aap")
			c.Do("HVALS", "aap")
			c.Do("HSET", "aaa", "bb", "1", "cc", "2")
			c.Do("HGET", "aaa", "bb")
			c.Do("HGET", "aaa", "cc")

			c.Do("HDEL", "aap", "noot")
			c.Do("HGET", "aap", "noot")
			c.Do("EXISTS", "aap") // key is gone

			// failure cases
			c.Error("wrong number", "HSET", "aap", "noot")
			c.Error("wrong number", "HGET", "aap")
			c.Error("wrong number", "HMGET", "aap")
			c.Error("wrong number", "HLEN")
			c.Error("wrong number", "HKEYS")
			c.Error("wrong number", "HVALS")
			c.Do("SET", "str", "I am a string")
			c.Error("wrong kind", "HSET", "str", "noot", "mies")
			c.Error("wrong kind", "HGET", "str", "noot")
			c.Error("wrong kind", "HMGET", "str", "noot")
			c.Error("wrong kind", "HLEN", "str")
			c.Error("wrong kind", "HKEYS", "str")
			c.Error("wrong kind", "HVALS", "str")
			c.Error("wrong number", "HSET")
			c.Error("wrong number", "HSET", "a1")
			c.Error("wrong number", "HSET", "a1", "b")
			c.Error("wrong number", "HSET", "a2", "b", "c", "d")
		})
	})

	t.Run("tx", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("MULTI")
			c.Do("HSET", "aap", "noot", "mies", "vuur", "wim")
			c.Do("EXEC")

			c.Do("MULTI")
			c.Do("HSET", "aap", "noot", "mies", "vuur") // uneven arg count
			c.Do("EXEC")
		})
	})

	t.Run("expire", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("HSET", "aap", "noot", "mies")
			c.Do("HEXPIRE", "aap", "3", "FIELDS", "2", "noot", "vuur")

			c.Error("wrong number", "HEXPIRE", "aap", "3", "FIELDS", "0")
			c.Error("wrong number", "HEXPIRE", "aap", "3")
			c.Error("wrong number", "HEXPIRE", "aap", "3", "FIELDS")
			c.Error("wrong number", "HEXPIRE", "aap", "-3", "FIELDS", "0")
			c.Error("wrong number", "HEXPIRE", "aap", "noot", "3")
			c.Error("not an int", "HEXPIRE", "aap", "3.14", "FIELDS", "noot", "3.14")
			c.Error("numfields", "HEXPIRE", "aap", "3", "FIELDS", "3", "noot", "vuur")
			c.Error("invalid expire time", "HEXPIRE", "aap", "-3", "FIELDS", "1", "noot")
		})
	})

	t.Run("expire now", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("HSET", "aap", "noot", "mies", "vuur", "wim", "zus", "jet")
			c.Do("HEXPIRE", "aap", "0", "FIELDS", "2", "noot", "nosuch")
			c.Do("HGET", "aap", "noot")
			c.Do("HLEN", "aap")

			c.Do("HEXPIRE", "aap", "0", "GT", "FIELDS", "1", "vuur")
			c.Do("HEXPIRE", "aap", "0", "LT", "FIELDS", "1", "vuur")
			c.Do("HEXPIRE", "aap", "10", "FIELDS", "1", "zus")
			c.Do("HEXPIRE", "aap", "0", "NX", "FIELDS", "1", "zus")
			c.Do("HEXPIRE", "aap", "0", "XX", "FIELDS", "1", "zus")
			c.Do("EXISTS", "aap") // key is gone

			c.Do("HSET", "aap", "noot", "mies")
			c.Do("MULTI")
			c.Do("HEXPIRE", "aap", "0", "FIELDS", "1", "noot")
			c.Do("EXISTS", "aap")
			c.Do("EXEC")
		})
	})
}

// Both commands must reject malformed syntax without changing the hash.
func TestHashExpireSyntax(t *testing.T) {
	skip(t)
	for _, cmd := range []string{"HEXPIRE", "HEXPIREAT"} {
		for _, tc := range []struct {
			name string
			args []string
			err  string
		}{
			{"missing arguments", []string{"h", "10"}, "ERR wrong number of arguments for '" + strings.ToLower(cmd) + "' command"},
			{"invalid time", []string{"h", "bad", "FIELDS", "1", "f"}, "ERR value is not an integer or out of range"},
			{"time overflow", []string{"h", "9223372036854775808", "FIELDS", "1", "f"}, "ERR value is not an integer or out of range"},
			{"negative time", []string{"h", "-1", "FIELDS", "1", "f"}, "ERR invalid expire time, must be >= 0"},
			{"plus sign", []string{"h", "+1", "FIELDS", "1", "f"}, "ERR value is not an integer or out of range"},
			{"leading zero", []string{"h", "01", "FIELDS", "1", "f"}, "ERR value is not an integer or out of range"},
			{"negative zero", []string{"h", "-0", "FIELDS", "1", "f"}, "ERR value is not an integer or out of range"},
			{"mixed conditions", []string{"h", "10", "XX", "GT", "FIELDS", "1", "f"}, "ERR Mandatory argument FIELDS is missing or not at the right position"},
			{"repeated condition", []string{"h", "10", "NX", "NX", "FIELDS", "1", "f"}, "ERR Mandatory argument FIELDS is missing or not at the right position"},
			{"missing FIELDS", []string{"h", "10", "bad", "1", "f"}, "ERR Mandatory argument FIELDS is missing or not at the right position"},
			{"invalid count", []string{"h", "10", "FIELDS", "bad", "f"}, "ERR Parameter `numFields` should be greater than 0"},
			{"zero count", []string{"h", "10", "FIELDS", "0", "f"}, "ERR Parameter `numFields` should be greater than 0"},
			{"negative count", []string{"h", "10", "FIELDS", "-1", "f"}, "ERR Parameter `numFields` should be greater than 0"},
			{"count overflow", []string{"h", "10", "FIELDS", "9223372036854775808", "f"}, "ERR Parameter `numFields` should be greater than 0"},
			{"count plus sign", []string{"h", "10", "FIELDS", "+1", "f"}, "ERR Parameter `numFields` should be greater than 0"},
			{"count leading zero", []string{"h", "10", "FIELDS", "01", "f"}, "ERR Parameter `numFields` should be greater than 0"},
			{"count exceeds 32 bits", []string{"h", "10", "FIELDS", "2147483648", "f"}, "ERR The `numfields` parameter must match the number of arguments"},
			{"too few fields", []string{"h", "10", "FIELDS", "2", "f"}, "ERR The `numfields` parameter must match the number of arguments"},
			{"extra field", []string{"h", "10", "FIELDS", "1", "f", "extra"}, "ERR The `numfields` parameter must match the number of arguments"},
			{"condition after fields", []string{"h", "10", "FIELDS", "1", "f", "NX"}, "ERR The `numfields` parameter must match the number of arguments"},
			{"second FIELDS clause", []string{"h", "10", "FIELDS", "1", "f", "FIELDS", "1", "other"}, "ERR The `numfields` parameter must match the number of arguments"},
		} {
			t.Run(cmd+"/"+tc.name, func(t *testing.T) {
				testRaw(t, func(c *client) {
					c.Do("HSET", "h", "f", "value")
					c.ErrorTheSame(tc.err, cmd, tc.args...)
					c.Do("HGET", "h", "f")
					c.Do("HTTL", "h", "FIELDS", "1", "f")
				})
			})
		}
	}
}

func TestHashExpireat(t *testing.T) {
	skip(t)
	t.Run("conditions and per-field replies", func(t *testing.T) {
		testRaw(t, func(c *client) {
			// Freeze conversions so repeated absolute deadlines compare equal.
			c.miniredis.SetTime(time.Now())
			deadline := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
			later := strconv.FormatInt(time.Now().Add(2*time.Hour).Unix(), 10)
			c.Do("HEXPIREAT", "missing", deadline, "FIELDS", "2", "a", "b")
			c.Do("HSET", "h", "a", "one", "b", "two")
			c.Do("HEXPIREAT", "h", deadline, "NX", "FIELDS", "3", "a", "a", "missing")
			c.Do("HEXPIREAT", "h", deadline, "XX", "FIELDS", "2", "a", "b")
			c.Do("HEXPIREAT", "h", deadline, "GT", "FIELDS", "2", "a", "b")
			c.Do("HEXPIREAT", "h", later, "GT", "FIELDS", "2", "a", "b")
			c.Do("HEXPIREAT", "h", later, "LT", "FIELDS", "2", "a", "b")
			c.Do("hexpireat", "h", deadline, "lt", "fields", "2", "a", "b")
			c.Do("HEXPIREAT", "h", "0", "NX", "FIELDS", "1", "a")
			c.Do("HEXPIREAT", "h", "0", "GT", "FIELDS", "1", "a")
			c.Do("HEXPIREAT", "h", "0", "XX", "FIELDS", "3", "a", "a", "missing")
			c.Do("HEXPIREAT", "h", "1", "LT", "FIELDS", "1", "b")
			c.Do("EXISTS", "h")
		})
	})
	t.Run("last accepted timestamp and first rejected second", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("HSET", "h", "f", "value")
			c.Do("HEXPIREAT", "h", "70368744177", "FIELDS", "1", "f")
			c.ErrorTheSame("ERR invalid expire time in 'hexpireat' command", "HEXPIREAT", "h", "70368744178", "FIELDS", "1", "f")
		})
	})
	t.Run("wrong key type", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("SET", "str", "value")
			c.ErrorTheSame("WRONGTYPE Operation against a key holding the wrong kind of value", "HEXPIREAT", "str", "10", "FIELDS", "1", "f")
		})
	})
}

func TestHashSetnx(t *testing.T) {
	skip(t)
	testRaw(t, func(c *client) {
		c.Do("HSETNX", "aap", "noot", "mies")
		c.Do("EXISTS", "aap")
		c.Do("HEXISTS", "aap", "noot")

		c.Do("HSETNX", "aap", "noot", "mies2")
		c.Do("HGET", "aap", "noot")

		// failure cases
		c.Error("wrong number", "HSETNX", "aap")
		c.Error("wrong number", "HSETNX", "aap", "noot")
		c.Error("wrong number", "HSETNX", "aap", "noot", "too", "many")
	})
}

func TestHashDelExists(t *testing.T) {
	skip(t)
	testRaw(t, func(c *client) {
		c.Do("HSET", "aap", "noot", "mies")
		c.Do("HSET", "aap", "vuur", "wim")
		c.Do("HEXISTS", "aap", "noot")
		c.Do("HEXISTS", "aap", "vuur")
		c.Do("HDEL", "aap", "noot")
		c.Do("HEXISTS", "aap", "noot")
		c.Do("HEXISTS", "aap", "vuur")

		c.Do("HEXISTS", "nosuch", "vuur")

		// failure cases
		c.Error("wrong number", "HDEL")
		c.Error("wrong number", "HDEL", "aap")
		c.Do("SET", "str", "I am a string")
		c.Error("wrong kind", "HDEL", "str", "key")

		c.Error("wrong number", "HEXISTS")
		c.Error("wrong number", "HEXISTS", "aap")
		c.Error("wrong number", "HEXISTS", "aap", "too", "many")
		c.Error("wrong kind", "HEXISTS", "str", "field")
	})
}

func TestHashGetall(t *testing.T) {
	skip(t)
	testRaw(t, func(c *client) {
		c.Do("HSET", "aap", "noot", "mies")
		c.Do("HSET", "aap", "vuur", "wim")
		c.DoSorted("HGETALL", "aap")

		c.Do("HGETALL", "nosuch")

		// failure cases
		c.Error("wrong number", "HGETALL")
		c.Error("wrong number", "HGETALL", "too", "many")
		c.Do("SET", "str", "I am a string")
		c.Error("wrong kind", "HGETALL", "str")
	})

	testRESP3(t, func(c *client) {
		c.Do("HSET", "aap", "noot", "mies")
		c.Do("HGETALL", "aap")
		c.Do("HSET", "aap", "vuur", "wim")
		c.DoSorted("HGETALL", "aap")

		c.Do("HGETALL", "nosuch")
	})
}

func TestHmset(t *testing.T) {
	skip(t)
	testRaw(t, func(c *client) {
		c.Do("HMSET", "aap", "noot", "mies", "vuur", "zus")
		c.Do("HGET", "aap", "noot")
		c.Do("HGET", "aap", "vuur")
		c.Do("HLEN", "aap")

		// failure cases
		c.Error("wrong number", "HMSET", "aap")
		c.Error("wrong number", "HMSET", "aap", "key")
		c.Error("wrong number", "HMSET", "aap", "key", "value", "odd")
		c.Do("SET", "str", "I am a string")
		c.Error("wrong kind", "HMSET", "str", "key", "value")
	})
}

func TestHashIncr(t *testing.T) {
	skip(t)
	testRaw(t, func(c *client) {
		c.Do("HINCRBY", "aap", "noot", "12")
		c.Do("HINCRBY", "aap", "noot", "-13")
		c.Do("HINCRBY", "aap", "noot", "2123")
		c.Do("HGET", "aap", "noot")

		// Simple failure cases.
		c.Error("wrong number", "HINCRBY")
		c.Error("wrong number", "HINCRBY", "aap")
		c.Error("wrong number", "HINCRBY", "aap", "noot")
		c.Error("not an integer", "HINCRBY", "aap", "noot", "noint")
		c.Error("wrong number", "HINCRBY", "aap", "noot", "12", "toomany")
		c.Do("SET", "str", "value")
		c.Error("wrong kind", "HINCRBY", "str", "value", "12")
		c.Do("HINCRBY", "aap", "noot", "12")
	})

	testRaw(t, func(c *client) {
		c.Do("HINCRBYFLOAT", "aap", "noot", "12.3")
		c.Do("HINCRBYFLOAT", "aap", "noot", "-13.1")
		c.Do("HINCRBYFLOAT", "aap", "noot", "200")
		c.Do("HGET", "aap", "noot")

		// Simple failure cases.
		c.Error("wrong number", "HINCRBYFLOAT")
		c.Error("wrong number", "HINCRBYFLOAT", "aap")
		c.Error("wrong number", "HINCRBYFLOAT", "aap", "noot")
		c.Error("not a valid float", "HINCRBYFLOAT", "aap", "noot", "noint")
		c.Error("wrong number", "HINCRBYFLOAT", "aap", "noot", "12", "toomany")
		c.Do("SET", "str", "value")
		c.Error("wrong kind", "HINCRBYFLOAT", "str", "value", "12")
		c.Do("HINCRBYFLOAT", "aap", "noot", "12")
	})
}

func TestHscan(t *testing.T) {
	skip(t)
	testRaw(t, func(c *client) {
		// No set yet
		c.Do("HSCAN", "h", "0")

		c.Do("HSET", "h", "key1", "value1")
		c.Do("HSCAN", "h", "0")
		c.Do("HSCAN", "h", "0", "COUNT", "12")
		c.Do("HSCAN", "h", "0", "cOuNt", "12")

		c.Do("HSET", "h", "anotherkey", "value2")
		c.Do("HSCAN", "h", "0", "MATCH", "anoth*")
		c.Do("HSCAN", "h", "0", "MATCH", "anoth*", "COUNT", "100")
		c.Do("HSCAN", "h", "0", "COUNT", "100", "MATCH", "anoth*")

		// Can't really test multiple keys.
		// c.Do("SET", "key2", "value2")
		// c.Do("SCAN", "0")

		// Error cases
		c.Error("wrong number", "HSCAN")
		c.Error("wrong number", "HSCAN", "noint")
		c.Error("not an integer", "HSCAN", "h", "0", "COUNT", "noint")
		c.Error("syntax error", "HSCAN", "h", "0", "COUNT")
		c.Error("syntax error", "HSCAN", "h", "0", "MATCH")
		c.Error("syntax error", "HSCAN", "h", "0", "garbage")
		c.Error("syntax error", "HSCAN", "h", "0", "COUNT", "12", "MATCH", "foo", "garbage")
		// c.Do("HSCAN", "nosuch", "0", "COUNT", "garbage")
		c.Do("SET", "str", "1")
		c.Error("wrong kind", "HSCAN", "str", "0")
	})
}

func TestHstrlen(t *testing.T) {
	skip(t)
	testRaw(t, func(c *client) {
		c.Do("HSTRLEN", "hash", "foo")
		c.Do("HSET", "hash", "foo", "bar")
		c.Do("HSTRLEN", "hash", "foo")
		c.Do("HSTRLEN", "hash", "nosuch")
		c.Do("HSTRLEN", "nosuch", "nosuch")

		c.Error("wrong number", "HSTRLEN")
		c.Error("wrong number", "HSTRLEN", "foo")
		c.Error("wrong number", "HSTRLEN", "foo", "baz", "bar")
		c.Do("SET", "str", "1")
		c.Error("wrong kind", "HSTRLEN", "str", "bar")
	})
}

func TestHrandfield(t *testing.T) {
	skip(t)
	testRaw(t, func(c *client) {
		c.Do("HSET", "one", "foo", "bar")
		c.Do("HRANDFIELD", "one")
		c.Do("HRANDFIELD", "one", "0")
		c.Do("HRANDFIELD", "one", "1")
		c.Do("HRANDFIELD", "one", "2") // limited to 1
		c.Do("HRANDFIELD", "one", "3") // limited to 1
		c.Do("HRANDFIELD", "one", "-1")
		c.Do("HRANDFIELD", "one", "-2") // padded
		c.Do("HRANDFIELD", "one", "-3") // padded

		c.Do("HSET", "more", "foo", "bar", "baz", "bak")
		c.DoLoosely("HRANDFIELD", "more")
		c.Do("HRANDFIELD", "more", "0")
		c.DoLoosely("HRANDFIELD", "more", "1")
		c.DoLoosely("HRANDFIELD", "more", "2")
		c.DoLoosely("HRANDFIELD", "more", "3") // limited to 2
		c.DoLoosely("HRANDFIELD", "more", "-1")
		c.DoLoosely("HRANDFIELD", "more", "-2")
		c.DoLoosely("HRANDFIELD", "more", "-3") // length padded to 3

		c.Do("HRANDFIELD", "nosuch", "1")
		c.Do("HRANDFIELD", "nosuch", "2")
		c.Do("HRANDFIELD", "nosuch", "3")
		c.Do("HRANDFIELD", "nosuch", "0")
		c.Do("HRANDFIELD", "nosuch")
		c.Do("HRANDFIELD", "nosuch", "-1") // still empty
		c.Do("HRANDFIELD", "nosuch", "-2") // still empty
		c.Do("HRANDFIELD", "nosuch", "-3") // still empty
		c.DoLoosely("HRANDFIELD", "one", "2")
		c.DoLoosely("HRANDFIELD", "one", "7")
		c.DoLoosely("HRANDFIELD", "one", "2", "WITHVALUE")
		c.DoLoosely("HRANDFIELD", "one", "7", "WITHVALUE")

		c.Error("ERR syntax error", "HRANDFIELD", "foo", "1", "2")
		c.Error("ERR wrong number", "HRANDFIELD")
		c.Do("SET", "nothash", "1")
		c.Error("wrong kind of value", "HRANDFIELD", "nothash")
	})
}

func TestHTTL(t *testing.T) {
	skip(t)
	t.Run("persist", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("HSET", "aap", "noot", "mies")
			c.Do("HEXPIRE", "aap", "10", "FIELDS", "1", "noot")
			c.Do("HPERSIST", "aap", "FIELDS", "1", "noot")

			c.Error("wrong number", "HPERSIST", "aap")
			c.Error("wrong number", "HPERSIST", "aap", "FIELDS")
			c.Error("Number of fields", "HPERSIST", "aap", "FIELDS", "0", "dummy")
		})
	})

	t.Run("set and delete", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("HSET", "aap", "noot", "1", "vuur", "2", "zus", "3")
			c.Do("HEXPIRE", "aap", "100", "FIELDS", "3", "noot", "vuur", "zus")
			c.Do("HSET", "aap", "noot", "4")
			c.Do("HMSET", "aap", "vuur", "5")
			c.Do("HINCRBY", "aap", "zus", "1")
			c.Do("HTTL", "aap", "FIELDS", "3", "noot", "vuur", "zus")

			c.Do("HDEL", "aap", "zus")
			c.Do("HSETNX", "aap", "zus", "6")
			c.Do("HTTL", "aap", "FIELDS", "1", "zus")

			c.Do("HEXPIRE", "aap", "100", "FIELDS", "1", "noot")
			c.Do("MULTI")
			c.Do("HSET", "aap", "noot", "7")
			c.Do("HTTL", "aap", "FIELDS", "1", "noot")
			c.Do("EXEC")
		})
	})

	t.Run("ttl", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("HSET", "aap", "noot", "mies")
			c.Do("HEXPIRE", "aap", "10", "FIELDS", "1", "noot")
			c.Do("HTTL", "aap", "FIELDS", "1", "noot")
			c.DoLoosely("HPTTL", "aap", "FIELDS", "1", "noot")

			c.Error("wrong number", "HTTL", "aap")
			c.Error("wrong number", "HPTTL", "aap")
			c.Error("wrong number", "HPTTL", "aap", "FIELDS")
		})
	})

	t.Run("setex", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("HSETEX", "aap", "EX", "10", "FIELDS", "1", "noot", "mies")
			c.Do("HGET", "aap", "noot")
			c.Do("HTTL", "aap", "FIELDS", "1", "noot")

			c.Do("HSETEX", "bbb", "FNX", "PX", "5000", "FIELDS", "1", "cc", "dd")
			c.Do("HSETEX", "bbb", "FNX", "PX", "5000", "FIELDS", "1", "cc", "ee")
			c.Do("HGET", "bbb", "cc")

			c.Error("wrong number", "HSETEX")
			c.Error("wrong number", "HSETEX", "k")
			c.Error("wrong number", "HSETEX", "k", "EX")
			c.Error("not an integer", "HSETEX", "bbb", "FNX", "PX", "3.1415", "FIELDS", "1", "cc", "dd")
			c.Error("invalid expire time, must be >= 0", "HSETEX", "bbb", "FNX", "PX", "-3415", "FIELDS", "1", "cc", "dd")
			c.Error("Only one of", "HSETEX", "k", "EX", "10", "PX", "5000", "FIELDS", "1", "f1", "v1")
		})
	})
}
