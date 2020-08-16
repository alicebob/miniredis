// +build int

package main

import (
	"strconv"
	"testing"
)

func TestString(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "bar")
		c.Do("GET", "foo")
		c.Do("SET", "foo", "bar\bbaz")
		c.Do("GET", "foo")
		c.Do("SET", "foo", "bar", "EX", "100")
		c.Do("SET", "foo", "bar", "EX", "noint")
		c.Do("SET", "utf8", "❆❅❄☃")

		// Failure cases
		c.Do("SET")
		c.Do("SET", "foo")
		c.Do("SET", "foo", "bar", "baz")
		c.Do("GET")
		c.Do("GET", "too", "many")
		c.Do("SET", "foo", "bar", "EX", "0")
		c.Do("SET", "foo", "bar", "EX", "-100")
		// Wrong type
		c.Do("HSET", "hash", "key", "value")
		c.Do("GET", "hash")
	})
}

func TestStringGetSet(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "bar")
		c.Do("GETSET", "foo", "new")
		c.Do("GET", "foo")
		c.Do("GET", "new")
		c.Do("GETSET", "nosuch", "new")
		c.Do("GET", "nosuch")

		// Failure cases
		c.Do("GETSET")
		c.Do("GETSET", "foo")
		c.Do("GETSET", "foo", "bar", "baz")
		// Wrong type
		c.Do("HSET", "hash", "key", "value")
		c.Do("GETSET", "hash", "new")
	})
}

func TestStringMget(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "bar")
		c.Do("SET", "foo2", "bar")
		c.Do("MGET", "foo")
		c.Do("MGET", "foo", "foo2")
		c.Do("MGET", "nosuch", "neither")
		c.Do("MGET", "nosuch", "neither", "foo")

		// Failure cases
		c.Do("MGET")
		// Wrong type
		c.Do("HSET", "hash", "key", "value")
		c.Do("MGET", "hash") // not an error.
	})
}

func TestStringSetnx(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SETNX", "foo", "bar")
		c.Do("GET", "foo")
		c.Do("SETNX", "foo", "bar2")
		c.Do("GET", "foo")

		// Failure cases
		c.Do("SETNX")
		c.Do("SETNX", "foo")
		c.Do("SETNX", "foo", "bar", "baz")
		// Wrong type
		c.Do("HSET", "hash", "key", "value")
		c.Do("SETNX", "hash", "value")
	})
}

func TestExpire(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "bar")
		c.Do("EXPIRE", "foo", "12")
		c.Do("TTL", "foo")
		c.Do("TTL", "nosuch")
		c.Do("SET", "foo", "bar")
		c.Do("PEXPIRE", "foo", "999999")
		c.Do("EXPIREAT", "foo", "2234567890")
		c.Do("PEXPIREAT", "foo", "2234567890000")
		// c.Do("PTTL", "foo")
		c.Do("PTTL", "nosuch")

		c.Do("SET", "foo", "bar")
		c.Do("EXPIRE", "foo", "0")
		c.Do("EXISTS", "foo")
		c.Do("SET", "foo", "bar")
		c.Do("EXPIRE", "foo", "-12")
		c.Do("EXISTS", "foo")

		c.Do("EXPIRE")
		c.Do("EXPIRE", "foo")
		c.Do("EXPIRE", "foo", "noint")
		c.Do("EXPIRE", "foo", "12", "toomany")
		c.Do("EXPIREAT")
		c.Do("TTL")
		c.Do("TTL", "too", "many")
		c.Do("PEXPIRE")
		c.Do("PEXPIRE", "foo")
		c.Do("PEXPIRE", "foo", "noint")
		c.Do("PEXPIRE", "foo", "12", "toomany")
		c.Do("PEXPIREAT")
		c.Do("PTTL")
		c.Do("PTTL", "too", "many")
	})
}

func TestMset(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("MSET", "foo", "bar")
		c.Do("MSET", "foo", "bar", "baz", "?")
		c.Do("MSET", "foo", "bar", "foo", "baz") // double key
		c.Do("GET", "foo")
		// Error cases
		c.Do("MSET")
		c.Do("MSET", "foo")
		c.Do("MSET", "foo", "bar", "baz")

		c.Do("MSETNX", "foo", "bar", "aap", "noot")
		c.Do("MSETNX", "one", "two", "three", "four")
		c.Do("MSETNX", "11", "12", "11", "14") // double key
		c.Do("GET", "11")

		// Wrong type of key doesn't matter
		c.Do("HSET", "aap", "noot", "mies")
		c.Do("MSET", "aap", "again", "eight", "nine")
		c.Do("MSETNX", "aap", "again", "eight", "nine")

		// Error cases
		c.Do("MSETNX")
		c.Do("MSETNX", "one")
		c.Do("MSETNX", "one", "two", "three")
	})
}

func TestSetx(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SETEX", "foo", "12", "bar")
		c.Do("GET", "foo")
		c.Do("TTL", "foo")
		c.Do("SETEX", "foo")
		c.Do("SETEX", "foo", "noint", "bar")
		c.Do("SETEX", "foo", "12")
		c.Do("SETEX", "foo", "12", "bar", "toomany")
		c.Do("SETEX", "foo", "0")
		c.Do("SETEX", "foo", "-12")

		c.Do("PSETEX", "foo", "12", "bar")
		c.Do("GET", "foo")
		// c.Do("PTTL", "foo") // counts down too quickly to compare
		c.Do("PSETEX", "foo")
		c.Do("PSETEX", "foo", "noint", "bar")
		c.Do("PSETEX", "foo", "12")
		c.Do("PSETEX", "foo", "12", "bar", "toomany")
		c.Do("PSETEX", "foo", "0")
		c.Do("PSETEX", "foo", "-12")
	})
}

func TestGetrange(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "The quick brown fox jumps over the lazy dog")
		c.Do("GETRANGE", "foo", "0", "100")
		c.Do("GETRANGE", "foo", "0", "0")
		c.Do("GETRANGE", "foo", "0", "-4")
		c.Do("GETRANGE", "foo", "0", "-400")
		c.Do("GETRANGE", "foo", "-4", "-4")
		c.Do("GETRANGE", "foo", "4", "2")
		c.Do("GETRANGE", "foo", "aap", "2")
		c.Do("GETRANGE", "foo", "4", "aap")
		c.Do("GETRANGE", "foo", "4", "2", "aap")
		c.Do("GETRANGE", "foo")
		c.Do("HSET", "aap", "noot", "mies")
		c.Do("GETRANGE", "aap", "4", "2")
	})
}

func TestStrlen(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "str", "The quick brown fox jumps over the lazy dog")
		c.Do("STRLEN", "str")
		// failure cases
		c.Do("STRLEN")
		c.Do("STRLEN", "str", "bar")
		c.Do("HSET", "hash", "key", "value")
		c.Do("STRLEN", "hash")
	})
}

func TestSetrange(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "The quick brown fox jumps over the lazy dog")
		c.Do("SETRANGE", "foo", "0", "aap")
		c.Do("GET", "foo")
		c.Do("SETRANGE", "foo", "10", "noot")
		c.Do("GET", "foo")
		c.Do("SETRANGE", "foo", "40", "overtheedge")
		c.Do("GET", "foo")
		c.Do("SETRANGE", "foo", "400", "oh, hey there")
		c.Do("GET", "foo")
		// Non existing key
		c.Do("SETRANGE", "nosuch", "2", "aap")
		c.Do("GET", "nosuch")

		// Error cases
		c.Do("SETRANGE", "foo")
		c.Do("SETRANGE", "foo", "1")
		c.Do("SETRANGE", "foo", "aap", "bar")
		c.Do("SETRANGE", "foo", "noint", "bar")
		c.Do("SETRANGE", "foo", "-1", "bar")
		c.Do("HSET", "aap", "noot", "mies")
		c.Do("SETRANGE", "aap", "4", "bar")
	})
}

func TestIncrAndFriends(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("INCR", "aap")
		c.Do("INCR", "aap")
		c.Do("INCR", "aap")
		c.Do("GET", "aap")
		c.Do("DECR", "aap")
		c.Do("DECR", "noot")
		c.Do("DECR", "noot")
		c.Do("GET", "noot")
		c.Do("INCRBY", "noot", "100")
		c.Do("INCRBY", "noot", "200")
		c.Do("INCRBY", "noot", "300")
		c.Do("GET", "noot")
		c.Do("DECRBY", "noot", "100")
		c.Do("DECRBY", "noot", "200")
		c.Do("DECRBY", "noot", "300")
		c.Do("DECRBY", "noot", "400")
		c.Do("GET", "noot")
		c.Do("INCRBYFLOAT", "zus", "1.23")
		c.Do("INCRBYFLOAT", "zus", "3.1456")
		c.Do("INCRBYFLOAT", "zus", "987.65432")
		c.Do("GET", "zus")
		c.Do("INCRBYFLOAT", "whole", "300")
		c.Do("INCRBYFLOAT", "whole", "300")
		c.Do("INCRBYFLOAT", "whole", "300")
		c.Do("GET", "whole")
		c.Do("INCRBYFLOAT", "big", "12345e10")
		c.Do("GET", "big")

		// Floats are not ints.
		c.Do("SET", "float", "1.23")
		c.Do("INCR", "float")
		c.Do("INCRBY", "float", "12")
		c.Do("DECR", "float")
		c.Do("DECRBY", "float", "12")
		c.Do("SET", "str", "I'm a string")
		c.Do("INCRBYFLOAT", "str", "123.5")

		// Error cases
		c.Do("HSET", "mies", "noot", "mies")
		c.Do("INCR", "mies")
		c.Do("INCRBY", "mies", "1")
		c.Do("INCRBY", "mies", "foo")
		c.Do("DECR", "mies")
		c.Do("DECRBY", "mies", "1")
		c.Do("INCRBYFLOAT", "mies", "1")
		c.Do("INCRBYFLOAT", "int", "foo")

		c.Do("INCR", "int", "err")
		c.Do("INCRBY", "int")
		c.Do("DECR", "int", "err")
		c.Do("DECRBY", "int")
		c.Do("INCRBYFLOAT", "int")

		// Rounding
		c.Do("INCRBYFLOAT", "zero", "12.3")
		c.Do("INCRBYFLOAT", "zero", "-13.1")

		// E
		c.Do("INCRBYFLOAT", "one", "12e12")
		// c.Do("INCRBYFLOAT", "one", "12e34") // FIXME
		c.Do("INCRBYFLOAT", "one", "12e34.1")
		// c.Do("INCRBYFLOAT", "one", "0x12e12") // FIXME
		// c.Do("INCRBYFLOAT", "one", "012e12") // FIXME
		c.Do("INCRBYFLOAT", "two", "012")
		c.Do("INCRBYFLOAT", "one", "0b12e12")
	})
}

func TestBitcount(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "str", "The quick brown fox jumps over the lazy dog")
		c.Do("SET", "utf8", "❆❅❄☃")
		c.Do("BITCOUNT", "str")
		c.Do("BITCOUNT", "utf8")
		c.Do("BITCOUNT", "str", "0", "0")
		c.Do("BITCOUNT", "str", "1", "2")
		c.Do("BITCOUNT", "str", "1", "-200")
		c.Do("BITCOUNT", "str", "-2", "-1")
		c.Do("BITCOUNT", "str", "-2", "-12")
		c.Do("BITCOUNT", "utf8", "0", "0")

		c.Do("BITCOUNT")
		c.Do("BITCOUNT", "wrong", "arguments")
		c.Do("BITCOUNT", "str", "4", "2", "2", "2", "2")
		c.Do("BITCOUNT", "str", "foo", "2")
		c.Do("HSET", "aap", "noot", "mies")
		c.Do("BITCOUNT", "aap", "4", "2")
	})
}

func TestBitop(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "a", "foo")
		c.Do("SET", "b", "aap")
		c.Do("SET", "c", "noot")
		c.Do("SET", "d", "mies")
		c.Do("SET", "e", "❆❅❄☃")

		// ANDs
		c.Do("BITOP", "AND", "target", "a", "b", "c", "d")
		c.Do("GET", "target")
		c.Do("BITOP", "AND", "target", "a", "nosuch", "c", "d")
		c.Do("GET", "target")
		c.Do("BITOP", "AND", "utf8", "e", "e")
		c.Do("GET", "utf8")
		c.Do("BITOP", "AND", "utf8", "b", "e")
		c.Do("GET", "utf8")
		// BITOP on only unknown keys:
		c.Do("BITOP", "AND", "bits", "nosuch", "nosucheither")
		c.Do("GET", "bits")

		// ORs
		c.Do("BITOP", "OR", "target", "a", "b", "c", "d")
		c.Do("GET", "target")
		c.Do("BITOP", "OR", "target", "a", "nosuch", "c", "d")
		c.Do("GET", "target")
		c.Do("BITOP", "OR", "utf8", "e", "e")
		c.Do("GET", "utf8")
		c.Do("BITOP", "OR", "utf8", "b", "e")
		c.Do("GET", "utf8")
		// BITOP on only unknown keys:
		c.Do("BITOP", "OR", "bits", "nosuch", "nosucheither")
		c.Do("GET", "bits")
		c.Do("SET", "empty", "")
		// BITOP on empty key
		c.Do("BITOP", "OR", "bits", "empty")
		c.Do("GET", "bits")

		// XORs
		c.Do("BITOP", "XOR", "target", "a", "b", "c", "d")
		c.Do("GET", "target")
		c.Do("BITOP", "XOR", "target", "a", "nosuch", "c", "d")
		c.Do("GET", "target")
		c.Do("BITOP", "XOR", "target", "a")
		c.Do("GET", "target")
		c.Do("BITOP", "XOR", "utf8", "e", "e")
		c.Do("GET", "utf8")
		c.Do("BITOP", "XOR", "utf8", "b", "e")
		c.Do("GET", "utf8")

		// NOTs
		c.Do("BITOP", "NOT", "target", "a")
		c.Do("GET", "target")
		c.Do("BITOP", "NOT", "target", "e")
		c.Do("GET", "target")
		c.Do("BITOP", "NOT", "bits", "nosuch")
		c.Do("GET", "bits")

		c.Do("BITOP", "AND", "utf8")
		c.Do("BITOP", "AND")
		c.Do("BITOP", "NOT", "foo", "bar", "baz")
		c.Do("BITOP", "WRONGOP", "key")
		c.Do("BITOP", "WRONGOP")

		c.Do("HSET", "hash", "aap", "noot")
		c.Do("BITOP", "AND", "t", "hash", "irrelevant")
		c.Do("BITOP", "OR", "t", "hash", "irrelevant")
		c.Do("BITOP", "XOR", "t", "hash", "irrelevant")
		c.Do("BITOP", "NOT", "t", "hash")
	})
}

func TestBitpos(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "a", "\x00\x0f")
		c.Do("SET", "b", "\xf0\xf0")
		c.Do("SET", "c", "\x00\x00\x00\x0f")
		c.Do("SET", "d", "\x00\x00\x00")
		c.Do("SET", "e", "\xff\xff\xff")

		c.Do("BITPOS", "a", "1")
		c.Do("BITPOS", "a", "0")
		c.Do("BITPOS", "a", "1", "1")
		c.Do("BITPOS", "a", "0", "1")
		c.Do("BITPOS", "a", "1", "1", "2")
		c.Do("BITPOS", "a", "0", "1", "2")
		c.Do("BITPOS", "b", "1")
		c.Do("BITPOS", "b", "0")
		c.Do("BITPOS", "c", "1")
		c.Do("BITPOS", "c", "0")
		c.Do("BITPOS", "d", "1")
		c.Do("BITPOS", "d", "0")
		c.Do("BITPOS", "e", "1")
		c.Do("BITPOS", "e", "0")
		c.Do("BITPOS", "e", "1", "1")
		c.Do("BITPOS", "e", "0", "1")
		c.Do("BITPOS", "e", "1", "1", "2")
		c.Do("BITPOS", "e", "0", "1", "2")
		c.Do("BITPOS", "e", "1", "100", "2")
		c.Do("BITPOS", "e", "0", "100", "2")
		c.Do("BITPOS", "e", "1", "1", "-2")
		c.Do("BITPOS", "e", "1", "1", "-2000")
		c.Do("BITPOS", "e", "0", "1", "2")
		c.Do("BITPOS", "nosuch", "1")
		c.Do("BITPOS", "nosuch", "0")

		c.Do("HSET", "hash", "aap", "noot")
		c.Do("BITPOS", "hash", "1")
		c.Do("BITPOS", "a", "aap")
	})
}

func TestGetbit(t *testing.T) {
	testRaw(t, func(c *client) {
		for i := 0; i < 100; i++ {
			c.Do("SET", "a", "\x00\x0f")
			c.Do("SET", "e", "\xff\xff\xff")
			c.Do("GETBIT", "nosuch", "1")
			c.Do("GETBIT", "nosuch", "0")

			// Error cases
			c.Do("HSET", "hash", "aap", "noot")
			c.Do("GETBIT", "hash", "1")
			c.Do("GETBIT", "a", "aap")
			c.Do("GETBIT", "a")
			c.Do("GETBIT", "too", "1", "many")

			c.Do("GETBIT", "a", strconv.Itoa(i))
			c.Do("GETBIT", "e", strconv.Itoa(i))
		}
	})
}

func TestSetbit(t *testing.T) {
	testRaw(t, func(c *client) {
		for i := 0; i < 100; i++ {
			c.Do("SET", "a", "\x00\x0f")
			c.Do("SETBIT", "a", "0", "1")
			c.Do("GET", "a")
			c.Do("SETBIT", "a", "0", "0")
			c.Do("GET", "a")
			c.Do("SETBIT", "a", "13", "0")
			c.Do("GET", "a")
			c.Do("SETBIT", "nosuch", "11111", "1")
			c.Do("GET", "nosuch")

			// Error cases
			c.Do("HSET", "hash", "aap", "noot")
			c.Do("SETBIT", "hash", "1", "1")
			c.Do("SETBIT", "a", "aap", "0")
			c.Do("SETBIT", "a", "0", "aap")
			c.Do("SETBIT", "a", "-1", "0")
			c.Do("SETBIT", "a", "1", "-1")
			c.Do("SETBIT", "a", "1", "2")
			c.Do("SETBIT", "too", "1", "2", "many")

			c.Do("GETBIT", "a", strconv.Itoa(i))
			c.Do("GETBIT", "e", strconv.Itoa(i))
		}
	})
}

func TestAppend(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "bar")
		c.Do("APPEND", "foo", "more")
		c.Do("GET", "foo")
		c.Do("APPEND", "nosuch", "more")
		c.Do("GET", "nosuch")

		// Failure cases
		c.Do("APPEND")
		c.Do("APPEND", "foo")
	})
}

func TestMove(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "bar")
		c.Do("EXPIRE", "foo", "12345")
		c.Do("MOVE", "foo", "2")
		c.Do("GET", "foo")
		c.Do("TTL", "foo")
		c.Do("SELECT", "2")
		c.Do("GET", "foo")
		c.Do("TTL", "foo")

		// Failure cases
		c.Do("MOVE")
		c.Do("MOVE", "foo")
		// c.Do("MOVE", "foo", "noint")
	})
	// hash key
	testRaw(t, func(c *client) {
		c.Do("HSET", "hash", "key", "value")
		c.Do("EXPIRE", "hash", "12345")
		c.Do("MOVE", "hash", "2")
		c.Do("MGET", "hash", "key")
		c.Do("TTL", "hash")
		c.Do("SELECT", "2")
		c.Do("MGET", "hash", "key")
		c.Do("TTL", "hash")
	})
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "bar")
		// to current DB.
		c.Do("MOVE", "foo", "0")
	})
}
