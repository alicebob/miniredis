// +build int

package main

// Hash keys.

import (
	"testing"
)

func TestHash(t *testing.T) {
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
		c.Do("HSET", "aap", "noot")
		c.Do("HGET", "aap")
		c.Do("HMGET", "aap")
		c.Do("HLEN")
		c.Do("HKEYS")
		c.Do("HVALS")
		c.Do("SET", "str", "I am a string")
		c.Do("HSET", "str", "noot", "mies")
		c.Do("HGET", "str", "noot")
		c.Do("HMGET", "str", "noot")
		c.Do("HLEN", "str")
		c.Do("HKEYS", "str")
		c.Do("HVALS", "str")
		c.Do("HSET")
		c.Do("HSET", "a1")
		c.Do("HSET", "a1", "b")
		c.Do("HSET", "a2", "b", "c", "d")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("HSET", "aap", "noot", "mies", "vuur", "wim")
		c.Do("EXEC")

		c.Do("MULTI")
		c.Do("HSET", "aap", "noot", "mies", "vuur") // uneven arg count
		c.Do("EXEC")
	})
}

func TestHashSetnx(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("HSETNX", "aap", "noot", "mies")
		c.Do("EXISTS", "aap")
		c.Do("HEXISTS", "aap", "noot")

		c.Do("HSETNX", "aap", "noot", "mies2")
		c.Do("HGET", "aap", "noot")

		// failure cases
		c.Do("HSETNX", "aap")
		c.Do("HSETNX", "aap", "noot")
		c.Do("HSETNX", "aap", "noot", "too", "many")
	})
}

func TestHashDelExists(t *testing.T) {
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
		c.Do("HDEL")
		c.Do("HDEL", "aap")
		c.Do("SET", "str", "I am a string")
		c.Do("HDEL", "str", "key")

		c.Do("HEXISTS")
		c.Do("HEXISTS", "aap")
		c.Do("HEXISTS", "aap", "too", "many")
		c.Do("HEXISTS", "str", "field")
	})
}

func TestHashGetall(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("HSET", "aap", "noot", "mies")
		c.Do("HSET", "aap", "vuur", "wim")
		c.DoSorted("HGETALL", "aap")

		c.Do("HGETALL", "nosuch")

		// failure cases
		c.Do("HGETALL")
		c.Do("HGETALL", "too", "many")
		c.Do("SET", "str", "I am a string")
		c.Do("HGETALL", "str")
	})
}

func TestHmset(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("HMSET", "aap", "noot", "mies", "vuur", "zus")
		c.Do("HGET", "aap", "noot")
		c.Do("HGET", "aap", "vuur")
		c.Do("HLEN", "aap")

		// failure cases
		c.Do("HMSET", "aap")
		c.Do("HMSET", "aap", "key")
		c.Do("HMSET", "aap", "key", "value", "odd")
		c.Do("SET", "str", "I am a string")
		c.Do("HMSET", "str", "key", "value")
	})
}

func TestHashIncr(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("HINCRBY", "aap", "noot", "12")
		c.Do("HINCRBY", "aap", "noot", "-13")
		c.Do("HINCRBY", "aap", "noot", "2123")
		c.Do("HGET", "aap", "noot")

		// Simple failure cases.
		c.Do("HINCRBY")
		c.Do("HINCRBY", "aap")
		c.Do("HINCRBY", "aap", "noot")
		c.Do("HINCRBY", "aap", "noot", "noint")
		c.Do("HINCRBY", "aap", "noot", "12", "toomany")
		c.Do("SET", "str", "value")
		c.Do("HINCRBY", "str", "value", "12")
		c.Do("HINCRBY", "aap", "noot", "12")
	})

	testRaw(t, func(c *client) {
		c.Do("HINCRBYFLOAT", "aap", "noot", "12.3")
		c.Do("HINCRBYFLOAT", "aap", "noot", "-13.1")
		c.Do("HINCRBYFLOAT", "aap", "noot", "200")
		c.Do("HGET", "aap", "noot")

		// Simple failure cases.
		c.Do("HINCRBYFLOAT")
		c.Do("HINCRBYFLOAT", "aap")
		c.Do("HINCRBYFLOAT", "aap", "noot")
		c.Do("HINCRBYFLOAT", "aap", "noot", "noint")
		c.Do("HINCRBYFLOAT", "aap", "noot", "12", "toomany")
		c.Do("SET", "str", "value")
		c.Do("HINCRBYFLOAT", "str", "value", "12")
		c.Do("HINCRBYFLOAT", "aap", "noot", "12")
	})
}

func TestHscan(t *testing.T) {
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
		c.Do("HSCAN")
		c.Do("HSCAN", "noint")
		c.Do("HSCAN", "h", "0", "COUNT", "noint")
		c.Do("HSCAN", "h", "0", "COUNT")
		c.Do("HSCAN", "h", "0", "MATCH")
		c.Do("HSCAN", "h", "0", "garbage")
		c.Do("HSCAN", "h", "0", "COUNT", "12", "MATCH", "foo", "garbage")
		// c.Do("HSCAN", "nosuch", "0", "COUNT", "garbage")
		c.Do("SET", "str", "1")
		c.Do("HSCAN", "str", "0")
	})
}

func TestHstrlen(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("HSTRLEN", "hash", "foo")
		c.Do("HSET", "hash", "foo", "bar")
		c.Do("HSTRLEN", "hash", "foo")
		c.Do("HSTRLEN", "hash", "nosuch")
		c.Do("HSTRLEN", "nosuch", "nosuch")

		c.Do("HSTRLEN")
		c.Do("HSTRLEN", "foo")
		c.Do("HSTRLEN", "foo", "baz", "bar")
		c.Do("SET", "str", "1")
		c.Do("HSTRLEN", "str", "bar")
	})
}
