package main

import (
	"testing"
)

func TestServer(t *testing.T) {
	skip(t)
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "bar")
		c.Do("SET", "baz", "bak")
		c.Do("XADD", "planets", "123-456", "name", "Earth")
		c.Do("DBSIZE")
		c.Do("SELECT", "2")
		c.Do("DBSIZE")
		c.Do("SET", "baz", "bak")

		c.Do("SELECT", "0")
		c.Do("FLUSHDB")
		c.Do("DBSIZE")

		c.Do("SELECT", "2")
		c.Do("DBSIZE")
		c.Do("FLUSHALL")
		c.Do("DBSIZE")

		c.Do("FLUSHDB", "aSyNc")
		c.Do("FLUSHALL", "AsYnC")

		// Failure cases
		c.Error("wrong number", "DBSIZE", "foo")
		c.Error("syntax error", "FLUSHDB", "foo")
		c.Error("syntax error", "FLUSHALL", "foo")
		c.Error("syntax error", "FLUSHDB", "ASYNC", "foo")
		c.Error("syntax error", "FLUSHDB", "ASYNC", "ASYNC")
		c.Error("syntax error", "FLUSHALL", "ASYNC", "foo")
	})

	testRaw(t, func(c *client) {
		c.Do("SET", "plain", "hello")
		c.DoLoosely("MEMORY", "USAGE", "plain")
		c.Do("LPUSH", "alist", "hello", "42")
		c.DoLoosely("MEMORY", "USAGE", "alist")
		c.Do("HSET", "ahash", "key", "value")
		c.DoLoosely("MEMORY", "USAGE", "ahash")
		c.Do("ZADD", "asset", "0", "line")
		c.DoLoosely("MEMORY", "USAGE", "asset")
		c.Do("PFADD", "ahll", "123")
		c.DoLoosely("MEMORY", "USAGE", "ahll")
		c.Do("XADD", "astream", "0-1", "name", "Mercury")
		c.DoLoosely("MEMORY", "USAGE", "astream")
	})
}

func TestServerTLS(t *testing.T) {
	skip(t)
	testTLS(t, func(c *client) {
		c.Do("PING", "foo")

		c.Do("SET", "foo", "bar")
		c.Do("GET", "foo")
	})
}
