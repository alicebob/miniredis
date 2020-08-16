// +build int

package main

import (
	"testing"
)

func TestServer(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "bar")
		c.Do("SET", "baz", "bak")
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
		c.Do("DBSIZE", "foo")
		c.Do("FLUSHDB", "foo")
		c.Do("FLUSHALL", "foo")
		c.Do("FLUSHDB", "ASYNC", "foo")
		c.Do("FLUSHDB", "ASYNC", "ASYNC")
		c.Do("FLUSHALL", "ASYNC", "foo")
	})
}

func TestServerTLS(t *testing.T) {
	testTLS(t, func(c *client) {
		c.Do("PING", "foo")

		c.Do("SET", "foo", "bar")
		c.Do("GET", "foo")
	})
}
