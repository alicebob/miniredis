// +build int

package main

import (
	"testing"
)

func TestEcho(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ECHO", "hello world")
		c.Do("ECHO", "42")
		c.Do("ECHO", "3.1415")
		c.Do("ECHO", "hello", "world")
		c.Do("ECHO")
		c.Do("eChO", "hello", "world")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("ECHO", "hi")
		c.Do("EXEC")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("ECHO")
		c.Do("EXEC")
	})
}

func TestPing(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("PING")
		c.Do("PING", "hello world")
		c.Do("PING", "hello", "world")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("PING", "hi")
		c.Do("EXEC")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("PING", "hi again")
		c.Do("EXEC")
	})
}

func TestSelect(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "bar")
		c.Do("GET", "foo")
		c.Do("SELECT", "2")
		c.Do("GET", "foo")
		c.Do("SET", "foo", "bar2")
		c.Do("GET", "foo")

		c.Do("SELECT")
		c.Do("SELECT", "-1")
		c.Do("SELECT", "aap")
		c.Do("SELECT", "1", "2")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("SET", "foo", "bar")
		c.Do("GET", "foo")
		c.Do("SELECT", "2")
		c.Do("GET", "foo")
		c.Do("SET", "foo", "bar2")
		c.Do("GET", "foo")
		c.Do("EXEC")
		c.Do("GET", "foo")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("SELECT", "-1")
		c.Do("EXEC")
	})
}

func TestAuth(t *testing.T) {
	testAuth(t,
		"supersecret",
		func(c *client) {
			c.Do("PING")
			c.Do("SET", "foo", "bar")
			c.Do("SET")
			c.Do("SET", "foo", "bar", "baz")
			c.Do("GET", "foo")
			c.Do("AUTH")
			c.Do("AUTH", "nosecret")
			c.Do("AUTH", "nosecret", "bar")
			c.Do("AUTH", "nosecret", "bar", "bar")
			c.Do("AUTH", "supersecret")
			c.Do("SET", "foo", "bar")
			c.Do("GET", "foo")
		},
	)

	testUserAuth(t,
		map[string]string{
			"agent1": "supersecret",
			"agent2": "dragon",
		},
		func(c *client) {
			c.Do("PING")
			c.Do("SET", "foo", "bar")
			c.Do("SET")
			c.Do("SET", "foo", "bar", "baz")
			c.Do("GET", "foo")
			c.Do("AUTH")
			c.Do("AUTH", "nosecret")
			c.Do("AUTH", "agent100", "supersecret")
			c.Do("AUTH", "agent100", "supersecret", "supersecret")
			c.Do("AUTH", "agent1", "bzzzt")
			c.Do("AUTH", "agent1", "supersecret")
			c.Do("SET", "foo", "bar")
			c.Do("GET", "foo")

			// go back to invalid user
			c.Do("AUTH", "agent100", "supersecret")
			c.Do("GET", "foo") // still agent1
		},
	)

	testRaw(t, func(c *client) {
		c.Do("AUTH")
		c.Do("AUTH", "foo")
		c.Do("AUTH", "foo", "bar")
		c.Do("AUTH", "foo", "bar", "bar")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("AUTH", "apassword")
		c.Do("EXEC")
	})
}
