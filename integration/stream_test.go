// +build int

package main

import (
	"testing"
)

func TestStream(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("XADD",
			"planets",
			"0-1",
			"name", "Mercury",
		)
		c.DoLoosely("XADD",
			"planets",
			"*",
			"name", "Venus",
		)
		c.Do("XADD",
			"planets",
			"18446744073709551000-0",
			"name", "Earth",
		)
		c.Do("XADD",
			"planets",
			"18446744073709551000-0", // <-- duplicate
			"name", "Earth",
		)
		c.Do("XLEN", "planets")
		c.Do("RENAME", "planets", "planets2")
		c.Do("DEL", "planets2")
		c.Do("XLEN", "planets")

		c.Do("XADD",
			"planets",
			"1000",
			"name", "Mercury",
			"ignored", // <-- not an even number of keys
		)
		c.Do("XADD",
			"newplanets",
			"0", // <-- invalid key
			"foo", "bar",
		)
		c.Do("XADD", "newplanets", "123-123") // no args
		c.Do("XADD", "newplanets", "123-bar", "foo", "bar")
		c.Do("XADD", "newplanets", "bar-123", "foo", "bar")
		c.Do("XADD", "newplanets", "123-123-123", "foo", "bar")
		c.Do("SET", "str", "I am a string")
		// c.Do("XADD", "str", "1000", "foo", "bar")
		// c.Do("XADD", "str", "invalid-key", "foo", "bar")

		c.Do("XADD", "planets")
		c.Do("XADD")
	})

	testRaw(t, func(c *client) {
		c.Do("XADD", "planets", "MAXLEN", "4", "456-1", "name", "Mercury")
		c.Do("XADD", "planets", "MAXLEN", "4", "456-2", "name", "Mercury")
		c.Do("XADD", "planets", "MAXLEN", "4", "456-3", "name", "Mercury")
		c.Do("XADD", "planets", "MAXLEN", "4", "456-4", "name", "Mercury")
		c.Do("XADD", "planets", "MAXLEN", "4", "456-5", "name", "Mercury")
		c.Do("XADD", "planets", "MAXLEN", "4", "456-6", "name", "Mercury")
		c.Do("XLEN", "planets")
		c.Do("XADD", "planets", "MAXLEN", "~", "4", "456-7", "name", "Mercury")

		c.Do("XADD", "planets", "MAXLEN", "!", "4", "*", "name", "Mercury")
		c.Do("XADD", "planets", "MAXLEN", " ~", "4", "*", "name", "Mercury")
		c.Do("XADD", "planets", "MAXLEN", "-4", "*", "name", "Mercury")
		c.Do("XADD", "planets", "MAXLEN", "", "*", "name", "Mercury")
		c.Do("XADD", "planets", "MAXLEN", "!", "four", "*", "name", "Mercury")
		c.Do("XADD", "planets", "MAXLEN", "~", "four")
		c.Do("XADD", "planets", "MAXLEN", "~")
		c.Do("XADD", "planets", "MAXLEN")

		c.Do("XADD", "planets", "MAXLEN", "0", "456-8", "name", "Mercury")
		c.Do("XLEN", "planets")

		c.Do("SET", "str", "I am a string")
		c.Do("XADD", "str", "MAXLEN", "four", "*", "foo", "bar")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("XADD", "planets", "0-1", "name", "Mercury")
		c.Do("EXEC")

		c.Do("MULTI")
		c.Do("XADD", "newplanets", "123-123") // no args
		c.Do("EXEC")

		c.Do("MULTI")
		c.Do("XADD", "planets", "foo-bar", "name", "Mercury")
		c.Do("EXEC")

		c.Do("MULTI")
		c.Do("XADD", "planets", "MAXLEN", "four", "*", "name", "Mercury")
		c.Do("EXEC")
	})
}

func TestStreamRange(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("XADD",
			"ordplanets",
			"0-1",
			"name", "Mercury",
			"greek-god", "Hermes",
		)
		c.Do("XADD",
			"ordplanets",
			"1-0",
			"name", "Venus",
			"greek-god", "Aphrodite",
		)
		c.Do("XADD",
			"ordplanets",
			"2-1",
			"greek-god", "",
			"name", "Earth",
		)
		c.Do("XADD",
			"ordplanets",
			"3-0",
			"name", "Mars",
			"greek-god", "Ares",
		)
		c.Do("XADD",
			"ordplanets",
			"4-1",
			"greek-god", "Dias",
			"name", "Jupiter",
		)
		c.Do("XRANGE", "ordplanets", "-", "+")
		c.Do("XRANGE", "ordplanets", "+", "-")
		c.Do("XRANGE", "ordplanets", "-", "99")
		c.Do("XRANGE", "ordplanets", "0", "4")
		c.Do("XRANGE", "ordplanets", "2", "2")
		c.Do("XRANGE", "ordplanets", "2-0", "2-1")
		c.Do("XRANGE", "ordplanets", "2-1", "2-1")
		c.Do("XRANGE", "ordplanets", "2-1", "2-2")
		c.Do("XRANGE", "ordplanets", "0", "1-0")
		c.Do("XRANGE", "ordplanets", "0", "1-99")
		c.Do("XRANGE", "ordplanets", "0", "2", "COUNT", "1")
		c.Do("XRANGE", "ordplanets", "1-42", "3-42", "COUNT", "1")

		c.Do("XREVRANGE", "ordplanets", "+", "-")
		c.Do("XREVRANGE", "ordplanets", "-", "+")
		c.Do("XREVRANGE", "ordplanets", "4", "0")
		c.Do("XREVRANGE", "ordplanets", "2", "2")
		c.Do("XREVRANGE", "ordplanets", "2-1", "2-0")
		c.Do("XREVRANGE", "ordplanets", "2-1", "2-1")
		c.Do("XREVRANGE", "ordplanets", "2-2", "2-1")
		c.Do("XREVRANGE", "ordplanets", "1-0", "0")
		c.Do("XREVRANGE", "ordplanets", "3-42", "1-0", "COUNT", "2")
		c.Do("DEL", "ordplanets")

		// failure cases
		c.Do("XRANGE")
		c.Do("XRANGE", "foo")
		c.Do("XRANGE", "foo", "1")
		c.Do("XRANGE", "foo", "2", "3", "toomany")
		c.Do("XRANGE", "foo", "2", "3", "COUNT", "noint")
		c.Do("XRANGE", "foo", "2", "3", "COUNT", "1", "toomany")
		c.Do("XRANGE", "foo", "-", "noint")
		c.Do("SET", "str", "I am a string")
		c.Do("XRANGE", "str", "-", "+")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("XADD",
			"ordplanets",
			"0-1",
			"name", "Mercury",
			"greek-god", "Hermes",
		)
		c.Do("XLEN", "ordplanets")
		c.Do("XRANGE", "ordplanets", "+", "-")
		c.Do("XRANGE", "ordplanets", "+", "-", "COUNT", "FOOBAR")
		c.Do("EXEC")
		c.Do("XLEN", "ordplanets")

		c.Do("MULTI")
		c.Do("XRANGE", "ordplanets", "+", "foo")
		c.Do("EXEC")

		c.Do("MULTI")
		c.Do("XRANGE", "ordplanets", "+")
		c.Do("EXEC")

		c.Do("MULTI")
		c.Do("XADD", "ordplanets", "123123-123", "name", "Mercury")
		c.Do("XADD", "ordplanets", "invalid", "name", "Mercury")
		c.Do("EXEC")
		c.Do("XLEN", "ordplanets")
	})
}

func TestStreamGroup(t *testing.T) {
	testRaw(t, func(c *client) {
		c.DoLoosely("XGROUP", "CREATE", "planets", "processing", "$")
		c.Do("XGROUP", "CREATE", "planets", "processing", "$", "MKSTREAM")
		// succNoResultCheck("XINFO", "STREAM", "planets"),
		c.DoLoosely("XINFO", "STREAMMM")
		c.DoLoosely("XINFO", "STREAM", "foo")
		c.DoLoosely("XINFO")
		c.Do("XADD", "planets", "0-1", "name", "Mercury")
		c.Do("XLEN", "planets")
		c.Do("XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">")
		c.Do("XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">")
		c.Do("XACK", "planets", "processing", "0-1")
		c.Do("XDEL", "planets", "0-1")
	})
}

func TestStreamRead(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("XADD",
			"ordplanets",
			"0-1",
			"name", "Mercury",
			"greek-god", "Hermes",
		)
		c.Do("XADD",
			"ordplanets",
			"1-0",
			"name", "Venus",
			"greek-god", "Aphrodite",
		)
		c.Do("XADD",
			"ordplanets",
			"2-1",
			"greek-god", "",
			"name", "Earth",
		)
		c.Do("XADD",
			"ordplanets",
			"3-0",
			"name", "Mars",
			"greek-god", "Ares",
		)
		c.Do("XADD",
			"ordplanets",
			"4-1",
			"greek-god", "Dias",
			"name", "Jupiter",
		)
		c.Do("XADD", "ordplanets2", "0-1", "name", "Mercury", "greek-god", "Hermes", "idx", "1")
		c.Do("XADD", "ordplanets2", "1-0", "name", "Venus", "greek-god", "Aphrodite", "idx", "2")
		c.Do("XADD", "ordplanets2", "2-1", "name", "Earth", "greek-god", "", "idx", "3")
		c.Do("XADD", "ordplanets2", "3-0", "greek-god", "Ares", "name", "Mars", "idx", "4")
		c.Do("XADD", "ordplanets2", "4-1", "name", "Jupiter", "greek-god", "Dias", "idx", "5")

		c.Do("XREAD", "STREAMS", "ordplanets", "0")
		c.Do("XREAD", "STREAMS", "ordplanets", "2")
		c.Do("XREAD", "STREAMS", "ordplanets", "ordplanets2", "0", "0")
		c.Do("XREAD", "STREAMS", "ordplanets", "ordplanets2", "2", "0")
		c.Do("XREAD", "STREAMS", "ordplanets", "ordplanets2", "0", "2")
		c.Do("XREAD", "STREAMS", "ordplanets", "ordplanets2", "1", "3")

		// failure cases
		c.Do("XREAD")
		c.Do("XREAD", "STREAMS", "foo")
		c.Do("XREAD", "STREAMS", "ordplanets")
		c.Do("XREAD", "STREAMS", "ordplanets", "foo", "0")
		c.Do("XREAD", "COUNT")
		c.Do("XREAD", "COUNT", "notint")
		c.Do("XREAD", "COUNT", "10") // No streams
		c.Do("XREAD", "STREAMS", "foo", "notint")
	})
}
