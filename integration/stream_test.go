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
		c.Error("ID specified", "XADD",
			"planets",
			"18446744073709551000-0", // <-- duplicate
			"name", "Earth",
		)
		c.Do("XLEN", "planets")
		c.Do("RENAME", "planets", "planets2")
		c.Do("DEL", "planets2")
		c.Do("XLEN", "planets")

		// error cases
		c.Error("wrong number", "XADD",
			"planets",
			"1000",
			"name", "Mercury",
			"ignored", // <-- not an even number of keys
		)
		c.Error("ID specified", "XADD",
			"newplanets",
			"0", // <-- invalid key
			"foo", "bar",
		)
		c.Error("wrong number", "XADD", "newplanets", "123-123") // no args
		c.Error("stream ID", "XADD", "newplanets", "123-bar", "foo", "bar")
		c.Error("stream ID", "XADD", "newplanets", "bar-123", "foo", "bar")
		c.Error("stream ID", "XADD", "newplanets", "123-123-123", "foo", "bar")
		c.Do("SET", "str", "I am a string")
		// c.Do("XADD", "str", "1000", "foo", "bar")
		// c.Do("XADD", "str", "invalid-key", "foo", "bar")

		c.Error("wrong number", "XADD", "planets")
		c.Error("wrong number", "XADD")
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

		c.Error("not an integer", "XADD", "planets", "MAXLEN", "!", "4", "*", "name", "Mercury")
		c.Error("not an integer", "XADD", "planets", "MAXLEN", " ~", "4", "*", "name", "Mercury")
		c.Error("MAXLEN argument", "XADD", "planets", "MAXLEN", "-4", "*", "name", "Mercury")
		c.Error("not an integer", "XADD", "planets", "MAXLEN", "", "*", "name", "Mercury")
		c.Error("not an integer", "XADD", "planets", "MAXLEN", "!", "four", "*", "name", "Mercury")
		c.Error("not an integer", "XADD", "planets", "MAXLEN", "~", "four")
		c.Error("wrong number", "XADD", "planets", "MAXLEN", "~")
		c.Error("wrong number", "XADD", "planets", "MAXLEN")

		c.Do("XADD", "planets", "MAXLEN", "0", "456-8", "name", "Mercury")
		c.Do("XLEN", "planets")

		c.Do("SET", "str", "I am a string")
		c.Error("not an integer", "XADD", "str", "MAXLEN", "four", "*", "foo", "bar")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("XADD", "planets", "0-1", "name", "Mercury")
		c.Do("EXEC")

		c.Do("MULTI")
		c.Error("wrong number", "XADD", "newplanets", "123-123") // no args
		c.Error("discarded", "EXEC")

		c.Do("MULTI")
		c.Do("XADD", "planets", "foo-bar", "name", "Mercury")
		c.Do("EXEC")

		c.Do("MULTI")
		c.Do("XADD", "planets", "MAXLEN", "four", "*", "name", "Mercury")
		c.Do("EXEC")
	})

	t.Run("XDEL", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("XDEL", "newplanets", "123-123")
			c.Do("XADD", "newplanets", "123-123", "foo", "bar")
			c.Do("XADD", "newplanets", "123-124", "baz", "bak")
			c.Do("XADD", "newplanets", "123-125", "bal", "bag")
			c.Do("XDEL", "newplanets", "123-123", "123-125", "123-123")
			c.Do("XDEL", "newplanets", "123-123")
			c.Do("XDEL", "notexisting", "123-123")

			c.Do("XADD", "gaps", "400-400", "foo", "bar")
			c.Do("XADD", "gaps", "400-600", "foo", "bar")
			c.Do("XDEL", "gaps", "400-500")

			// errors
			c.Do("XADD", "existing", "123-123", "foo", "bar")
			c.Error("wrong number", "XDEL")             // no key
			c.Error("wrong number", "XDEL", "existing") // no id
			c.Error("Invalid stream ID", "XDEL", "existing", "aa-bb")
			c.Do("XDEL", "notexisting", "aa-bb") // invalid id

			c.Do("MULTI")
			c.Do("XDEL", "existing", "aa-bb")
			c.Do("EXEC")
		})
	})

	t.Run("FLUSHALL", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("XADD", "planets", "0-1", "name", "Mercury")
			c.Do("XGROUP", "CREATE", "planets", "universe", "$")
			c.Do("FLUSHALL")
			c.Do("XREAD", "STREAMS", "planets", "0")
			c.Error("consumer group", "XREADGROUP", "GROUP", "universe", "alice", "STREAMS", "planets", ">")
		})
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
		c.Error("wrong number", "XRANGE")
		c.Error("wrong number", "XRANGE", "foo")
		c.Error("wrong number", "XRANGE", "foo", "1")
		c.Error("syntax error", "XRANGE", "foo", "2", "3", "toomany")
		c.Error("not an integer", "XRANGE", "foo", "2", "3", "COUNT", "noint")
		c.Error("syntax error", "XRANGE", "foo", "2", "3", "COUNT", "1", "toomany")
		c.Error("stream ID", "XRANGE", "foo", "-", "noint")
		c.Do("SET", "str", "I am a string")
		c.Error("wrong kind", "XRANGE", "str", "-", "+")
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
		c.Error("wrong number", "XRANGE", "ordplanets", "+")
		c.Error("discarded", "EXEC")

		c.Do("MULTI")
		c.Do("XADD", "ordplanets", "123123-123", "name", "Mercury")
		c.Do("XDEL", "ordplanets", "123123-123")
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

		// errors
		c.Error("consumer group", "XREADGROUP", "GROUP", "nosuch", "alice", "STREAMS", "planets", ">")
		c.Error("consumer group", "XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "nosuchplanets", ">")
	})

	t.Run("XACK", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("XGROUP", "CREATE", "planets", "processing", "$", "MKSTREAM")
			c.Do("XADD", "planets", "4000-1", "name", "Mercury")
			c.Do("XADD", "planets", "4000-2", "name", "Venus")
			c.Do("XADD", "planets", "4000-3", "name", "not Pluto")
			c.Do("XADD", "planets", "4000-4", "name", "Mars")
			c.Do("XREADGROUP", "GROUP", "processing", "alice", "COUNT", "1", "STREAMS", "planets", ">")
			c.Do("XACK", "planets", "processing", "4000-2", "4000-3")
			c.Do("XACK", "planets", "processing", "4000-4")
			c.Do("XACK", "planets", "processing", "2000-1")

			c.Do("XACK", "nosuch", "processing", "0-1")
			c.Do("XACK", "planets", "nosuch", "0-1")

			// error cases
			c.Error("wrong number", "XACK")
			c.Error("wrong number", "XACK", "planets")
			c.Error("wrong number", "XACK", "planets", "processing")
			c.Error("Invalid stream", "XACK", "planets", "processing", "invalid")
		})
	})

	testRESP3(t, func(c *client) {
		c.DoLoosely("XINFO", "STREAM", "foo")
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
		c.Error("wrong number", "XREAD")
		c.Error("wrong number", "XREAD", "STREAMS")
		c.Error("wrong number", "XREAD", "STREAMS", "foo")
		c.Do("XREAD", "STREAMS", "foo", "0")
		c.Error("wrong number", "XREAD", "STREAMS", "ordplanets")
		c.Error("Unbalanced XREAD", "XREAD", "STREAMS", "ordplanets", "foo", "0")
		c.Error("wrong number", "XREAD", "COUNT")
		c.Error("wrong number", "XREAD", "COUNT", "notint")
		c.Error("wrong number", "XREAD", "COUNT", "10") // No streams
		c.Error("stream ID", "XREAD", "STREAMS", "foo", "notint")
	})
}
