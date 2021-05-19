// +build int

package main

import (
	"sync"
	"testing"
	"time"
)

func TestStream(t *testing.T) {
	t.Run("XADD", func(t *testing.T) {
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
	})

	t.Run("transactions", func(t *testing.T) {
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
	})

	t.Run("XDEL", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("XDEL", "newplanets", "123-123")
			c.Do("XADD", "newplanets", "123-123", "foo", "bar")
			c.Do("XADD", "newplanets", "123-124", "baz", "bak")
			c.Do("XADD", "newplanets", "123-125", "bal", "bag")
			c.Do("XDEL", "newplanets", "123-123", "123-125", "123-123")
			c.Do("XREAD", "STREAMS", "newplanets", "0")
			c.Do("XDEL", "newplanets", "123-123")
			c.Do("XREAD", "STREAMS", "newplanets", "0")
			c.Do("XDEL", "notexisting", "123-123")
			c.Do("XREAD", "STREAMS", "newplanets", "0")

			c.Do("XADD", "gaps", "400-400", "foo", "bar")
			c.Do("XADD", "gaps", "400-600", "foo", "bar")
			c.Do("XDEL", "gaps", "400-500")
			c.Do("XREAD", "STREAMS", "newplanets", "0")

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

	t.Run("XINFO", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("XADD", "planets", "0-1", "name", "Mercury")
			// c.DoLoosely("XINFO", "STREAM", "planets")

			c.Error("syntax error", "XINFO", "STREAMMM")
			c.Error("no such key", "XINFO", "STREAM", "foo")
			c.Error("wrong number", "XINFO")
			c.Do("SET", "scalar", "foo")
			c.Error("wrong kind", "XINFO", "STREAM", "scalar")
		})
	})

	t.Run("XREAD", func(t *testing.T) {
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
			c.Do("XREAD", "STREAMS", "ordplanets", "ordplanets2", "0", "999")
			c.Do("XREAD", "COUNT", "1", "STREAMS", "ordplanets", "ordplanets2", "0", "0")

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

		testRaw2(t, func(c, c2 *client) {
			c.Do("XADD", "pl", "55-88", "name", "Mercury")
			// something is available: doesn't block
			c.Do("XREAD", "BLOCK", "10", "STREAMS", "pl", "0")
			c.Do("XREAD", "BLOCK", "0", "STREAMS", "pl", "0")

			// blocks
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				c.Do("XREAD", "BLOCK", "1000", "STREAMS", "pl", "60")
				wg.Done()
			}()
			time.Sleep(10 * time.Millisecond)
			c2.Do("XADD", "pl", "60-1", "name", "Mercury")
			wg.Wait()

			// timeout
			c.Do("XREAD", "BLOCK", "10", "STREAMS", "pl", "70")

			c.Error("not an int", "XREAD", "BLOCK", "foo", "STREAMS", "pl", "0")
			c.Error("negative", "XREAD", "BLOCK", "-12", "STREAMS", "pl", "0")
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
	t.Run("XGROUP", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Error("to exist", "XGROUP", "CREATE", "planets", "processing", "$")
			c.Do("XADD", "planets", "123-500", "foo", "bar")
			c.Do("XGROUP", "CREATE", "planets", "processing", "$")
			c.Error("already exist", "XGROUP", "CREATE", "planets", "processing", "$")
		})
	})

	t.Run("XREADGROUP", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("XGROUP", "CREATE", "planets", "processing", "$", "MKSTREAM")
			// succNoResultCheck("XINFO", "STREAM", "planets"),
			c.Do("XADD", "planets", "42-1", "name", "Mercury")
			c.Do("XADD", "planets", "42-2", "name", "Neptune")
			c.Do("XLEN", "planets")
			c.Do("XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">")
			c.Do("XREADGROUP", "GROUP", "processing", "alice", "COUNT", "1", "STREAMS", "planets", ">")
			c.Do("XREADGROUP", "GROUP", "processing", "alice", "COUNT", "999", "STREAMS", "planets", ">")
			c.Do("XACK", "planets", "processing", "42-1")
			c.Do("XDEL", "planets", "42-1")
			c.Do("XGROUP", "CREATE", "planets", "newcons", "$", "MKSTREAM")

			c.Do("XREADGROUP", "GROUP", "processing", "bob", "STREAMS", "planets", ">")
			c.Do("XADD", "planets", "42-3", "name", "Venus")
			c.Do("XREADGROUP", "GROUP", "processing", "bob", "STREAMS", "planets", "42-1")
			c.Do("XREADGROUP", "GROUP", "processing", "bob", "STREAMS", "planets", "42-9")
			c.Error("stream ID", "XREADGROUP", "GROUP", "processing", "bob", "STREAMS", "planets", "foo")

			// errors
			c.Error("wrong number", "XREADGROUP")
			c.Error("wrong number", "XREADGROUP", "GROUP")
			c.Error("wrong number", "XREADGROUP", "foo")
			c.Error("wrong number", "XREADGROUP", "GROUP", "foo")
			c.Error("wrong number", "XREADGROUP", "GROUP", "foo", "bar")
			c.Error("wrong number", "XREADGROUP", "GROUP", "foo", "bar", "ZTREAMZ")
			c.Error("wrong number", "XREADGROUP", "GROUP", "foo", "bar", "STREAMS", "foo")
			c.Error("Unbalanced", "XREADGROUP", "GROUP", "foo", "bar", "STREAMS", "foo", "bar", ">")
			c.Error("syntax error", "XREADGROUP", "_____", "foo", "bar", "STREAMS", "foo", ">")
			c.Error("consumer group", "XREADGROUP", "GROUP", "nosuch", "alice", "STREAMS", "planets", ">")
			c.Error("consumer group", "XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "nosuchplanets", ">")
			c.Do("SET", "scalar", "bar")
			c.Error("wrong kind", "XGROUP", "CREATE", "scalar", "processing", "$", "MKSTREAM")
			c.Error("BUSYGROUP", "XGROUP", "CREATE", "planets", "processing", "$", "MKSTREAM")
		})
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
			c.Do("SET", "scalar", "bar")
			c.Error("wrong kind", "XACK", "scalar", "processing", "123-456")
		})
	})

	t.Run("XPENDING", func(t *testing.T) {
		// summary mode
		testRaw(t, func(c *client) {
			c.Do("XGROUP", "CREATE", "planets", "processing", "$", "MKSTREAM")
			c.Do("XADD", "planets", "4000-1", "name", "Mercury")
			c.Do("XADD", "planets", "4000-2", "name", "Venus")
			c.Do("XADD", "planets", "4000-3", "name", "not Pluto")
			c.Do("XADD", "planets", "4000-4", "name", "Mars")
			c.Do("XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">")
			c.Do("XPENDING", "planets", "processing")
			c.Do("XACK", "planets", "processing", "4000-4")
			c.Do("XPENDING", "planets", "processing")
			c.Do("XACK", "planets", "processing", "4000-1")
			c.Do("XACK", "planets", "processing", "4000-2")
			c.Do("XACK", "planets", "processing", "4000-3")
			c.Do("XPENDING", "planets", "processing")

			// more consumers
			c.Do("XADD", "planets", "4000-5", "name", "Earth")
			c.Do("XADD", "planets", "4000-6", "name", "Neptune")
			c.Do("XREADGROUP", "GROUP", "processing", "alice", "COUNT", "1", "STREAMS", "planets", ">")
			c.Do("XREADGROUP", "GROUP", "processing", "bob", "COUNT", "1", "STREAMS", "planets", ">")
			c.Do("XPENDING", "planets", "processing")

			// no entries doesn't show up in pending
			c.Do("XREADGROUP", "GROUP", "processing", "eve", "COUNT", "1", "STREAMS", "planets", ">")
			c.Do("XPENDING", "planets", "processing")

			c.Error("consumer group", "XPENDING", "foo", "processing")
			c.Error("consumer group", "XPENDING", "planets", "foo")

			// error cases
			c.Error("wrong number", "XPENDING")
			c.Error("wrong number", "XPENDING", "planets")
			c.Error("syntax", "XPENDING", "planets", "processing", "too many")
		})

		// full mode
		testRaw(t, func(c *client) {
			c.Do("XGROUP", "CREATE", "planets", "processing", "$", "MKSTREAM")
			c.Do("XADD", "planets", "4000-1", "name", "Mercury")
			c.Do("XADD", "planets", "4000-2", "name", "Venus")
			c.Do("XADD", "planets", "4000-3", "name", "not Pluto")
			c.Do("XADD", "planets", "4000-4", "name", "Mars")
			c.Do("XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">")

			c.DoLoosely("XPENDING", "planets", "processing", "-", "+", "999")
			c.DoLoosely("XPENDING", "planets", "processing", "4000-2", "+", "999")
			c.DoLoosely("XPENDING", "planets", "processing", "-", "4000-3", "999")
			c.DoLoosely("XPENDING", "planets", "processing", "-", "+", "1")
			c.DoLoosely("XPENDING", "planets", "processing", "-", "+", "0")
			c.DoLoosely("XPENDING", "planets", "processing", "-", "+", "-1")

			c.Do("XADD", "planets", "4000-5", "name", "Earth")
			c.Do("XREADGROUP", "GROUP", "processing", "bob", "STREAMS", "planets", ">")
			c.DoLoosely("XPENDING", "planets", "processing", "-", "+", "999")
			c.DoLoosely("XPENDING", "planets", "processing", "-", "+", "999", "bob")
			c.DoLoosely("XPENDING", "planets", "processing", "-", "+", "999", "eve")

			// update delivery counts (which we can't test thanks to the time field)
			c.Do("XREADGROUP", "GROUP", "processing", "bob", "STREAMS", "planets", "99")
			c.DoLoosely("XPENDING", "planets", "processing", "-", "+", "999", "bob")

			c.Error("Invalid", "XPENDING", "planets", "processing", "foo", "+", "999")
			c.Error("Invalid", "XPENDING", "planets", "processing", "-", "foo", "999")
			c.Error("not an integer", "XPENDING", "planets", "processing", "-", "+", "foo")
		})
	})

	testRESP3(t, func(c *client) {
		c.DoLoosely("XINFO", "STREAM", "foo")
	})
}
