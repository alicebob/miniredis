//go:build int
// +build int

package main

import (
	"strings"
	"testing"
	"time"
)

func TestKeys(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "one", "1")
		c.Do("SET", "two", "2")
		c.Do("SET", "three", "3")
		c.Do("SET", "four", "4")
		c.DoSorted("KEYS", `*o*`)
		c.DoSorted("KEYS", `t??`)
		c.DoSorted("KEYS", `t?*`)
		c.DoSorted("KEYS", `*`)
		c.DoSorted("KEYS", `t*`)
		c.DoSorted("KEYS", `t\*`)
		c.DoSorted("KEYS", `[tf]*`)

		// zero length key
		c.Do("SET", "", "nothing")
		c.Do("GET", "")

		// Simple failure cases
		c.Error("wrong number", "KEYS")
		c.Error("wrong number", "KEYS", "foo", "bar")
	})

	testRaw(t, func(c *client) {
		c.Do("SET", "[one]", "1")
		c.Do("SET", "two", "2")
		c.DoSorted("KEYS", `[\[o]*`)
		c.DoSorted("KEYS", `\[*`)
		c.DoSorted("KEYS", `*o*`)
		c.DoSorted("KEYS", `[]*`) // nothing
	})
}

func TestRandom(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("RANDOMKEY")
		// A random key from a DB with a single key. We can test that.
		c.Do("SET", "one", "1")
		c.Do("RANDOMKEY")

		// Simple failure cases
		c.Error("wrong number", "RANDOMKEY", "bar")
	})
}

func TestUnknownCommand(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Error("unknown", "nosuch")
		c.Error("unknown", "noSUCH")
		c.Error("unknown", "noSUCH", "1", "2", "3")
	})
}

func TestQuit(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("QUIT")
	})
}

func TestExists(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "a", "3")
		c.Do("HSET", "b", "c", "d")
		c.Do("EXISTS", "a", "b")
		c.Do("EXISTS", "a", "b", "q")
		c.Do("EXISTS", "a", "b", "b", "b", "a", "q")

		// Error cases
		c.Error("wrong number", "EXISTS")
	})
}

func TestRename(t *testing.T) {
	testRaw(t, func(c *client) {
		// No 'a' key
		c.Error("no such", "RENAME", "a", "b")

		// Move a key with the TTL.
		c.Do("SET", "a", "3")
		c.Do("EXPIRE", "a", "123")
		c.Do("SET", "b", "12")
		c.Do("RENAME", "a", "b")
		c.Do("EXISTS", "a")
		c.Do("GET", "a")
		c.Do("TYPE", "a")
		c.Do("TTL", "a")
		c.Do("EXISTS", "b")
		c.Do("GET", "b")
		c.Do("TYPE", "b")
		c.Do("TTL", "b")

		// move a key without TTL
		c.Do("SET", "nottl", "3")
		c.Do("RENAME", "nottl", "stillnottl")
		c.Do("TTL", "nottl")
		c.Do("TTL", "stillnottl")

		// Error cases
		c.Error("wrong number", "RENAME")
		c.Error("wrong number", "RENAME", "a")
		c.Error("wrong number", "RENAME", "a", "b", "toomany")
	})
}

func TestRenamenx(t *testing.T) {
	testRaw(t, func(c *client) {
		// No 'a' key
		c.Error("no such", "RENAMENX", "a", "b")

		c.Do("SET", "a", "value")
		c.Do("SET", "str", "value")
		c.Do("RENAMENX", "a", "str")
		c.Do("EXISTS", "a")
		c.Do("EXISTS", "str")
		c.Do("GET", "a")
		c.Do("GET", "str")

		c.Do("RENAMENX", "a", "nosuch")
		c.Do("EXISTS", "a")
		c.Do("EXISTS", "nosuch")

		// Error cases
		c.Error("wrong number", "RENAMENX")
		c.Error("wrong number", "RENAMENX", "a")
		c.Error("wrong number", "RENAMENX", "a", "b", "toomany")
	})
}

func TestScan(t *testing.T) {
	testRaw(t, func(c *client) {
		// No keys yet
		c.Do("SCAN", "0")

		c.Do("SET", "key", "value")
		c.Do("SCAN", "0")
		c.Do("SCAN", "0", "COUNT", "12")
		c.Do("SCAN", "0", "cOuNt", "12")

		c.Do("SET", "anotherkey", "value")
		c.Do("SCAN", "0", "MATCH", "anoth*")
		c.Do("SCAN", "0", "MATCH", "anoth*", "COUNT", "100")
		c.Do("SCAN", "0", "COUNT", "100", "MATCH", "anoth*")

		// Can't really test multiple keys.
		// c.Do("SET", "key2", "value2")
		// c.Do("SCAN", "0")

		// Error cases
		c.Error("wrong number", "SCAN")
		c.Error("invalid cursor", "SCAN", "noint")
		c.Error("not an integer", "SCAN", "0", "COUNT", "noint")
		c.Error("syntax error", "SCAN", "0", "COUNT")
		c.Error("syntax error", "SCAN", "0", "MATCH")
		c.Error("syntax error", "SCAN", "0", "garbage")
		c.Error("syntax error", "SCAN", "0", "COUNT", "12", "MATCH", "foo", "garbage")
	})
}

func TestFastForward(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "key1", "value")
		c.Do("SET", "key", "value", "PX", "100")
		c.DoSorted("KEYS", "*")
		time.Sleep(200 * time.Millisecond)
		c.miniredis.FastForward(200 * time.Millisecond)
		c.DoSorted("KEYS", "*")
	})

	testRaw(t, func(c *client) {
		c.Error("invalid expire", "SET", "key1", "value", "PX", "-100")
		c.Error("invalid expire", "SET", "key2", "value", "EX", "-100")
		c.Error("invalid expire", "SET", "key3", "value", "EX", "0")
		c.DoSorted("KEYS", "*")

		c.Do("SET", "key4", "value")
		c.DoSorted("KEYS", "*")
		c.Do("EXPIRE", "key4", "-100")
		c.DoSorted("KEYS", "*")

		c.Do("SET", "key4", "value")
		c.DoSorted("KEYS", "*")
		c.Do("EXPIRE", "key4", "0")
		c.DoSorted("KEYS", "*")
	})
}

func TestProto(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ECHO", strings.Repeat("X", 1<<24))
	})
}

func TestSwapdb(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "key1", "val1")
		c.Do("SWAPDB", "0", "1")
		c.Do("SELECT", "1")
		c.Do("GET", "key1")

		c.Do("SWAPDB", "1", "1")
		c.Do("GET", "key1")

		c.Error("wrong number", "SWAPDB")
		c.Error("wrong number", "SWAPDB", "1")
		c.Error("wrong number", "SWAPDB", "1", "2", "3")
		c.Error("invalid first", "SWAPDB", "foo", "2")
		c.Error("invalid second", "SWAPDB", "1", "bar")
		c.Error("invalid first", "SWAPDB", "foo", "bar")
		c.Error("out of range", "SWAPDB", "-1", "2")
		c.Error("out of range", "SWAPDB", "1", "-2")
		// c.Do("SWAPDB", "1", "1000") // miniredis has no upperlimit
	})

	// SWAPDB with transactions
	testRaw2(t, func(c1, c2 *client) {
		c1.Do("SET", "foo", "foooooo")

		c1.Do("MULTI")
		c1.Do("SWAPDB", "0", "2")
		c1.Do("GET", "foo")
		c2.Do("GET", "foo")

		c1.Do("EXEC")
		c1.Do("GET", "foo")
		c2.Do("GET", "foo")
	})
}

func TestDel(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "one", "1")
		c.Do("SET", "two", "2")
		c.Do("SET", "three", "3")
		c.Do("SET", "four", "4")
		c.Do("DEL", "one")
		c.DoSorted("KEYS", "*")

		c.Do("DEL", "twoooo")
		c.DoSorted("KEYS", "*")

		c.Do("DEL", "two", "four")
		c.DoSorted("KEYS", "*")

		c.Error("wrong number", "DEL")
		c.DoSorted("KEYS", "*")
	})
}

func TestUnlink(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "one", "1")
		c.Do("SET", "two", "2")
		c.Do("SET", "three", "3")
		c.Do("SET", "four", "4")
		c.Do("UNLINK", "one")
		c.DoSorted("KEYS", "*")

		c.Do("UNLINK", "twoooo")
		c.DoSorted("KEYS", "*")

		c.Do("UNLINK", "two", "four")
		c.DoSorted("KEYS", "*")

		c.Error("wrong number", "UNLINK")
		c.DoSorted("KEYS", "*")
	})
}

func TestTouch(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "a", "some value")
		c.Do("TOUCH", "a")
		c.Do("GET", "a")
		c.Do("TTL", "a")

		c.Do("TOUCH", "a", "foobar", "a")

		c.Error("wrong number", "TOUCH")
	})
}

func TestPersist(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "bar")
		c.Do("EXPIRE", "foo", "12")
		c.Do("TTL", "foo")
		c.Do("PERSIST", "foo")
		c.Do("TTL", "foo")
	})
}

func TestCopy(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Error("wrong number", "COPY")
		c.Error("wrong number", "COPY", "a")

		c.Do("SET", "a", "1")
		c.Do("COPY", "a", "b") // returns 1 - successfully copied
		c.Do("EXISTS", "b")
		c.Do("COPY", "nonexistent", "c") // returns 1 - not successfully copied
		c.Do("RENAME", "b", "c")         // rename the copied key
	})
}
