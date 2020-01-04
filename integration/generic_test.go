// +build int

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestKeys(t *testing.T) {
	testCommands(t,
		succ("SET", "one", "1"),
		succ("SET", "two", "2"),
		succ("SET", "three", "3"),
		succ("SET", "four", "4"),
		succSorted("KEYS", `*o*`),
		succSorted("KEYS", `t??`),
		succSorted("KEYS", `t?*`),
		succSorted("KEYS", `*`),
		succSorted("KEYS", `t*`),
		succSorted("KEYS", `t\*`),
		succSorted("KEYS", `[tf]*`),

		// zero length key
		succ("SET", "", "nothing"),
		succ("GET", ""),

		// Simple failure cases
		fail("KEYS"),
		fail("KEYS", "foo", "bar"),
	)

	testCommands(t,
		succ("SET", "[one]", "1"),
		succ("SET", "two", "2"),
		succSorted("KEYS", `[\[o]*`),
		succSorted("KEYS", `\[*`),
		succSorted("KEYS", `*o*`),
		succSorted("KEYS", `[]*`), // nothing
	)
}

func TestRandom(t *testing.T) {
	testCommands(t,
		succ("RANDOMKEY"),
		// A random key from a DB with a single key. We can test that.
		succ("SET", "one", "1"),
		succ("RANDOMKEY"),

		// Simple failure cases
		fail("RANDOMKEY", "bar"),
	)
}

func TestUnknownCommand(t *testing.T) {
	testCommands(t,
		fail("nosuch"),
		fail("noSUCH"),
		fail("noSUCH", 1, 2, 3),
	)
}

func TestQuit(t *testing.T) {
	testCommands(t,
		succ("QUIT"),
	)
}

func TestExists(t *testing.T) {
	testCommands(t,
		succ("SET", "a", "3"),
		succ("HSET", "b", "c", "d"),
		succ("EXISTS", "a", "b"),
		succ("EXISTS", "a", "b", "q"),
		succ("EXISTS", "a", "b", "b", "b", "a", "q"),

		// Error cases
		fail("EXISTS"),
	)
}

func TestRename(t *testing.T) {
	testCommands(t,
		// No 'a' key
		fail("RENAME", "a", "b"),

		// Move a key with the TTL.
		succ("SET", "a", "3"),
		succ("EXPIRE", "a", "123"),
		succ("SET", "b", "12"),
		succ("RENAME", "a", "b"),
		succ("EXISTS", "a"),
		succ("GET", "a"),
		succ("TYPE", "a"),
		succ("TTL", "a"),
		succ("EXISTS", "b"),
		succ("GET", "b"),
		succ("TYPE", "b"),
		succ("TTL", "b"),

		// move a key without TTL
		succ("SET", "nottl", "3"),
		succ("RENAME", "nottl", "stillnottl"),
		succ("TTL", "nottl"),
		succ("TTL", "stillnottl"),

		// Error cases
		fail("RENAME"),
		fail("RENAME", "a"),
		fail("RENAME", "a", "b", "toomany"),
	)
}

func TestRenamenx(t *testing.T) {
	testCommands(t,
		// No 'a' key
		fail("RENAMENX", "a", "b"),

		succ("SET", "a", "value"),
		succ("SET", "str", "value"),
		succ("RENAMENX", "a", "str"),
		succ("EXISTS", "a"),
		succ("EXISTS", "str"),
		succ("GET", "a"),
		succ("GET", "str"),

		succ("RENAMENX", "a", "nosuch"),
		succ("EXISTS", "a"),
		succ("EXISTS", "nosuch"),

		// Error cases
		fail("RENAMENX"),
		fail("RENAMENX", "a"),
		fail("RENAMENX", "a", "b", "toomany"),
	)
}

func TestScan(t *testing.T) {
	testCommands(t,
		// No keys yet
		succ("SCAN", 0),

		succ("SET", "key", "value"),
		succ("SCAN", 0),
		succ("SCAN", 0, "COUNT", 12),
		succ("SCAN", 0, "cOuNt", 12),

		succ("SET", "anotherkey", "value"),
		succ("SCAN", 0, "MATCH", "anoth*"),
		succ("SCAN", 0, "MATCH", "anoth*", "COUNT", 100),
		succ("SCAN", 0, "COUNT", 100, "MATCH", "anoth*"),

		// Can't really test multiple keys.
		// succ("SET", "key2", "value2"),
		// succ("SCAN", 0),

		// Error cases
		fail("SCAN"),
		fail("SCAN", "noint"),
		fail("SCAN", 0, "COUNT", "noint"),
		fail("SCAN", 0, "COUNT"),
		fail("SCAN", 0, "MATCH"),
		fail("SCAN", 0, "garbage"),
		fail("SCAN", 0, "COUNT", 12, "MATCH", "foo", "garbage"),
	)
}

func TestFastForward(t *testing.T) {
	testMultiCommands(t,
		func(r chan<- command, m *miniredis.Miniredis) {
			r <- succ("SET", "key1", "value")
			r <- succ("SET", "key", "value", "PX", 100)
			r <- succSorted("KEYS", "*")
			time.Sleep(200 * time.Millisecond)
			m.FastForward(200 * time.Millisecond)
			r <- succSorted("KEYS", "*")
		},
	)

	testCommands(t,
		fail("SET", "key1", "value", "PX", -100),
		fail("SET", "key2", "value", "EX", -100),
		fail("SET", "key3", "value", "EX", 0),
		succSorted("KEYS", "*"),

		succ("SET", "key4", "value"),
		succSorted("KEYS", "*"),
		succ("EXPIRE", "key4", -100),
		succSorted("KEYS", "*"),

		succ("SET", "key4", "value"),
		succSorted("KEYS", "*"),
		succ("EXPIRE", "key4", 0),
		succSorted("KEYS", "*"),
	)
}

func TestProto(t *testing.T) {
	testCommands(t,
		succ("ECHO", strings.Repeat("X", 1<<24)),
	)
}

func TestSwapdb(t *testing.T) {
	testCommands(t,
		succ("SET", "key1", "val1"),
		succ("SWAPDB", "0", "1"),
		succ("SELECT", "1"),
		succ("GET", "key1"),

		succ("SWAPDB", "1", "1"),
		succ("GET", "key1"),

		fail("SWAPDB"),
		fail("SWAPDB", 1),
		fail("SWAPDB", 1, 2, 3),
		fail("SWAPDB", "foo", 2),
		fail("SWAPDB", 1, "bar"),
		fail("SWAPDB", "foo", "bar"),
		fail("SWAPDB", -1, 2),
		fail("SWAPDB", 1, -2),
		// fail("SWAPDB", 1, 1000), // miniredis has no upperlimit
	)

	// SWAPDB with transactions
	testClients2(t, func(r1, r2 chan<- command) {
		r1 <- succ("SET", "foo", "foooooo")

		r1 <- succ("MULTI")
		r1 <- succ("SWAPDB", 0, 2)
		r1 <- succ("GET", "foo")
		r2 <- succ("GET", "foo")

		r1 <- succ("EXEC")
		r1 <- succ("GET", "foo")
		r2 <- succ("GET", "foo")
	})
}

func TestDel(t *testing.T) {
	testCommands(t,
		succ("SET", "one", "1"),
		succ("SET", "two", "2"),
		succ("SET", "three", "3"),
		succ("SET", "four", "4"),
		succ("DEL", "one"),
		succSorted("KEYS", "*"),

		succ("DEL", "twoooo"),
		succSorted("KEYS", "*"),

		succ("DEL", "two", "four"),
		succSorted("KEYS", "*"),

		fail("DEL"),
		succSorted("KEYS", "*"),
	)
}

func TestUnlink(t *testing.T) {
	testCommands(t,
		succ("SET", "one", "1"),
		succ("SET", "two", "2"),
		succ("SET", "three", "3"),
		succ("SET", "four", "4"),
		succ("UNLINK", "one"),
		succSorted("KEYS", "*"),

		succ("UNLINK", "twoooo"),
		succSorted("KEYS", "*"),

		succ("UNLINK", "two", "four"),
		succSorted("KEYS", "*"),

		fail("UNLINK"),
		succSorted("KEYS", "*"),
	)
}
