// +build int

package main

import (
	"testing"
)

func TestStream(t *testing.T) {
	testCommands(t,
		succ("XADD",
			"planets",
			"0-1",
			"name", "Mercury",
		),
		succLoosely("XADD",
			"planets",
			"*",
			"name", "Venus",
		),
		succ("XADD",
			"planets",
			"18446744073709551000-0",
			"name", "Earth",
		),
		fail("XADD",
			"planets",
			"18446744073709551000-0", // <-- duplicate
			"name", "Earth",
		),
		succ("XLEN", "planets"),
		succ("RENAME", "planets", "planets2"),
		succ("DEL", "planets2"),
		succ("XLEN", "planets"),

		fail("XADD",
			"planets",
			"1000",
			"name", "Mercury",
			"ignored", // <-- not an even number of keys
		),
		fail("XADD",
			"newplanets",
			"0", // <-- invalid key
			"foo", "bar",
		),
		fail("XADD", "newplanets", "123-123"), // no args
		fail("XADD", "newplanets", "123-bar", "foo", "bar"),
		fail("XADD", "newplanets", "bar-123", "foo", "bar"),
		fail("XADD", "newplanets", "123-123-123", "foo", "bar"),
		succ("SET", "str", "I am a string"),
		// fail("XADD", "str", "1000", "foo", "bar"),
		// fail("XADD", "str", "invalid-key", "foo", "bar"),

		fail("XADD", "planets"),
		fail("XADD"),
	)

	testCommands(t,
		succ("XADD", "planets", "MAXLEN", "4", "456-1", "name", "Mercury"),
		succ("XADD", "planets", "MAXLEN", "4", "456-2", "name", "Mercury"),
		succ("XADD", "planets", "MAXLEN", "4", "456-3", "name", "Mercury"),
		succ("XADD", "planets", "MAXLEN", "4", "456-4", "name", "Mercury"),
		succ("XADD", "planets", "MAXLEN", "4", "456-5", "name", "Mercury"),
		succ("XADD", "planets", "MAXLEN", "4", "456-6", "name", "Mercury"),
		succ("XLEN", "planets"),
		succ("XADD", "planets", "MAXLEN", "~", "4", "456-7", "name", "Mercury"),

		fail("XADD", "planets", "MAXLEN", "!", "4", "*", "name", "Mercury"),
		fail("XADD", "planets", "MAXLEN", " ~", "4", "*", "name", "Mercury"),
		fail("XADD", "planets", "MAXLEN", "-4", "*", "name", "Mercury"),
		fail("XADD", "planets", "MAXLEN", "", "*", "name", "Mercury"),
		fail("XADD", "planets", "MAXLEN", "!", "four", "*", "name", "Mercury"),
		fail("XADD", "planets", "MAXLEN", "~", "four"),
		fail("XADD", "planets", "MAXLEN", "~"),
		fail("XADD", "planets", "MAXLEN"),

		succ("XADD", "planets", "MAXLEN", "0", "456-8", "name", "Mercury"),
		succ("XLEN", "planets"),

		succ("SET", "str", "I am a string"),
		fail("XADD", "str", "MAXLEN", "four", "*", "foo", "bar"),
	)

	testCommands(t,
		succ("MULTI"),
		succ("XADD", "planets", "0-1", "name", "Mercury"),
		succ("EXEC"),

		succ("MULTI"),
		fail("XADD", "newplanets", "123-123"), // no args
		fail("EXEC"),

		succ("MULTI"),
		succ("XADD", "planets", "foo-bar", "name", "Mercury"),
		succ("EXEC"),

		succ("MULTI"),
		succ("XADD", "planets", "MAXLEN", "four", "*", "name", "Mercury"),
		succ("EXEC"),
	)
}

func TestStreamRange(t *testing.T) {
	testCommands(t,
		succ("XADD",
			"ordplanets",
			"0-1",
			"name", "Mercury",
			"greek-god", "Hermes",
		),
		succ("XADD",
			"ordplanets",
			"1-0",
			"name", "Venus",
			"greek-god", "Aphrodite",
		),
		succ("XADD",
			"ordplanets",
			"2-1",
			"greek-god", "",
			"name", "Earth",
		),
		succ("XADD",
			"ordplanets",
			"3-0",
			"name", "Mars",
			"greek-god", "Ares",
		),
		succ("XADD",
			"ordplanets",
			"4-1",
			"greek-god", "Dias",
			"name", "Jupiter",
		),
		succ("XRANGE", "ordplanets", "-", "+"),
		succ("XRANGE", "ordplanets", "+", "-"),
		succ("XRANGE", "ordplanets", "-", "99"),
		succ("XRANGE", "ordplanets", "0", "4"),
		succ("XRANGE", "ordplanets", "2", "2"),
		succ("XRANGE", "ordplanets", "2-0", "2-1"),
		succ("XRANGE", "ordplanets", "2-1", "2-1"),
		succ("XRANGE", "ordplanets", "2-1", "2-2"),
		succ("XRANGE", "ordplanets", "0", "1-0"),
		succ("XRANGE", "ordplanets", "0", "1-99"),
		succ("XRANGE", "ordplanets", "0", "2", "COUNT", "1"),
		succ("XRANGE", "ordplanets", "1-42", "3-42", "COUNT", "1"),

		succ("XREVRANGE", "ordplanets", "+", "-"),
		succ("XREVRANGE", "ordplanets", "-", "+"),
		succ("XREVRANGE", "ordplanets", "4", "0"),
		succ("XREVRANGE", "ordplanets", "2", "2"),
		succ("XREVRANGE", "ordplanets", "2-1", "2-0"),
		succ("XREVRANGE", "ordplanets", "2-1", "2-1"),
		succ("XREVRANGE", "ordplanets", "2-2", "2-1"),
		succ("XREVRANGE", "ordplanets", "1-0", "0"),
		succ("XREVRANGE", "ordplanets", "3-42", "1-0", "COUNT", "2"),
		succ("DEL", "ordplanets"),

		// failure cases
		fail("XRANGE"),
		fail("XRANGE", "foo"),
		fail("XRANGE", "foo", 1),
		fail("XRANGE", "foo", 2, 3, "toomany"),
		fail("XRANGE", "foo", 2, 3, "COUNT", "noint"),
		fail("XRANGE", "foo", 2, 3, "COUNT", 1, "toomany"),
		fail("XRANGE", "foo", "-", "noint"),
		succ("SET", "str", "I am a string"),
		fail("XRANGE", "str", "-", "+"),
	)

	testCommands(t,
		succ("MULTI"),
		succ("XADD",
			"ordplanets",
			"0-1",
			"name", "Mercury",
			"greek-god", "Hermes",
		),
		succ("XLEN", "ordplanets"),
		succ("XRANGE", "ordplanets", "+", "-"),
		succ("XRANGE", "ordplanets", "+", "-", "COUNT", "FOOBAR"),
		succ("EXEC"),
		succ("XLEN", "ordplanets"),

		succ("MULTI"),
		succ("XRANGE", "ordplanets", "+", "foo"),
		succ("EXEC"),

		succ("MULTI"),
		fail("XRANGE", "ordplanets", "+"),
		fail("EXEC"),

		succ("MULTI"),
		succ("XADD", "ordplanets", "123123-123", "name", "Mercury"),
		succ("XADD", "ordplanets", "invalid", "name", "Mercury"),
		succ("EXEC"),
		succ("XLEN", "ordplanets"),
	)
}
