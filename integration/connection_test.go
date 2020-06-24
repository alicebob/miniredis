// +build int

package main

import (
	"testing"
)

func TestEcho(t *testing.T) {
	testCommands(t,
		succ("ECHO", "hello world"),
		succ("ECHO", 42),
		succ("ECHO", 3.1415),
		fail("ECHO", "hello", "world"),
		fail("ECHO"),
		fail("eChO", "hello", "world"),
	)

	testCommands(t,
		succ("MULTI"),
		succ("ECHO", "hi"),
		succ("EXEC"),
	)

	testCommands(t,
		succ("MULTI"),
		fail("ECHO"),
		fail("EXEC"),
	)
}

func TestPing(t *testing.T) {
	testCommands(t,
		succ("PING"),
		succ("PING", "hello world"),
		fail("PING", "hello", "world"),
	)

	testCommands(t,
		succ("MULTI"),
		succ("PING", "hi"),
		succ("EXEC"),
	)

	testCommands(t,
		succ("MULTI"),
		succ("PING", "hi again"),
		succ("EXEC"),
	)
}

func TestSelect(t *testing.T) {
	testCommands(t,
		succ("SET", "foo", "bar"),
		succ("GET", "foo"),
		succ("SELECT", 2),
		succ("GET", "foo"),
		succ("SET", "foo", "bar2"),
		succ("GET", "foo"),

		fail("SELECT"),
		fail("SELECT", -1),
		fail("SELECT", "aap"),
		fail("SELECT", 1, 2),
	)

	testCommands(t,
		succ("MULTI"),
		succ("SET", "foo", "bar"),
		succ("GET", "foo"),
		succ("SELECT", 2),
		succ("GET", "foo"),
		succ("SET", "foo", "bar2"),
		succ("GET", "foo"),
		succ("EXEC"),
		succ("GET", "foo"),
	)

	testCommands(t,
		succ("MULTI"),
		succ("SELECT", -1),
		succ("EXEC"),
	)
}

func TestAuth(t *testing.T) {
	testAuthCommands(t,
		"supersecret",
		fail("PING"),
		fail("SET", "foo", "bar"),
		fail("SET"),
		fail("SET", "foo", "bar", "baz"),
		fail("GET", "foo"),
		fail("AUTH"),
		fail("AUTH", "nosecret"),
		fail("AUTH", "nosecret", "bar"),
		fail("AUTH", "nosecret", "bar", "bar"),
		succ("AUTH", "supersecret"),
		succ("SET", "foo", "bar"),
		succ("GET", "foo"),
	)

	testUserAuthCommands(t,
		map[string]string{
			"agent1": "supersecret",
			"agent2": "dragon",
		},
		fail("PING"),
		fail("SET", "foo", "bar"),
		fail("SET"),
		fail("SET", "foo", "bar", "baz"),
		fail("GET", "foo"),
		fail("AUTH"),
		fail("AUTH", "nosecret"),
		fail("AUTH", "agent100", "supersecret"),
		fail("AUTH", "agent100", "supersecret", "supersecret"),
		fail("AUTH", "agent1", "bzzzt"),
		succ("AUTH", "agent1", "supersecret"),
		succ("SET", "foo", "bar"),
		succ("GET", "foo"),

		// go back to invalid user
		fail("AUTH", "agent100", "supersecret"),
		succ("GET", "foo"), // still agent1
	)

	testCommands(t,
		fail("AUTH"),
		fail("AUTH", "foo"),
		fail("AUTH", "foo", "bar"),
		fail("AUTH", "foo", "bar", "bar"),
	)

	testCommands(t,
		succ("MULTI"),
		succ("AUTH", "apassword"),
		succ("EXEC"),
	)
}
