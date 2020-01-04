// +build int

package main

import (
	"testing"
)

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
