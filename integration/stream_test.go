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
			"18446744073709551000-0",
			"name", "Earth",
		),
		succ("XLEN", "planets"),
		succ("RENAME", "planets", "planets2"),
		succ("DEL", "planets2"),
		succ("XLEN", "planets"),
	)
}
