// +build int

package main

import "testing"

func TestCommand(t *testing.T) {
	testCommands(t,
		succNoResultCheck("COMMAND"),
	)
}
