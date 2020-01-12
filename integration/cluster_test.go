// +build int

package main

import "testing"

func TestCluster(t *testing.T) {
	testClusterCommands(t,
		succNoResultCheck("CLUSTER", "SLOTS"),
		failLoosely("CLUSTER"),
	)
}
