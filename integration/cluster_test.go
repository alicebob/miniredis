// +build int

package main

import "testing"

func TestCluster(t *testing.T) {
	testClusterCommands(t,
		succNoResultCheck("CLUSTER", "SLOTS"),
		succNoResultCheck("CLUSTER", "KEYSLOT", "{test}"),
		succNoResultCheck("CLUSTER", "NODES"),
		failLoosely("CLUSTER"),
	)
}
