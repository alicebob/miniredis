// +build int

package main

import "testing"

func TestCluster(t *testing.T) {
	testCluster(t,
		func(c *client) {
			// c.DoLoosly("CLUSTER", "SLOTS")
			c.DoLoosely("CLUSTER", "KEYSLOT", "{test}")
			c.DoLoosely("CLUSTER", "NODES")
			c.Error("wrong number","CLUSTER")
		},
	)
}
