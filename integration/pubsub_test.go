// +build int

package main

import (
	"testing"
)

func TestSubscribe(t *testing.T) {
	testCommands(t,
		fail("SUBSCRIBE"),

		succ("SUBSCRIBE", "foo"),
		succ("UNSUBSCRIBE"),

		succ("SUBSCRIBE", "foo"),
		succ("UNSUBSCRIBE", "foo"),

		succ("SUBSCRIBE", "foo", "bar"),
		succ("UNSUBSCRIBE", "foo", "bar"),

		succ("SUBSCRIBE", -1),
		succ("UNSUBSCRIBE", -1),
	)
}

func TestPSubscribe(t *testing.T) {
	testCommands(t,
		fail("PSUBSCRIBE"),

		succ("PSUBSCRIBE", "foo"),
		succ("PUNSUBSCRIBE"),

		succ("PSUBSCRIBE", "foo"),
		succ("PUNSUBSCRIBE", "foo"),

		succ("PSUBSCRIBE", "foo", "bar"),
		succ("PUNSUBSCRIBE", "foo", "bar"),

		succ("PSUBSCRIBE", "f?o"),
		succ("PUNSUBSCRIBE", "f?o"),

		succ("PSUBSCRIBE", "f*o"),
		succ("PUNSUBSCRIBE", "f*o"),

		succ("PSUBSCRIBE", "f[oO]o"),
		succ("PUNSUBSCRIBE", "f[oO]o"),

		succ("PSUBSCRIBE", "f\\?o"),
		succ("PUNSUBSCRIBE", "f\\?o"),

		succ("PSUBSCRIBE", "f\\*o"),
		succ("PUNSUBSCRIBE", "f\\*o"),

		succ("PSUBSCRIBE", "f\\[oO]o"),
		succ("PUNSUBSCRIBE", "f\\[oO]o"),

		succ("PSUBSCRIBE", "f\\\\oo"),
		succ("PUNSUBSCRIBE", "f\\\\oo"),

		succ("PSUBSCRIBE", -1),
		succ("PUNSUBSCRIBE", -1),
	)
}

func TestPublish(t *testing.T) {
	testCommands(t,
		fail("PUBLISH"),
		fail("PUBLISH", "foo"),
		succ("PUBLISH", "foo", "bar"),
		fail("PUBLISH", "foo", "bar", "deadbeef"),
		succ("PUBLISH", -1, -2),
	)
}

func TestPubSub(t *testing.T) {
	testCommands(t,
		fail("PUBSUB"),
		fail("PUBSUB", "FOO"),

		succ("PUBSUB", "CHANNELS"),
		succ("PUBSUB", "CHANNELS", "foo"),
		fail("PUBSUB", "CHANNELS", "foo", "bar"),
		succ("PUBSUB", "CHANNELS", "f?o"),
		succ("PUBSUB", "CHANNELS", "f*o"),
		succ("PUBSUB", "CHANNELS", "f[oO]o"),
		succ("PUBSUB", "CHANNELS", "f\\?o"),
		succ("PUBSUB", "CHANNELS", "f\\*o"),
		succ("PUBSUB", "CHANNELS", "f\\[oO]o"),
		succ("PUBSUB", "CHANNELS", "f\\\\oo"),
		succ("PUBSUB", "CHANNELS", -1),

		succ("PUBSUB", "NUMSUB"),
		succ("PUBSUB", "NUMSUB", "foo"),
		succ("PUBSUB", "NUMSUB", "foo", "bar"),
		succ("PUBSUB", "NUMSUB", -1),

		succ("PUBSUB", "NUMPAT"),
		fail("PUBSUB", "NUMPAT", "foo"),
	)
}
