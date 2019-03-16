// +build int

package main

import (
	"sync"
	"testing"

	"github.com/alicebob/miniredis"
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

func TestPubsubFull(t *testing.T) {
	t.Skip() // exit 1, no idea why
	var wg1 sync.WaitGroup
	wg1.Add(1)
	testMultiCommands(t,
		func(r chan<- command, _ *miniredis.Miniredis) {
			r <- succ("SUBSCRIBE", "news", "sport")
			r <- receive()
			/*
				wg1.Done()
				r <- receive()
				r <- receive()
				r <- receive()
				r <- succ("UNSUBSCRIBE", "news", "sport")
				r <- receive()
			*/
		},
		/*
			func(r chan<- command, _ *miniredis.Miniredis) {
				wg1.Wait()
				r <- succ("PUBLISH", "news", "revolution!")
				r <- succ("PUBLISH", "news", "alien invasion!")
				r <- succ("PUBLISH", "sport", "lady biked too fast")
				r <- succ("PUBLISH", "gossip", "man bites dog")
			},
		*/
	)
}

func TestPubsubMulti(t *testing.T) {
	t.Skip() // hangs. No idea why.
	var wg1 sync.WaitGroup
	wg1.Add(2)
	testMultiCommands(t,
		func(r chan<- command, _ *miniredis.Miniredis) {
			r <- succ("SUBSCRIBE", "news", "sport")
			r <- receive()
			wg1.Done()
			r <- receive()
			r <- receive()
			r <- receive()
			r <- succ("UNSUBSCRIBE", "news", "sport")
			r <- receive()
		},
		func(r chan<- command, _ *miniredis.Miniredis) {
			r <- succ("SUBSCRIBE", "sport")
			r <- receive()
			wg1.Done()
			r <- receive()
			r <- succ("UNSUBSCRIBE", "sport")
			r <- receive()
		},
		func(r chan<- command, _ *miniredis.Miniredis) {
			wg1.Wait()
			r <- succ("PUBLISH", "news", "revolution!")
			r <- succ("PUBLISH", "news", "alien invasion!")
			r <- succ("PUBLISH", "sport", "lady biked too fast")
		},
	)
}

func TestPubsubSelect(t *testing.T) {
	t.Skip() // known broken
	var wg1 sync.WaitGroup
	wg1.Add(1)
	testMultiCommands(t,
		func(r chan<- command, _ *miniredis.Miniredis) {
			r <- succ("SUBSCRIBE", "news", "sport")
			r <- receive()
			wg1.Done()
			r <- receive()
		},
		func(r chan<- command, _ *miniredis.Miniredis) {
			wg1.Wait()
			r <- succ("SELECT", 3)
			r <- succ("PUBLISH", "news", "revolution!")
		},
	)
}

func TestPubsubMode(t *testing.T) {
	t.Skip() // known broken
	testCommands(t,
		succ("SUBSCRIBE", "news", "sport"),
		receive(),
		fail("ECHO", "foo"),
		fail("HGET", "foo", "bar"),
	)
}
