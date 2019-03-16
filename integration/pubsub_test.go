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
	var wg1 sync.WaitGroup
	wg1.Add(1)
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
			wg1.Wait()
			r <- succ("PUBLISH", "news", "revolution!")
			r <- succ("PUBLISH", "news", "alien invasion!")
			r <- succ("PUBLISH", "sport", "lady biked too fast")
			r <- succ("PUBLISH", "gossip", "man bites dog")
		},
	)
}

func TestPubsubMulti(t *testing.T) {
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
			wg1.Done()
			r <- receive()
			r <- succ("UNSUBSCRIBE", "sport")
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
	testClients2(t, func(r1, r2 chan<- command) {
		r1 <- succ("SUBSCRIBE", "news", "sport")
		r1 <- receive()
		r2 <- succ("SELECT", 3)
		r2 <- succ("PUBLISH", "news", "revolution!")
		r1 <- receive()
	})
}

func TestPubsubMode(t *testing.T) {
	testCommands(t,
		succ("SUBSCRIBE", "news", "sport"),
		receive(),
		succ("PING"),
		succ("PING", "foo"),
		fail("ECHO", "foo"),
		fail("HGET", "foo", "bar"),
		fail("SET", "foo", "bar"),
		succ("QUIT"),
	)
}

func TestSubscriptions(t *testing.T) {
	testClients2(t, func(r1, r2 chan<- command) {
		r1 <- succ("SUBSCRIBE", "foo", "bar", "foo")
		r2 <- succ("PUBSUB", "NUMSUB")
		r1 <- succ("UNSUBSCRIBE", "bar", "bar", "bar")
		r2 <- succ("PUBSUB", "NUMSUB")
	})
}

func TestPubsubUnsub(t *testing.T) {
	testClients2(t, func(c1, c2 chan<- command) {
		c1 <- succ("SUBSCRIBE", "news", "sport")
		c1 <- receive()
		c2 <- succSorted("PUBSUB", "CHANNELS")
		c1 <- succ("QUIT")
		c2 <- succSorted("PUBSUB", "CHANNELS")
	})
}
