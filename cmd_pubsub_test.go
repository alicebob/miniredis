package miniredis

import (
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

func TestSubscribe(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"SUBSCRIBE", "event1",
		proto.Array(
			proto.String("subscribe"),
			proto.String("event1"),
			proto.Int(1),
		),
	)
	mustDo(t, c,
		"SUBSCRIBE", "event2",
		proto.Array(
			proto.String("subscribe"),
			proto.String("event2"),
			proto.Int(2),
		),
	)
	mustDo(t, c,
		"SUBSCRIBE", "event3", "event4",
		proto.Array(
			proto.String("subscribe"),
			proto.String("event3"),
			proto.Int(3),
		),
	)
	mustRead(t, c,
		proto.Array(
			proto.String("subscribe"),
			proto.String("event4"),
			proto.Int(4),
		),
	)

	{
		// publish something!
		mustDo(t, c,
			"SUBSCRIBE", "colors",
			proto.Array(
				proto.String("subscribe"),
				proto.String("colors"),
				proto.Int(5),
			),
		)
		n := s.Publish("colors", "green")
		equals(t, 1, n)

		mustRead(t, c,
			proto.Strings("message", "colors", "green"),
		)
	}
}

func TestUnsubscribe(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"SUBSCRIBE", "event1", "event2", "event3", "event4", "event5",
		proto.Array(
			proto.String("subscribe"),
			proto.String("event1"),
			proto.Int(1),
		),
	)
	mustRead(t, c, proto.Array(proto.String("subscribe"), proto.String("event2"), proto.Int(2)))
	mustRead(t, c, proto.Array(proto.String("subscribe"), proto.String("event3"), proto.Int(3)))
	mustRead(t, c, proto.Array(proto.String("subscribe"), proto.String("event4"), proto.Int(4)))
	mustRead(t, c, proto.Array(proto.String("subscribe"), proto.String("event5"), proto.Int(5)))

	mustDo(t, c,
		"UNSUBSCRIBE", "event1", "event2",
		proto.Array(
			proto.String("unsubscribe"),
			proto.String("event1"),
			proto.Int(4),
		),
	)
	mustRead(t, c, proto.Array(proto.String("unsubscribe"), proto.String("event2"), proto.Int(3)))

	mustDo(t, c,
		"UNSUBSCRIBE", "event3",
		proto.Array(
			proto.String("unsubscribe"),
			proto.String("event3"),
			proto.Int(2),
		),
	)

	mustDo(t, c,
		"UNSUBSCRIBE", "event999",
		proto.Array(
			proto.String("unsubscribe"),
			proto.String("event999"),
			proto.Int(2),
		),
	)

	{
		// unsub the rest
		mustDo(t, c,
			"UNSUBSCRIBE", "event4",
			proto.Array(
				proto.String("unsubscribe"),
				proto.String("event4"),
				proto.Int(1),
			),
		)
		mustDo(t, c,
			"UNSUBSCRIBE", "event5",
			proto.Array(
				proto.String("unsubscribe"),
				proto.String("event5"),
				proto.Int(0),
			),
		)
	}
}

func TestPsubscribe(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"PSUBSCRIBE", "event1",
		proto.Array(proto.String("psubscribe"), proto.String("event1"), proto.Int(1)),
	)

	mustDo(t, c,
		"PSUBSCRIBE", "event2?",
		proto.Array(proto.String("psubscribe"), proto.String("event2?"), proto.Int(2)),
	)

	{
		mustDo(t, c,
			"PSUBSCRIBE", "event3*", "event4[abc]",
			proto.Array(proto.String("psubscribe"), proto.String("event3*"), proto.Int(3)),
		)
		mustRead(t, c,
			proto.Array(proto.String("psubscribe"), proto.String("event4[abc]"), proto.Int(4)),
		)
	}

	mustDo(t, c,
		"PSUBSCRIBE", "event5[]",
		proto.Array(proto.String("psubscribe"), proto.String("event5[]"), proto.Int(5)),
	)

	{
		// publish some things!
		n := s.Publish("event4b", "hello 4b!")
		equals(t, 1, n)

		n = s.Publish("event4d", "hello 4d?")
		equals(t, 0, n)

		mustRead(t, c,
			proto.Strings("pmessage", "event4[abc]", "event4b", "hello 4b!"),
		)
	}
}

func TestPunsubscribe(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"PSUBSCRIBE", "event1", "event2?", "event3*", "event4[abc]", "event5[]",
		proto.Array(
			proto.String("psubscribe"),
			proto.String("event1"),
			proto.Int(1),
		),
	)
	mustRead(t, c, proto.Array(proto.String("psubscribe"), proto.String("event2?"), proto.Int(2)))
	mustRead(t, c, proto.Array(proto.String("psubscribe"), proto.String("event3*"), proto.Int(3)))
	mustRead(t, c, proto.Array(proto.String("psubscribe"), proto.String("event4[abc]"), proto.Int(4)))
	mustRead(t, c, proto.Array(proto.String("psubscribe"), proto.String("event5[]"), proto.Int(5)))

	{
		mustDo(t, c,
			"PUNSUBSCRIBE", "event1", "event2?",
			proto.Array(proto.String("punsubscribe"), proto.String("event1"), proto.Int(4)),
		)
		mustRead(t, c,
			proto.Array(proto.String("punsubscribe"), proto.String("event2?"), proto.Int(3)),
		)
	}

	// punsub the rest
	{
		mustDo(t, c,
			"PUNSUBSCRIBE",
			proto.Array(proto.String("punsubscribe"), proto.String("event3*"), proto.Int(2)),
		)
		mustRead(t, c,
			proto.Array(proto.String("punsubscribe"), proto.String("event4[abc]"), proto.Int(1)),
		)
		mustRead(t, c,
			proto.Array(proto.String("punsubscribe"), proto.String("event5[]"), proto.Int(0)),
		)
	}
}

func TestPublishMode(t *testing.T) {
	// only pubsub related commands should be accepted while there are
	// subscriptions.
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"SUBSCRIBE", "birds",
		proto.Array(
			proto.String("subscribe"),
			proto.String("birds"),
			proto.Int(1),
		),
	)

	mustDo(t, c,
		"SET", "foo", "bar",
		proto.Error("ERR Can't execute 'set': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context"),
	)

	mustDo(t, c,
		"UNSUBSCRIBE", "birds",
		proto.Array(
			proto.String("unsubscribe"),
			proto.String("birds"),
			proto.Int(0),
		),
	)

	// no subs left. All should be fine now.
	mustOK(t, c,
		"SET", "foo", "bar",
	)
}

func TestPublish(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c1, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c1.Close()
	c2, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c2.Close()

	mustDo(t, c2,
		"SUBSCRIBE", "event1",
		proto.Array(
			proto.String("subscribe"),
			proto.String("event1"),
			proto.Int(1),
		),
	)

	{
		must1(t, c1,
			"PUBLISH", "event1", "message2",
		)
		mustRead(t, c2,
			proto.Strings("message", "event1", "message2"),
		)
	}

	// direct access
	{
		equals(t, 1, s.Publish("event1", "message3"))

		mustRead(t, c2,
			proto.Strings("message", "event1", "message3"),
		)
	}

	// Wrong usage
	mustDo(t, c2,
		"PUBLISH", "foo", "bar",
		proto.Error("ERR Can't execute 'publish': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context"),
	)
}

func TestPublishMix(t *testing.T) {
	// SUBSCRIBE and PSUBSCRIBE
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"SUBSCRIBE", "c1",
		proto.Array(
			proto.String("subscribe"),
			proto.String("c1"),
			proto.Int(1),
		),
	)

	mustDo(t, c,
		"PSUBSCRIBE", "c1",
		proto.Array(
			proto.String("psubscribe"),
			proto.String("c1"),
			proto.Int(2),
		),
	)

	mustDo(t, c,
		"SUBSCRIBE", "c2",
		proto.Array(
			proto.String("subscribe"),
			proto.String("c2"),
			proto.Int(3),
		),
	)

	mustDo(t, c,
		"PUNSUBSCRIBE", "c1",
		proto.Array(
			proto.String("punsubscribe"),
			proto.String("c1"),
			proto.Int(2),
		),
	)

	mustDo(t, c,
		"UNSUBSCRIBE", "c1",
		proto.Array(
			proto.String("unsubscribe"),
			proto.String("c1"),
			proto.Int(1),
		),
	)
}

func TestPubsubChannels(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c1, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c1.Close()
	c2, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c2.Close()

	mustDo(t, c1,
		"PUBSUB", "CHANNELS",
		proto.Strings(),
	)

	mustDo(t, c1,
		"PUBSUB", "CHANNELS", "event1[abc]",
		proto.Strings(),
	)

	mustDo(t, c2,
		"SUBSCRIBE", "event1", "event1b", "event1c",
		proto.Array(
			proto.String("subscribe"),
			proto.String("event1"),
			proto.Int(1),
		),
	)
	mustRead(t, c2, proto.Array(proto.String("subscribe"), proto.String("event1b"), proto.Int(2)))
	mustRead(t, c2, proto.Array(proto.String("subscribe"), proto.String("event1c"), proto.Int(3)))

	mustDo(t, c1,
		"PUBSUB", "CHANNELS",
		proto.Strings("event1", "event1b", "event1c"),
	)
	mustDo(t, c1,
		"PUBSUB", "CHANNELS", "event1b",
		proto.Strings("event1b"),
	)
	mustDo(t, c1,
		"PUBSUB", "CHANNELS", "event1[abc]",
		proto.Strings("event1b", "event1c"),
	)

	// workaround to make sure c2 stays alive; likely a go1.12-ism
	mustDo(t, c1, "PING", proto.Inline("PONG"))
	mustDo(t, c2, "PING", "foo", proto.Strings("pong", "foo"))
}

func TestPubsubNumsub(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c1, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c1.Close()
	c2, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c2.Close()

	mustDo(t, c2,
		"SUBSCRIBE", "event1", "event2", "event3",
		proto.Array(proto.String("subscribe"), proto.String("event1"), proto.Int(1)),
	)
	mustRead(t, c2, proto.Array(proto.String("subscribe"), proto.String("event2"), proto.Int(2)))
	mustRead(t, c2, proto.Array(proto.String("subscribe"), proto.String("event3"), proto.Int(3)))

	mustDo(t, c1,
		"PUBSUB", "NUMSUB",
		proto.Strings(),
	)
	mustDo(t, c1,
		"PUBSUB", "NUMSUB", "event1",
		proto.Array(
			proto.String("event1"),
			proto.Int(1),
		),
	)
	mustDo(t, c1,
		"PUBSUB", "NUMSUB", "event12", "event3",
		proto.Array(
			proto.String("event12"),
			proto.Int(0),
			proto.String("event3"),
			proto.Int(1),
		),
	)

}

func TestPubsubNumpat(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	must0(t, c,
		"PUBSUB", "NUMPAT",
	)

	equals(t, 0, s.PubSubNumPat())
}

func TestPubSubBadArgs(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"SUBSCRIBE",
		proto.Error("ERR wrong number of arguments for 'subscribe' command"),
	)
	mustDo(t, c,
		"PSUBSCRIBE",
		proto.Error("ERR wrong number of arguments for 'psubscribe' command"),
	)
	mustDo(t, c,
		"PUBLISH",
		proto.Error("ERR wrong number of arguments for 'publish' command"),
	)
	mustDo(t, c,
		"PUBLISH", "event1",
		proto.Error("ERR wrong number of arguments for 'publish' command"),
	)
	mustDo(t, c,
		"PUBLISH", "event1", "message2", "message3",
		proto.Error("ERR wrong number of arguments for 'publish' command"),
	)
	mustDo(t, c,
		"PUBSUB",
		proto.Error("ERR wrong number of arguments for 'pubsub' command"),
	)
	mustDo(t, c,
		"PUBSUB", "FOOBAR",
		proto.Error("ERR Unknown subcommand or wrong number of arguments for 'FOOBAR'. Try PUBSUB HELP."),
	)
	mustDo(t, c,
		"PUBSUB", "CHANNELS", "FOOBAR1", "FOOBAR2",
		proto.Error("ERR Unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP."),
	)
}
