package miniredis

import (
	"testing"

	"github.com/gomodule/redigo/redis"
)

func TestSubscribe(t *testing.T) {
	s, c, done := setup(t)
	defer done()
	defer c.Close()

	{
		a, err := redis.Values(c.Do("SUBSCRIBE", "event1"))
		ok(t, err)
		equals(t, []interface{}{[]byte("subscribe"), []byte("event1"), int64(1)}, a)
	}

	{
		a, err := redis.Values(c.Do("SUBSCRIBE", "event2"))
		ok(t, err)
		equals(t, []interface{}{[]byte("subscribe"), []byte("event2"), int64(2)}, a)
	}

	{
		a, err := redis.Values(c.Do("SUBSCRIBE", "event3", "event4"))
		ok(t, err)
		equals(t, []interface{}{[]byte("subscribe"), []byte("event3"), int64(3)}, a)

		a, err = redis.Values(c.Receive())
		ok(t, err)
		equals(t, []interface{}{[]byte("subscribe"), []byte("event4"), int64(4)}, a)
	}

	{
		// publish something!
		a, err := redis.Values(c.Do("SUBSCRIBE", "colors"))
		ok(t, err)
		equals(t, []interface{}{[]byte("subscribe"), []byte("colors"), int64(5)}, a)

		n := s.Publish("colors", "green")
		equals(t, 1, n)

		s, err := redis.Strings(c.Receive())
		ok(t, err)
		equals(t, []string{"message", "colors", "green"}, s)
	}
}

func TestUnsubscribe(t *testing.T) {
	_, c, done := setup(t)
	defer done()

	ok(t, c.Send("SUBSCRIBE", "event1", "event2", "event3", "event4", "event5"))
	c.Flush()
	c.Receive()
	c.Receive()
	c.Receive()
	c.Receive()
	c.Receive()

	{
		a, err := redis.Values(c.Do("UNSUBSCRIBE", "event1", "event2"))
		ok(t, err)
		equals(t, []interface{}{[]byte("unsubscribe"), []byte("event1"), int64(4)}, a)

		a, err = redis.Values(c.Receive())
		ok(t, err)
		equals(t, []interface{}{[]byte("unsubscribe"), []byte("event2"), int64(3)}, a)
	}

	{
		a, err := redis.Values(c.Do("UNSUBSCRIBE", "event3"))
		ok(t, err)
		equals(t, []interface{}{[]byte("unsubscribe"), []byte("event3"), int64(2)}, a)
	}

	{
		a, err := redis.Values(c.Do("UNSUBSCRIBE", "event999"))
		ok(t, err)
		equals(t, []interface{}{[]byte("unsubscribe"), []byte("event999"), int64(2)}, a)
	}

	{
		// unsub the rest
		ok(t, c.Send("UNSUBSCRIBE"))
		c.Flush()
		seen := map[string]bool{}
		for i := 0; i < 2; i++ {
			vs, err := redis.Values(c.Receive())
			ok(t, err)
			equals(t, 3, len(vs))
			equals(t, "unsubscribe", string(vs[0].([]byte)))
			seen[string(vs[1].([]byte))] = true
			equals(t, 1-i, int(vs[2].(int64)))
		}
		equals(t,
			map[string]bool{
				"event4": true,
				"event5": true,
			},
			seen,
		)
	}
}

func TestPsubscribe(t *testing.T) {
	s, c, done := setup(t)
	defer done()

	{
		a, err := redis.Values(c.Do("PSUBSCRIBE", "event1"))
		ok(t, err)
		equals(t, []interface{}{[]byte("psubscribe"), []byte("event1"), int64(1)}, a)
	}

	{
		a, err := redis.Values(c.Do("PSUBSCRIBE", "event2?"))
		ok(t, err)
		equals(t, []interface{}{[]byte("psubscribe"), []byte("event2?"), int64(2)}, a)
	}

	{
		a, err := redis.Values(c.Do("PSUBSCRIBE", "event3*", "event4[abc]"))
		ok(t, err)
		equals(t, []interface{}{[]byte("psubscribe"), []byte("event3*"), int64(3)}, a)

		a, err = redis.Values(c.Receive())
		ok(t, err)
		equals(t, []interface{}{[]byte("psubscribe"), []byte("event4[abc]"), int64(4)}, a)
	}

	{
		a, err := redis.Values(c.Do("PSUBSCRIBE", "event5[]"))
		ok(t, err)
		equals(t, []interface{}{[]byte("psubscribe"), []byte("event5[]"), int64(5)}, a)
	}

	{
		// publish some things!
		n := s.Publish("event4b", "hello 4b!")
		equals(t, 1, n)

		n = s.Publish("event4d", "hello 4d?")
		equals(t, 0, n)

		s, err := redis.Strings(c.Receive())
		ok(t, err)
		equals(t, []string{"pmessage", "event4[abc]", "event4b", "hello 4b!"}, s)
	}
}

func TestPunsubscribe(t *testing.T) {
	_, c, done := setup(t)
	defer done()

	c.Send("PSUBSCRIBE", "event1", "event2?", "event3*", "event4[abc]", "event5[]")
	c.Flush()
	c.Receive()
	c.Receive()
	c.Receive()
	c.Receive()
	c.Receive()

	{
		ok(t, c.Send("PUNSUBSCRIBE", "event1", "event2?"))
		c.Flush()
		seen := map[string]bool{}
		for i := 0; i < 2; i++ {
			vs, err := redis.Values(c.Receive())
			ok(t, err)
			equals(t, 3, len(vs))
			equals(t, "punsubscribe", string(vs[0].([]byte)))
			seen[string(vs[1].([]byte))] = true
			equals(t, 4-i, int(vs[2].(int64)))
		}
		equals(t,
			map[string]bool{
				"event1":  true,
				"event2?": true,
			},
			seen,
		)
	}

	// punsub the rest
	{
		ok(t, c.Send("PUNSUBSCRIBE"))
		c.Flush()
		seen := map[string]bool{}
		for i := 0; i < 3; i++ {
			vs, err := redis.Values(c.Receive())
			ok(t, err)
			equals(t, 3, len(vs))
			equals(t, "punsubscribe", string(vs[0].([]byte)))
			seen[string(vs[1].([]byte))] = true
			equals(t, 2-i, int(vs[2].(int64)))
		}
		equals(t,
			map[string]bool{
				"event3*":     true,
				"event4[abc]": true,
				"event5[]":    true,
			},
			seen,
		)
	}
}

func TestPublishMode(t *testing.T) {
	// only pubsub related commands should be accepted while there are
	// subscriptions.
	_, c, done := setup(t)
	defer done()

	_, err := c.Do("SUBSCRIBE", "birds")
	ok(t, err)

	_, err = c.Do("SET", "foo", "bar")
	mustFail(t, err, "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context")

	_, err = c.Do("UNSUBSCRIBE", "birds")
	ok(t, err)

	// no subs left. All should be fine now.
	_, err = c.Do("SET", "foo", "bar")
	ok(t, err)
}

func TestPublish(t *testing.T) {
	s, c, c2, done := setup2(t)
	defer done()

	a, err := redis.Values(c2.Do("SUBSCRIBE", "event1"))
	ok(t, err)
	equals(t, []interface{}{[]byte("subscribe"), []byte("event1"), int64(1)}, a)

	{
		n, err := redis.Int(c.Do("PUBLISH", "event1", "message2"))
		ok(t, err)
		equals(t, 1, n)

		s, err := redis.Strings(c2.Receive())
		ok(t, err)
		equals(t, []string{"message", "event1", "message2"}, s)
	}

	// direct access
	{
		equals(t, 1, s.Publish("event1", "message3"))

		s, err := redis.Strings(c2.Receive())
		ok(t, err)
		equals(t, []string{"message", "event1", "message3"}, s)
	}

	// Wrong usage
	{
		_, err := c2.Do("PUBLISH", "foo", "bar")
		mustFail(t, err, "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context")
	}
}

func TestPublishMix(t *testing.T) {
	// SUBSCRIBE and PSUBSCRIBE
	_, c, done := setup(t)
	defer done()

	a, err := redis.Values(c.Do("SUBSCRIBE", "c1"))
	ok(t, err)
	equals(t, 1, int(a[2].(int64)))

	a, err = redis.Values(c.Do("PSUBSCRIBE", "c1"))
	ok(t, err)
	equals(t, 2, int(a[2].(int64)))

	a, err = redis.Values(c.Do("SUBSCRIBE", "c2"))
	ok(t, err)
	equals(t, 3, int(a[2].(int64)))

	a, err = redis.Values(c.Do("PUNSUBSCRIBE", "c1"))
	ok(t, err)
	equals(t, 2, int(a[2].(int64)))

	a, err = redis.Values(c.Do("UNSUBSCRIBE", "c1"))
	ok(t, err)
	equals(t, 1, int(a[2].(int64)))
}

func TestPubsubChannels(t *testing.T) {
	_, c1, c2, done := setup2(t)
	defer done()

	a, err := redis.Strings(c1.Do("PUBSUB", "CHANNELS"))
	ok(t, err)
	equals(t, []string{}, a)

	a, err = redis.Strings(c1.Do("PUBSUB", "CHANNELS", "event1[abc]"))
	ok(t, err)
	equals(t, []string{}, a)

	n, err := redis.Values(c2.Do("SUBSCRIBE", "event1", "event1b", "event1c"))
	ok(t, err)
	ni, _ := n[2].(int64)
	equals(t, 1, int(ni))
	// sub "event1b"
	n, err = redis.Values(c2.Receive())
	ok(t, err)
	ni, _ = n[2].(int64)
	equals(t, 2, int(ni))
	// sub "event1c"
	n, err = redis.Values(c2.Receive())
	ok(t, err)
	ni, _ = n[2].(int64)
	equals(t, 3, int(ni))

	a, err = redis.Strings(c1.Do("PUBSUB", "CHANNELS"))
	ok(t, err)
	equals(t, []string{"event1", "event1b", "event1c"}, a)

	a, err = redis.Strings(c1.Do("PUBSUB", "CHANNELS", "event1b"))
	ok(t, err)
	equals(t, []string{"event1b"}, a)

	a, err = redis.Strings(c1.Do("PUBSUB", "CHANNELS", "event1[abc]"))
	ok(t, err)
	equals(t, []string{"event1b", "event1c"}, a)
}

func TestPubsubNumsub(t *testing.T) {
	_, c, c2, done := setup2(t)
	defer done()

	_, err := c2.Do("SUBSCRIBE", "event1", "event2", "event3")
	ok(t, err)

	{
		a, err := redis.Values(c.Do("PUBSUB", "NUMSUB"))
		ok(t, err)
		equals(t, []interface{}{}, a)
	}

	{
		a, err := redis.Values(c.Do("PUBSUB", "NUMSUB", "event1"))
		ok(t, err)
		equals(t, []interface{}{[]byte("event1"), int64(1)}, a)
	}

	{
		a, err := redis.Values(c.Do("PUBSUB", "NUMSUB", "event12", "event3"))
		ok(t, err)
		equals(t,
			[]interface{}{
				[]byte("event12"), int64(0),
				[]byte("event3"), int64(1),
			},
			a,
		)
	}
}

func TestPubsubNumpat(t *testing.T) {
	s, c, done := setup(t)
	defer done()

	{
		a, err := redis.Int(c.Do("PUBSUB", "NUMPAT"))
		ok(t, err)
		equals(t, 0, a)
	}

	equals(t, 0, s.PubSubNumPat())
}

func TestPubSubBadArgs(t *testing.T) {
	for _, command := range [9]struct {
		command string
		args    []interface{}
		err     string
	}{
		{"SUBSCRIBE", []interface{}{}, "ERR wrong number of arguments for 'subscribe' command"},
		{"PSUBSCRIBE", []interface{}{}, "ERR wrong number of arguments for 'psubscribe' command"},
		{"PUBLISH", []interface{}{}, "ERR wrong number of arguments for 'publish' command"},
		{"PUBLISH", []interface{}{"event1"}, "ERR wrong number of arguments for 'publish' command"},
		{"PUBLISH", []interface{}{"event1", "message2", "message3"}, "ERR wrong number of arguments for 'publish' command"},
		{"PUBSUB", []interface{}{}, "ERR wrong number of arguments for 'pubsub' command"},
		{"PUBSUB", []interface{}{"FOOBAR"}, "ERR Unknown subcommand or wrong number of arguments for 'FOOBAR'. Try PUBSUB HELP."},
		{"PUBSUB", []interface{}{"NUMPAT", "FOOBAR"}, "ERR Unknown subcommand or wrong number of arguments for 'NUMPAT'. Try PUBSUB HELP."},
		{"PUBSUB", []interface{}{"CHANNELS", "FOOBAR1", "FOOBAR2"}, "ERR Unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP."},
	} {
		_, c, done := setup(t)

		_, err := c.Do(command.command, command.args...)
		mustFail(t, err, command.err)

		done()
	}
}
