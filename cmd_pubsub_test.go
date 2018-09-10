package miniredis

import (
	"github.com/gomodule/redigo/redis"
	"regexp"
	"testing"
)

func TestSubscribe(t *testing.T) {
	s, c, done := setup(t)
	defer done()

	{
		a, err := redis.Values(c.Do("SUBSCRIBE", "event1"))
		ok(t, err)
		equals(t, []interface{}{[]byte("subscribe"), []byte("event1"), int64(1)}, a)

		equals(t, 1, len(s.dbs[s.selectedDB].subscribedChannels))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Do("SUBSCRIBE", "event2"))
		ok(t, err)
		equals(t, []interface{}{[]byte("subscribe"), []byte("event2"), int64(2)}, a)

		equals(t, 2, len(s.dbs[s.selectedDB].subscribedChannels))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Do("SUBSCRIBE", "event3", "event4"))
		ok(t, err)
		equals(t, []interface{}{[]byte("subscribe"), []byte("event3"), int64(3)}, a)

		equals(t, 4, len(s.dbs[s.selectedDB].subscribedChannels))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Receive())
		ok(t, err)
		equals(t, []interface{}{[]byte("subscribe"), []byte("event4"), int64(4)}, a)

		equals(t, 4, len(s.dbs[s.selectedDB].subscribedChannels))
		equals(t, 1, len(s.peers))
	}

	{
		sub := s.NewSubscriber()
		defer sub.Close()

		equals(t, map[string]struct{}{}, sub.channels)
		equals(t, map[string]map[*Subscriber]struct{}{}, sub.db.directlySubscribedChannels)

		sub.Subscribe()
		equals(t, map[string]struct{}{}, sub.channels)
		equals(t, map[string]map[*Subscriber]struct{}{}, sub.db.directlySubscribedChannels)

		sub.Subscribe("event1")
		equals(t, map[string]struct{}{"event1": {}}, sub.channels)
		equals(t, map[string]map[*Subscriber]struct{}{"event1": {sub: {}}}, sub.db.directlySubscribedChannels)

		sub.Subscribe("event2")
		equals(t, map[string]struct{}{"event1": {}, "event2": {}}, sub.channels)
		equals(t, map[string]map[*Subscriber]struct{}{"event1": {sub: {}}, "event2": {sub: {}}}, sub.db.directlySubscribedChannels)

		sub.Subscribe("event3", "event4")
		equals(t, map[string]struct{}{"event1": {}, "event2": {}, "event3": {}, "event4": {}}, sub.channels)
		equals(t, map[string]map[*Subscriber]struct{}{"event1": {sub: {}}, "event2": {sub: {}}, "event3": {sub: {}}, "event4": {sub: {}}}, sub.db.directlySubscribedChannels)
	}
}

func TestUnsubscribe(t *testing.T) {
	s, c, done := setup(t)
	defer done()

	c.Do("SUBSCRIBE", "event1", "event2", "event3")
	c.Receive()
	c.Receive()

	{
		a, err := redis.Values(c.Do("UNSUBSCRIBE", "event1", "event2"))
		ok(t, err)
		equals(t, []interface{}{[]byte("unsubscribe"), []byte("event1"), int64(2)}, a)

		equals(t, 1, len(s.dbs[s.selectedDB].subscribedChannels))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Receive())
		ok(t, err)
		equals(t, []interface{}{[]byte("unsubscribe"), []byte("event2"), int64(1)}, a)

		equals(t, 1, len(s.dbs[s.selectedDB].subscribedChannels))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Do("UNSUBSCRIBE", "event3"))
		ok(t, err)
		equals(t, []interface{}{[]byte("unsubscribe"), []byte("event3"), int64(0)}, a)

		equals(t, 0, len(s.dbs[s.selectedDB].subscribedChannels))
		equals(t, 0, len(s.peers))
	}

	{
		a, err := redis.Values(c.Do("UNSUBSCRIBE", "event4"))
		ok(t, err)
		equals(t, []interface{}{[]byte("unsubscribe"), []byte("event4"), int64(0)}, a)

		equals(t, 0, len(s.dbs[s.selectedDB].subscribedChannels))
		equals(t, 0, len(s.peers))
	}

	{
		sub := s.NewSubscriber()
		defer sub.Close()

		sub.Subscribe("event1", "event2", "event3")

		sub.Unsubscribe()
		equals(t, map[string]struct{}{"event1": {}, "event2": {}, "event3": {}}, sub.channels)
		equals(t, map[string]map[*Subscriber]struct{}{"event1": {sub: {}}, "event2": {sub: {}}, "event3": {sub: {}}}, sub.db.directlySubscribedChannels)

		sub.Unsubscribe("event1", "event2")
		equals(t, map[string]struct{}{"event3": {}}, sub.channels)
		equals(t, map[string]map[*Subscriber]struct{}{"event3": {sub: {}}}, sub.db.directlySubscribedChannels)

		sub.Unsubscribe("event3")
		equals(t, map[string]struct{}{}, sub.channels)
		equals(t, map[string]map[*Subscriber]struct{}{}, sub.db.directlySubscribedChannels)

		sub.Unsubscribe("event4")
		equals(t, map[string]struct{}{}, sub.channels)
		equals(t, map[string]map[*Subscriber]struct{}{}, sub.db.directlySubscribedChannels)
	}
}

func TestUnsubscribeAll(t *testing.T) {
	s, c, done := setup(t)
	defer done()

	c.Do("SUBSCRIBE", "event1", "event2", "event3")
	c.Receive()
	c.Receive()

	channels := map[string]struct{}{"event1": {}, "event2": {}, "event3": {}}

	{
		a, err := redis.Values(c.Do("UNSUBSCRIBE"))
		ok(t, err)

		if oneOf(t, mkSubReplySet([]byte("unsubscribe"), channels, 2), a) {
			delete(channels, string(a[1].([]byte)))
		}

		equals(t, 0, len(s.dbs[s.selectedDB].subscribedChannels))
		equals(t, 0, len(s.peers))
	}

	{
		a, err := redis.Values(c.Receive())
		ok(t, err)

		if oneOf(t, mkSubReplySet([]byte("unsubscribe"), channels, 1), a) {
			delete(channels, string(a[1].([]byte)))
		}

		equals(t, 0, len(s.dbs[s.selectedDB].subscribedChannels))
		equals(t, 0, len(s.peers))
	}

	{
		a, err := redis.Values(c.Receive())
		ok(t, err)

		if oneOf(t, mkSubReplySet([]byte("unsubscribe"), channels, 0), a) {
			delete(channels, string(a[1].([]byte)))
		}

		equals(t, 0, len(s.dbs[s.selectedDB].subscribedChannels))
		equals(t, 0, len(s.peers))
	}

	{
		sub := s.NewSubscriber()
		defer sub.Close()

		sub.Subscribe("event1", "event2", "event3")

		sub.UnsubscribeAll()
		equals(t, map[string]struct{}{}, sub.channels)
		equals(t, map[string]map[*Subscriber]struct{}{}, sub.db.directlySubscribedChannels)
	}
}

func TestPSubscribe(t *testing.T) {
	s, c, done := setup(t)
	defer done()

	{
		a, err := redis.Values(c.Do("PSUBSCRIBE", "event1"))
		ok(t, err)
		equals(t, []interface{}{[]byte("psubscribe"), []byte("event1"), int64(1)}, a)

		equals(t, 1, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Do("PSUBSCRIBE", "event2?"))
		ok(t, err)
		equals(t, []interface{}{[]byte("psubscribe"), []byte("event2?"), int64(2)}, a)

		equals(t, 2, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Do("PSUBSCRIBE", "event3*", "event4[abc]"))
		ok(t, err)
		equals(t, []interface{}{[]byte("psubscribe"), []byte("event3*"), int64(3)}, a)

		equals(t, 4, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Receive())
		ok(t, err)
		equals(t, []interface{}{[]byte("psubscribe"), []byte("event4[abc]"), int64(4)}, a)

		equals(t, 4, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Do("PSUBSCRIBE", "event5[]"))
		ok(t, err)
		equals(t, []interface{}{[]byte("psubscribe"), []byte("event5[]"), int64(5)}, a)

		equals(t, 5, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 1, len(s.peers))
	}

	{
		sub := s.NewSubscriber()
		defer sub.Close()

		rgxs := [5]*regexp.Regexp{
			regexp.MustCompile(`\Aevent1\z`),
			regexp.MustCompile(`\Aevent2.\z`),
			regexp.MustCompile(`\Aevent3`),
			regexp.MustCompile(`\Aevent4[abc]\z`),
			regexp.MustCompile(`\Aevent5X\bY\z`),
		}

		equals(t, map[*regexp.Regexp]struct{}{}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{}, sub.db.directlySubscribedPatterns)

		sub.PSubscribe()
		equals(t, map[*regexp.Regexp]struct{}{}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{}, sub.db.directlySubscribedPatterns)

		sub.PSubscribe(rgxs[0])
		equals(t, map[*regexp.Regexp]struct{}{rgxs[0]: {}}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{rgxs[0]: {sub: {}}}, sub.db.directlySubscribedPatterns)

		sub.PSubscribe(rgxs[1])
		equals(t, map[*regexp.Regexp]struct{}{rgxs[0]: {}, rgxs[1]: {}}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{rgxs[0]: {sub: {}}, rgxs[1]: {sub: {}}}, sub.db.directlySubscribedPatterns)

		sub.PSubscribe(rgxs[2], rgxs[3])
		equals(t, map[*regexp.Regexp]struct{}{rgxs[0]: {}, rgxs[1]: {}, rgxs[2]: {}, rgxs[3]: {}}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{rgxs[0]: {sub: {}}, rgxs[1]: {sub: {}}, rgxs[2]: {sub: {}}, rgxs[3]: {sub: {}}}, sub.db.directlySubscribedPatterns)

		sub.PSubscribe(rgxs[4])
		equals(t, map[*regexp.Regexp]struct{}{rgxs[0]: {}, rgxs[1]: {}, rgxs[2]: {}, rgxs[3]: {}, rgxs[4]: {}}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{rgxs[0]: {sub: {}}, rgxs[1]: {sub: {}}, rgxs[2]: {sub: {}}, rgxs[3]: {sub: {}}, rgxs[4]: {sub: {}}}, sub.db.directlySubscribedPatterns)
	}
}

func TestPUnsubscribe(t *testing.T) {
	s, c, done := setup(t)
	defer done()

	c.Do("PSUBSCRIBE", "event1", "event2?", "event3*", "event4[abc]", "event5[]")
	c.Receive()
	c.Receive()
	c.Receive()
	c.Receive()

	{
		a, err := redis.Values(c.Do("PUNSUBSCRIBE", "event1", "event2?"))
		ok(t, err)
		equals(t, []interface{}{[]byte("punsubscribe"), []byte("event1"), int64(4)}, a)

		equals(t, 3, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Receive())
		ok(t, err)
		equals(t, []interface{}{[]byte("punsubscribe"), []byte("event2?"), int64(3)}, a)

		equals(t, 3, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Do("PUNSUBSCRIBE", "event3*"))
		ok(t, err)
		equals(t, []interface{}{[]byte("punsubscribe"), []byte("event3*"), int64(2)}, a)

		equals(t, 2, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Do("PUNSUBSCRIBE", "event4[abc]"))
		ok(t, err)
		equals(t, []interface{}{[]byte("punsubscribe"), []byte("event4[abc]"), int64(1)}, a)

		equals(t, 1, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 1, len(s.peers))
	}

	{
		a, err := redis.Values(c.Do("PUNSUBSCRIBE", "event5[]"))
		ok(t, err)
		equals(t, []interface{}{[]byte("punsubscribe"), []byte("event5[]"), int64(0)}, a)

		equals(t, 0, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 0, len(s.peers))
	}

	{
		a, err := redis.Values(c.Do("PUNSUBSCRIBE", "event6"))
		ok(t, err)
		equals(t, []interface{}{[]byte("punsubscribe"), []byte("event6"), int64(0)}, a)

		equals(t, 0, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 0, len(s.peers))
	}

	{
		sub := s.NewSubscriber()
		defer sub.Close()

		rgxs := [5]*regexp.Regexp{
			regexp.MustCompile(`\Aevent1\z`),
			regexp.MustCompile(`\Aevent2.\z`),
			regexp.MustCompile(`\Aevent3`),
			regexp.MustCompile(`\Aevent4[abc]\z`),
			regexp.MustCompile(`\Aevent5X\bY\z`),
		}

		sub.PSubscribe(rgxs[:]...)

		sub.PUnsubscribe()
		equals(t, map[*regexp.Regexp]struct{}{rgxs[0]: {}, rgxs[1]: {}, rgxs[2]: {}, rgxs[3]: {}, rgxs[4]: {}}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{rgxs[0]: {sub: {}}, rgxs[1]: {sub: {}}, rgxs[2]: {sub: {}}, rgxs[3]: {sub: {}}, rgxs[4]: {sub: {}}}, sub.db.directlySubscribedPatterns)

		sub.PUnsubscribe(rgxs[0], rgxs[1])
		equals(t, map[*regexp.Regexp]struct{}{rgxs[2]: {}, rgxs[3]: {}, rgxs[4]: {}}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{rgxs[2]: {sub: {}}, rgxs[3]: {sub: {}}, rgxs[4]: {sub: {}}}, sub.db.directlySubscribedPatterns)

		sub.PUnsubscribe(rgxs[2])
		equals(t, map[*regexp.Regexp]struct{}{rgxs[3]: {}, rgxs[4]: {}}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{rgxs[3]: {sub: {}}, rgxs[4]: {sub: {}}}, sub.db.directlySubscribedPatterns)

		sub.PUnsubscribe(rgxs[3])
		equals(t, map[*regexp.Regexp]struct{}{rgxs[4]: {}}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{rgxs[4]: {sub: {}}}, sub.db.directlySubscribedPatterns)

		sub.PUnsubscribe(rgxs[4])
		equals(t, map[*regexp.Regexp]struct{}{}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{}, sub.db.directlySubscribedPatterns)

		sub.PUnsubscribe(regexp.MustCompile(`\Aevent6\z`))
		equals(t, map[*regexp.Regexp]struct{}{}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{}, sub.db.directlySubscribedPatterns)
	}
}

func TestPUnsubscribeAll(t *testing.T) {
	s, c, done := setup(t)
	defer done()

	c.Do("PSUBSCRIBE", "event1", "event2?", "event3*", "event4[abc]", "event5[]")
	c.Receive()
	c.Receive()
	c.Receive()
	c.Receive()

	patterns := map[string]struct{}{"event1": {}, "event2?": {}, "event3*": {}, "event4[abc]": {}, "event5[]": {}}

	{
		a, err := redis.Values(c.Do("PUNSUBSCRIBE"))
		ok(t, err)

		if oneOf(t, mkSubReplySet([]byte("punsubscribe"), patterns, 4), a) {
			delete(patterns, string(a[1].([]byte)))
		}

		equals(t, 0, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 0, len(s.peers))
	}

	{
		a, err := redis.Values(c.Receive())
		ok(t, err)

		if oneOf(t, mkSubReplySet([]byte("punsubscribe"), patterns, 3), a) {
			delete(patterns, string(a[1].([]byte)))
		}

		equals(t, 0, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 0, len(s.peers))
	}

	{
		a, err := redis.Values(c.Receive())
		ok(t, err)

		if oneOf(t, mkSubReplySet([]byte("punsubscribe"), patterns, 2), a) {
			delete(patterns, string(a[1].([]byte)))
		}

		equals(t, 0, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 0, len(s.peers))
	}

	{
		a, err := redis.Values(c.Receive())
		ok(t, err)

		if oneOf(t, mkSubReplySet([]byte("punsubscribe"), patterns, 1), a) {
			delete(patterns, string(a[1].([]byte)))
		}

		equals(t, 0, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 0, len(s.peers))
	}

	{
		a, err := redis.Values(c.Receive())
		ok(t, err)

		if oneOf(t, mkSubReplySet([]byte("punsubscribe"), patterns, 0), a) {
			delete(patterns, string(a[1].([]byte)))
		}

		equals(t, 0, len(s.dbs[s.selectedDB].subscribedPatterns))
		equals(t, 0, len(s.peers))
	}

	{
		sub := s.NewSubscriber()
		defer sub.Close()

		sub.PSubscribe(
			regexp.MustCompile(`\Aevent1\z`),
			regexp.MustCompile(`\Aevent2.\z`),
			regexp.MustCompile(`\Aevent3`),
			regexp.MustCompile(`\Aevent4[abc]\z`),
			regexp.MustCompile(`\Aevent5X\bY\z`),
		)

		sub.PUnsubscribeAll()
		equals(t, map[*regexp.Regexp]struct{}{}, sub.patterns)
		equals(t, map[*regexp.Regexp]map[*Subscriber]struct{}{}, sub.db.directlySubscribedPatterns)
	}
}

func mkSubReplySet(subject []byte, channels map[string]struct{}, subs int64) []interface{} {
	result := make([]interface{}, len(channels))
	i := 0

	for channel := range channels {
		result[i] = []interface{}{subject, []byte(channel), subs}
		i++
	}

	return result
}

func TestPublish(t *testing.T) {
	s, c, done := setup(t)
	defer done()

	{
		a, err := redis.Int(c.Do("PUBLISH", "event1", "message2"))
		ok(t, err)
		equals(t, 0, a)
	}

	equals(t, 0, s.Publish("event1", "message2"))
}

func TestPubSubChannels(t *testing.T) {
	s, c, done := setup(t)
	defer done()

	{
		a, err := redis.Values(c.Do("PUBSUB", "CHANNELS"))
		ok(t, err)
		equals(t, []interface{}{}, a)
	}

	{
		a, err := redis.Values(c.Do("PUBSUB", "CHANNELS", "event1?*[abc]"))
		ok(t, err)
		equals(t, []interface{}{}, a)
	}

	equals(t, map[string]struct{}{}, s.PubSubChannels(nil))
	equals(t, map[string]struct{}{}, s.PubSubChannels(regexp.MustCompile(`\Aevent1..*[abc]\z`)))
}

func TestPubSubNumSub(t *testing.T) {
	s, c, done := setup(t)
	defer done()

	{
		a, err := redis.Values(c.Do("PUBSUB", "NUMSUB"))
		ok(t, err)
		equals(t, []interface{}{}, a)
	}

	{
		a, err := redis.Values(c.Do("PUBSUB", "NUMSUB", "event1"))
		ok(t, err)
		equals(t, []interface{}{[]byte("event1"), int64(0)}, a)
	}

	{
		a, err := redis.Values(c.Do("PUBSUB", "NUMSUB", "event1", "event2"))
		ok(t, err)
		equals(t, []interface{}{[]byte("event1"), int64(0), []byte("event2"), int64(0)}, a)
	}

	equals(t, map[string]int{"event1": 0}, s.PubSubNumSub("event1"))
}

func TestPubSubNumPat(t *testing.T) {
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
		{"PUBSUB", []interface{}{"FOOBAR"}, "ERR Unknown PUBSUB subcommand or wrong number of arguments for 'FOOBAR'"},
		{"PUBSUB", []interface{}{"NUMPAT", "FOOBAR"}, "ERR Unknown PUBSUB subcommand or wrong number of arguments for 'NUMPAT'"},
		{"PUBSUB", []interface{}{"CHANNELS", "FOOBAR1", "FOOBAR2"}, "ERR Unknown PUBSUB subcommand or wrong number of arguments for 'CHANNELS'"},
	} {
		_, c, done := setup(t)

		_, err := c.Do(command.command, command.args...)
		mustFail(t, err, command.err)

		done()
	}
}
