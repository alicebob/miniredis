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
		oneOf(t, []interface{}{
			[]interface{}{[]byte("event1"), int64(0), []byte("event2"), int64(0)},
			[]interface{}{[]byte("event2"), int64(0), []byte("event1"), int64(0)},
		}, a)
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

func TestPubSubInteraction(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	ch := make(chan struct{}, 8)
	tasks := [5]func(){}
	directTasks := [4]func(){}

	for i, tester := range [5]func(t *testing.T, c redis.Conn, chCtl chan struct{}){
		testPubSubInteractionSub1,
		testPubSubInteractionSub2,
		testPubSubInteractionPsub1,
		testPubSubInteractionPsub2,
		testPubSubInteractionPub,
	} {
		tasks[i] = runActualRedisClientForPubSub(t, s, ch, tester)
	}

	for i, tester := range [4]func(t *testing.T, s *Miniredis, chCtl chan struct{}){
		testPubSubInteractionDirectSub1,
		testPubSubInteractionDirectSub2,
		testPubSubInteractionDirectPsub1,
		testPubSubInteractionDirectPsub2,
	} {
		directTasks[i] = runDirectRedisClientForPubSub(t, s, ch, tester)
	}

	for _, task := range tasks {
		task()
	}

	for _, task := range directTasks {
		task()
	}
}

func testPubSubInteractionSub1(t *testing.T, c redis.Conn, ch chan struct{}) {
	assertCorrectSubscriptionsCounts(
		t,
		[]int64{1, 2, 3, 4},
		runCmdDuringPubSub(t, c, 3, "SUBSCRIBE", "event1", "event2", "event3", "event4"),
	)

	ch <- struct{}{}
	receiveMessagesDuringPubSub(t, c, '1', '2', '3', '4')

	assertCorrectSubscriptionsCounts(
		t,
		[]int64{3, 2},
		runCmdDuringPubSub(t, c, 1, "UNSUBSCRIBE", "event2", "event3"),
	)

	ch <- struct{}{}
	receiveMessagesDuringPubSub(t, c, '1', '4')
}

func testPubSubInteractionSub2(t *testing.T, c redis.Conn, ch chan struct{}) {
	assertCorrectSubscriptionsCounts(
		t,
		[]int64{1, 2, 3, 4},
		runCmdDuringPubSub(t, c, 3, "SUBSCRIBE", "event3", "event4", "event5", "event6"),
	)

	ch <- struct{}{}
	receiveMessagesDuringPubSub(t, c, '3', '4', '5', '6')

	assertCorrectSubscriptionsCounts(
		t,
		[]int64{3, 2},
		runCmdDuringPubSub(t, c, 1, "UNSUBSCRIBE", "event4", "event5"),
	)

	ch <- struct{}{}
	receiveMessagesDuringPubSub(t, c, '3', '6')
}

func testPubSubInteractionDirectSub1(t *testing.T, s *Miniredis, ch chan struct{}) {
	sub := s.NewSubscriber()
	defer sub.Close()

	sub.Subscribe("event1", "event3", "event4", "event6")

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '1', '3', '4', '6')

	sub.Unsubscribe("event1", "event4")

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '3', '6')
}

func testPubSubInteractionDirectSub2(t *testing.T, s *Miniredis, ch chan struct{}) {
	sub := s.NewSubscriber()
	defer sub.Close()

	sub.Subscribe("event2", "event3", "event4", "event5")

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '2', '3', '4', '5')

	sub.Unsubscribe("event3", "event5")

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '2', '4')
}

func testPubSubInteractionPsub1(t *testing.T, c redis.Conn, ch chan struct{}) {
	assertCorrectSubscriptionsCounts(
		t,
		[]int64{1, 2, 3, 4},
		runCmdDuringPubSub(t, c, 3, "PSUBSCRIBE", "event[ab1]", "event[cd]", "event[ef3]", "event[gh]"),
	)

	ch <- struct{}{}
	receiveMessagesDuringPubSub(t, c, '1', '3', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h')

	assertCorrectSubscriptionsCounts(
		t,
		[]int64{3, 2},
		runCmdDuringPubSub(t, c, 1, "PUNSUBSCRIBE", "event[cd]", "event[ef3]"),
	)

	ch <- struct{}{}
	receiveMessagesDuringPubSub(t, c, '1', 'a', 'b', 'g', 'h')
}

func testPubSubInteractionPsub2(t *testing.T, c redis.Conn, ch chan struct{}) {
	assertCorrectSubscriptionsCounts(
		t,
		[]int64{1, 2, 3, 4},
		runCmdDuringPubSub(t, c, 3, "PSUBSCRIBE", "event[ef]", "event[gh4]", "event[ij]", "event[kl6]"),
	)

	ch <- struct{}{}
	receiveMessagesDuringPubSub(t, c, '4', '6', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l')

	assertCorrectSubscriptionsCounts(
		t,
		[]int64{3, 2},
		runCmdDuringPubSub(t, c, 1, "PUNSUBSCRIBE", "event[gh4]", "event[ij]"),
	)

	ch <- struct{}{}
	receiveMessagesDuringPubSub(t, c, '6', 'e', 'f', 'k', 'l')
}

func testPubSubInteractionDirectPsub1(t *testing.T, s *Miniredis, ch chan struct{}) {
	rgx := regexp.MustCompile
	sub := s.NewSubscriber()
	defer sub.Close()

	sub.PSubscribe(rgx(`\Aevent[ab1]\z`), rgx(`\Aevent[ef3]\z`), rgx(`\Aevent[gh]\z`), rgx(`\Aevent[kl6]\z`))

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '1', '3', '6', 'a', 'b', 'e', 'f', 'g', 'h', 'k', 'l')

	sub.PUnsubscribe(rgx(`\Aevent[ab1]\z`), rgx(`\Aevent[gh]\z`))

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '3', '6', 'e', 'f', 'k', 'l')
}

func testPubSubInteractionDirectPsub2(t *testing.T, s *Miniredis, ch chan struct{}) {
	rgx := regexp.MustCompile
	sub := s.NewSubscriber()
	defer sub.Close()

	sub.PSubscribe(rgx(`\Aevent[cd]\z`), rgx(`\Aevent[ef]\z`), rgx(`\Aevent[gh4]\z`), rgx(`\Aevent[ij]\z`))

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '4', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j')

	sub.PUnsubscribe(rgx(`\Aevent[ef]\z`), rgx(`\Aevent[ij]\z`))

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '4', 'c', 'd', 'g', 'h')
}

func testPubSubInteractionPub(t *testing.T, c redis.Conn, ch chan struct{}) {
	testPubSubInteractionPubStage1(t, c, ch)
	testPubSubInteractionPubStage2(t, c, ch)
}

func testPubSubInteractionPubStage1(t *testing.T, c redis.Conn, ch chan struct{}) {
	for i := uint8(0); i < 8; i++ {
		<-ch
	}

	for _, pattern := range [2]string{"", "event?"} {
		assertActiveChannelsDuringPubSub(t, c, pattern, map[string]struct{}{
			"event1": {}, "event2": {}, "event3": {}, "event4": {}, "event5": {}, "event6": {},
		})
	}

	assertActiveChannelsDuringPubSub(t, c, "*[123]", map[string]struct{}{
		"event1": {}, "event2": {}, "event3": {},
	})

	assertNumSubDuringPubSub(t, c, map[string]int64{
		"event1": 2, "event2": 2, "event3": 4, "event4": 4, "event5": 2, "event6": 2,
		"event[ab1]": 0, "event[cd]": 0, "event[ef3]": 0, "event[gh]": 0, "event[ij]": 0, "event[kl6]": 0,
	})

	assertNumPatDuringPubSub(t, c, 16)

	for _, message := range [18]struct {
		channelSuffix rune
		subscribers   uint8
	}{
		{'1', 4}, {'2', 2}, {'3', 6}, {'4', 6}, {'5', 2}, {'6', 4},
		{'a', 2}, {'b', 2}, {'c', 2}, {'d', 2}, {'e', 4}, {'f', 4},
		{'g', 4}, {'h', 4}, {'i', 2}, {'j', 2}, {'k', 2}, {'l', 2},
	} {
		suffix := string([]rune{message.channelSuffix})
		replies := runCmdDuringPubSub(t, c, 0, "PUBLISH", "event"+suffix, "message"+suffix)
		equals(t, []interface{}{int64(message.subscribers)}, replies)
	}
}

func testPubSubInteractionPubStage2(t *testing.T, c redis.Conn, ch chan struct{}) {
	for i := uint8(0); i < 8; i++ {
		<-ch
	}

	for _, pattern := range [2]string{"", "event?"} {
		assertActiveChannelsDuringPubSub(t, c, pattern, map[string]struct{}{
			"event1": {}, "event2": {}, "event3": {}, "event4": {}, "event6": {},
		})
	}

	assertActiveChannelsDuringPubSub(t, c, "*[123]", map[string]struct{}{
		"event1": {}, "event2": {}, "event3": {},
	})

	assertNumSubDuringPubSub(t, c, map[string]int64{
		"event1": 1, "event2": 1, "event3": 2, "event4": 2, "event5": 0, "event6": 2,
		"event[ab1]": 0, "event[cd]": 0, "event[ef3]": 0, "event[gh]": 0, "event[ij]": 0, "event[kl6]": 0,
	})

	assertNumPatDuringPubSub(t, c, 8)

	for _, message := range [18]struct {
		channelSuffix rune
		subscribers   uint8
	}{
		{'1', 2}, {'2', 1}, {'3', 3}, {'4', 3}, {'5', 0}, {'6', 4},
		{'a', 1}, {'b', 1}, {'c', 1}, {'d', 1}, {'e', 2}, {'f', 2},
		{'g', 2}, {'h', 2}, {'i', 0}, {'j', 0}, {'k', 2}, {'l', 2},
	} {
		suffix := string([]rune{message.channelSuffix})
		replies := runCmdDuringPubSub(t, c, 0, "PUBLISH", "event"+suffix, "message"+suffix)
		equals(t, []interface{}{int64(message.subscribers)}, replies)
	}
}

func runActualRedisClientForPubSub(t *testing.T, s *Miniredis, chCtl chan struct{}, tester func(t *testing.T, c redis.Conn, chCtl chan struct{})) (wait func()) {
	t.Helper()

	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	ch := make(chan struct{})

	go func() {
		t.Helper()

		tester(t, c, chCtl)
		c.Close()
		close(ch)
	}()

	return func() { <-ch }
}

func runDirectRedisClientForPubSub(t *testing.T, s *Miniredis, chCtl chan struct{}, tester func(t *testing.T, s *Miniredis, chCtl chan struct{})) (wait func()) {
	t.Helper()

	ch := make(chan struct{})

	go func() {
		t.Helper()

		tester(t, s, chCtl)
		close(ch)
	}()

	return func() { <-ch }
}

func runCmdDuringPubSub(t *testing.T, c redis.Conn, followUpMessages uint8, command string, args ...interface{}) (replies []interface{}) {
	t.Helper()

	replies = make([]interface{}, followUpMessages+1)

	reply, err := c.Do(command, args...)
	ok(t, err)

	replies[0] = reply
	i := 1

	for ; followUpMessages > 0; followUpMessages-- {
		reply, err := c.Receive()
		ok(t, err)

		replies[i] = reply
		i++
	}

	return
}

func assertCorrectSubscriptionsCounts(t *testing.T, subscriptionsCounts []int64, replies []interface{}) {
	t.Helper()

	for i, subscriptionsCount := range subscriptionsCounts {
		if arrayReply, isArrayReply := replies[i].([]interface{}); isArrayReply && len(arrayReply) > 2 {
			equals(t, subscriptionsCount, arrayReply[2])
		}
	}
}

func receiveMessagesDuringPubSub(t *testing.T, c redis.Conn, suffixes ...rune) {
	t.Helper()

	for _, suffix := range suffixes {
		msg, err := c.Receive()
		ok(t, err)

		suff := string([]rune{suffix})
		equals(t, []interface{}{[]byte("message"), []byte("event" + suff), []byte("message" + suff)}, msg)
	}
}

func receiveMessagesDirectlyDuringPubSub(t *testing.T, sub *Subscriber, suffixes ...rune) {
	t.Helper()

	for _, suffix := range suffixes {
		suff := string([]rune{suffix})
		equals(t, Message{"event" + suff, "message" + suff}, <-sub.Messages)
	}
}

func assertActiveChannelsDuringPubSub(t *testing.T, c redis.Conn, pattern string, channels map[string]struct{}) {
	t.Helper()

	var args []interface{}
	if pattern == "" {
		args = []interface{}{"CHANNELS"}
	} else {
		args = []interface{}{"CHANNELS", pattern}
	}

	a, err := redis.Values(c.Do("PUBSUB", args...))
	ok(t, err)

	actualChannels := make(map[string]struct{}, len(a))

	for _, channel := range a {
		if channelString, channelIsString := channel.([]byte); channelIsString {
			actualChannels[string(channelString)] = struct{}{}
		}
	}

	equals(t, channels, actualChannels)
}

func assertNumSubDuringPubSub(t *testing.T, c redis.Conn, channels map[string]int64) {
	t.Helper()

	args := make([]interface{}, 1+len(channels))
	args[0] = "NUMSUB"
	i := 1

	for channel := range channels {
		args[i] = channel
		i++
	}

	a, err := redis.Values(c.Do("PUBSUB", args...))
	ok(t, err)
	equals(t, len(channels)*2, len(a))

	actualChannels := make(map[string]int64, len(a))

	var currentChannel string
	currentState := uint8(0)

	for _, item := range a {
		if currentState&uint8(1) == 0 {
			if channelString, channelIsString := item.([]byte); channelIsString {
				currentChannel = string(channelString)
				currentState |= 2
			} else {
				currentState &= ^uint8(2)
			}

			currentState |= 1
		} else {
			if subsInt, subsIsInt := item.(int64); subsIsInt && currentState&uint8(2) != 0 {
				actualChannels[currentChannel] = subsInt
			}

			currentState &= ^uint8(1)
		}
	}

	equals(t, channels, actualChannels)
}

func assertNumPatDuringPubSub(t *testing.T, c redis.Conn, numPat int) {
	t.Helper()

	a, err := redis.Int(c.Do("PUBSUB", "NUMPAT"))
	ok(t, err)
	equals(t, numPat, a)
}
