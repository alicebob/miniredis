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
		equals(t, []string{"message", "event4b", "hello 4b!"}, s)
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

	_, err = c2.Do("SUBSCRIBE", "event1", "event1b", "event1c")
	ok(t, err)

	a, err = redis.Strings(c1.Do("PUBSUB", "CHANNELS"))
	ok(t, err)
	equals(t, []string{"event1", "event1b", "event1c"}, a)

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

func TestPubSubInteraction(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	ch := make(chan struct{}, 8)
	tasks := [5]func(){}
	directTasks := [4]func(){}

	for i, tester := range [5]func(t *testing.T, s *Miniredis, c redis.Conn, chCtl chan struct{}){
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

func testPubSubInteractionSub1(t *testing.T, _ *Miniredis, c redis.Conn, ch chan struct{}) {
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

func testPubSubInteractionSub2(t *testing.T, _ *Miniredis, c redis.Conn, ch chan struct{}) {
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

	sub.Subscribe("event1")
	sub.Subscribe("event3")
	sub.Subscribe("event4")
	sub.Subscribe("event6")

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '1', '3', '4', '6')

	sub.Unsubscribe("event1")
	sub.Unsubscribe("event4")

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '3', '6')
}

func testPubSubInteractionDirectSub2(t *testing.T, s *Miniredis, ch chan struct{}) {
	sub := s.NewSubscriber()
	defer sub.Close()

	sub.Subscribe("event2")
	sub.Subscribe("event3")
	sub.Subscribe("event4")
	sub.Subscribe("event5")

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '2', '3', '4', '5')

	sub.Unsubscribe("event3")
	sub.Unsubscribe("event5")

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '2', '4')
}

func testPubSubInteractionPsub1(t *testing.T, _ *Miniredis, c redis.Conn, ch chan struct{}) {
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

func testPubSubInteractionPsub2(t *testing.T, _ *Miniredis, c redis.Conn, ch chan struct{}) {
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
	sub := s.NewSubscriber()
	defer sub.Close()

	sub.Psubscribe(`event[ab1]`)
	sub.Psubscribe(`event[ef3]`)
	sub.Psubscribe(`event[gh]`)
	sub.Psubscribe(`event[kl6]`)

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '1', '3', '6', 'a', 'b', 'e', 'f', 'g', 'h', 'k', 'l')

	sub.Punsubscribe(`event[ab1]`)
	sub.Punsubscribe(`event[gh]`)

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '3', '6', 'e', 'f', 'k', 'l')
}

func testPubSubInteractionDirectPsub2(t *testing.T, s *Miniredis, ch chan struct{}) {
	sub := s.NewSubscriber()
	defer sub.Close()

	sub.Psubscribe(`event[cd]`)
	sub.Psubscribe(`event[ef]`)
	sub.Psubscribe(`event[gh4]`)
	sub.Psubscribe(`event[ij]`)

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '4', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j')

	sub.Punsubscribe(`event[ef]`)
	sub.Punsubscribe(`event[ij]`)

	ch <- struct{}{}
	receiveMessagesDirectlyDuringPubSub(t, sub, '4', 'c', 'd', 'g', 'h')
}

func testPubSubInteractionPub(t *testing.T, s *Miniredis, c redis.Conn, ch chan struct{}) {
	testPubSubInteractionPubStage1(t, s, c, ch)
	testPubSubInteractionPubStage2(t, s, c, ch)
}

func testPubSubInteractionPubStage1(t *testing.T, s *Miniredis, c redis.Conn, ch chan struct{}) {
	for i := uint8(0); i < 8; i++ {
		<-ch
	}

	for _, pattern := range []string{
		"",
		"event?",
	} {
		assertActiveChannelsDuringPubSub(t, s, c, pattern, []string{
			"event1", "event2", "event3", "event4", "event5", "event6",
		})
	}

	assertActiveChannelsDuringPubSub(t, s, c, "*[123]", []string{
		"event1", "event2", "event3",
	})

	assertNumSubDuringPubSub(t, s, c, map[string]int{
		"event1": 2, "event2": 2, "event3": 4, "event4": 4, "event5": 2, "event6": 2,
		"event[ab1]": 0, "event[cd]": 0, "event[ef3]": 0, "event[gh]": 0, "event[ij]": 0, "event[kl6]": 0,
	})

	assertNumPatDuringPubSub(t, s, c, 16)

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

func testPubSubInteractionPubStage2(t *testing.T, s *Miniredis, c redis.Conn, ch chan struct{}) {
	for i := uint8(0); i < 8; i++ {
		<-ch
	}

	for _, pattern := range []string{
		"",
		"event?",
	} {
		assertActiveChannelsDuringPubSub(t, s, c, pattern, []string{
			"event1", "event2", "event3", "event4", "event6",
		})
	}

	assertActiveChannelsDuringPubSub(t, s, c, "*[123]", []string{"event1", "event2", "event3"})

	assertNumSubDuringPubSub(t, s, c, map[string]int{
		"event1": 1, "event2": 1, "event3": 2, "event4": 2, "event5": 0, "event6": 2,
		"event[ab1]": 0, "event[cd]": 0, "event[ef3]": 0, "event[gh]": 0, "event[ij]": 0, "event[kl6]": 0,
	})

	assertNumPatDuringPubSub(t, s, c, 8)

	for _, message := range [18]struct {
		channelSuffix rune
		subscribers   uint8
	}{
		{'1', 2}, {'2', 1}, {'3', 3}, {'4', 3}, {'5', 0}, {'6', 4},
		{'a', 1}, {'b', 1}, {'c', 1}, {'d', 1}, {'e', 2}, {'f', 2},
		{'g', 2}, {'h', 2}, {'i', 0}, {'j', 0}, {'k', 2}, {'l', 2},
	} {
		suffix := string([]rune{message.channelSuffix})
		equals(t, int(message.subscribers), s.Publish("event"+suffix, "message"+suffix))
	}
}

func runActualRedisClientForPubSub(t *testing.T, s *Miniredis, chCtl chan struct{}, tester func(t *testing.T, s *Miniredis, c redis.Conn, chCtl chan struct{})) (wait func()) {
	t.Helper()

	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	ch := make(chan struct{})

	go func() {
		t.Helper()

		tester(t, s, c, chCtl)
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
		equals(t, PubsubMessage{"event" + suff, "message" + suff}, <-sub.Messages())
	}
}

func assertActiveChannelsDuringPubSub(t *testing.T, s *Miniredis, c redis.Conn, pattern string, channels []string) {
	var args []interface{}
	if pattern == "" {
		args = []interface{}{"CHANNELS"}
	} else {
		args = []interface{}{"CHANNELS", pattern}
	}

	actual, err := redis.Strings(c.Do("PUBSUB", args...))
	ok(t, err)

	equals(t, channels, actual)

	equals(t, channels, s.PubSubChannels(pattern))
}

func assertNumSubDuringPubSub(t *testing.T, s *Miniredis, c redis.Conn, channels map[string]int) {
	t.Helper()

	args := make([]interface{}, 1+len(channels))
	args[0] = "NUMSUB"
	i := 1

	flatChannels := make([]string, len(channels))
	j := 0

	for channel := range channels {
		args[i] = channel
		i++

		flatChannels[j] = channel
		j++
	}

	a, err := redis.Values(c.Do("PUBSUB", args...))
	ok(t, err)
	equals(t, len(channels)*2, len(a))

	actualChannels := make(map[string]int, len(a))

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
				actualChannels[currentChannel] = int(subsInt)
			}

			currentState &= ^uint8(1)
		}
	}

	equals(t, channels, actualChannels)

	equals(t, channels, s.PubSubNumSub(flatChannels...))
}

func assertNumPatDuringPubSub(t *testing.T, s *Miniredis, c redis.Conn, numPat int) {
	t.Helper()

	a, err := redis.Int(c.Do("PUBSUB", "NUMPAT"))
	ok(t, err)
	equals(t, numPat, a)

	equals(t, numPat, s.PubSubNumPat())
}
