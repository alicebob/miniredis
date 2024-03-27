package miniredis

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/proto"
)

// Test XADD / XLEN / XRANGE
func TestStream(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"XADD", "s", "1234567-89", "one", "1", "two", "2",
		proto.String("1234567-89"),
	)

	must1(t, c,
		"XLEN", "s",
	)

	t.Run("TYPE", func(t *testing.T) {
		mustDo(t, c,
			"TYPE", "s",
			proto.Inline("stream"),
		)
	})

	mustDo(t, c,
		"XINFO", "STREAM", "s",
		proto.Array(proto.String("length"), proto.Int(1)),
	)

	now := time.Date(2001, 1, 1, 4, 4, 5, 4000000, time.UTC)
	s.SetTime(now)

	t.Run("direct usage", func(t *testing.T) {
		_, err := s.XAdd("s1", "0-0", []string{"name", "foo"})
		mustFail(t, err, msgStreamIDZero)

		id, err := s.XAdd("s1", "12345-67", []string{"name", "bar"})
		ok(t, err)
		equals(t, "12345-67", id)

		_, err = s.XAdd("s1", "12345-0", []string{"name", "foo"})
		mustFail(t, err, msgStreamIDTooSmall)

		id, err = s.XAdd("s1", "*", []string{"name", "baz"})
		ok(t, err)
		equals(t, "978321845004-0", id)

		stream, err := s.Stream("s1")
		ok(t, err)
		equals(t, 2, len(stream))
		equals(t, StreamEntry{
			ID:     "12345-67",
			Values: []string{"name", "bar"},
		}, stream[0])
		equals(t, StreamEntry{
			ID:     "978321845004-0",
			Values: []string{"name", "baz"},
		}, stream[1])
	})

	useRESP3(t, c)
	t.Run("resp3", func(t *testing.T) {
		mustDo(t, c,
			"XINFO", "STREAM", "s",
			proto.Map(proto.String("length"), proto.Int(1)),
		)
	})
}

// Test XADD
func TestStreamAdd(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("XADD", func(t *testing.T) {
		mustDo(t, c,
			"XADD", "s", "123456", "one", "11", "two", "22",
			proto.String("123456-0"),
		)

		res, err := c.Do("XADD", "s", "*", "one", "1", "two", "2")
		ok(t, err)
		exp := `\d+-0`
		matched, err := regexp.MatchString(exp, res)
		ok(t, err)
		assert(t, matched, "expected: %#v got: %#v", exp, res)

		k := fmt.Sprintf("%d-0", uint64(math.MaxUint64-100))
		mustDo(t, c,
			"XADD", "s", k, "one", "11", "two", "22",
			proto.String(k),
		)

		mustDo(t, c,
			"XADD", "s", "*", "one", "111", "two", "222",
			proto.String(fmt.Sprintf("%d-1", uint64(math.MaxUint64-100))),
		)
	})

	t.Run("XADD SetTime", func(t *testing.T) {
		now := time.Date(2001, 1, 1, 4, 4, 5, 4000000, time.UTC)
		s.SetTime(now)
		mustDo(t, c,
			"XADD", "now", "*", "one", "1",
			proto.String("978321845004-0"),
		)

		mustDo(t, c,
			"XADD", "now", "*", "two", "2",
			proto.String("978321845004-1"),
		)
	})

	t.Run("XADD MAXLEN", func(t *testing.T) {
		now := time.Date(2001, 1, 1, 4, 4, 5, 4000000, time.UTC)
		s.SetTime(now)

		for i := 0; i < 100; i++ {
			_, err := c.Do("XADD", "nowy", "MAXLEN", "10", "*", "one", "1")
			ok(t, err)
			nowy, _ := s.Stream("nowy")
			assert(t, len(nowy) <= 10, "deleted entries")
		}
		nowy, _ := s.Stream("nowy")
		equals(t, 10, len(nowy))

		for i := 0; i < 100; i++ {
			_, err := c.Do("XADD", "nowz", "MAXLEN", "~", "10", "*", "one", "1")
			ok(t, err)
			nowz, _ := s.Stream("nowz")
			assert(t, len(nowz) <= 10, "deleted entries")
		}
		nowz, _ := s.Stream("nowz")
		equals(t, 10, len(nowz))
	})

	t.Run("XADD MINID", func(t *testing.T) {
		now := time.Date(2023, 1, 1, 4, 4, 5, 4000000, time.UTC)
		s.SetTime(now)

		minID := strconv.FormatInt(now.Add(-time.Second).UnixNano()/time.Millisecond.Nanoseconds(), 10)
		_, err := c.Do("XADD", "mid", "MINID", minID, "*", "one", "1")
		ok(t, err)
		_, err = c.Do("XADD", "mid", "MINID", minID, "*", "two", "2")
		ok(t, err)
		now = now.Add(time.Second)
		s.SetTime(now)
		_, err = c.Do("XADD", "mid", "MINID", minID, "*", "three", "3")
		ok(t, err)
		now = now.Add(time.Second)
		s.SetTime(now)
		// advance the minID, older entries will be dropped
		minID = strconv.FormatInt(now.Add(-time.Second).UnixNano()/time.Millisecond.Nanoseconds(), 10)
		_, err = c.Do("XADD", "mid", "MINID", minID, "*", "four", "4")
		ok(t, err)

		mustDo(t, c,
			"XRANGE", "mid", "-", "+",
			proto.Array(
				proto.Array(proto.String("1672545846004-0"), proto.Strings("three", "3")),
				proto.Array(proto.String("1672545847004-0"), proto.Strings("four", "4")),
			),
		)
		// advance now & minID and test with ~
		now = now.Add(time.Second)
		s.SetTime(now)
		minID = strconv.FormatInt(now.Add(-time.Second).UnixNano()/time.Millisecond.Nanoseconds(), 10)
		_, err = c.Do("XADD", "mid", "MINID", "~", minID, "*", "five", "5")
		ok(t, err)

		mustDo(t, c,
			"XRANGE", "mid", "-", "+",
			proto.Array(
				proto.Array(proto.String("1672545847004-0"), proto.Strings("four", "4")),
				proto.Array(proto.String("1672545848004-0"), proto.Strings("five", "5")),
			),
		)
	})

	t.Run("error cases", func(t *testing.T) {
		// Wrong type of key
		mustOK(t, c,
			"SET", "str", "value",
		)
		_, err = s.XAdd("str", "*", []string{"hi", "1"})
		mustFail(t, err, msgWrongType)
		mustDo(t, c,
			"XADD", "str", "*", "hi", "1",
			proto.Error(msgWrongType),
		)

		mustDo(t, c,
			"XADD",
			proto.Error(errWrongNumber("xadd")),
		)
		mustDo(t, c,
			"XADD", "s",
			proto.Error(errWrongNumber("xadd")),
		)
		mustDo(t, c,
			"XADD", "s", "*",
			proto.Error(errWrongNumber("xadd")),
		)
		mustDo(t, c,
			"XADD", "s", "*", "key",
			proto.Error(errWrongNumber("xadd")),
		)
		mustDo(t, c,
			"XADD", "s", "MAXLEN", "!!!", "1000", "*", "key",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"XADD", "s", "MAXLEN", "~", "thousand", "*", "key",
			proto.Error(msgInvalidInt),
		)

		mustDo(t, c,
			"XADD", "s", "a-b", "one", "111", "two", "222",
			proto.Error("ERR Invalid stream ID specified as stream command argument"),
		)
		mustDo(t, c,
			"XADD", "s", "0-0", "one", "111", "two", "222",
			proto.Error("ERR The ID specified in XADD must be greater than 0-0"),
		)
		mustDo(t, c,
			"XADD", "s", "1234567-89", "one", "111", "two", "222",
			proto.Error("ERR The ID specified in XADD is equal or smaller than the target stream top item"),
		)
		mustDo(t, c,
			"XADD", "s", fmt.Sprintf("%d-0", uint64(math.MaxUint64-100)),
			proto.Error(errWrongNumber("xadd")),
		)
	})
}

// Test XLEN
func TestStreamLen(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	_, err = c.Do("XADD", "s", "*", "one", "1", "two", "2")
	ok(t, err)
	_, err = c.Do("XADD", "s", "*", "one", "11", "two", "22")
	ok(t, err)

	t.Run("XLEN", func(t *testing.T) {
		mustDo(t, c,
			"XLEN", "s",
			proto.Int(2),
		)

		must0(t, c,
			"XLEN", "s3",
		)
	})

	t.Run("error cases", func(t *testing.T) {
		mustDo(t, c,
			"XLEN",
			proto.Error(errWrongNumber("xlen")),
		)

		mustOK(t, c,
			"SET", "str", "value",
		)
		mustDo(t, c,
			"XLEN", "str",
			proto.Error(msgWrongType),
		)
	})
}

// Test XRANGE / XREVRANGE
func TestStreamRange(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	_, err = c.Do("XADD", "planets", "0-1", "name", "Mercury", "greek-god", "Hermes", "idx", "1")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "1-0", "name", "Venus", "greek-god", "Aphrodite", "idx", "2")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "2-1", "name", "Earth", "greek-god", "", "idx", "3")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "3-0", "greek-god", "Ares", "name", "Mars", "idx", "4")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "4-1", "name", "Jupiter", "greek-god", "Dias", "idx", "5")
	ok(t, err)

	t.Run("XRANGE", func(t *testing.T) {
		mustDo(t, c,
			"XRANGE", "planets", "1", "+",
			proto.Array(
				proto.Array(proto.String("1-0"), proto.Strings("name", "Venus", "greek-god", "Aphrodite", "idx", "2")),
				proto.Array(proto.String("2-1"), proto.Strings("name", "Earth", "greek-god", "", "idx", "3")),
				proto.Array(proto.String("3-0"), proto.Strings("greek-god", "Ares", "name", "Mars", "idx", "4")),
				proto.Array(proto.String("4-1"), proto.Strings("name", "Jupiter", "greek-god", "Dias", "idx", "5")),
			),
		)

		mustDo(t, c,
			"XREVRANGE", "planets", "3", "1",
			proto.Array(
				proto.Array(proto.String("3-0"), proto.Strings("greek-god", "Ares", "name", "Mars", "idx", "4")),
				proto.Array(proto.String("2-1"), proto.Strings("name", "Earth", "greek-god", "", "idx", "3")),
				proto.Array(proto.String("1-0"), proto.Strings("name", "Venus", "greek-god", "Aphrodite", "idx", "2")),
			),
		)
	})

	t.Run("XRANGE exclusive ranges", func(t *testing.T) {
		mustDo(t, c,
			"XRANGE", "planets", "(1", "+",
			proto.Array(
				proto.Array(proto.String("2-1"), proto.Strings("name", "Earth", "greek-god", "", "idx", "3")),
				proto.Array(proto.String("3-0"), proto.Strings("greek-god", "Ares", "name", "Mars", "idx", "4")),
				proto.Array(proto.String("4-1"), proto.Strings("name", "Jupiter", "greek-god", "Dias", "idx", "5")),
			),
		)

		mustDo(t, c,
			"XREVRANGE", "planets", "3", "(1",
			proto.Array(
				proto.Array(proto.String("3-0"), proto.Strings("greek-god", "Ares", "name", "Mars", "idx", "4")),
				proto.Array(proto.String("2-1"), proto.Strings("name", "Earth", "greek-god", "", "idx", "3")),
			),
		)
	})

	t.Run("error cases", func(t *testing.T) {
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"XRANGE", "str", "-", "+",
			proto.Error(msgWrongType),
		)

		mustDo(t, c,
			"XRANGE",
			proto.Error(errWrongNumber("xrange")),
		)
		mustDo(t, c,
			"XRANGE", "foo",
			proto.Error(errWrongNumber("xrange")),
		)
		mustDo(t, c,
			"XRANGE", "foo", "1",
			proto.Error(errWrongNumber("xrange")),
		)
		mustDo(t, c,
			"XRANGE", "foo", "2", "3", "toomany",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"XRANGE", "foo", "2", "3", "COUNT", "noint",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"XRANGE", "foo", "2", "3", "COUNT", "1", "toomany",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"XRANGE", "foo", "-", "noint",
			proto.Error(msgInvalidStreamID),
		)
		mustDo(t, c,
			"XRANGE", "foo", "(-", "+",
			proto.Error(msgInvalidStreamID),
		)
		mustDo(t, c,
			"XRANGE", "foo", "-", "(+",
			proto.Error(msgInvalidStreamID),
		)
	})
}

// Test XREAD
func TestStreamRead(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	_, err = c.Do("XADD", "planets", "0-1", "name", "Mercury", "greek-god", "Hermes", "idx", "1")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "1-0", "name", "Venus", "greek-god", "Aphrodite", "idx", "2")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "2-1", "name", "Earth", "greek-god", "", "idx", "3")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "3-0", "greek-god", "Ares", "name", "Mars", "idx", "4")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "4-1", "name", "Jupiter", "greek-god", "Dias", "idx", "5")
	ok(t, err)

	_, err = c.Do("XADD", "planets2", "0-1", "name", "Mercury", "greek-god", "Hermes", "idx", "1")
	ok(t, err)
	_, err = c.Do("XADD", "planets2", "1-0", "name", "Venus", "greek-god", "Aphrodite", "idx", "2")
	ok(t, err)
	_, err = c.Do("XADD", "planets2", "2-1", "name", "Earth", "greek-god", "", "idx", "3")
	ok(t, err)
	_, err = c.Do("XADD", "planets2", "3-0", "greek-god", "Ares", "name", "Mars", "idx", "4")
	ok(t, err)
	_, err = c.Do("XADD", "planets2", "4-1", "name", "Jupiter", "greek-god", "Dias", "idx", "5")
	ok(t, err)

	t.Run("XREAD", func(t *testing.T) {
		mustDo(t, c,
			"XREAD", "STREAMS", "planets", "1",
			proto.Array(
				proto.Array(proto.String("planets"),
					proto.Array(
						proto.Array(proto.String("2-1"), proto.Strings("name", "Earth", "greek-god", "", "idx", "3")),
						proto.Array(proto.String("3-0"), proto.Strings("greek-god", "Ares", "name", "Mars", "idx", "4")),
						proto.Array(proto.String("4-1"), proto.Strings("name", "Jupiter", "greek-god", "Dias", "idx", "5")),
					),
				),
			),
		)

		mustDo(t, c,
			"XREAD", "STREAMS", "planets", "planets2", "1", "3",
			proto.Array(
				proto.Array(proto.String("planets"),
					proto.Array(
						proto.Array(proto.String("2-1"), proto.Strings("name", "Earth", "greek-god", "", "idx", "3")),
						proto.Array(proto.String("3-0"), proto.Strings("greek-god", "Ares", "name", "Mars", "idx", "4")),
						proto.Array(proto.String("4-1"), proto.Strings("name", "Jupiter", "greek-god", "Dias", "idx", "5")),
					),
				),
				proto.Array(proto.String("planets2"),
					proto.Array(
						proto.Array(proto.String("4-1"), proto.Strings("name", "Jupiter", "greek-god", "Dias", "idx", "5")),
					),
				),
			),
		)

		t.Run("blocking async", func(t *testing.T) {
			// XREAD blocking test using latest ID
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				xaddClient, err := proto.Dial(s.Addr())
				ok(t, err)
				defer xaddClient.Close()
				for {
					select {
					case <-time.After(10 * time.Millisecond):
					case <-ctx.Done():
						return
					}
					_, err = xaddClient.Do("XADD", "planets", "5-1", "name", "block", "idx", "6")
					ok(t, err)
				}
			}()

			mustDo(t, c,
				"XREAD", "BLOCK", "0", "STREAMS", "planets", "$",
				proto.Array(
					proto.Array(proto.String("planets"),
						proto.Array(
							proto.Array(proto.String("5-1"), proto.Strings("name", "block", "idx", "6")),
						),
					),
				),
			)
			cancel()
			wg.Wait()
		})
	})

	t.Run("error cases", func(t *testing.T) {
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"XREAD",
			proto.Error(errWrongNumber("xread")),
		)
		mustDo(t, c,
			"XREAD", "STREAMS", "foo",
			proto.Error(errWrongNumber("xread")),
		)
		mustDo(t, c,
			"XREAD", "STREAMS", "foo", "bar", "1",
			proto.Error(msgXreadUnbalanced),
		)
		mustDo(t, c,
			"XREAD", "COUNT",
			proto.Error(errWrongNumber("xread")),
		)
		mustDo(t, c,
			"XREAD", "COUNT", "notint",
			proto.Error(errWrongNumber("xread")),
		)
		mustDo(t, c,
			"XREAD", "COUNT", "10", // no STREAMS
			proto.Error(errWrongNumber("xread")),
		)
		mustDo(t, c,
			"XREAD", "STREAMS", "foo", "noint",
			proto.Error(msgInvalidStreamID),
		)
		mustDo(t, c,
			"XREAD", "STREAMS", "str", "noint",
			proto.Error(msgInvalidStreamID),
		)
		mustDo(t, c,
			"XREAD", "STREAMS", "foo", "2", "noint",
			proto.Error(msgXreadUnbalanced),
		)
	})
}

// Test XINFO
func TestStreamInfo(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"XINFO", "STREAM", "planets",
		proto.Error("ERR no such key"),
	)

	mustDo(t, c,
		"XADD", "planets", "0-1", "name", "Mercury", "greek-god", "Hermes", "idx", "1",
		proto.String("0-1"),
	)

	mustDo(t, c,
		"XINFO", "STREAM", "planets",
		proto.Array(proto.String("length"), proto.Int(1)),
	)

	mustDo(t, c,
		"XINFO", "GROUPS", "planets", "foo", "bar",
		proto.Error("ERR wrong number of arguments for 'groups' command"),
	)
	mustDo(t, c,
		"XINFO", "GROUPS", "foo",
		proto.Error("ERR no such key"),
	)
	mustDo(t, c,
		"XINFO", "GROUPS", "planets",
		proto.Array(),
	)

	mustDo(t, c,
		"XINFO", "CONSUMERS", "foo", "bar",
		proto.Error("ERR no such key"),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Error("NOGROUP No such consumer group 'processing' for key name 'planets'"),
	)
}

// Test XGROUP
func TestStreamGroup(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"XGROUP", "CREATE", "s", "processing", "$",
		proto.Error(msgXgroupKeyNotFound),
	)
	mustDo(t, c,
		"XGROUP", "DESTROY", "s", "processing",
		proto.Error(msgXgroupKeyNotFound),
	)
	mustDo(t, c,
		"XGROUP", "DELCONSUMER", "s", "processing", "foo",
		proto.Error(msgXgroupKeyNotFound),
	)

	mustOK(t, c,
		"XGROUP", "CREATE", "s", "processing", "$", "MKSTREAM",
	)
	mustDo(t, c,
		"XGROUP", "DESTROY", "s", "foo",
		proto.Int(0),
	)

	mustDo(t, c,
		"XINFO", "GROUPS", "s",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("processing"),
				proto.String("consumers"), proto.Int(0),
				proto.String("pending"), proto.Int(0),
				proto.String("last-delivered-id"), proto.String("0-0"),
				proto.String("entries-read"), proto.Nil,
				proto.String("lag"), proto.Int(0),
			),
		),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "s", "processing",
		proto.Array(),
	)

	mustDo(t, c,
		"XGROUP", "CREATECONSUMER", "s", "processing", "alice",
		proto.Int(1),
	)
	mustDo(t, c,
		"XINFO", "GROUPS", "s",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("processing"),
				proto.String("consumers"), proto.Int(1),
				proto.String("pending"), proto.Int(0),
				proto.String("last-delivered-id"), proto.String("0-0"),
				proto.String("entries-read"), proto.Nil,
				proto.String("lag"), proto.Int(0),
			),
		),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "s", "processing",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("alice"),
				proto.String("pending"), proto.Int(0),
				proto.String("idle"), proto.Int(-1),
				proto.String("inactive"), proto.Int(-1),
			),
		),
	)

	mustDo(t, c,
		"XGROUP", "DELCONSUMER", "s", "processing", "foo",
		proto.Int(0),
	)
	mustDo(t, c,
		"XGROUP", "DELCONSUMER", "s", "processing", "alice",
		proto.Int(0),
	)
	mustDo(t, c,
		"XINFO", "GROUPS", "s",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("processing"),
				proto.String("consumers"), proto.Int(0),
				proto.String("pending"), proto.Int(0),
				proto.String("last-delivered-id"), proto.String("0-0"),
				proto.String("entries-read"), proto.Nil,
				proto.String("lag"), proto.Int(0),
			),
		),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "s", "processing",
		proto.Array(),
	)

	mustDo(t, c,
		"XGROUP", "DESTROY", "s", "processing",
		proto.Int(1),
	)
	must0(t, c,
		"XLEN", "s",
	)
	mustDo(t, c,
		"XINFO", "GROUPS", "s",
		proto.Array(),
	)

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"XGROUP",
			proto.Error("ERR wrong number of arguments for 'xgroup' command"),
		)
		mustDo(t, c,
			"XGROUP", "HELP",
			proto.Error("ERR 'XGROUP help' not supported"),
		)
		mustDo(t, c,
			"XGROUP", "foo",
			proto.Error("ERR unknown subcommand 'foo'. Try XGROUP HELP."),
		)
		mustDo(t, c,
			"XGROUP", "SETID",
			proto.Error("ERR 'XGROUP setid' not supported"),
		)
	})
}

// Test XREADGROUP
func TestStreamReadGroup(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">",
		proto.Error("NOGROUP No such key 'planets' or consumer group 'processing' in XREADGROUP with GROUP option"),
	)

	mustOK(t, c,
		"XGROUP", "CREATE", "planets", "processing", "$", "MKSTREAM",
	)

	mustNilList(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">",
	)

	mustDo(t, c,
		"XINFO", "GROUPS", "planets",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("processing"),
				proto.String("consumers"), proto.Int(0),
				proto.String("pending"), proto.Int(0),
				proto.String("last-delivered-id"), proto.String("0-0"),
				proto.String("entries-read"), proto.Nil,
				proto.String("lag"), proto.Int(0),
			),
		),
	)

	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(),
	)

	mustDo(t, c,
		"XADD", "planets", "0-1", "name", "Mercury",
		proto.String("0-1"),
	)

	must1(t, c,
		"XLEN", "planets",
	)

	mustDo(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">",
		proto.Array(
			proto.Array(proto.String("planets"), proto.Array(proto.Array(proto.String("0-1"), proto.Strings("name", "Mercury")))),
		),
	)

	mustDo(t, c,
		"XINFO", "GROUPS", "planets",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("processing"),
				proto.String("consumers"), proto.Int(1),
				proto.String("pending"), proto.Int(1),
				proto.String("last-delivered-id"), proto.String("0-1"),
				proto.String("entries-read"), proto.Nil,
				proto.String("lag"), proto.Int(1),
			),
		),
	)

	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("alice"),
				proto.String("pending"), proto.Int(1),
				proto.String("idle"), proto.Int(-1),
				proto.String("inactive"), proto.Int(-1),
			),
		),
	)

	mustNilList(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">",
	)

	// Read from PEL
	mustDo(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", "0-0",
		proto.Array(
			proto.Array(proto.String("planets"), proto.Array(proto.Array(proto.String("0-1"), proto.Strings("name", "Mercury")))),
		),
	)
}

// Test XDEL
func TestStreamDelete(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustOK(t, c,
		"XGROUP", "CREATE", "planets", "processing", "$", "MKSTREAM",
	)

	mustDo(t, c,
		"XADD", "planets", "0-1", "name", "Mercury",
		proto.String("0-1"),
	)

	mustDo(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">",
		proto.Array(
			proto.Array(
				proto.String("planets"),
				proto.Array(
					proto.Array(
						proto.String("0-1"),
						proto.Strings("name", "Mercury"),
					),
				),
			),
		),
	)

	mustDo(t, c,
		"XADD", "planets", "0-2", "name", "Mercury",
		proto.String("0-2"),
	)

	must1(t, c,
		"XDEL", "planets", "0-1",
	)

	must1(t, c,
		"XDEL", "planets", "0-2",
	)

	mustDo(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", "0-0",
		proto.Array(
			proto.Array(
				proto.String("planets"),
				proto.Array(),
			),
		),
	)
}

// Test XACK
func TestStreamAck(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustOK(t, c,
		"XGROUP", "CREATE", "planets", "processing", "$", "MKSTREAM",
	)

	mustDo(t, c,
		"XADD", "planets", "0-1", "name", "Mercury",
		proto.String("0-1"),
	)

	mustDo(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">",
		proto.Array(
			proto.Array(proto.String("planets"), proto.Array(proto.Array(proto.String("0-1"), proto.Strings("name", "Mercury")))),
		),
	)

	must1(t, c,
		"XACK", "planets", "processing", "0-1",
	)
	mustDo(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", "0-0",
		proto.Array(
			proto.Array(
				proto.String("planets"),
				proto.Array(),
			),
		),
	)
	mustDo(t, c,
		"XINFO", "GROUPS", "planets",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("processing"),
				proto.String("consumers"), proto.Int(1),
				proto.String("pending"), proto.Int(0),
				proto.String("last-delivered-id"), proto.String("0-1"),
				proto.String("entries-read"), proto.Nil,
				proto.String("lag"), proto.Int(1),
			),
		),
	)

	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("alice"),
				proto.String("pending"), proto.Int(0),
				proto.String("idle"), proto.Int(-1),
				proto.String("inactive"), proto.Int(-1),
			),
		),
	)

	mustDo(t, c,
		"XGROUP", "DELCONSUMER", "planets", "processing", "alice",
		proto.Int(0),
	)
}

// Test XPENDING
func TestStreamXpending(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()
	now := time.Now()
	s.SetTime(now)

	mustOK(t, c, "XGROUP", "CREATE", "planets", "processing", "$", "MKSTREAM")
	mustDo(t, c, "XADD", "planets", "99-1", "name", "Mercury",
		proto.String("99-1"),
	)
	mustDo(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">",
		proto.Array(
			proto.Array(
				proto.String("planets"),
				proto.Array(proto.Array(proto.String("99-1"), proto.Strings("name", "Mercury"))),
			),
		),
	)

	t.Run("summary mode", func(t *testing.T) {
		mustDo(t, c,
			"XPENDING", "planets", "processing",
			proto.Array(
				proto.Int(1),
				proto.String("99-1"),
				proto.String("99-1"),
				proto.Array(
					proto.Array(proto.String("alice"), proto.String("1")),
				),
			),
		)
		mustDo(t, c,
			"XPENDING", "nosuch", "processing",
			proto.Error("NOGROUP No such key 'nosuch' or consumer group 'processing'"),
		)
		mustDo(t, c,
			"XPENDING", "planets", "nosuch",
			proto.Error("NOGROUP No such key 'planets' or consumer group 'nosuch'"),
		)
	})

	t.Run("full mode", func(t *testing.T) {
		s.SetTime(now.Add(3 * time.Second))
		mustDo(t, c,
			"XPENDING", "planets", "processing", "-", "+", "999",
			proto.Array(
				proto.Array(
					proto.String("99-1"),
					proto.String("alice"),
					proto.Int(3000),
					proto.Int(1),
				),
			),
		)
		mustDo(t, c,
			"XPENDING", "planets", "processing", "-", "+", "-99",
			proto.NilList,
		)

		// Increase delivery count
		s.SetTime(now.Add(5 * time.Second))
		mustDo(t, c,
			"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", "99-0",
			proto.Array(
				proto.Array(proto.String("planets"), proto.Array(proto.Array(proto.String("99-1"), proto.Strings("name", "Mercury")))),
			),
		)
		s.SetTime(now.Add(9 * time.Second))
		mustDo(t, c,
			"XPENDING", "planets", "processing", "-", "+", "999",
			proto.Array(
				proto.Array(
					proto.String("99-1"),
					proto.String("alice"),
					proto.Int(4000),
					proto.Int(2),
				),
			),
		)

		mustDo(t, c,
			"XPENDING", "planets", "processing", "IDLE", "5000", "-", "+", "999",
			proto.NilList,
		)
		mustDo(t, c,
			"XPENDING", "planets", "processing", "-", "+", "999", "bob",
			proto.NilList,
		)
		mustDo(t, c,
			"XPENDING", "planets", "processing", "IDLE", "4000", "-", "+", "999", "alice",
			proto.Array(
				proto.Array(
					proto.String("99-1"),
					proto.String("alice"),
					proto.Int(4000),
					proto.Int(2),
				),
			),
		)

		mustDo(t, c,
			"XGROUP", "DELCONSUMER", "planets", "processing", "alice",
			proto.Int(1),
		)
		mustDo(t, c,
			"XPENDING", "planets", "processing", "-", "+", "999",
			proto.NilList,
		)
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"XPENDING",
			proto.Error("ERR wrong number of arguments for 'xpending' command"),
		)
		mustDo(t, c,
			"XPENDING", "planets",
			proto.Error("ERR wrong number of arguments for 'xpending' command"),
		)
		mustDo(t, c,
			"XPENDING", "planets", "processing", "toomany",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"XPENDING", "planets", "processing", "IDLE", "1000",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"XPENDING", "planets", "processing", "-", "+", "cons", "nine",
			proto.Error("ERR value is not an integer or out of range"),
		)
		mustDo(t, c,
			"XPENDING", "planets", "processing", "-", "+", "99", "cons", "foo",
			proto.Error("ERR syntax error"),
		)
	})
}

// Test XTRIM
func TestStreamTrim(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("error cases", func(t *testing.T) {
		mustDo(t, c,
			"XTRIM", "planets", "UNKNOWN_STRATEGY", "4",
			proto.Error(msgXtrimInvalidStrategy))
		mustDo(t, c,
			"XTRIM", "planets",
			proto.Error(errWrongNumber("xtrim")))
		mustDo(t, c,
			"XTRIM", "planets", "MAXLEN", "notANumber",
			proto.Error(msgXtrimInvalidMaxLen))
	})

	_, err = c.Do("XADD", "planets", "0-1", "name", "Mercury")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "1-0", "name", "Venus")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "2-1", "name", "Earth")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "3-0", "name", "Mars")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "4-1", "name", "Jupiter")
	ok(t, err)
	_, err = c.Do("XADD", "planets", "5-1", "name", "Saturn")
	ok(t, err)

	mustDo(t, c,
		"XTRIM", "planets", "MAXLEN", "=", "3", proto.Int(3))

	mustDo(t, c,
		"XRANGE", "planets", "-", "+",
		proto.Array(
			proto.Array(proto.String("3-0"), proto.Strings("name", "Mars")),
			proto.Array(proto.String("4-1"), proto.Strings("name", "Jupiter")),
			proto.Array(proto.String("5-1"), proto.Strings("name", "Saturn")),
		))

	mustDo(t, c,
		"XTRIM", "planets", "MINID", "~", "4", "LIMIT", "50", proto.Int(1))

	mustDo(t, c,
		"XRANGE", "planets", "-", "+",
		proto.Array(
			proto.Array(proto.String("4-1"), proto.Strings("name", "Jupiter")),
			proto.Array(proto.String("5-1"), proto.Strings("name", "Saturn")),
		))

	mustDo(t, c,
		"XTRIM", "planets", "MINID", "5", proto.Int(1))

	mustDo(t, c,
		"XRANGE", "planets", "-", "+",
		proto.Array(
			proto.Array(proto.String("5-1"), proto.Strings("name", "Saturn")),
		))
}

func TestStreamAutoClaim(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	now := time.Now()
	s.SetTime(now)

	mustDo(t, c,
		"XAUTOCLAIM", "planets", "processing", "alice", "0", "0",
		proto.Error("NOGROUP No such key 'planets' or consumer group 'processing'"),
	)

	mustOK(t, c,
		"XGROUP", "CREATE", "planets", "processing", "$", "MKSTREAM",
	)

	mustDo(t, c,
		"XAUTOCLAIM", "planets", "processing", "alice", "0", "0",
		proto.Array(
			proto.String("0-0"),
			proto.Array(),
			proto.Array(),
		),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(),
	)

	mustDo(t, c,
		"XADD", "planets", "0-1", "name", "Mercury",
		proto.String("0-1"),
	)

	must1(t, c,
		"XLEN", "planets",
	)

	mustDo(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">",
		proto.Array(
			proto.Array(proto.String("planets"), proto.Array(proto.Array(proto.String("0-1"), proto.Strings("name", "Mercury")))),
		),
	)

	// Read message already claimed
	mustDo(t, c,
		"XAUTOCLAIM", "planets", "processing", "alice", "0", "0",
		proto.Array(
			proto.String("0-0"),
			proto.Array(proto.Array(proto.String("0-1"), proto.Strings("name", "Mercury"))),
			proto.Array(),
		),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("alice"),
				proto.String("pending"), proto.Int(1),
				proto.String("idle"), proto.Int(-1),
				proto.String("inactive"), proto.Int(-1),
			),
		),
	)

	// Add an additional item to pending
	s.SetTime(now.Add(5000 * time.Millisecond))
	mustDo(t, c,
		"XADD", "planets", "0-2", "name", "Venus",
		proto.String("0-2"),
	)
	mustDo(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">",
		proto.Array(
			proto.Array(proto.String("planets"), proto.Array(proto.Array(proto.String("0-2"), proto.Strings("name", "Venus")))),
		),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("alice"),
				proto.String("pending"), proto.Int(2),
				proto.String("idle"), proto.Int(-1),
				proto.String("inactive"), proto.Int(-1),
			),
		),
	)

	// Autoclaim with a min idle time that should not catch any items
	s.SetTime(now.Add(10000 * time.Millisecond))
	mustDo(t, c,
		"XAUTOCLAIM", "planets", "processing", "alice", "15000", "0",
		proto.Array(
			proto.String("0-0"),
			proto.Array(),
			proto.Array(),
		),
	)

	// Set time further in the future where autoclaim with min idle time should
	// return only one result
	s.SetTime(now.Add(15000 * time.Millisecond))
	mustDo(t, c,
		"XAUTOCLAIM", "planets", "processing", "alice", "15000", "0",
		proto.Array(
			proto.String("0-0"),
			proto.Array(
				proto.Array(proto.String("0-1"), proto.Strings("name", "Mercury")),
			),
			proto.Array(),
		),
	)

	// Further in the future we should return Venus but not Mercury since it is
	// claimed more recently
	s.SetTime(now.Add(25000 * time.Millisecond))
	mustDo(t, c,
		"XAUTOCLAIM", "planets", "processing", "alice", "15000", "0",
		proto.Array(
			proto.String("0-0"),
			proto.Array(
				proto.Array(proto.String("0-2"), proto.Strings("name", "Venus")),
			),
			proto.Array(),
		),
	)

	// Even further in the future we should return both
	s.SetTime(now.Add(40000 * time.Millisecond))
	mustDo(t, c,
		"XAUTOCLAIM", "planets", "processing", "alice", "15000", "0",
		proto.Array(
			proto.String("0-0"),
			proto.Array(
				proto.Array(proto.String("0-1"), proto.Strings("name", "Mercury")),
				proto.Array(proto.String("0-2"), proto.Strings("name", "Venus")),
			),
			proto.Array(),
		),
	)

	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("alice"),
				proto.String("pending"), proto.Int(2),
				proto.String("idle"), proto.Int(-1),
				proto.String("inactive"), proto.Int(-1),
			),
		),
	)

	s.SetTime(now.Add(60000 * time.Millisecond))
	mustDo(t, c,
		"XAUTOCLAIM", "planets", "processing", "bob", "15000", "0",
		proto.Array(
			proto.String("0-0"),
			proto.Array(
				proto.Array(proto.String("0-1"), proto.Strings("name", "Mercury")),
				proto.Array(proto.String("0-2"), proto.Strings("name", "Venus")),
			),
			proto.Array(),
		),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("alice"),
				proto.String("pending"), proto.Int(0),
				proto.String("idle"), proto.Int(-1),
				proto.String("inactive"), proto.Int(-1),
			),
			proto.Array(
				proto.String("name"), proto.String("bob"),
				proto.String("pending"), proto.Int(2),
				proto.String("idle"), proto.Int(-1),
				proto.String("inactive"), proto.Int(-1),
			),
		),
	)

	s.SetTime(now.Add(80000 * time.Millisecond))
	mustDo(t, c,
		"XAUTOCLAIM", "planets", "processing", "alice", "15000", "0", "COUNT", "1",
		proto.Array(
			proto.String("0-2"),
			proto.Array(
				proto.Array(proto.String("0-1"), proto.Strings("name", "Mercury")),
			),
			proto.Array(),
		),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("alice"),
				proto.String("pending"), proto.Int(1),
				proto.String("idle"), proto.Int(-1),
				proto.String("inactive"), proto.Int(-1),
			),
			proto.Array(
				proto.String("name"), proto.String("bob"),
				proto.String("pending"), proto.Int(1),
				proto.String("idle"), proto.Int(-1),
				proto.String("inactive"), proto.Int(-1),
			),
		),
	)

	mustDo(t, c,
		"XGROUP", "DELCONSUMER", "planets", "processing", "alice",
		proto.Int(1),
	)
	mustDo(t, c,
		"XPENDING", "planets", "processing", "-", "+", "999",
		proto.Array(
			proto.Array(
				proto.String("0-2"),
				proto.String("bob"),
				proto.Int(20000),
				proto.Int(4),
			),
		),
	)

	mustDo(t, c,
		"XGROUP", "DELCONSUMER", "planets", "processing", "bob",
		proto.Int(1),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(),
	)
	mustDo(t, c,
		"XINFO", "GROUPS", "planets",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("processing"),
				proto.String("consumers"), proto.Int(0),
				proto.String("pending"), proto.Int(0),
				proto.String("last-delivered-id"), proto.String("0-2"),
				proto.String("entries-read"), proto.Nil,
				proto.String("lag"), proto.Int(2),
			),
		),
	)
}

func TestStreamClaim(t *testing.T) {
	s := RunT(t)
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	now := time.Now()
	s.SetTime(now)

	mustDo(t, c,
		"XCLAIM", "planets", "processing", "alice", "0", "0-0",
		proto.Error("NOGROUP No such key 'planets' or consumer group 'processing'"),
	)

	mustOK(t, c,
		"XGROUP", "CREATE", "planets", "processing", "$", "MKSTREAM",
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(),
	)

	mustDo(t, c,
		"XCLAIM", "planets", "processing", "alice", "0", "0-0",
		proto.Array(),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("alice"),
				proto.String("pending"), proto.Int(0),
				proto.String("idle"), proto.Int(0),
				proto.String("inactive"), proto.Int(-1),
			),
		),
	)

	mustDo(t, c,
		"XADD", "planets", "0-1", "name", "Mercury",
		proto.String("0-1"),
	)
	mustDo(t, c,
		"XADD", "planets", "0-2", "name", "Venus",
		proto.String("0-2"),
	)

	mustDo(t, c,
		"XCLAIM", "planets", "processing", "alice", "0", "0-1",
		proto.Array(),
	)
	mustDo(t, c,
		"XPENDING", "planets", "processing", "-", "+", "999",
		proto.NilList,
	)

	mustDo(t, c,
		"XCLAIM", "planets", "processing", "alice", "0", "0-1", "0-2", "FORCE",
		proto.Array(
			proto.Array(proto.String("0-1"), proto.Strings("name", "Mercury")),
			proto.Array(proto.String("0-2"), proto.Strings("name", "Venus")),
		),
	)
	mustDo(t, c,
		"XINFO", "GROUPS", "planets",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("processing"),
				proto.String("consumers"), proto.Int(1),
				proto.String("pending"), proto.Int(2),
				proto.String("last-delivered-id"), proto.String("0-0"),
				proto.String("entries-read"), proto.Nil,
				proto.String("lag"), proto.Int(2),
			),
		),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("alice"),
				proto.String("pending"), proto.Int(2),
				proto.String("idle"), proto.Int(0),
				proto.String("inactive"), proto.Int(0),
			),
		),
	)
	mustDo(t, c,
		"XPENDING", "planets", "processing", "-", "+", "999",
		proto.Array(
			proto.Array(
				proto.String("0-1"),
				proto.String("alice"),
				proto.Int(0),
				proto.Int(2),
			),
			proto.Array(
				proto.String("0-2"),
				proto.String("alice"),
				proto.Int(0),
				proto.Int(2),
			),
		),
	)

	s.SetTime(now.Add(20000 * time.Millisecond))
	mustDo(t, c,
		"XDEL", "planets", "0-1",
		proto.Int(1),
	)
	mustDo(t, c,
		"XCLAIM", "planets", "processing", "bob", "0", "0-1",
		proto.Array(),
	)
	mustDo(t, c,
		"XINFO", "GROUPS", "planets",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("processing"),
				proto.String("consumers"), proto.Int(2),
				proto.String("pending"), proto.Int(1),
				proto.String("last-delivered-id"), proto.String("0-0"),
				proto.String("entries-read"), proto.Nil,
				proto.String("lag"), proto.Int(1),
			),
		),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("alice"),
				proto.String("pending"), proto.Int(1),
				proto.String("idle"), proto.Int(20000),
				proto.String("inactive"), proto.Int(20000),
			),
			proto.Array(
				proto.String("name"), proto.String("bob"),
				proto.String("pending"), proto.Int(0),
				proto.String("idle"), proto.Int(0),
				proto.String("inactive"), proto.Int(-1),
			),
		),
	)
	mustDo(t, c,
		"XPENDING", "planets", "processing", "-", "+", "999",
		proto.Array(
			proto.Array(
				proto.String("0-1"),
				proto.String("bob"),
				proto.Int(0),
				proto.Int(3),
			),
			proto.Array(
				proto.String("0-2"),
				proto.String("alice"),
				proto.Int(20000),
				proto.Int(2),
			),
		),
	)

	mustDo(t, c,
		"XADD", "planets", "0-3", "name", "Earth",
		proto.String("0-3"),
	)
	mustDo(t, c,
		"XADD", "planets", "0-4", "name", "Mars",
		proto.String("0-4"),
	)
	mustDo(t, c,
		"XCLAIM", "planets", "processing", "bob", "0", "0-4", "FORCE",
		proto.Array(
			proto.Array(proto.String("0-4"), proto.Strings("name", "Mars")),
		),
	)
	mustDo(t, c,
		"XCLAIM", "planets", "processing", "bob", "0", "0-4",
		proto.Array(
			proto.Array(proto.String("0-4"), proto.Strings("name", "Mars")),
		),
	)
	mustDo(t, c,
		"XPENDING", "planets", "processing", "-", "+", "999",
		proto.Array(
			proto.Array(
				proto.String("0-1"),
				proto.String("bob"),
				proto.Int(0),
				proto.Int(3),
			),
			proto.Array(
				proto.String("0-2"),
				proto.String("alice"),
				proto.Int(20000),
				proto.Int(2),
			),
			proto.Array(
				proto.String("0-4"),
				proto.String("bob"),
				proto.Int(0),
				proto.Int(3),
			),
		),
	)

	mustDo(t, c,
		"XREADGROUP", "GROUP", "processing", "alice", "STREAMS", "planets", ">",
		proto.Array(
			proto.Array(
				proto.String("planets"),
				proto.Array(
					proto.Array(proto.String("0-2"), proto.Strings("name", "Venus")),
					proto.Array(proto.String("0-3"), proto.Strings("name", "Earth")),
					proto.Array(proto.String("0-4"), proto.Strings("name", "Mars")),
				),
			),
		),
	)
	mustDo(t, c,
		"XPENDING", "planets", "processing", "-", "+", "999",
		proto.Array(
			proto.Array(
				proto.String("0-1"),
				proto.String("bob"),
				proto.Int(0),
				proto.Int(3),
			),
			proto.Array(
				proto.String("0-2"),
				proto.String("alice"),
				proto.Int(0),
				proto.Int(1),
			),
			proto.Array(
				proto.String("0-3"),
				proto.String("alice"),
				proto.Int(0),
				proto.Int(1),
			),
			proto.Array(
				proto.String("0-4"),
				proto.String("alice"),
				proto.Int(0),
				proto.Int(1),
			),
		),
	)
	mustDo(t, c,
		"XINFO", "CONSUMERS", "planets", "processing",
		proto.Array(
			proto.Array(
				proto.String("name"), proto.String("alice"),
				proto.String("pending"), proto.Int(3),
				proto.String("idle"), proto.Int(20000),
				proto.String("inactive"), proto.Int(20000),
			),
			proto.Array(
				proto.String("name"), proto.String("bob"),
				proto.String("pending"), proto.Int(0), // deleted
				proto.String("idle"), proto.Int(0),
				proto.String("inactive"), proto.Int(0),
			),
		),
	)

	mustDo(t, c,
		"XCLAIM", "planets", "processing", "alice", "0", "0-3", "RETRYCOUNT", "10", "IDLE", "5000", "JUSTID",
		proto.Array(proto.String("0-3")),
	)
	newTime := s.effectiveNow().Add(time.Millisecond * time.Duration(-10000))
	newTimeString := strconv.FormatInt(newTime.UnixNano()/time.Millisecond.Nanoseconds(), 10)
	mustDo(t, c,
		"XCLAIM", "planets", "processing", "alice", "0", "0-1", "RETRYCOUNT", "1", "TIME", newTimeString, "JUSTID",
		proto.Array(),
	)
	mustDo(t, c,
		"XPENDING", "planets", "processing", "-", "+", "999",
		proto.Array(
			proto.Array(
				proto.String("0-1"),
				proto.String("alice"),
				proto.Int(10000),
				proto.Int(1),
			),
			proto.Array(
				proto.String("0-2"),
				proto.String("alice"),
				proto.Int(0),
				proto.Int(1),
			),
			proto.Array(
				proto.String("0-3"),
				proto.String("alice"),
				proto.Int(5000),
				proto.Int(10),
			),
			proto.Array(
				proto.String("0-4"),
				proto.String("alice"),
				proto.Int(0),
				proto.Int(1),
			),
		),
	)

	mustDo(t, c,
		"XACK", "planets", "processing", "0-1", "0-2", "0-3", "0-4",
		proto.Int(3),
	)
	mustDo(t, c,
		"XPENDING", "planets", "processing", "-", "+", "999",
		proto.NilList,
	)
}
