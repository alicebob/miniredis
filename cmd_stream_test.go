package miniredis

import (
	"fmt"
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/proto"
)

// Test XADD / XLEN / XRANGE
func TestStream(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
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
	s, err := Run()
	ok(t, err)
	defer s.Close()
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
	s, err := Run()
	ok(t, err)
	defer s.Close()
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
	s, err := Run()
	ok(t, err)
	defer s.Close()
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
	})
}

// Test XREAD
func TestStreamRead(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
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
	s, err := Run()
	ok(t, err)
	defer s.Close()
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
}

// Test XGROUP
func TestStreamGroup(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"XGROUP", "CREATE", "s", "processing", "$",
		proto.Error(msgXgroupKeyNotFound),
	)

	mustOK(t, c,
		"XGROUP", "CREATE", "s", "processing", "$", "MKSTREAM",
	)

	must0(t, c,
		"XLEN", "s",
	)
}

// Test XREADGROUP
func TestStreamReadGroup(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
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
	s, err := Run()
	ok(t, err)
	defer s.Close()
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
	s, err := Run()
	ok(t, err)
	defer s.Close()
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
}
