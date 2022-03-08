package miniredis

import (
	"math"
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

// Test ZADD / ZCARD / ZRANK / ZREVRANK.
func TestSortedSet(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		mustDo(t, c,
			"ZADD", "z", "1", "one", "2", "two", "3", "three",
			proto.Int(3),
		)

		mustDo(t, c,
			"ZCARD", "z",
			proto.Int(3),
		)

		must0(t, c,
			"ZRANK", "z", "one",
		)
		mustDo(t, c,
			"ZRANK", "z", "three",
			proto.Int(2),
		)

		mustDo(t, c,
			"ZREVRANK", "z", "one",
			proto.Int(2),
		)
		must0(t, c,
			"ZREVRANK", "z", "three",
		)
	}

	// TYPE of our zset
	mustDo(t, c,
		"TYPE", "z",
		proto.Inline("zset"),
	)

	// Replace a key
	{
		must0(t, c,
			"ZADD", "z", "2.1", "two",
		)

		mustDo(t, c,
			"ZCARD", "z",
			proto.Int(3),
		)
	}

	// To infinity!
	{
		mustDo(t, c,
			"ZADD", "zinf", "inf", "plus inf", "-inf", "minus inf", "10", "ten",
			proto.Int(3),
		)

		mustDo(t, c,
			"ZCARD", "zinf",
			proto.Int(3),
		)

		smap, err := s.SortedSet("zinf")
		ok(t, err)
		equals(t, map[string]float64{
			"plus inf":  math.Inf(+1),
			"minus inf": math.Inf(-1),
			"ten":       10.0,
		}, smap)
	}

	// Invalid score
	{
		mustDo(t, c,
			"ZADD", "z", "noint", "two",
			proto.Error("ERR value is not a valid float"),
		)
	}

	// ZRANK on non-existing key/member
	{
		mustNil(t, c,
			"ZRANK", "z", "nosuch",
		)

		mustNil(t, c,
			"ZRANK", "nosuch", "nosuch",
		)
	}

	// Direct usage
	{
		added, err := s.ZAdd("s1", 12.4, "aap")
		ok(t, err)
		equals(t, true, added)
		added, err = s.ZAdd("s1", 3.4, "noot")
		ok(t, err)
		equals(t, true, added)
		added, err = s.ZAdd("s1", 3.5, "noot")
		ok(t, err)
		equals(t, false, added)

		members, err := s.ZMembers("s1")
		ok(t, err)
		equals(t, []string{"noot", "aap"}, members)
	}

	t.Run("errors", func(t *testing.T) {
		// Wrong type of key
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"ZRANK", "str", "foo",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"ZRANK",
			proto.Error(errWrongNumber("zrank")),
		)
		mustDo(t, c,
			"ZRANK", "set", "spurious", "args",
			proto.Error(errWrongNumber("zrank")),
		)

		mustDo(t, c,
			"ZREVRANK",
			proto.Error(errWrongNumber("zrevrank")),
		)

		mustDo(t, c,
			"ZCARD", "str",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"ZCARD",
			proto.Error(errWrongNumber("zcard")),
		)
		mustDo(t, c,
			"ZCARD", "set", "spurious",
			proto.Error(errWrongNumber("zcard")),
		)
	})
}

// Test ZADD
func TestSortedSetAdd(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		mustDo(t, c,
			"ZADD", "z", "1", "one", "2", "two", "3", "three",
			proto.Int(3),
		)

		must0(t, c,
			"ZADD", "z", "1", "one", "2.1", "two", "3", "three",
		)

		must1(t, c,
			"ZADD", "z", "CH", "1", "one", "2.2", "two", "3", "three",
		)

		must0(t, c,
			"ZADD", "z", "NX", "1", "one", "2.2", "two", "3", "three",
		)

		must1(t, c,
			"ZADD", "z", "NX", "1", "one", "4", "four",
		)

		must0(t, c,
			"ZADD", "z", "XX", "1.1", "one", "4", "four",
		)

		must1(t, c,
			"ZADD", "z", "XX", "CH", "1.2", "one", "4", "four",
		)

		mustDo(t, c,
			"ZADD", "z", "INCR", "1.2", "one",
			proto.String("2.4"),
		)

		mustNil(t, c,
			"ZADD", "z", "INCR", "NX", "1.2", "one",
		)

		mustDo(t, c,
			"ZADD", "z", "INCR", "XX", "1.2", "one",
			proto.String("3.6"),
		)

		mustNil(t, c,
			"ZADD", "q", "INCR", "XX", "1.2", "one",
		)

		mustDo(t, c,
			"ZADD", "q", "INCR", "NX", "1.2", "one",
			proto.String("1.2"),
		)

		mustNil(t, c,
			"ZADD", "q", "INCR", "NX", "1.2", "one",
		)

		// CH is ignored with INCR
		mustDo(t, c,
			"ZADD", "z", "INCR", "CH", "1.2", "one",
			proto.String("4.8"),
		)
	}

	t.Run("errors", func(t *testing.T) {
		// Wrong type of key
		mustOK(t, c, "SET", "str", "value")

		_, err = s.ZAdd("str", 1.0, "hi")
		mustFail(t, err, msgWrongType)

		mustDo(t, c,
			"ZADD", "str", "1.0", "hi",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"ZADD",
			proto.Error(errWrongNumber("zadd")),
		)
		mustDo(t, c,
			"ZADD", "set",
			proto.Error(errWrongNumber("zadd")),
		)
		mustDo(t, c,
			"ZADD", "set", "1.0",
			proto.Error(errWrongNumber("zadd")),
		)
		mustDo(t, c,
			"ZADD", "set", "1.0", "foo", "1.0",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZADD", "set", "MX", "1.0",
			proto.Error("ERR value is not a valid float"),
		)
		mustDo(t, c,
			"ZADD", "set", "1.0", "key", "MX",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZADD", "set", "MX", "XX", "1.0", "foo",
			proto.Error("ERR value is not a valid float"),
		)
		mustDo(t, c,
			"ZADD", "set", "INCR", "1.0", "foo", "2.3", "bar",
			proto.Error("ERR INCR option supports a single increment-element pair"),
		)
	})

	useRESP3(t, c)
	t.Run("RESP3", func(t *testing.T) {
		mustDo(t, c,
			"ZADD", "foo", "INCR", "1.2", "bar",
			proto.Float(1.2),
		)
	})
}

func TestSortedSetRange(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")
	s.ZAdd("z", 3, "three")
	s.ZAdd("z", 3, "drei")
	s.ZAdd("z", math.Inf(+1), "inf")

	t.Run("basic", func(t *testing.T) {
		mustDo(t, c,
			"ZRANGE", "z", "0", "-1",
			proto.Strings("one", "two", "zwei", "drei", "three", "inf"),
		)
		mustDo(t, c,
			"ZRANGE", "z", "0", "1",
			proto.Strings("one", "two"),
		)
		mustDo(t, c,
			"ZRANGE", "z", "-1", "-1",
			proto.Strings("inf"),
		)
		// weird cases.
		mustDo(t, c,
			"ZRANGE", "z", "-100", "-100",
			proto.Strings(),
		)
		mustDo(t, c,
			"ZRANGE", "z", "100", "400",
			proto.Strings(),
		)

		// Nonexistent key
		mustDo(t, c,
			"ZRANGE", "nosuch", "1", "4",
			proto.Strings(),
		)
	})

	t.Run("withscores", func(t *testing.T) {
		mustDo(t, c,
			"ZRANGE", "z", "1", "2", "WITHSCORES",
			proto.Strings("two", "2", "zwei", "2"),
		)
		// INF in WITHSCORES
		mustDo(t, c,
			"ZRANGE", "z", "4", "-1", "WITHSCORES",
			proto.Strings("three", "3", "inf", "inf"),
		)
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZRANGE",
			proto.Error(errWrongNumber("zrange")),
		)
		mustDo(t, c,
			"ZRANGE", "set",
			proto.Error(errWrongNumber("zrange")),
		)
		mustDo(t, c,
			"ZRANGE", "set", "1",
			proto.Error(errWrongNumber("zrange")),
		)
		mustDo(t, c,
			"ZRANGE", "set", "noint", "1",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"ZRANGE", "set", "1", "noint",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"ZRANGE", "set", "1", "2", "toomany",
			proto.Error(msgSyntaxError),
		)
		// Wrong type of key
		s.Set("str", "value")
		mustDo(t, c,
			"ZRANGE", "str", "1", "2",
			proto.Error(msgWrongType),
		)
	})
}

// Test ZREVRANGE
func TestSortedSetRevRange(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")
	s.ZAdd("z", 3, "three")
	s.ZAdd("z", 3, "drei")
	s.ZAdd("z", math.Inf(+1), "inf")

	mustDo(t, c,
		"ZREVRANGE", "z", "0", "-1",
		proto.Strings("inf", "three", "drei", "zwei", "two", "one"),
	)
	mustDo(t, c,
		"ZREVRANGE", "z", "0", "1",
		proto.Strings("inf", "three"),
	)
	mustDo(t, c,
		"ZREVRANGE", "z", "-1", "-1",
		proto.Strings("one"),
	)

	// weird cases.
	mustDo(t, c,
		"ZREVRANGE", "z", "-100", "-100",
		proto.Strings(),
	)
	mustDo(t, c,
		"ZREVRANGE", "z", "100", "400",
		proto.Strings(),
	)

	// Nonexistent key
	mustDo(t, c,
		"ZREVRANGE", "nosuch", "1", "4",
		proto.Strings(),
	)

	// With scores
	mustDo(t, c,
		"ZREVRANGE", "z", "1", "2", "WITHSCORES",
		proto.Strings("three", "3", "drei", "3"),
	)

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZREVRANGE",
			proto.Error(errWrongNumber("zrevrange")),
		)
		mustDo(t, c,
			"ZREVRANGE", "set",
			proto.Error(errWrongNumber("zrevrange")),
		)
		mustDo(t, c,
			"ZREVRANGE", "set", "1",
			proto.Error(errWrongNumber("zrevrange")),
		)
		mustDo(t, c,
			"ZREVRANGE", "set", "noint", "1",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"ZREVRANGE", "set", "1", "noint",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"ZREVRANGE", "set", "1", "2", "toomany",
			proto.Error(msgSyntaxError),
		)
		// Wrong type of key
		s.Set("str", "value")
		mustDo(t, c,
			"ZREVRANGE", "str", "1", "2",
			proto.Error(msgWrongType),
		)
	})
}

// Test ZRANGEBYSCORE,  ZREVRANGEBYSCORE, and ZCOUNT
func TestSortedSetRangeByScore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("z", -273.15, "zero kelvin")
	s.ZAdd("z", -4, "minusfour")
	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")
	s.ZAdd("z", 3, "three")
	s.ZAdd("z", 3, "drei")
	s.ZAdd("z", math.Inf(+1), "inf")

	// Normal cases
	{
		mustDo(t, c,
			"ZRANGEBYSCORE", "z", "-inf", "inf",
			proto.Strings("zero kelvin", "minusfour", "one", "two", "zwei", "drei", "three", "inf"),
		)

		mustDo(t, c,
			"ZREVRANGEBYSCORE", "z", "inf", "-inf",
			proto.Strings("inf", "three", "drei", "zwei", "two", "one", "minusfour", "zero kelvin"),
		)

		mustDo(t, c,
			"ZCOUNT", "z", "-inf", "inf",
			proto.Int(8),
		)
	}
	{
		mustDo(t, c,
			"ZRANGEBYSCORE", "z", "2", "3",
			proto.Strings("two", "zwei", "drei", "three"),
		)

		mustDo(t, c,
			"ZRANGEBYSCORE", "z", "4", "4",
			proto.Strings(),
		)

		mustDo(t, c,
			"ZREVRANGEBYSCORE", "z", "3", "2",
			proto.Strings("three", "drei", "zwei", "two"),
		)

		mustDo(t, c,
			"ZREVRANGEBYSCORE", "z", "4", "4",
			proto.Strings(),
		)

		mustDo(t, c,
			"ZCOUNT", "z", "2", "3",
			proto.Int(4),
		)
	}
	// Exclusive min
	{
		mustDo(t, c,
			"ZRANGEBYSCORE", "z", "(2", "3",
			proto.Strings("drei", "three"),
		)

		mustDo(t, c,
			"ZCOUNT", "z", "(2", "3",
			proto.Int(2),
		)
	}
	// Exclusive max
	mustDo(t, c,
		"ZRANGEBYSCORE", "z", "2", "(3",
		proto.Strings("two", "zwei"),
	)

	// Exclusive both
	mustDo(t, c,
		"ZRANGEBYSCORE", "z", "(2", "(3",
		proto.Strings(),
	)

	// Wrong ranges
	{
		mustDo(t, c,
			"ZRANGEBYSCORE", "z", "+inf", "-inf",
			proto.Strings(),
		)

		mustDo(t, c,
			"ZREVRANGEBYSCORE", "z", "-inf", "+inf",
			proto.Strings(),
		)
	}

	// No such key
	mustDo(t, c,
		"ZRANGEBYSCORE", "nosuch", "-inf", "inf",
		proto.Strings(),
	)

	// With scores
	mustDo(t, c,
		"ZRANGEBYSCORE", "z", "(1", "2", "WITHSCORES",
		proto.Strings("two", "2", "zwei", "2"),
	)

	// With LIMIT
	// (note, this is SQL like logic, not the redis RANGE logic)
	{
		mustDo(t, c,
			"ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "1", "2",
			proto.Strings("minusfour", "one"),
		)

		mustDo(t, c,
			"ZREVRANGEBYSCORE", "z", "inf", "-inf", "LIMIT", "1", "2",
			proto.Strings("three", "drei"),
		)

		mustDo(t, c,
			"ZRANGEBYSCORE", "z", "1", "inf", "LIMIT", "1", "2000",
			proto.Strings("two", "zwei", "drei", "three", "inf"),
		)

		mustDo(t, c,
			"ZREVRANGEBYSCORE", "z", "inf", "1", "LIMIT", "1", "2000",
			proto.Strings("three", "drei", "zwei", "two", "one"),
		)

		// Negative start limit. No go.
		mustDo(t, c,
			"ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "-1", "2",
			proto.Strings(),
		)

		// Negative end limit. Is fine but ignored.
		mustDo(t, c,
			"ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "1", "-2",
			proto.Strings("minusfour", "one", "two", "zwei", "drei", "three", "inf"),
		)
	}
	// Everything
	{
		mustDo(t, c,
			"ZRANGEBYSCORE", "z", "-inf", "inf", "WITHSCORES", "LIMIT", "1", "2",
			proto.Strings("minusfour", "-4", "one", "1"),
		)

		mustDo(t, c,
			"ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "1", "2", "WITHSCORES",
			proto.Strings("minusfour", "-4", "one", "1"),
		)
	}

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZRANGEBYSCORE",
			proto.Error(errWrongNumber("zrangebyscore")),
		)
		mustDo(t, c,
			"ZRANGEBYSCORE", "set",
			proto.Error(errWrongNumber("zrangebyscore")),
		)
		mustDo(t, c,
			"ZRANGEBYSCORE", "set", "1",
			proto.Error(errWrongNumber("zrangebyscore")),
		)
		mustDo(t, c,
			"ZRANGEBYSCORE", "set", "nofloat", "1",
			proto.Error("ERR min or max is not a float"),
		)
		mustDo(t, c,
			"ZRANGEBYSCORE", "set", "1", "nofloat",
			proto.Error("ERR min or max is not a float"),
		)
		mustDo(t, c,
			"ZRANGEBYSCORE", "set", "1", "2", "toomany",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZRANGEBYSCORE", "set", "[1", "2", "toomany",
			proto.Error("ERR min or max is not a float"),
		)
		mustDo(t, c,
			"ZRANGEBYSCORE", "set", "1", "[2", "toomany",
			proto.Error("ERR min or max is not a float"),
		)
		mustDo(t, c,
			"ZRANGEBYSCORE", "set", "[1", "2", "LIMIT", "noint", "1",
			proto.Error("ERR min or max is not a float"),
		)
		mustDo(t, c,
			"ZRANGEBYSCORE", "set", "[1", "2", "LIMIT", "1", "noint",
			proto.Error("ERR min or max is not a float"),
		)
		// Wrong type of key
		s.Set("str", "value")
		mustDo(t, c,
			"ZRANGEBYSCORE", "str", "1", "2",
			proto.Error(msgWrongType),
		)

		mustDo(t, c,
			"ZREVRANGEBYSCORE",
			proto.Error(errWrongNumber("zrevrangebyscore")),
		)

		mustDo(t, c,
			"ZCOUNT",
			proto.Error(errWrongNumber("zcount")),
		)
	})
}

func TestIssue10(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("key", 3.3, "element")

	mustDo(t, c,
		"ZRANGEBYSCORE", "key", "3.3", "3.3",
		proto.Strings("element"),
	)

	mustDo(t, c,
		"ZRANGEBYSCORE", "key", "4.3", "4.3",
		proto.Strings(),
	)
}

// Test ZREM
func TestSortedSetRem(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")

	// Simple delete
	{
		mustDo(t, c,
			"ZREM", "z", "two", "zwei", "nosuch",
			proto.Int(2),
		)
		assert(t, s.Exists("z"), "key is there")
	}
	// Delete the last member
	{
		must1(t, c,
			"ZREM", "z", "one",
		)
		assert(t, !s.Exists("z"), "key is gone")
	}
	// Nonexistent key
	must0(t, c,
		"ZREM", "nosuch", "member",
	)

	// Direct
	{
		s.ZAdd("z2", 1, "one")
		s.ZAdd("z2", 2, "two")
		s.ZAdd("z2", 2, "zwei")
		gone, err := s.ZRem("z2", "two")
		ok(t, err)
		assert(t, gone, "member gone")
		members, err := s.ZMembers("z2")
		ok(t, err)
		equals(t, []string{"one", "zwei"}, members)
	}

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZREM",
			proto.Error(errWrongNumber("zrem")),
		)
		mustDo(t, c,
			"ZREM", "set",
			proto.Error(errWrongNumber("zrem")),
		)
		// Wrong type of key
		s.Set("str", "value")
		mustDo(t, c,
			"ZREM", "str", "aap",
			proto.Error(msgWrongType),
		)
	})
}

// Test ZREMRANGEBYLEX
func TestSortedSetRemRangeByLex(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("z", 12, "zero kelvin")
	s.ZAdd("z", 12, "minusfour")
	s.ZAdd("z", 12, "one")
	s.ZAdd("z", 12, "oneone")
	s.ZAdd("z", 12, "two")
	s.ZAdd("z", 12, "zwei")
	s.ZAdd("z", 12, "three")
	s.ZAdd("z", 12, "drei")
	s.ZAdd("z", 12, "inf")

	// Inclusive range
	{
		mustDo(t, c,
			"ZREMRANGEBYLEX", "z", "[o", "[three",
			proto.Int(3),
		)

		members, err := s.ZMembers("z")
		ok(t, err)
		equals(t,
			[]string{"drei", "inf", "minusfour", "two", "zero kelvin", "zwei"},
			members,
		)
	}

	// Wrong ranges
	must0(t, c,
		"ZREMRANGEBYLEX", "z", "+", "(z",
	)

	// No such key
	must0(t, c,
		"ZREMRANGEBYLEX", "nosuch", "-", "+",
	)

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZREMRANGEBYLEX",
			proto.Error(errWrongNumber("zremrangebylex")),
		)
		mustDo(t, c,
			"ZREMRANGEBYLEX", "set",
			proto.Error(errWrongNumber("zremrangebylex")),
		)
		mustDo(t, c,
			"ZREMRANGEBYLEX", "set", "1", "[a",
			proto.Error("ERR min or max not valid string range item"),
		)
		mustDo(t, c,
			"ZREMRANGEBYLEX", "set", "[a", "1",
			proto.Error("ERR min or max not valid string range item"),
		)
		mustDo(t, c,
			"ZREMRANGEBYLEX", "set", "[a", "!a",
			proto.Error("ERR min or max not valid string range item"),
		)
		mustDo(t, c,
			"ZREMRANGEBYLEX", "set", "-", "+", "toomany",
			proto.Error(errWrongNumber("zremrangebylex")),
		)

		// Wrong type of key
		s.Set("str", "value")
		mustDo(t, c,
			"ZREMRANGEBYLEX", "str", "-", "+",
			proto.Error(msgWrongType),
		)
	})
}

// Test ZREMRANGEBYRANK
func TestSortedSetRemRangeByRank(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")
	s.ZAdd("z", 3, "three")
	s.ZAdd("z", 3, "drei")
	s.ZAdd("z", math.Inf(+1), "inf")

	{
		mustDo(t, c,
			"ZREMRANGEBYRANK", "z", "-2", "-1",
			proto.Int(2),
		)

		mustDo(t, c,
			"ZRANGE", "z", "0", "-1",
			proto.Strings("one", "two", "zwei", "drei"),
		)
	}

	// weird cases.
	must0(t, c,
		"ZREMRANGEBYRANK", "z", "-100", "-100",
	)
	must0(t, c,
		"ZREMRANGEBYRANK", "z", "100", "400",
	)

	// Nonexistent key
	must0(t, c,
		"ZREMRANGEBYRANK", "nosuch", "1", "4",
	)

	// Delete all. Key should be gone.
	{
		mustDo(t, c,
			"ZREMRANGEBYRANK", "z", "0", "-1",
			proto.Int(4),
		)
		equals(t, false, s.Exists("z"))
	}

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZREMRANGEBYRANK",
			proto.Error(errWrongNumber("zremrangebyrank")),
		)
		mustDo(t, c,
			"ZREMRANGEBYRANK", "set",
			proto.Error(errWrongNumber("zremrangebyrank")),
		)
		mustDo(t, c,
			"ZREMRANGEBYRANK", "set", "1",
			proto.Error(errWrongNumber("zremrangebyrank")),
		)
		mustDo(t, c,
			"ZREMRANGEBYRANK", "set", "noint", "1",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"ZREMRANGEBYRANK", "set", "1", "noint",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"ZREMRANGEBYRANK", "set", "1", "2", "toomany",
			proto.Error(errWrongNumber("zremrangebyrank")),
		)
		// Wrong type of key
		s.Set("str", "value")
		mustDo(t, c,
			"ZREMRANGEBYRANK", "str", "1", "2",
			proto.Error(msgWrongType),
		)
	})
}

// Test ZREMRANGEBYSCORE
func TestSortedSetRangeRemByScore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("z", -273.15, "zero kelvin")
	s.ZAdd("z", -4, "minusfour")
	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")
	s.ZAdd("z", 3, "three")
	s.ZAdd("z", 3, "drei")
	s.ZAdd("z", math.Inf(+1), "inf")

	// Normal cases
	{
		mustDo(t, c,
			"ZREMRANGEBYSCORE", "z", "-inf", "1",
			proto.Int(3),
		)

		mustDo(t, c,
			"ZRANGE", "z", "0", "-1",
			proto.Strings("two", "zwei", "drei", "three", "inf"),
		)
	}
	// Exclusive min
	{
		mustDo(t, c,
			"ZREMRANGEBYSCORE", "z", "(2", "(4",
			proto.Int(2),
		)

		mustDo(t, c,
			"ZRANGE", "z", "0", "-1",
			proto.Strings("two", "zwei", "inf"),
		)
	}

	// Wrong ranges
	must0(t, c,
		"ZREMRANGEBYSCORE", "z", "+inf", "-inf",
	)

	// No such key
	must0(t, c,
		"ZREMRANGEBYSCORE", "nosuch", "-inf", "inf",
	)

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZREMRANGEBYSCORE",
			proto.Error(errWrongNumber("zremrangebyscore")),
		)
		mustDo(t, c,
			"ZREMRANGEBYSCORE", "set",
			proto.Error(errWrongNumber("zremrangebyscore")),
		)
		mustDo(t, c,
			"ZREMRANGEBYSCORE", "set", "1",
			proto.Error(errWrongNumber("zremrangebyscore")),
		)
		mustDo(t, c,
			"ZREMRANGEBYSCORE", "set", "nofloat", "1",
			proto.Error(msgInvalidMinMax),
		)
		mustDo(t, c,
			"ZREMRANGEBYSCORE", "set", "1", "nofloat",
			proto.Error(msgInvalidMinMax),
		)
		mustDo(t, c,
			"ZREMRANGEBYSCORE", "set", "1", "2", "toomany",
			proto.Error(errWrongNumber("zremrangebyscore")),
		)
		// Wrong type of key
		s.Set("str", "value")
		mustDo(t, c,
			"ZREMRANGEBYSCORE", "str", "1", "2",
			proto.Error(msgWrongType),
		)
	})
}

// Test ZSCORE
func TestSortedSetScore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")

	// Simple case
	mustDo(t, c,
		"ZSCORE", "z", "two",
		proto.String("2"),
	)

	// no such member
	mustNil(t, c,
		"ZSCORE", "z", "nosuch",
	)

	// no such key
	mustNil(t, c,
		"ZSCORE", "nosuch", "nosuch",
	)

	// Direct
	{
		s.ZAdd("z2", 1, "one")
		s.ZAdd("z2", 2, "two")
		score, err := s.ZScore("z2", "two")
		ok(t, err)
		equals(t, 2.0, score)
	}

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZSCORE",
			proto.Error(errWrongNumber("zscore")),
		)
		mustDo(t, c,
			"ZSCORE", "key",
			proto.Error(errWrongNumber("zscore")),
		)
		mustDo(t, c,
			"ZSCORE", "too", "many", "arguments",
			proto.Error(errWrongNumber("zscore")),
		)

		s.Set("str", "value")
		mustDo(t, c,
			"ZSCORE", "str", "aap",
			proto.Error(msgWrongType),
		)
	})
}

// Test ZRANGEBYLEX, ZREVRANGEBYLEX, ZLEXCOUNT
func TestSortedSetRangeByLex(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("z", 12, "zero kelvin")
	s.ZAdd("z", 12, "minusfour")
	s.ZAdd("z", 12, "one")
	s.ZAdd("z", 12, "oneone")
	s.ZAdd("z", 12, "two")
	s.ZAdd("z", 12, "zwei")
	s.ZAdd("z", 12, "three")
	s.ZAdd("z", 12, "drei")
	s.ZAdd("z", 12, "inf")

	// Normal cases
	{
		mustDo(t, c,
			"ZRANGEBYLEX", "z", "-", "+",
			proto.Strings(
				"drei",
				"inf",
				"minusfour",
				"one",
				"oneone",
				"three",
				"two",
				"zero kelvin",
				"zwei",
			),
		)

		mustDo(t, c,
			"ZRANGEBYLEX", "z", "[zz", "+",
			proto.Strings(),
		)

		mustDo(t, c,
			"ZREVRANGEBYLEX", "z", "+", "-",
			proto.Strings(
				"zwei",
				"zero kelvin",
				"two",
				"three",
				"oneone",
				"one",
				"minusfour",
				"inf",
				"drei",
			),
		)

		mustDo(t, c,
			"ZLEXCOUNT", "z", "-", "+",
			proto.Int(9),
		)
	}

	// Inclusive range
	{
		mustDo(t, c,
			"ZRANGEBYLEX", "z", "[o", "[three",
			proto.Strings("one", "oneone", "three"),
		)

		mustDo(t, c,
			"ZREVRANGEBYLEX", "z", "[three", "[o",
			proto.Strings("three", "oneone", "one"),
		)

		mustDo(t, c,
			"ZLEXCOUNT", "z", "[o", "[three",
			proto.Int(3),
		)
	}

	// Exclusive range
	{
		mustDo(t, c,
			"ZRANGEBYLEX", "z", "(o", "(z",
			proto.Strings("one", "oneone", "three", "two"),
		)

		mustDo(t, c,
			"ZREVRANGEBYLEX", "z", "(z", "(o",
			proto.Strings("two", "three", "oneone", "one"),
		)

		mustDo(t, c,
			"ZLEXCOUNT", "z", "(o", "(z",
			proto.Int(4),
		)
	}

	// Wrong ranges
	{
		mustDo(t, c,
			"ZRANGEBYLEX", "z", "+", "(z",
			proto.Strings(),
		)

		mustDo(t, c,
			"ZREVRANGEBYLEX", "z", "(z", "+",
			proto.Strings(),
		)

		mustDo(t, c,
			"ZRANGEBYLEX", "z", "(a", "-",
			proto.Strings(),
		)

		mustDo(t, c,
			"ZREVRANGEBYLEX", "z", "-", "(a",
			proto.Strings(),
		)

		mustDo(t, c,
			"ZRANGEBYLEX", "z", "(z", "(a",
			proto.Strings(),
		)

		must0(t, c,
			"ZLEXCOUNT", "z", "(z", "(z",
		)
	}

	// No such key
	{
		mustDo(t, c,
			"ZRANGEBYLEX", "nosuch", "-", "+",
			proto.Strings(),
		)

		must0(t, c,
			"ZLEXCOUNT", "nosuch", "-", "+",
		)
	}

	// With LIMIT
	// (note, this is SQL like logic, not the redis RANGE logic)
	{
		mustDo(t, c,
			"ZRANGEBYLEX", "z", "-", "+", "LIMIT", "1", "2",
			proto.Strings("inf", "minusfour"),
		)

		// Negative start limit. No go.
		mustDo(t, c,
			"ZRANGEBYLEX", "z", "-", "+", "LIMIT", "-1", "2",
			proto.Strings(),
		)

		// Negative end limit. Is fine but ignored.
		mustDo(t, c,
			"ZRANGEBYLEX", "z", "-", "+", "LIMIT", "1", "-2",
			proto.Strings("inf", "minusfour", "one", "oneone", "three", "two", "zero kelvin", "zwei"),
		)
	}

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZRANGEBYLEX",
			proto.Error(errWrongNumber("zrangebylex")),
		)
		mustDo(t, c,
			"ZRANGEBYLEX", "set",
			proto.Error(errWrongNumber("zrangebylex")),
		)
		mustDo(t, c,
			"ZRANGEBYLEX", "set", "1", "[a",
			proto.Error(msgInvalidRangeItem),
		)
		mustDo(t, c,
			"ZRANGEBYLEX", "set", "[a", "1",
			proto.Error(msgInvalidRangeItem),
		)
		mustDo(t, c,
			"ZRANGEBYLEX", "set", "[a", "!a",
			proto.Error(msgInvalidRangeItem),
		)
		mustDo(t, c,
			"ZRANGEBYLEX", "set", "-", "+", "toomany",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZRANGEBYLEX", "set", "[1", "(1", "LIMIT", "noint", "1",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"ZRANGEBYLEX", "set", "[1", "(1", "LIMIT", "1", "noint",
			proto.Error(msgInvalidInt),
		)

		s.Set("str", "value")
		mustDo(t, c,
			"ZRANGEBYLEX", "str", "-", "+",
			proto.Error(msgWrongType),
		)

		mustDo(t, c,
			"ZLEXCOUNT",
			proto.Error(errWrongNumber("zlexcount")),
		)
		mustDo(t, c,
			"ZLEXCOUNT", "k",
			proto.Error(errWrongNumber("zlexcount")),
		)
		mustDo(t, c,
			"ZLEXCOUNT", "k", "[a", "a",
			proto.Error(msgInvalidRangeItem),
		)
		mustDo(t, c,
			"ZLEXCOUNT", "k", "a", "(a",
			proto.Error(msgInvalidRangeItem),
		)
		mustDo(t, c,
			"ZLEXCOUNT", "k", "(a", "(a", "toomany",
			proto.Error(errWrongNumber("zlexcount")),
		)
	})
}

// Test ZINCRBY
func TestSortedSetIncrby(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Normal cases
	{
		// New key
		mustDo(t, c,
			"ZINCRBY", "z", "1", "member",
			proto.String("1"),
		)

		// Existing key
		mustDo(t, c,
			"ZINCRBY", "z", "2.5", "member",
			proto.String("3.5"),
		)

		// New member
		mustDo(t, c,
			"ZINCRBY", "z", "1", "othermember",
			proto.String("1"),
		)
	}

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZINCRBY",
			proto.Error(errWrongNumber("zincrby")),
		)
		mustDo(t, c,
			"ZINCRBY", "set",
			proto.Error(errWrongNumber("zincrby")),
		)
		mustDo(t, c,
			"ZINCRBY", "set", "nofloat", "a",
			proto.Error(msgInvalidFloat),
		)
		mustDo(t, c,
			"ZINCRBY", "set", "1.0", "too", "many",
			proto.Error(errWrongNumber("zincrby")),
		)

		s.Set("str", "value")
		mustDo(t, c,
			"ZINCRBY", "str", "1.0", "member",
			proto.Error(msgWrongType),
		)
	})
}

func TestZscan(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// We cheat with zscan. It always returns everything.

	s.ZAdd("h", 1.0, "field1")
	s.ZAdd("h", 2.0, "field2")

	// No problem
	mustDo(t, c,
		"ZSCAN", "h", "0",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("field1"),
				proto.String("1"),
				proto.String("field2"),
				proto.String("2"),
			),
		),
	)

	// Invalid cursor
	mustDo(t, c,
		"ZSCAN", "h", "42",
		proto.Array(
			proto.String("0"),
			proto.Array(),
		),
	)

	// COUNT (ignored)
	mustDo(t, c,
		"ZSCAN", "h", "0", "COUNT", "200",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("field1"),
				proto.String("1"),
				proto.String("field2"),
				proto.String("2"),
			),
		),
	)

	// MATCH
	s.ZAdd("h", 3.0, "aap")
	s.ZAdd("h", 4.0, "noot")
	s.ZAdd("h", 5.0, "mies")
	mustDo(t, c,
		"ZSCAN", "h", "0", "MATCH", "mi*",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("mies"),
				proto.String("5"),
			),
		),
	)

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZSCAN",
			proto.Error(errWrongNumber("zscan")),
		)
		mustDo(t, c,
			"ZSCAN", "set",
			proto.Error(errWrongNumber("zscan")),
		)
		mustDo(t, c,
			"ZSCAN", "set", "noint",
			proto.Error("ERR invalid cursor"),
		)
		mustDo(t, c,
			"ZSCAN", "set", "0", "MATCH",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZSCAN", "set", "0", "COUNT",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZSCAN", "set", "0", "COUNT", "noint",
			proto.Error(msgInvalidInt),
		)

		s.Set("str", "value")
		mustDo(t, c,
			"ZSCAN", "str", "0",
			proto.Error(msgWrongType),
		)
	})
}

func TestZunionstore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("h1", 1.0, "field1")
	s.ZAdd("h1", 2.0, "field2")
	s.ZAdd("h2", 1.0, "field1")
	s.ZAdd("h2", 2.0, "field2")

	t.Run("simple case", func(t *testing.T) {
		mustDo(t, c,
			"ZUNIONSTORE", "new", "2", "h1", "h2",
			proto.Int(2),
		)

		ss, err := s.SortedSet("new")
		ok(t, err)
		equals(t, map[string]float64{"field1": 2, "field2": 4}, ss)
	})

	t.Run("merge destination with itself", func(t *testing.T) {
		s.ZAdd("h3", 1.0, "field1")
		s.ZAdd("h3", 3.0, "field3")

		mustDo(t, c,
			"ZUNIONSTORE", "h3", "2", "h1", "h3",
			proto.Int(3),
		)

		ss, err := s.SortedSet("h3")
		ok(t, err)
		equals(t, map[string]float64{"field1": 2, "field2": 2, "field3": 3}, ss)
	})

	t.Run("WEIGHTS", func(t *testing.T) {
		mustDo(t, c,
			"ZUNIONSTORE", "weighted", "2", "h1", "h2", "WeIgHtS", "4.5", "12",
			proto.Int(2),
		)

		ss, err := s.SortedSet("weighted")
		ok(t, err)
		equals(t, map[string]float64{"field1": 16.5, "field2": 33}, ss)
	})

	t.Run("AGGREGATE", func(t *testing.T) {
		mustDo(t, c,
			"ZUNIONSTORE", "aggr", "2", "h1", "h2", "AgGrEgAtE", "min",
			proto.Int(2),
		)

		ss, err := s.SortedSet("aggr")
		ok(t, err)
		equals(t, map[string]float64{"field1": 1.0, "field2": 2.0}, ss)
	})

	t.Run("normal set", func(t *testing.T) {
		mustDo(t, c,
			"SADD", "set", "aap", "noot", "mies",
			proto.Int(3),
		)
		mustDo(t, c,
			"ZUNIONSTORE", "aggr", "1", "set",
			proto.Int(3),
		)
	})

	t.Run("wrong usage", func(t *testing.T) {
		mustDo(t, c,
			"ZUNIONSTORE",
			proto.Error(errWrongNumber("zunionstore")),
		)
		mustDo(t, c,
			"ZUNIONSTORE", "set",
			proto.Error(errWrongNumber("zunionstore")),
		)
		mustDo(t, c,
			"ZUNIONSTORE", "set", "noint",
			proto.Error(errWrongNumber("zunionstore")),
		)
		mustDo(t, c,
			"ZUNIONSTORE", "set", "0", "key",
			proto.Error("ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE"),
		)
		mustDo(t, c,
			"ZUNIONSTORE", "set", "-1", "key",
			proto.Error("ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE"),
		)
		mustDo(t, c,
			"ZUNIONSTORE", "set", "1", "too", "many",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZUNIONSTORE", "set", "2", "key",
			proto.Error(msgSyntaxError),
		)

		mustDo(t, c,
			"ZUNIONSTORE", "set", "2", "k1", "k2", "WEIGHTS",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZUNIONSTORE", "set", "2", "k1", "k2", "WEIGHTS", "1", "2", "3",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZUNIONSTORE", "set", "2", "k1", "k2", "WEIGHTS", "1", "nof",
			proto.Error("ERR weight value is not a float"),
		)

		mustDo(t, c,
			"ZUNIONSTORE", "set", "2", "k1", "k2", "AGGREGATE",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZUNIONSTORE", "set", "2", "k1", "k2", "AGGREGATE", "foo",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZUNIONSTORE", "set", "2", "k1", "k2", "AGGREGATE", "sum", "foo",
			proto.Error(msgSyntaxError),
		)

		s.Set("str", "value")
		mustDo(t, c,
			"ZUNIONSTORE", "set", "1", "str",
			proto.Error(msgWrongType),
		)
	})
}

func TestZunion(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("h1", 1.0, "field1")
	s.ZAdd("h1", 2.0, "field2")
	s.ZAdd("h2", 1.0, "field1")
	s.ZAdd("h2", 2.0, "field2")

	t.Run("simple case", func(t *testing.T) {
		mustDo(t, c,
			"ZUNION", "2", "h1", "h2", proto.Strings("field1", "field2"),
		)
	})

	t.Run("WITHSCORES", func(t *testing.T) {
		mustDo(t, c,
			"ZUNION", "2", "h1", "h2", "WITHSCORES", proto.Strings("field1", "2", "field2", "4"),
		)
	})

	t.Run("WEIGHTS", func(t *testing.T) {
		mustDo(t, c,
			"ZUNION", "2", "h1", "h2", "WeiGHtS", "4.5", "12", "WITHSCORES",
			proto.Strings("field1", "16.5", "field2", "33"),
		)
	})

	t.Run("AGGREGATE", func(t *testing.T) {
		mustDo(t, c,
			"ZUNION", "2", "h1", "h2", "AgGrEgAtE", "min", "WITHSCORES",
			proto.Strings("field1", "1", "field2", "2"),
		)
	})

	t.Run("wrong usage", func(t *testing.T) {
		mustDo(t, c,
			"ZUNION",
			proto.Error(errWrongNumber("zunion")),
		)
		mustDo(t, c,
			"ZUNION", "2",
			proto.Error(errWrongNumber("zunion")),
		)
		mustDo(t, c,
			"ZUNION", "noint",
			proto.Error(errWrongNumber("zunion")),
		)
		mustDo(t, c,
			"ZUNION", "0", "key",
			proto.Error("ERR at least 1 input key is needed for ZUNION"),
		)
		mustDo(t, c,
			"ZUNION", "-1", "key",
			proto.Error("ERR at least 1 input key is needed for ZUNION"),
		)
		mustDo(t, c,
			"ZUNION", "1", "too", "many",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZUNION", "2", "key",
			proto.Error(msgSyntaxError),
		)

		mustDo(t, c,
			"ZUNION", "2", "k1", "k2", "WEIGHTS",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZUNION", "2", "k1", "k2", "WEIGHTS", "1", "2", "3",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZUNION", "2", "k1", "k2", "WEIGHTS", "1", "nof",
			proto.Error("ERR weight value is not a float"),
		)

		mustDo(t, c,
			"ZUNION", "2", "k1", "k2", "AGGREGATE",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZUNION", "2", "k1", "k2", "AGGREGATE", "foo",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZUNION", "2", "k1", "k2", "AGGREGATE", "sum", "foo",
			proto.Error(msgSyntaxError),
		)
	})
}

func TestZinterstore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("h1", 1.0, "field1")
	s.ZAdd("h1", 2.0, "field2")
	s.ZAdd("h1", 3.0, "field3")
	s.ZAdd("h2", 1.0, "field1")
	s.ZAdd("h2", 2.0, "field2")
	s.ZAdd("h2", 4.0, "field4")
	s.SAdd("s2", "field1")

	// Simple case
	{
		mustDo(t, c,
			"ZINTERSTORE", "new", "2", "h1", "h2",
			proto.Int(2),
		)

		ss, err := s.SortedSet("new")
		ok(t, err)
		equals(t, map[string]float64{"field1": 2, "field2": 4}, ss)
	}

	// WEIGHTS
	{
		mustDo(t, c,
			"ZINTERSTORE", "weighted", "2", "h1", "h2", "WeIgHtS", "4.5", "12",
			proto.Int(2),
		)

		ss, err := s.SortedSet("weighted")
		ok(t, err)
		equals(t, map[string]float64{"field1": 16.5, "field2": 33}, ss)
	}

	// AGGREGATE
	{
		mustDo(t, c,
			"ZINTERSTORE", "aggr", "2", "h1", "h2", "AgGrEgAtE", "min",
			proto.Int(2),
		)

		ss, err := s.SortedSet("aggr")
		ok(t, err)
		equals(t, map[string]float64{"field1": 1.0, "field2": 2.0}, ss)
	}

	// compatible set
	{
		mustDo(t, c,
			"ZINTERSTORE", "cnew", "2", "h1", "s2",
			proto.Int(1),
		)

		ss, err := s.SortedSet("cnew")
		ok(t, err)
		equals(t, map[string]float64{"field1": 2}, ss)
	}

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZINTERSTORE",
			proto.Error(errWrongNumber("zinterstore")),
		)
		mustDo(t, c,
			"ZINTERSTORE", "set",
			proto.Error(errWrongNumber("zinterstore")),
		)
		mustDo(t, c,
			"ZINTERSTORE", "set", "noint",
			proto.Error(errWrongNumber("zinterstore")),
		)
		mustDo(t, c,
			"ZINTERSTORE", "set", "0", "key",
			proto.Error("ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE"),
		)
		mustDo(t, c,
			"ZINTERSTORE", "set", "-1", "key",
			proto.Error("ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE"),
		)
		mustDo(t, c,
			"ZINTERSTORE", "set", "1", "too", "many",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZINTERSTORE", "set", "2", "key",
			proto.Error(msgSyntaxError),
		)

		mustDo(t, c,
			"ZINTERSTORE", "set", "2", "k1", "k2", "WEIGHTS",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZINTERSTORE", "set", "2", "k1", "k2", "WEIGHTS", "1", "2", "3",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZINTERSTORE", "set", "2", "k1", "k2", "WEIGHTS", "1", "nof",
			proto.Error("ERR weight value is not a float"),
		)

		mustDo(t, c,
			"ZINTERSTORE", "set", "2", "k1", "k2", "AGGREGATE",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZINTERSTORE", "set", "2", "k1", "k2", "AGGREGATE", "foo",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"ZINTERSTORE", "set", "2", "k1", "k2", "AGGREGATE", "sum", "foo",
			proto.Error(msgSyntaxError),
		)

		s.Set("str", "value")
		mustDo(t, c,
			"ZINTERSTORE", "set", "1", "str",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"ZINTERSTORE", "set", "2", "set", "str",
			proto.Error(msgWrongType),
		)
	})
}

func TestSSRange(t *testing.T) {
	ss := newSortedSet()
	ss.set(1.0, "key1")
	ss.set(5.0, "key5")
	elems := ss.byScore(asc)
	type cas struct {
		min, max       float64
		minInc, maxInc bool
		want           []string
	}
	for _, c := range []cas{
		{
			min:    2.0,
			minInc: true,
			max:    3.0,
			maxInc: true,
			want:   []string(nil),
		},
		{
			min:    -2.0,
			minInc: true,
			max:    -3.0,
			maxInc: true,
			want:   []string(nil),
		},
		{
			min:    12.0,
			minInc: true,
			max:    13.0,
			maxInc: true,
			want:   []string(nil),
		},
		{
			min:    1.0,
			minInc: false,
			max:    3.0,
			maxInc: true,
			want:   []string(nil),
		},
		{
			min:    2.0,
			minInc: true,
			max:    5.0,
			maxInc: false,
			want:   []string(nil),
		},
		{
			min:  0.0,
			max:  2.0,
			want: []string{"key1"},
		},
		{
			min:  2.0,
			max:  7.0,
			want: []string{"key5"},
		},
		{
			min:  0.0,
			max:  7.0,
			want: []string{"key1", "key5"},
		},
		{
			min:    1.0,
			minInc: false,
			max:    5.0,
			maxInc: false,
			want:   []string(nil),
		},
	} {
		var have []string
		for _, v := range withSSRange(elems, c.min, c.minInc, c.max, c.maxInc) {
			have = append(have, v.member)
		}
		equals(t, have, c.want)
	}
}

// Test ZPOPMIN
func TestSortedSetPopMin(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")
	s.ZAdd("z", 3, "three")
	s.ZAdd("z", 3, "drei")
	s.ZAdd("z", math.Inf(+1), "inf")

	mustDo(t, c,
		"ZPOPMIN", "z", "2",
		proto.Strings("one", "1", "two", "2"),
	)

	// Get one - without count
	mustDo(t, c,
		"ZPOPMIN", "z",
		proto.Strings("zwei", "2"),
	)

	// weird cases.
	mustDo(t, c,
		"ZPOPMIN", "z", "-100",
		proto.Strings(),
	)

	// Nonexistent key
	mustDo(t, c,
		"ZPOPMIN", "nosuch", "1",
		proto.Strings(),
	)

	// Get more than exist
	mustDo(t, c,
		"ZPOPMIN", "z", "100",
		proto.Strings("drei", "3", "three", "3", "inf", "inf"),
	)

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZPOPMIN",
			proto.Error(errWrongNumber("zpopmin")),
		)
		mustDo(t, c,
			"ZPOPMIN", "set", "noint",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"ZPOPMIN", "set", "1", "toomany",
			proto.Error(msgSyntaxError),
		)

		s.Set("str", "value")
		mustDo(t, c,
			"ZPOPMIN", "str", "1",
			proto.Error(msgWrongType),
		)
	})
}

// Test ZPOPMAX
func TestSortedSetPopMax(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")
	s.ZAdd("z", 3, "three")
	s.ZAdd("z", 3, "drei")
	s.ZAdd("z", math.Inf(+1), "inf")

	mustDo(t, c,
		"ZPOPMAX", "z", "2",
		proto.Strings("inf", "inf", "three", "3"),
	)

	// Get one - without count
	mustDo(t, c,
		"ZPOPMAX", "z",
		proto.Strings("drei", "3"),
	)

	// weird cases.
	mustDo(t, c,
		"ZPOPMAX", "z", "-100",
		proto.Strings(),
	)

	// Nonexistent key
	mustDo(t, c,
		"ZPOPMAX", "nosuch", "1",
		proto.Strings(),
	)

	// Get more than exist
	mustDo(t, c,
		"ZPOPMAX", "z", "100",
		proto.Strings("zwei", "2", "two", "2", "one", "1"),
	)

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZPOPMAX",
			proto.Error(errWrongNumber("zpopmax")),
		)

		mustDo(t, c,
			"ZPOPMAX", "set", "noint",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"ZPOPMAX", "set", "1", "toomany",
			proto.Error(msgSyntaxError),
		)

		s.Set("str", "value")
		mustDo(t, c,
			"ZPOPMAX", "str", "1",
			proto.Error(msgWrongType),
		)
	})
}

// Test ZRANDMEMBER
func TestSortedSetRandmember(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")
	s.ZAdd("z", 3, "three")
	s.ZAdd("z", 3, "drei")

	t.Run("no count", func(t *testing.T) {
		s.Seed(12)
		mustDo(t, c,
			"ZRANDMEMBER", "z",
			proto.String("three"),
		)

		mustDo(t, c,
			"ZRANDMEMBER", "nosuch",
			proto.Nil,
		)
	})

	t.Run("positive count", func(t *testing.T) {
		s.Seed(81)
		mustDo(t, c,
			"ZRANDMEMBER", "z", "2",
			proto.Strings("one", "two"),
		)

		mustDo(t, c,
			"ZRANDMEMBER", "z", "2", "WITHSCORES",
			proto.Strings("drei", "3", "zwei", "2"),
		)

		s.Seed(81)
		mustDo(t, c,
			"ZRANDMEMBER", "z", "7",
			proto.Strings("one", "two", "zwei", "three", "drei"),
		)

		mustDo(t, c,
			"ZRANDMEMBER", "z", "0",
			proto.Strings(),
		)

		mustDo(t, c,
			"ZRANDMEMBER", "nosuch", "40",
			proto.Array(),
		)
	})

	t.Run("negative count", func(t *testing.T) {
		s.Seed(-12)
		mustDo(t, c,
			"ZRANDMEMBER", "z", "-2",
			proto.Strings("one", "one"),
		)

		mustDo(t, c,
			"ZRANDMEMBER", "z", "-2", "WITHSCORES",
			proto.Strings("zwei", "2", "two", "2"),
		)

		mustDo(t, c,
			"ZRANDMEMBER", "z", "-7",
			proto.Strings("two", "two", "one", "drei", "drei", "two", "two"),
		)

		mustDo(t, c,
			"ZRANDMEMBER", "z", "-0",
			proto.Strings(),
		)

		mustDo(t, c,
			"ZRANDMEMBER", "nosuch", "-33",
			proto.Array(),
		)

		mustDo(t, c,
			"ZRANDMEMBER", "nosuch", "-33", "WITHSCORES",
			proto.Array(),
		)
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"ZRANDMEMBER",
			proto.Error(errWrongNumber("zrandmember")),
		)

		mustDo(t, c,
			"ZRANDMEMBER", "z", "noint",
			proto.Error(msgInvalidInt),
		)

		mustDo(t, c,
			"ZRANDMEMBER", "z", "WITHSCORES",
			proto.Error(msgInvalidInt),
		)

		s.Set("str", "value")
		mustDo(t, c,
			"ZRANDMEMBER", "str", "1",
			proto.Error(msgWrongType),
		)
	})
}
