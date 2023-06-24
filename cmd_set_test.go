package miniredis

import (
	"sort"
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

// Test SADD / SMEMBERS.
func TestSadd(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		mustDo(t, c,
			"SADD", "s", "aap", "noot", "mies",
			proto.Int(3),
		)

		members, err := s.Members("s")
		ok(t, err)
		equals(t, []string{"aap", "mies", "noot"}, members)

		mustDo(t, c,
			"SMEMBERS", "s",
			proto.Strings("aap", "mies", "noot"),
		)
	}

	mustDo(t, c,
		"TYPE", "s",
		proto.Inline("set"),
	)

	// SMEMBERS on an nonexisting key
	mustDo(t, c,
		"SMEMBERS", "nosuch",
		proto.Strings(),
	)

	{
		mustDo(t, c,
			"SADD", "s", "new", "noot", "mies",
			proto.Int(1), // Only one new field.
		)

		members, err := s.Members("s")
		ok(t, err)
		equals(t, []string{"aap", "mies", "new", "noot"}, members)
	}

	t.Run("direct usage", func(t *testing.T) {
		added, err := s.SetAdd("s1", "aap")
		ok(t, err)
		equals(t, 1, added)

		members, err := s.Members("s1")
		ok(t, err)
		equals(t, []string{"aap"}, members)
	})

	t.Run("errors", func(t *testing.T) {
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"SADD", "str", "hi",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"SMEMBERS", "str",
			proto.Error(msgWrongType),
		)
		// Wrong argument counts
		mustDo(t, c,
			"SADD",
			proto.Error(errWrongNumber("sadd")),
		)
		mustDo(t, c,
			"SADD", "set",
			proto.Error(errWrongNumber("sadd")),
		)
		mustDo(t, c,
			"SMEMBERS",
			proto.Error(errWrongNumber("smembers")),
		)
		mustDo(t, c,
			"SMEMBERS", "set", "spurious",
			proto.Error(errWrongNumber("smembers")),
		)
	})

	useRESP3(t, c)
	t.Run("RESP3", func(t *testing.T) {
		mustDo(t, c, "SMEMBERS", "resp", proto.Set())
		mustDo(t, c, "SADD", "resp", "aap", proto.Int(1))
		mustDo(t, c, "SMEMBERS", "resp", proto.StringSet("aap"))
	})
}

// Test SISMEMBER
func TestSismember(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.SetAdd("s", "aap", "noot", "mies")

	{
		must1(t, c, "SISMEMBER", "s", "aap")

		must0(t, c, "SISMEMBER", "s", "nosuch")
	}

	// a nonexisting key
	must0(t, c, "SISMEMBER", "nosuch", "nosuch")

	t.Run("direct usage", func(t *testing.T) {
		isMember, err := s.IsMember("s", "noot")
		ok(t, err)
		equals(t, true, isMember)
	})

	t.Run("errors", func(t *testing.T) {
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"SISMEMBER", "str", "foo",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"SISMEMBER",
			proto.Error(errWrongNumber("sismember")),
		)
		mustDo(t, c,
			"SISMEMBER", "set",
			proto.Error(errWrongNumber("sismember")),
		)
		mustDo(t, c,
			"SISMEMBER", "set", "spurious", "args",
			proto.Error(errWrongNumber("sismember")),
		)
	})
}

// Test SREM
func TestSrem(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.SetAdd("s", "aap", "noot", "mies", "vuur")

	{
		mustDo(t, c,
			"SREM", "s", "aap", "noot",
			proto.Int(2),
		)

		members, err := s.Members("s")
		ok(t, err)
		equals(t, []string{"mies", "vuur"}, members)
	}

	// a nonexisting key
	must0(t, c,
		"SREM", "s", "nosuch",
		proto.Int(9),
	)

	// a nonexisting key
	must0(t, c,
		"SREM", "nosuch", "nosuch",
	)

	t.Run("direct usage", func(t *testing.T) {
		b, err := s.SRem("s", "mies")
		ok(t, err)
		equals(t, 1, b)

		members, err := s.Members("s")
		ok(t, err)
		equals(t, []string{"vuur"}, members)
	})

	t.Run("errors", func(t *testing.T) {
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"SREM", "str", "value",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"SREM",
			proto.Error(errWrongNumber("srem")),
		)
		mustDo(t, c,
			"SREM", "set",
			proto.Error(errWrongNumber("srem")),
		)
	})
}

// Test SMOVE
func TestSmove(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.SetAdd("s", "aap", "noot")

	{
		must1(t, c,
			"SMOVE", "s", "s2", "aap",
		)

		m, err := s.IsMember("s", "aap")
		ok(t, err)
		equals(t, false, m)
		m, err = s.IsMember("s2", "aap")
		ok(t, err)
		equals(t, true, m)
	}

	// Move away the last member
	{
		must1(t, c,
			"SMOVE", "s", "s2", "noot",
		)

		equals(t, false, s.Exists("s"))

		m, err := s.IsMember("s2", "noot")
		ok(t, err)
		equals(t, true, m)
	}

	// a nonexisting member
	must0(t, c, "SMOVE", "s", "s2", "nosuch")

	// a nonexisting key
	must0(t, c, "SMOVE", "nosuch", "nosuch2", "nosuch")

	t.Run("errors", func(t *testing.T) {
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"SMOVE", "str", "dst", "value",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"SMOVE", "s2", "str", "value",
			proto.Error(msgWrongType),
		)

		mustDo(t, c,
			"SMOVE",
			proto.Error(errWrongNumber("smove")),
		)
		mustDo(t, c,
			"SMOVE", "set",
			proto.Error(errWrongNumber("smove")),
		)
		mustDo(t, c,
			"SMOVE", "set", "set2",
			proto.Error(errWrongNumber("smove")),
		)
		mustDo(t, c,
			"SMOVE", "set", "set2", "spurious", "args",
			proto.Error(errWrongNumber("smove")),
		)
	})
}

// Test SPOP
func TestSpop(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("basics", func(t *testing.T) {
		s.SetAdd("s", "aap", "noot")

		res, err := c.Do("SPOP", "s")
		ok(t, err)
		assert(t, res == proto.String("aap") || res == proto.String("noot"), "spop got something")

		res, err = c.Do("SPOP", "s")
		ok(t, err)
		assert(t, res == proto.String("aap") || res == proto.String("noot"), "spop got something")

		assert(t, !s.Exists("s"), "all spopped away")
	})

	t.Run("nonexisting key", func(t *testing.T) {
		mustNil(t, c, "SPOP", "nosuch")
	})

	t.Run("various errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		mustDo(t, c,
			"SMOVE",
			proto.Error(errWrongNumber("smove")),
		)
		mustDo(t, c,
			"SMOVE", "chk", "set2",
			proto.Error(errWrongNumber("smove")),
		)

		mustDo(t, c,
			"SPOP", "str",
			proto.Error(msgWrongType),
		)
	})

	t.Run("count argument", func(t *testing.T) {
		s.Seed(42)
		s.SetAdd("s", "aap", "noot", "mies", "vuur")
		mustDo(t, c,
			"SPOP", "s", "2",
			proto.Strings("mies", "vuur"),
		)
		members, err := s.Members("s")
		ok(t, err)
		assert(t, len(members) == 2, "SPOP s 2")

		mustDo(t, c,
			"SPOP", "str", "-12",
			proto.Error(msgOutOfRange),
		)
	})
}

// Test SRANDMEMBER
func TestSrandmember(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.SetAdd("s", "aap", "noot", "mies")

	s.Seed(42)
	// No count
	{
		res, err := c.Do("SRANDMEMBER", "s")
		ok(t, err)
		assert(t, res == proto.String("aap") ||
			res == proto.String("noot") ||
			res == proto.String("mies"),
			"srandmember got something",
		)
	}

	// Positive count
	mustDo(t, c,
		"SRANDMEMBER", "s", "2",
		proto.Strings("noot", "mies"),
	)

	// Negative count
	mustDo(t, c,
		"SRANDMEMBER", "s", "-2",
		proto.Strings("aap", "mies"),
	)

	// a nonexisting key
	mustNil(t, c,
		"SRANDMEMBER", "nosuch",
	)

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		mustDo(t, c,
			"SRANDMEMBER",
			proto.Error(errWrongNumber("srandmember")),
		)
		mustDo(t, c,
			"SRANDMEMBER", "chk", "noint",
			proto.Error("ERR value is not an integer or out of range"),
		)
		mustDo(t, c,
			"SRANDMEMBER", "chk", "1", "toomanu",
			proto.Error("ERR syntax error"),
		)

		mustDo(t, c,
			"SRANDMEMBER", "str",
			proto.Error(msgWrongType),
		)
	})

	useRESP3(t, c)
	t.Run("RESP3", func(t *testing.T) {
		s.SetAdd("q", "aap")
		mustDo(t, c,
			"SRANDMEMBER", "q",
			proto.String("aap"),
		)
		mustDo(t, c,
			"SRANDMEMBER", "q", "1",
			proto.Strings("aap"),
		)
	})
}

// Test SDIFF
func TestSdiff(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.SetAdd("s1", "aap", "noot", "mies")
	s.SetAdd("s2", "noot", "mies", "vuur")
	s.SetAdd("s3", "aap", "mies", "wim")

	// Simple case
	mustDo(t, c,
		"SDIFF", "s1", "s2",
		proto.Strings("aap"),
	)

	// No other set
	{
		res, err := c.DoStrings("SDIFF", "s1")
		ok(t, err)
		sort.Strings(res)
		equals(t, []string{"aap", "mies", "noot"}, res)
	}

	// 3 sets
	mustDo(t, c,
		"SDIFF", "s1", "s2", "s3",
		proto.Strings(),
	)

	// A nonexisting key
	mustDo(t, c,
		"SDIFF", "s9",
		proto.Strings(),
	)

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		mustDo(t, c,
			"SDIFF",
			proto.Error(errWrongNumber("sdiff")),
		)
		mustDo(t, c,
			"SDIFF", "str",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"SDIFF", "chk", "str",
			proto.Error(msgWrongType),
		)
	})
}

// Test SDIFFSTORE
func TestSdiffstore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.SetAdd("s1", "aap", "noot", "mies")
	s.SetAdd("s2", "noot", "mies", "vuur")
	s.SetAdd("s3", "aap", "mies", "wim")

	// Simple case
	{
		must1(t, c,
			"SDIFFSTORE", "res", "s1", "s3",
		)
		s.CheckSet(t, "res", "noot")
	}

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		mustDo(t, c,
			"SDIFFSTORE",
			proto.Error(errWrongNumber("sdiffstore")),
		)
		mustDo(t, c,
			"SDIFFSTORE", "t",
			proto.Error(errWrongNumber("sdiffstore")),
		)
		mustDo(t, c,
			"SDIFFSTORE", "t", "str",
			proto.Error(msgWrongType),
		)
	})
}

// Test SINTER
func TestSinter(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.SetAdd("s1", "aap", "noot", "mies")
	s.SetAdd("s2", "noot", "mies", "vuur")
	s.SetAdd("s3", "aap", "mies", "wim")

	// Simple case
	{
		res, err := c.DoStrings("SINTER", "s1", "s2")
		ok(t, err)
		sort.Strings(res)
		equals(t, []string{"mies", "noot"}, res)
	}

	// No other set
	{
		res, err := c.DoStrings("SINTER", "s1")
		ok(t, err)
		sort.Strings(res)
		equals(t, []string{"aap", "mies", "noot"}, res)
	}

	// 3 sets
	mustDo(t, c,
		"SINTER", "s1", "s2", "s3",
		proto.Strings("mies"),
	)

	// A nonexisting key
	mustDo(t, c,
		"SINTER", "s9",
		proto.Strings(),
	)

	// With one of the keys being an empty set, the resulting set is also empty
	mustDo(t, c,
		"SINTER", "s1", "s9",
		proto.Strings(),
	)

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		mustDo(t, c,
			"SINTER",
			proto.Error(errWrongNumber("sinter")),
		)
		mustDo(t, c,
			"SINTER", "str",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"SINTER", "chk", "str",
			proto.Error(msgWrongType),
		)
	})
}

// Test SINTERSTORE
func TestSinterstore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.SetAdd("s1", "aap", "noot", "mies")
	s.SetAdd("s2", "noot", "mies", "vuur")
	s.SetAdd("s3", "aap", "mies", "wim")

	// Simple case
	{
		mustDo(t, c,
			"SINTERSTORE", "res", "s1", "s3",
			proto.Int(2),
		)
		s.CheckSet(t, "res", "aap", "mies")
	}

	// With one of the keys being an empty set, the resulting set is also empty
	{
		must0(t, c,
			"SINTERSTORE", "res", "s1", "s9",
		)
		s.CheckSet(t, "res", []string{}...)
	}

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		mustDo(t, c,
			"SINTERSTORE",
			proto.Error(errWrongNumber("sinterstore")),
		)
		mustDo(t, c,
			"SINTERSTORE", "t",
			proto.Error(errWrongNumber("sinterstore")),
		)
		mustDo(t, c,
			"SINTERSTORE", "t", "str",
			proto.Error(msgWrongType),
		)
	})
}

// Test SUNION
func TestSunion(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.SetAdd("s1", "aap", "noot", "mies")
	s.SetAdd("s2", "noot", "mies", "vuur")
	s.SetAdd("s3", "aap", "mies", "wim")

	// Simple case
	{
		res, err := c.DoStrings("SUNION", "s1", "s2")
		ok(t, err)
		sort.Strings(res)
		equals(t, []string{"aap", "mies", "noot", "vuur"}, res)
	}

	// No other set
	{
		res, err := c.DoStrings("SUNION", "s1")
		ok(t, err)
		sort.Strings(res)
		equals(t, []string{"aap", "mies", "noot"}, res)
	}

	// 3 sets
	{
		res, err := c.DoStrings("SUNION", "s1", "s2", "s3")
		ok(t, err)
		sort.Strings(res)
		equals(t, []string{"aap", "mies", "noot", "vuur", "wim"}, res)
	}

	// A nonexisting key
	{
		mustDo(t, c,
			"SUNION", "s9",
			proto.Strings(),
		)
	}

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		mustDo(t, c,
			"SUNION",
			proto.Error(errWrongNumber("sunion")),
		)
		mustDo(t, c,
			"SUNION", "str",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"SUNION", "chk", "str",
			proto.Error(msgWrongType),
		)
	})
}

// Test SUNIONSTORE
func TestSunionstore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.SetAdd("s1", "aap", "noot", "mies")
	s.SetAdd("s2", "noot", "mies", "vuur")
	s.SetAdd("s3", "aap", "mies", "wim")

	// Simple case
	{
		mustDo(t, c,
			"SUNIONSTORE", "res", "s1", "s3",
			proto.Int(4),
		)
		s.CheckSet(t, "res", "aap", "mies", "noot", "wim")
	}

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		mustDo(t, c,
			"SUNIONSTORE",
			proto.Error(errWrongNumber("sunionstore")),
		)
		mustDo(t, c,
			"SUNIONSTORE", "t",
			proto.Error(errWrongNumber("sunionstore")),
		)
		mustDo(t, c,
			"SUNIONSTORE", "t", "str",
			proto.Error(msgWrongType),
		)
	})
}

func TestSscan(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// We cheat with sscan. It always returns everything.

	s.SetAdd("set", "value1", "value2")
	// No problem
	mustDo(t, c,
		"SSCAN", "set", "0",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("value1"),
				proto.String("value2"),
			),
		),
	)

	// Invalid cursor
	mustDo(t, c,
		"SSCAN", "set", "42",
		proto.Array(
			proto.String("0"),
			proto.Strings(),
		),
	)

	// COUNT (ignored)
	mustDo(t, c,
		"SSCAN", "set", "0", "COUNT", "200",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("value1"),
				proto.String("value2"),
			),
		),
	)

	// MATCH
	s.SetAdd("set", "aap", "noot", "mies")
	mustDo(t, c,
		"SSCAN", "set", "0", "MATCH", "mi*",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("mies"),
			),
		),
	)

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"SSCAN",
			proto.Error(errWrongNumber("sscan")),
		)
		mustDo(t, c,
			"SSCAN", "set",
			proto.Error(errWrongNumber("sscan")),
		)
		mustDo(t, c,
			"SSCAN", "set", "noint",
			proto.Error(msgInvalidCursor),
		)
		mustDo(t, c,
			"SSCAN", "set", "0", "MATCH",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"SSCAN", "set", "0", "COUNT",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"SSCAN", "set", "0", "COUNT", "0",
			proto.Error(msgSyntaxError),
		)
		mustDo(t, c,
			"SSCAN", "set", "0", "COUNT", "noint",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"SSCAN", "set", "0", "COUNT", "-3",
			proto.Error(msgInvalidInt),
		)
		s.Set("str", "value")
		mustDo(t, c,
			"SSCAN", "str", "0",
			proto.Error(msgWrongType),
		)
	})

	s.SetAdd("largeset", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8")
	mustDo(t, c,
		"SSCAN", "largeset", "0", "COUNT", "3",
		proto.Array(
			proto.String("3"),
			proto.Array(
				proto.String("v1"),
				proto.String("v2"),
				proto.String("v3"),
			),
		),
	)
	mustDo(t, c,
		"SSCAN", "largeset", "3", "COUNT", "3",
		proto.Array(
			proto.String("6"),
			proto.Array(
				proto.String("v4"),
				proto.String("v5"),
				proto.String("v6"),
			),
		),
	)
	mustDo(t, c,
		"SSCAN", "largeset", "6", "COUNT", "3",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("v7"),
				proto.String("v8"),
			),
		),
	)
}

func TestDelElem(t *testing.T) {
	equals(t, []string{"b", "c", "d"}, delElem([]string{"a", "b", "c", "d"}, 0))
	equals(t, []string{"a", "c", "d"}, delElem([]string{"a", "b", "c", "d"}, 1))
	equals(t, []string{"a", "b", "d"}, delElem([]string{"a", "b", "c", "d"}, 2))
	equals(t, []string{"a", "b", "c"}, delElem([]string{"a", "b", "c", "d"}, 3))
}
