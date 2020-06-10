package miniredis

import (
	"sort"
	"testing"

	"github.com/gomodule/redigo/redis"
)

// Test SADD / SMEMBERS.
func TestSadd(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	{
		b, err := redis.Int(c.Do("SADD", "s", "aap", "noot", "mies"))
		ok(t, err)
		equals(t, 3, b) // New elements.

		members, err := s.Members("s")
		ok(t, err)
		equals(t, []string{"aap", "mies", "noot"}, members)

		m, err := redis.Strings(c.Do("SMEMBERS", "s"))
		ok(t, err)
		equals(t, []string{"aap", "mies", "noot"}, m)
	}

	{
		b, err := redis.String(c.Do("TYPE", "s"))
		ok(t, err)
		equals(t, "set", b)
	}

	// SMEMBERS on an nonexisting key
	{
		m, err := redis.Strings(c.Do("SMEMBERS", "nosuch"))
		ok(t, err)
		equals(t, []string{}, m)
	}

	{
		b, err := redis.Int(c.Do("SADD", "s", "new", "noot", "mies"))
		ok(t, err)
		equals(t, 1, b) // Only one new field.

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
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("SADD", "str", "hi"))
		mustFail(t, err, msgWrongType)
		_, err = redis.Int(c.Do("SMEMBERS", "str"))
		mustFail(t, err, msgWrongType)
		// Wrong argument counts
		_, err = redis.String(c.Do("SADD"))
		mustFail(t, err, "ERR wrong number of arguments for 'sadd' command")
		_, err = redis.String(c.Do("SADD", "set"))
		mustFail(t, err, "ERR wrong number of arguments for 'sadd' command")
		_, err = redis.String(c.Do("SMEMBERS"))
		mustFail(t, err, "ERR wrong number of arguments for 'smembers' command")
		_, err = redis.String(c.Do("SMEMBERS", "set", "spurious"))
		mustFail(t, err, "ERR wrong number of arguments for 'smembers' command")
	})
}

// Test SISMEMBER
func TestSismember(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.SetAdd("s", "aap", "noot", "mies")

	{
		b, err := redis.Int(c.Do("SISMEMBER", "s", "aap"))
		ok(t, err)
		equals(t, 1, b)

		b, err = redis.Int(c.Do("SISMEMBER", "s", "nosuch"))
		ok(t, err)
		equals(t, 0, b)
	}

	// a nonexisting key
	{
		b, err := redis.Int(c.Do("SISMEMBER", "nosuch", "nosuch"))
		ok(t, err)
		equals(t, 0, b)
	}

	t.Run("direct usage", func(t *testing.T) {
		isMember, err := s.IsMember("s", "noot")
		ok(t, err)
		equals(t, true, isMember)
	})

	t.Run("errors", func(t *testing.T) {
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("SISMEMBER", "str"))
		mustFail(t, err, "ERR wrong number of arguments for 'sismember' command")
		// Wrong argument counts
		_, err = redis.String(c.Do("SISMEMBER"))
		mustFail(t, err, "ERR wrong number of arguments for 'sismember' command")
		_, err = redis.String(c.Do("SISMEMBER", "set"))
		mustFail(t, err, "ERR wrong number of arguments for 'sismember' command")
		_, err = redis.String(c.Do("SISMEMBER", "set", "spurious", "args"))
		mustFail(t, err, "ERR wrong number of arguments for 'sismember' command")
	})
}

// Test SREM
func TestSrem(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.SetAdd("s", "aap", "noot", "mies", "vuur")

	{
		b, err := redis.Int(c.Do("SREM", "s", "aap", "noot"))
		ok(t, err)
		equals(t, 2, b)

		members, err := s.Members("s")
		ok(t, err)
		equals(t, []string{"mies", "vuur"}, members)
	}

	// a nonexisting field
	{
		b, err := redis.Int(c.Do("SREM", "s", "nosuch"))
		ok(t, err)
		equals(t, 0, b)
	}

	// a nonexisting key
	{
		b, err := redis.Int(c.Do("SREM", "nosuch", "nosuch"))
		ok(t, err)
		equals(t, 0, b)
	}

	t.Run("direct usage", func(t *testing.T) {
		b, err := s.SRem("s", "mies")
		ok(t, err)
		equals(t, 1, b)

		members, err := s.Members("s")
		ok(t, err)
		equals(t, []string{"vuur"}, members)
	})

	t.Run("errors", func(t *testing.T) {
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("SREM", "str", "value"))
		mustFail(t, err, msgWrongType)
		// Wrong argument counts
		_, err = redis.String(c.Do("SREM"))
		mustFail(t, err, "ERR wrong number of arguments for 'srem' command")
		_, err = redis.String(c.Do("SREM", "set"))
		mustFail(t, err, "ERR wrong number of arguments for 'srem' command")
		_, err = redis.String(c.Do("SREM", "set", "spurious", "args"))
		assert(t, err != nil, "SREM error")
	})
}

// Test SMOVE
func TestSmove(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.SetAdd("s", "aap", "noot")

	{
		b, err := redis.Int(c.Do("SMOVE", "s", "s2", "aap"))
		ok(t, err)
		equals(t, 1, b)

		m, err := s.IsMember("s", "aap")
		ok(t, err)
		equals(t, false, m)
		m, err = s.IsMember("s2", "aap")
		ok(t, err)
		equals(t, true, m)
	}

	// Move away the last member
	{
		b, err := redis.Int(c.Do("SMOVE", "s", "s2", "noot"))
		ok(t, err)
		equals(t, 1, b)

		equals(t, false, s.Exists("s"))

		m, err := s.IsMember("s2", "noot")
		ok(t, err)
		equals(t, true, m)
	}

	// a nonexisting member
	{
		b, err := redis.Int(c.Do("SMOVE", "s", "s2", "nosuch"))
		ok(t, err)
		equals(t, 0, b)
	}

	// a nonexisting key
	{
		b, err := redis.Int(c.Do("SMOVE", "nosuch", "nosuch2", "nosuch"))
		ok(t, err)
		equals(t, 0, b)
	}

	t.Run("errors", func(t *testing.T) {
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("SMOVE", "str", "dst", "value"))
		mustFail(t, err, msgWrongType)
		_, err = redis.Int(c.Do("SMOVE", "s2", "str", "value"))
		mustFail(t, err, msgWrongType)
		// Wrong argument counts
		_, err = redis.String(c.Do("SMOVE"))
		mustFail(t, err, "ERR wrong number of arguments for 'smove' command")
		_, err = redis.String(c.Do("SMOVE", "set"))
		mustFail(t, err, "ERR wrong number of arguments for 'smove' command")
		_, err = redis.String(c.Do("SMOVE", "set", "set2"))
		mustFail(t, err, "ERR wrong number of arguments for 'smove' command")
		_, err = redis.String(c.Do("SMOVE", "set", "set2", "spurious", "args"))
		mustFail(t, err, "ERR wrong number of arguments for 'smove' command")
	})
}

// Test SPOP
func TestSpop(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	t.Run("basics", func(t *testing.T) {
		s.SetAdd("s", "aap", "noot")
		el, err := redis.String(c.Do("SPOP", "s"))
		ok(t, err)
		assert(t, el == "aap" || el == "noot", "spop got something")

		el, err = redis.String(c.Do("SPOP", "s"))
		ok(t, err)
		assert(t, el == "aap" || el == "noot", "spop got something")

		assert(t, !s.Exists("s"), "all spopped away")
	})

	t.Run("nonexisting key", func(t *testing.T) {
		b, err := c.Do("SPOP", "nosuch")
		ok(t, err)
		equals(t, nil, b)
	})

	t.Run("various errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		_, err = redis.String(c.Do("SMOVE"))
		mustFail(t, err, "ERR wrong number of arguments for 'smove' command")
		_, err = redis.String(c.Do("SMOVE", "chk", "set2"))
		mustFail(t, err, "ERR wrong number of arguments for 'smove' command")

		_, err = c.Do("SPOP", "str")
		mustFail(t, err, msgWrongType)
	})

	t.Run("count argument", func(t *testing.T) {
		s.SetAdd("s", "aap", "noot", "mies", "vuur")
		el, err := redis.Strings(c.Do("SPOP", "s", 2))
		ok(t, err)
		assert(t, len(el) == 2, "SPOP s 2")
		members, err := s.Members("s")
		ok(t, err)
		assert(t, len(members) == 2, "SPOP s 2")

		_, err = c.Do("SPOP", "str", -12)
		mustFail(t, err, msgOutOfRange)
	})
}

// Test SRANDMEMBER
func TestSrandmember(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.SetAdd("s", "aap", "noot", "mies")

	s.Seed(42)
	// No count
	{
		el, err := redis.String(c.Do("SRANDMEMBER", "s"))
		ok(t, err)
		assert(t, el == "aap" || el == "noot" || el == "mies", "srandmember got something")
	}

	// Positive count
	{
		els, err := redis.Strings(c.Do("SRANDMEMBER", "s", 2))
		ok(t, err)
		equals(t, 2, len(els))
		equals(t, "noot", els[0])
		equals(t, "mies", els[1])
	}

	// Negative count
	{
		els, err := redis.Strings(c.Do("SRANDMEMBER", "s", -2))
		ok(t, err)
		equals(t, 2, len(els))
		equals(t, "aap", els[0])
		equals(t, "mies", els[1])
	}

	// a nonexisting key
	{
		b, err := c.Do("SRANDMEMBER", "nosuch")
		ok(t, err)
		equals(t, nil, b)
	}

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		_, err = redis.String(c.Do("SRANDMEMBER"))
		mustFail(t, err, "ERR wrong number of arguments for 'srandmember' command")
		_, err = redis.String(c.Do("SRANDMEMBER", "chk", "noint"))
		mustFail(t, err, "ERR value is not an integer or out of range")
		_, err = redis.String(c.Do("SRANDMEMBER", "chk", 1, "toomanu"))
		mustFail(t, err, "ERR syntax error")

		_, err = c.Do("SRANDMEMBER", "str")
		mustFail(t, err, msgWrongType)
	})
}

// Test SDIFF
func TestSdiff(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.SetAdd("s1", "aap", "noot", "mies")
	s.SetAdd("s2", "noot", "mies", "vuur")
	s.SetAdd("s3", "aap", "mies", "wim")

	// Simple case
	{
		els, err := redis.Strings(c.Do("SDIFF", "s1", "s2"))
		ok(t, err)
		equals(t, []string{"aap"}, els)
	}

	// No other set
	{
		els, err := redis.Strings(c.Do("SDIFF", "s1"))
		ok(t, err)
		sort.Strings(els)
		equals(t, []string{"aap", "mies", "noot"}, els)
	}

	// 3 sets
	{
		els, err := redis.Strings(c.Do("SDIFF", "s1", "s2", "s3"))
		ok(t, err)
		equals(t, []string{}, els)
	}

	// A nonexisting key
	{
		els, err := redis.Strings(c.Do("SDIFF", "s9"))
		ok(t, err)
		equals(t, []string{}, els)
	}

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		_, err = redis.String(c.Do("SDIFF"))
		mustFail(t, err, "ERR wrong number of arguments for 'sdiff' command")
		_, err = redis.String(c.Do("SDIFF", "str"))
		mustFail(t, err, msgWrongType)
		_, err = redis.String(c.Do("SDIFF", "chk", "str"))
		mustFail(t, err, msgWrongType)
	})
}

// Test SDIFFSTORE
func TestSdiffstore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.SetAdd("s1", "aap", "noot", "mies")
	s.SetAdd("s2", "noot", "mies", "vuur")
	s.SetAdd("s3", "aap", "mies", "wim")

	// Simple case
	{
		i, err := redis.Int(c.Do("SDIFFSTORE", "res", "s1", "s3"))
		ok(t, err)
		equals(t, 1, i)
		s.CheckSet(t, "res", "noot")
	}

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		_, err = redis.String(c.Do("SDIFFSTORE"))
		mustFail(t, err, "ERR wrong number of arguments for 'sdiffstore' command")
		_, err = redis.String(c.Do("SDIFFSTORE", "t"))
		mustFail(t, err, "ERR wrong number of arguments for 'sdiffstore' command")
		_, err = redis.String(c.Do("SDIFFSTORE", "t", "str"))
		mustFail(t, err, msgWrongType)
	})
}

// Test SINTER
func TestSinter(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.SetAdd("s1", "aap", "noot", "mies")
	s.SetAdd("s2", "noot", "mies", "vuur")
	s.SetAdd("s3", "aap", "mies", "wim")

	// Simple case
	{
		els, err := redis.Strings(c.Do("SINTER", "s1", "s2"))
		ok(t, err)
		sort.Strings(els)
		equals(t, []string{"mies", "noot"}, els)
	}

	// No other set
	{
		els, err := redis.Strings(c.Do("SINTER", "s1"))
		ok(t, err)
		sort.Strings(els)
		equals(t, []string{"aap", "mies", "noot"}, els)
	}

	// 3 sets
	{
		els, err := redis.Strings(c.Do("SINTER", "s1", "s2", "s3"))
		ok(t, err)
		equals(t, []string{"mies"}, els)
	}

	// A nonexisting key
	{
		els, err := redis.Strings(c.Do("SINTER", "s9"))
		ok(t, err)
		equals(t, []string{}, els)
	}

	// With one of the keys being an empty set, the resulting set is also empty
	{
		els, err := redis.Strings(c.Do("SINTER", "s1", "s9"))
		ok(t, err)
		equals(t, []string{}, els)
	}

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		_, err = redis.String(c.Do("SINTER"))
		mustFail(t, err, "ERR wrong number of arguments for 'sinter' command")
		_, err = redis.String(c.Do("SINTER", "str"))
		mustFail(t, err, msgWrongType)
		_, err = redis.String(c.Do("SINTER", "chk", "str"))
		mustFail(t, err, msgWrongType)
	})
}

// Test SINTERSTORE
func TestSinterstore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.SetAdd("s1", "aap", "noot", "mies")
	s.SetAdd("s2", "noot", "mies", "vuur")
	s.SetAdd("s3", "aap", "mies", "wim")

	// Simple case
	{
		i, err := redis.Int(c.Do("SINTERSTORE", "res", "s1", "s3"))
		ok(t, err)
		equals(t, 2, i)
		s.CheckSet(t, "res", "aap", "mies")
	}

	// With one of the keys being an empty set, the resulting set is also empty
	{
		i, err := redis.Int(c.Do("SINTERSTORE", "res", "s1", "s9"))
		ok(t, err)
		equals(t, 0, i)
		s.CheckSet(t, "res", []string{}...)
	}

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		_, err = redis.String(c.Do("SINTERSTORE"))
		mustFail(t, err, "ERR wrong number of arguments for 'sinterstore' command")
		_, err = redis.String(c.Do("SINTERSTORE", "t"))
		mustFail(t, err, "ERR wrong number of arguments for 'sinterstore' command")
		_, err = redis.String(c.Do("SINTERSTORE", "t", "str"))
		mustFail(t, err, msgWrongType)
	})
}

// Test SUNION
func TestSunion(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.SetAdd("s1", "aap", "noot", "mies")
	s.SetAdd("s2", "noot", "mies", "vuur")
	s.SetAdd("s3", "aap", "mies", "wim")

	// Simple case
	{
		els, err := redis.Strings(c.Do("SUNION", "s1", "s2"))
		ok(t, err)
		sort.Strings(els)
		equals(t, []string{"aap", "mies", "noot", "vuur"}, els)
	}

	// No other set
	{
		els, err := redis.Strings(c.Do("SUNION", "s1"))
		ok(t, err)
		sort.Strings(els)
		equals(t, []string{"aap", "mies", "noot"}, els)
	}

	// 3 sets
	{
		els, err := redis.Strings(c.Do("SUNION", "s1", "s2", "s3"))
		ok(t, err)
		sort.Strings(els)
		equals(t, []string{"aap", "mies", "noot", "vuur", "wim"}, els)
	}

	// A nonexisting key
	{
		els, err := redis.Strings(c.Do("SUNION", "s9"))
		ok(t, err)
		equals(t, []string{}, els)
	}

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		_, err = redis.String(c.Do("SUNION"))
		mustFail(t, err, "ERR wrong number of arguments for 'sunion' command")
		_, err = redis.String(c.Do("SUNION", "str"))
		mustFail(t, err, msgWrongType)
		_, err = redis.String(c.Do("SUNION", "chk", "str"))
		mustFail(t, err, msgWrongType)
	})
}

// Test SUNIONSTORE
func TestSunionstore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.SetAdd("s1", "aap", "noot", "mies")
	s.SetAdd("s2", "noot", "mies", "vuur")
	s.SetAdd("s3", "aap", "mies", "wim")

	// Simple case
	{
		i, err := redis.Int(c.Do("SUNIONSTORE", "res", "s1", "s3"))
		ok(t, err)
		equals(t, 4, i)
		s.CheckSet(t, "res", "aap", "mies", "noot", "wim")
	}

	t.Run("errors", func(t *testing.T) {
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		_, err = redis.String(c.Do("SUNIONSTORE"))
		mustFail(t, err, "ERR wrong number of arguments for 'sunionstore' command")
		_, err = redis.String(c.Do("SUNIONSTORE", "t"))
		mustFail(t, err, "ERR wrong number of arguments for 'sunionstore' command")
		_, err = redis.String(c.Do("SUNIONSTORE", "t", "str"))
		mustFail(t, err, msgWrongType)
	})
}

func TestSscan(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// We cheat with sscan. It always returns everything.

	s.SetAdd("set", "value1", "value2")

	// No problem
	{
		res, err := redis.Values(c.Do("SSCAN", "set", 0))
		ok(t, err)
		equals(t, 2, len(res))

		var c int
		var keys []string
		_, err = redis.Scan(res, &c, &keys)
		ok(t, err)
		equals(t, 0, c)
		equals(t, []string{"value1", "value2"}, keys)
	}

	// Invalid cursor
	{
		res, err := redis.Values(c.Do("SSCAN", "set", 42))
		ok(t, err)
		equals(t, 2, len(res))

		var c int
		var keys []string
		_, err = redis.Scan(res, &c, &keys)
		ok(t, err)
		equals(t, 0, c)
		equals(t, []string(nil), keys)
	}

	// COUNT (ignored)
	{
		res, err := redis.Values(c.Do("SSCAN", "set", 0, "COUNT", 200))
		ok(t, err)
		equals(t, 2, len(res))

		var c int
		var keys []string
		_, err = redis.Scan(res, &c, &keys)
		ok(t, err)
		equals(t, 0, c)
		equals(t, []string{"value1", "value2"}, keys)
	}

	// MATCH
	{
		s.SetAdd("set", "aap", "noot", "mies")
		res, err := redis.Values(c.Do("SSCAN", "set", 0, "MATCH", "mi*"))
		ok(t, err)
		equals(t, 2, len(res))

		var c int
		var keys []string
		_, err = redis.Scan(res, &c, &keys)
		ok(t, err)
		equals(t, 0, c)
		equals(t, []string{"mies"}, keys)
	}

	t.Run("errors", func(t *testing.T) {
		_, err := redis.Int(c.Do("SSCAN"))
		mustFail(t, err, "ERR wrong number of arguments for 'sscan' command")
		_, err = redis.Int(c.Do("SSCAN", "set"))
		mustFail(t, err, "ERR wrong number of arguments for 'sscan' command")
		_, err = redis.Int(c.Do("SSCAN", "set", "noint"))
		mustFail(t, err, msgInvalidCursor)
		_, err = redis.Int(c.Do("SSCAN", "set", 1, "MATCH"))
		mustFail(t, err, msgSyntaxError)
		_, err = redis.Int(c.Do("SSCAN", "set", 1, "COUNT"))
		mustFail(t, err, msgSyntaxError)
		_, err = redis.Int(c.Do("SSCAN", "set", 1, "COUNT", "noint"))
		mustFail(t, err, msgInvalidInt)
		s.Set("str", "value")
		_, err = redis.Int(c.Do("SSCAN", "str", 1))
		assert(t, err != nil, "do SSCAN error")
	})
}
