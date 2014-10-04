package miniredis

import (
	"testing"

	"github.com/garyburd/redigo/redis"
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

	// Direct usage
	{
		added, err := s.SetAdd("s1", "aap")
		ok(t, err)
		equals(t, 1, added)

		members, err := s.Members("s1")
		ok(t, err)
		equals(t, []string{"aap"}, members)
	}

	// Wrong type of key
	{
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("SADD", "str", "hi"))
		assert(t, err != nil, "SADD error")
		_, err = redis.Int(c.Do("SMEMBERS", "str"))
		assert(t, err != nil, "MEMBERS error")
		// Wrong argument counts
		_, err = redis.String(c.Do("SADD"))
		assert(t, err != nil, "SADD error")
		_, err = redis.String(c.Do("SADD", "set"))
		assert(t, err != nil, "SADD error")
		_, err = redis.String(c.Do("SMEMBERS"))
		assert(t, err != nil, "SMEMBERS error")
		_, err = redis.String(c.Do("SMEMBERS", "set", "spurious"))
		assert(t, err != nil, "SMEMBERS error")
	}

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

	// Direct usage
	{
		isMember, err := s.IsMember("s", "noot")
		ok(t, err)
		equals(t, true, isMember)
	}

	// Wrong type of key
	{
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("SISMEMBER", "str"))
		assert(t, err != nil, "SISMEMBER error")
		// Wrong argument counts
		_, err = redis.String(c.Do("SISMEMBER"))
		assert(t, err != nil, "SISMEMBER error")
		_, err = redis.String(c.Do("SISMEMBER", "set"))
		assert(t, err != nil, "SISMEMBER error")
		_, err = redis.String(c.Do("SISMEMBER", "set", "spurious", "args"))
		assert(t, err != nil, "SISMEMBER error")
	}

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

	// Direct usage
	{
		b, err := s.SRem("s", "mies")
		ok(t, err)
		equals(t, 1, b)

		members, err := s.Members("s")
		ok(t, err)
		equals(t, []string{"vuur"}, members)
	}

	// Wrong type of key
	{
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("SREM", "str", "value"))
		assert(t, err != nil, "SREM error")
		// Wrong argument counts
		_, err = redis.String(c.Do("SREM"))
		assert(t, err != nil, "SREM error")
		_, err = redis.String(c.Do("SREM", "set"))
		assert(t, err != nil, "SREM error")
		_, err = redis.String(c.Do("SREM", "set", "spurious", "args"))
		assert(t, err != nil, "SREM error")
	}
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

	// Wrong type of key
	{
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("SMOVE", "str", "dst", "value"))
		assert(t, err != nil, "SMOVE error")
		_, err = redis.Int(c.Do("SMOVE", "s2", "str", "value"))
		assert(t, err != nil, "SMOVE error")
		// Wrong argument counts
		_, err = redis.String(c.Do("SMOVE"))
		assert(t, err != nil, "SMOVE error")
		_, err = redis.String(c.Do("SMOVE", "set"))
		assert(t, err != nil, "SMOVE error")
		_, err = redis.String(c.Do("SMOVE", "set", "set2"))
		assert(t, err != nil, "SMOVE error")
		_, err = redis.String(c.Do("SMOVE", "set", "set2", "spurious", "args"))
		assert(t, err != nil, "SMOVE error")
	}
}

// Test SPOP
func TestSpop(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.SetAdd("s", "aap", "noot")

	{
		el, err := redis.String(c.Do("SPOP", "s"))
		ok(t, err)
		assert(t, el == "aap" || el == "noot", "spop got something")

		el, err = redis.String(c.Do("SPOP", "s"))
		ok(t, err)
		assert(t, el == "aap" || el == "noot", "spop got something")

		assert(t, !s.Exists("s"), "all spopped away")
	}

	// a nonexisting key
	{
		b, err := c.Do("SPOP", "nosuch")
		ok(t, err)
		equals(t, nil, b)
	}

	// Various errors
	{
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		_, err = redis.String(c.Do("SMOVE"))
		assert(t, err != nil, "SMOVE error")
		_, err = redis.String(c.Do("SMOVE", "chk", "set2"))
		assert(t, err != nil, "SMOVE error")

		_, err = c.Do("SPOP", "str")
		assert(t, err != nil, "SPOP error")
	}
}

// Test SRANDMEMBER
func TestSrandmember(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.SetAdd("s", "aap", "noot", "mies")

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
	}

	// Negative count
	{
		els, err := redis.Strings(c.Do("SRANDMEMBER", "s", -2))
		ok(t, err)
		equals(t, 2, len(els))
	}

	// a nonexisting key
	{
		b, err := c.Do("SRANDMEMBER", "nosuch")
		ok(t, err)
		equals(t, nil, b)
	}

	// Various errors
	{
		s.SetAdd("chk", "aap", "noot")
		s.Set("str", "value")

		_, err = redis.String(c.Do("SRANDMEMBER"))
		assert(t, err != nil, "SRANDMEMBER error")
		_, err = redis.String(c.Do("SRANDMEMBER", "chk", "noint"))
		assert(t, err != nil, "SRANDMEMBER error")
		_, err = redis.String(c.Do("SRANDMEMBER", "chk", 1, "toomanu"))
		assert(t, err != nil, "SRANDMEMBER error")

		_, err = c.Do("SRANDMEMBER", "str")
		assert(t, err != nil, "SRANDMEMBER error")
	}
}
