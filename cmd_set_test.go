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
		_, err := redis.Int(c.Do("SET", "str", "value"))
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
