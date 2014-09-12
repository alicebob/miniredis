package miniredis

import (
	"testing"

	"github.com/garyburd/redigo/redis"
)

// Test EXPIRE. Keys with an expiration are called volatile in Redis parlance.
func TestExpire(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Not volatile yet
	{
		equals(t, 0, s.Expire("foo"))
		b, err := redis.Int(c.Do("TTL", "foo"))
		ok(t, err)
		equals(t, -2, b)
	}

	// Set something
	{
		_, err := c.Do("SET", "foo", "bar")
		ok(t, err)
		// Key exists, but no Expire set yet.
		b, err := redis.Int(c.Do("TTL", "foo"))
		ok(t, err)
		equals(t, -1, b)

		n, err := redis.Int(c.Do("EXPIRE", "foo", "1200"))
		ok(t, err)
		equals(t, 1, n) // EXPIRE returns 1 on success.

		equals(t, 1200, s.Expire("foo"))
		b, err = redis.Int(c.Do("TTL", "foo"))
		ok(t, err)
		equals(t, 1200, b)
	}

	// A SET resets the expire.
	{
		_, err := c.Do("SET", "foo", "bar")
		ok(t, err)
		b, err := redis.Int(c.Do("TTL", "foo"))
		ok(t, err)
		equals(t, -1, b)
	}

	// Set a non-existing key
	{
		n, err := redis.Int(c.Do("EXPIRE", "nokey", "1200"))
		ok(t, err)
		equals(t, 0, n) // EXPIRE returns 0 on failure.
	}

	// Remove an expire
	{

		// No key yet
		n, err := redis.Int(c.Do("PERSIST", "exkey"))
		ok(t, err)
		equals(t, 0, n)

		_, err = c.Do("SET", "exkey", "bar")
		ok(t, err)

		// No timeout yet
		n, err = redis.Int(c.Do("PERSIST", "exkey"))
		ok(t, err)
		equals(t, 0, n)

		_, err = redis.Int(c.Do("EXPIRE", "exkey", "1200"))
		ok(t, err)

		// All fine now
		n, err = redis.Int(c.Do("PERSIST", "exkey"))
		ok(t, err)
		equals(t, 1, n)

		// No TTL left
		b, err := redis.Int(c.Do("TTL", "exkey"))
		ok(t, err)
		equals(t, -1, b)
	}

	// Hash key works fine, too
	{
		_, err := c.Do("HSET", "wim", "zus", "jet")
		ok(t, err)
		b, err := redis.Int(c.Do("EXPIRE", "wim", "1234"))
		ok(t, err)
		equals(t, 1, b)
	}
}

func TestExpireat(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Not volatile yet
	{
		equals(t, 0, s.Expire("foo"))
		b, err := redis.Int(c.Do("TTL", "foo"))
		ok(t, err)
		equals(t, -2, b)
	}

	// Set something
	{
		_, err := c.Do("SET", "foo", "bar")
		ok(t, err)
		// Key exists, but no Expire set yet.
		b, err := redis.Int(c.Do("TTL", "foo"))
		ok(t, err)
		equals(t, -1, b)

		n, err := redis.Int(c.Do("EXPIREAT", "foo", 1234567890))
		ok(t, err)
		equals(t, 1, n) // EXPIREAT returns 1 on success.

		equals(t, 1234567890, s.Expire("foo"))
		b, err = redis.Int(c.Do("TTL", "foo"))
		ok(t, err)
		equals(t, 1234567890, b)
		equals(t, 1234567890, s.Expire("foo"))
	}
}

func TestPexpire(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Key exists
	{
		s.Set("foo", "bar")
		b, err := redis.Int(c.Do("PEXPIRE", "foo", 12))
		ok(t, err)
		equals(t, 1, b)

		e, err := redis.Int(c.Do("PTTL", "foo"))
		ok(t, err)
		equals(t, 12, e)
	}
	// Key doesn't exist
	{
		b, err := redis.Int(c.Do("PEXPIRE", "nosuch", 12))
		ok(t, err)
		equals(t, 0, b)

		e, err := redis.Int(c.Do("PTTL", "nosuch"))
		ok(t, err)
		equals(t, -2, e)
	}

	// No expire
	{
		s.Set("aap", "noot")
		e, err := redis.Int(c.Do("PTTL", "aap"))
		ok(t, err)
		equals(t, -1, e)
	}
}

func TestDel(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.Set("foo", "bar")
	s.HSet("aap", "noot", "mies")
	s.Set("one", "two")
	s.SetExpire("one", 1234)
	s.Set("three", "four")
	r, err := redis.Int(c.Do("DEL", "one", "aap", "nosuch"))
	ok(t, err)
	equals(t, 2, r)
	equals(t, 0, s.Expire("one"))

	// Direct also works:
	s.Set("foo", "bar")
	s.Del("foo")
	equals(t, "", s.Get("foo"))
}

func TestType(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// String key
	{
		s.Set("foo", "bar!")
		v, err := redis.String(c.Do("TYPE", "foo"))
		ok(t, err)
		equals(t, "string", v)
	}

	// Hash key
	{
		s.HSet("aap", "noot", "mies")
		v, err := redis.String(c.Do("TYPE", "aap"))
		ok(t, err)
		equals(t, "hash", v)
	}

	// New key
	{
		v, err := redis.String(c.Do("TYPE", "nosuch"))
		ok(t, err)
		equals(t, "none", v)
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("TYPE"))
		assert(t, err != nil, "do TYPE error")
		_, err = redis.Int(c.Do("TYPE", "spurious", "arguments"))
		assert(t, err != nil, "do TYPE error")
	}

	// Direct usage:
	{
		equals(t, "hash", s.Type("aap"))
		equals(t, "", s.Type("nokey"))
	}
}

func TestExists(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// String key
	{
		s.Set("foo", "bar!")
		v, err := redis.Int(c.Do("EXISTS", "foo"))
		ok(t, err)
		equals(t, 1, v)
	}

	// Hash key
	{
		s.HSet("aap", "noot", "mies")
		v, err := redis.Int(c.Do("EXISTS", "aap"))
		ok(t, err)
		equals(t, 1, v)
	}

	// New key
	{
		v, err := redis.Int(c.Do("EXISTS", "nosuch"))
		ok(t, err)
		equals(t, 0, v)
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("EXISTS"))
		assert(t, err != nil, "do EXISTS error")
		_, err = redis.Int(c.Do("EXISTS", "spurious", "arguments"))
		assert(t, err != nil, "do EXISTS error")
	}

	// Direct usage:
	{
		equals(t, true, s.Exists("aap"))
		equals(t, false, s.Exists("nokey"))
	}
}

func TestMove(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// No problem.
	{
		s.Set("foo", "bar!")
		v, err := redis.Int(c.Do("MOVE", "foo", 1))
		ok(t, err)
		equals(t, 1, v)
	}

	// Src key doesn't exists.
	{
		v, err := redis.Int(c.Do("MOVE", "nosuch", 1))
		ok(t, err)
		equals(t, 0, v)
	}

	// Target key already exists.
	{
		s.DB(0).Set("two", "orig")
		s.DB(1).Set("two", "taken")
		v, err := redis.Int(c.Do("MOVE", "two", 1))
		ok(t, err)
		equals(t, 0, v)
		equals(t, "orig", s.Get("two"))
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("MOVE"))
		assert(t, err != nil, "do MOVE error")
		_, err = redis.Int(c.Do("MOVE", "foo"))
		assert(t, err != nil, "do MOVE error")
		_, err = redis.Int(c.Do("MOVE", "foo", "noint"))
		assert(t, err != nil, "do TYPE error")
		_, err = redis.Int(c.Do("MOVE", "foo", 2, "toomany"))
		assert(t, err != nil, "do TYPE error")
	}
}
