package miniredis

import (
	"testing"

	"github.com/garyburd/redigo/redis"
)

// Test simple GET/SET keys
func TestString(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// SET command
	{
		_, err = c.Do("SET", "foo", "bar")
		ok(t, err)
	}
	// GET command
	{
		v, err := redis.String(c.Do("GET", "foo"))
		ok(t, err)
		equals(t, "bar", v)
	}

	// Query server directly.
	equals(t, "bar", s.Get("foo"))

	// Use Set directly
	{
		s.Set("aap", "noot")
		equals(t, "noot", s.Get("aap"))
		v, err := redis.String(c.Do("GET", "aap"))
		ok(t, err)
		equals(t, "noot", v)
	}

	// GET a Non-existing key. Should be nil.
	{
		b, err := c.Do("GET", "reallynosuchkey")
		ok(t, err)
		equals(t, nil, b)
	}

	// Wrong types.
	{
		_, err := c.Do("HSET", "wim", "zus", "jet")
		ok(t, err)
		_, err = c.Do("SET", "wim", "zus")
		assert(t, err != nil, "no SET error")
		_, err = c.Do("GET", "wim")
		assert(t, err != nil, "no GET error")
	}
}

func TestMget(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.Set("zus", "jet")
	s.Set("teun", "vuur")
	s.Set("gijs", "lam")
	s.Set("kees", "bok")
	{
		v, err := redis.Values(c.Do("MGET", "zus", "nosuch", "kees"))
		ok(t, err)
		equals(t, 3, len(v))
		equals(t, "jet", string(v[0].([]byte)))
		equals(t, nil, v[1])
		equals(t, "bok", string(v[2].([]byte)))
	}

	// Wrong key type returns nil
	{
		s.HSet("aap", "foo", "bar")
		v, err := redis.Values(c.Do("MGET", "aap"))
		ok(t, err)
		equals(t, 1, len(v))
		equals(t, nil, v[0])
	}
}

func TestMset(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	{
		v, err := redis.String(c.Do("MSET", "zus", "jet", "teun", "vuur", "gijs", "lam"))
		ok(t, err)
		equals(t, "OK", v)
		equals(t, "jet", s.Get("zus"))
		equals(t, "vuur", s.Get("teun"))
		equals(t, "lam", s.Get("gijs"))
	}

	// Other types are overwritten
	{
		s.HSet("aap", "foo", "bar")
		v, err := redis.String(c.Do("MSET", "aap", "jet"))
		ok(t, err)
		equals(t, "OK", v)
		equals(t, "jet", s.Get("aap"))
	}

	// Odd argument list is not OK
	{
		_, err := redis.String(c.Do("MSET", "zus", "jet", "teun"))
		assert(t, err != nil, "No MSET error")
	}

	// TTL is cleared
	{
		s.Set("foo", "bar")
		s.HSet("aap", "foo", "bar") // even for weird keys.
		s.SetExpire("aap", 999)
		s.SetExpire("foo", 999)
		v, err := redis.String(c.Do("MSET", "aap", "noot", "foo", "baz"))
		ok(t, err)
		equals(t, "OK", v)
		equals(t, 0, s.Expire("aap"))
		equals(t, 0, s.Expire("foo"))
	}
}
