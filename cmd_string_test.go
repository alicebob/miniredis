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
