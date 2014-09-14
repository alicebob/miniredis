package miniredis

import (
	"testing"

	"github.com/garyburd/redigo/redis"
)

func TestLpush(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	{
		b, err := redis.Int(c.Do("LPUSH", "l", "aap", "noot", "mies"))
		ok(t, err)
		equals(t, 3, b) // New length.

		r, err := redis.Strings(c.Do("LRANGE", "l", "0", "0"))
		ok(t, err)
		equals(t, []string{"mies"}, r)

		r, err = redis.Strings(c.Do("LRANGE", "l", "-1", "-1"))
		ok(t, err)
		equals(t, []string{"aap"}, r)
	}

	// Push more.
	{
		b, err := redis.Int(c.Do("LPUSH", "l", "aap2", "noot2", "mies2"))
		ok(t, err)
		equals(t, 6, b) // New length.

		r, err := redis.Strings(c.Do("LRANGE", "l", "0", "0"))
		ok(t, err)
		equals(t, []string{"mies2"}, r)

		r, err = redis.Strings(c.Do("LRANGE", "l", "-1", "-1"))
		ok(t, err)
		equals(t, []string{"aap"}, r)
	}

	// Direct usage
	{
		l, err := s.Lpush("l2", "a")
		ok(t, err)
		equals(t, 1, l)
		l, err = s.Lpush("l2", "b")
		ok(t, err)
		equals(t, 2, l)
		list, err := s.List("l2")
		ok(t, err)
		equals(t, []string{"b", "a"}, list)
	}

	// Wrong type of key
	{
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("LPUSH", "str", "noot", "mies"))
		assert(t, err != nil, "LPUSH error")
	}

}
