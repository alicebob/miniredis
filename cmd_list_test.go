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

		el, err := s.Lpop("l2")
		ok(t, err)
		equals(t, "b", el)
		el, err = s.Lpop("l2")
		ok(t, err)
		equals(t, "a", el)
		// Key is removed on pop-empty.
		equals(t, false, s.Exists("l2"))
	}

	// Wrong type of key
	{
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("LPUSH", "str", "noot", "mies"))
		assert(t, err != nil, "LPUSH error")
	}

}

func TestLpop(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	b, err := redis.Int(c.Do("LPUSH", "l", "aap", "noot", "mies"))
	ok(t, err)
	equals(t, 3, b) // New length.

	// Simple pops.
	{
		el, err := redis.String(c.Do("LPOP", "l"))
		ok(t, err)
		equals(t, "mies", el)

		el, err = redis.String(c.Do("LPOP", "l"))
		ok(t, err)
		equals(t, "noot", el)

		el, err = redis.String(c.Do("LPOP", "l"))
		ok(t, err)
		equals(t, "aap", el)

		// Last element has been popped. Key is gone.
		i, err := redis.Int(c.Do("EXISTS", "l"))
		ok(t, err)
		equals(t, 0, i)

		// Can pop non-existing keys just fine.
		v, err := c.Do("LPOP", "l")
		ok(t, err)
		equals(t, nil, v)
	}
}

func TestRPushPop(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	{
		b, err := redis.Int(c.Do("RPUSH", "l", "aap", "noot", "mies"))
		ok(t, err)
		equals(t, 3, b) // New length.

		r, err := redis.Strings(c.Do("LRANGE", "l", "0", "0"))
		ok(t, err)
		equals(t, []string{"aap"}, r)

		r, err = redis.Strings(c.Do("LRANGE", "l", "-1", "-1"))
		ok(t, err)
		equals(t, []string{"mies"}, r)
	}

	// Push more.
	{
		b, err := redis.Int(c.Do("RPUSH", "l", "aap2", "noot2", "mies2"))
		ok(t, err)
		equals(t, 6, b) // New length.

		r, err := redis.Strings(c.Do("LRANGE", "l", "0", "0"))
		ok(t, err)
		equals(t, []string{"aap"}, r)

		r, err = redis.Strings(c.Do("LRANGE", "l", "-1", "-1"))
		ok(t, err)
		equals(t, []string{"mies2"}, r)
	}

	// Direct usage
	{
		l, err := s.Push("l2", "a")
		ok(t, err)
		equals(t, 1, l)
		l, err = s.Push("l2", "b")
		ok(t, err)
		equals(t, 2, l)
		list, err := s.List("l2")
		ok(t, err)
		equals(t, []string{"a", "b"}, list)

		el, err := s.Pop("l2")
		ok(t, err)
		equals(t, "b", el)
		el, err = s.Pop("l2")
		ok(t, err)
		equals(t, "a", el)
		// Key is removed on pop-empty.
		equals(t, false, s.Exists("l2"))
	}

	// Wrong type of key
	{
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("RPUSH", "str", "noot", "mies"))
		assert(t, err != nil, "RPUSH error")
	}

}

func TestRpop(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	b, err := redis.Int(c.Do("RPUSH", "l", "aap", "noot", "mies"))
	ok(t, err)
	equals(t, 3, b) // New length.

	// Simple pops.
	{
		el, err := redis.String(c.Do("RPOP", "l"))
		ok(t, err)
		equals(t, "mies", el)

		el, err = redis.String(c.Do("RPOP", "l"))
		ok(t, err)
		equals(t, "noot", el)

		el, err = redis.String(c.Do("RPOP", "l"))
		ok(t, err)
		equals(t, "aap", el)

		// Last element has been popped. Key is gone.
		i, err := redis.Int(c.Do("EXISTS", "l"))
		ok(t, err)
		equals(t, 0, i)

		// Can pop non-existing keys just fine.
		v, err := c.Do("RPOP", "l")
		ok(t, err)
		equals(t, nil, v)
	}
}
