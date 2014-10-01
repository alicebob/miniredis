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

	s.Push("l", "aap", "noot", "mies")

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

func TestLindex(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.Push("l", "aap", "noot", "mies", "vuur")

	{
		el, err := redis.String(c.Do("LINDEX", "l", "0"))
		ok(t, err)
		equals(t, "aap", el)
	}
	{
		el, err := redis.String(c.Do("LINDEX", "l", "1"))
		ok(t, err)
		equals(t, "noot", el)
	}
	{
		el, err := redis.String(c.Do("LINDEX", "l", "3"))
		ok(t, err)
		equals(t, "vuur", el)
	}
	// Too many
	{
		el, err := c.Do("LINDEX", "l", "3000")
		ok(t, err)
		equals(t, nil, el)
	}
	{
		el, err := redis.String(c.Do("LINDEX", "l", "-1"))
		ok(t, err)
		equals(t, "vuur", el)
	}
	{
		el, err := redis.String(c.Do("LINDEX", "l", "-2"))
		ok(t, err)
		equals(t, "mies", el)
	}
	// Too big
	{
		el, err := c.Do("LINDEX", "l", "-400")
		ok(t, err)
		equals(t, nil, el)
	}
	// Non exising key
	{
		el, err := c.Do("LINDEX", "nonexisting", "400")
		ok(t, err)
		equals(t, nil, el)
	}

	// Wrong type of key
	{
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("LINDEX", "str", "1"))
		assert(t, err != nil, "LINDEX error")
		// Not an integer
		_, err = redis.String(c.Do("LINDEX", "l", "noint"))
		assert(t, err != nil, "LINDEX error")
		// Too many arguments
		_, err = redis.String(c.Do("LINDEX", "str", "l", "foo"))
		assert(t, err != nil, "LINDEX error")
	}
}

func TestLlen(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.Push("l", "aap", "noot", "mies", "vuur")

	{
		el, err := redis.Int(c.Do("LLEN", "l"))
		ok(t, err)
		equals(t, 4, el)
	}

	// Non exising key
	{
		el, err := redis.Int(c.Do("LLEN", "nonexisting"))
		ok(t, err)
		equals(t, 0, el)
	}

	// Wrong type of key
	{
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = redis.Int(c.Do("LLEN", "str"))
		assert(t, err != nil, "LLEN error")
		// Too many arguments
		_, err = redis.String(c.Do("LLEN", "too", "many"))
		assert(t, err != nil, "LLEN error")
	}
}

func TestLtrim(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.Push("l", "aap", "noot", "mies", "vuur")

	{
		el, err := redis.String(c.Do("LTRIM", "l", 0, 2))
		ok(t, err)
		equals(t, "OK", el)
		l, err := s.List("l")
		ok(t, err)
		equals(t, []string{"aap", "noot", "mies"}, l)
	}

	// Non exising key
	{
		el, err := redis.String(c.Do("LTRIM", "nonexisting", 0, 1))
		ok(t, err)
		equals(t, "OK", el)
	}

	// Wrong type of key
	{
		s.Set("str", "string!")
		_, err = redis.Int(c.Do("LTRIM", "str", 0, 1))
		assert(t, err != nil, "LTRIM error")
		// Too many/little/wrong arguments
		_, err = redis.String(c.Do("LTRIM", "l", 1, 2, "toomany"))
		assert(t, err != nil, "LTRIM error")
		_, err = redis.String(c.Do("LTRIM", "l", 1, "noint"))
		assert(t, err != nil, "LTRIM error")
		_, err = redis.String(c.Do("LTRIM", "l", "noint", 1))
		assert(t, err != nil, "LTRIM error")
		_, err = redis.String(c.Do("LTRIM", "l", 1))
		assert(t, err != nil, "LTRIM error")
		_, err = redis.String(c.Do("LTRIM", "l"))
		assert(t, err != nil, "LTRIM error")
		_, err = redis.String(c.Do("LTRIM"))
		assert(t, err != nil, "LTRIM error")
	}
}

func TestLrem(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Reverse
	{
		s.Push("l", "aap", "noot", "mies", "vuur", "noot", "noot")
		n, err := redis.Int(c.Do("LREM", "l", -1, "noot"))
		ok(t, err)
		equals(t, 1, n)
		l, err := s.List("l")
		ok(t, err)
		equals(t, []string{"aap", "noot", "mies", "vuur", "noot"}, l)
	}
	// Normal
	{
		s.Push("l2", "aap", "noot", "mies", "vuur", "noot", "noot")
		n, err := redis.Int(c.Do("LREM", "l2", 2, "noot"))
		ok(t, err)
		equals(t, 2, n)
		l, err := s.List("l2")
		ok(t, err)
		equals(t, []string{"aap", "mies", "vuur", "noot"}, l)
	}

	// All
	{
		s.Push("l3", "aap", "noot", "mies", "vuur", "noot", "noot")
		n, err := redis.Int(c.Do("LREM", "l3", 0, "noot"))
		ok(t, err)
		equals(t, 3, n)
		l, err := s.List("l3")
		ok(t, err)
		equals(t, []string{"aap", "mies", "vuur"}, l)
	}

	// All
	{
		s.Push("l4", "aap", "noot", "mies", "vuur", "noot", "noot")
		n, err := redis.Int(c.Do("LREM", "l4", 200, "noot"))
		ok(t, err)
		equals(t, 3, n)
		l, err := s.List("l4")
		ok(t, err)
		equals(t, []string{"aap", "mies", "vuur"}, l)
	}

	// Non exising key
	{
		n, err := redis.Int(c.Do("LREM", "nonexisting", 0, "aap"))
		ok(t, err)
		equals(t, 0, n)
	}

	// Error cases
	{
		_, err = redis.String(c.Do("LREM"))
		assert(t, err != nil, "LREM error")
		_, err = redis.String(c.Do("LREM", "l"))
		assert(t, err != nil, "LREM error")
		_, err = redis.String(c.Do("LREM", "l", 1))
		assert(t, err != nil, "LREM error")
		_, err = redis.String(c.Do("LREM", "l", "noint", "aap"))
		assert(t, err != nil, "LREM error")
		_, err = redis.String(c.Do("LREM", "l", 1, "aap", "toomany"))
		assert(t, err != nil, "LREM error")
		s.Set("str", "string!")
		_, err = redis.Int(c.Do("LREM", "str", 0, "aap"))
		assert(t, err != nil, "LREM error")
	}
}
