package miniredis

import (
	"math"
	"testing"

	"github.com/garyburd/redigo/redis"
)

// Test ZADD / ZCARD / ZRANK.
func TestSortedSet(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	{
		b, err := redis.Int(c.Do("ZADD", "z", 1, "one", 2, "two", 3, "three"))
		ok(t, err)
		equals(t, 3, b) // New elements.

		b, err = redis.Int(c.Do("ZCARD", "z"))
		ok(t, err)
		equals(t, 3, b)

		m, err := redis.Int(c.Do("ZRANK", "z", "one"))
		ok(t, err)
		equals(t, 0, m)
		m, err = redis.Int(c.Do("ZRANK", "z", "three"))
		ok(t, err)
		equals(t, 2, m)
	}

	// TYPE of our zset
	{
		s, err := redis.String(c.Do("TYPE", "z"))
		ok(t, err)
		equals(t, "zset", s)
	}

	// Replace a key
	{
		b, err := redis.Int(c.Do("ZADD", "z", 2.1, "two"))
		ok(t, err)
		equals(t, 0, b) // No new elements.

		b, err = redis.Int(c.Do("ZCARD", "z"))
		ok(t, err)
		equals(t, 3, b)
	}

	// To infinity!
	{
		b, err := redis.Int(c.Do("ZADD", "zinf", "inf", "plus inf", "-inf", "minus inf", 10, "ten"))
		ok(t, err)
		equals(t, 3, b)

		b, err = redis.Int(c.Do("ZCARD", "zinf"))
		ok(t, err)
		equals(t, 3, b)

		smap, err := s.SortedSet("zinf")
		ok(t, err)
		equals(t, map[string]float64{
			"plus inf":  math.Inf(+1),
			"minus inf": math.Inf(-1),
			"ten":       10.0,
		}, smap)
	}

	// Invalid score
	{
		_, err := c.Do("ZADD", "z", "noint", "two")
		assert(t, err != nil, "ZADD err")
	}

	// ZRANK on non-existing key/member
	{
		m, err := c.Do("ZRANK", "z", "nosuch")
		ok(t, err)
		equals(t, nil, m)

		m, err = c.Do("ZRANK", "nosuch", "nosuch")
		ok(t, err)
		equals(t, nil, m)
	}

	// Direct usage
	{
		added, err := s.ZAdd("s1", 12.4, "aap")
		ok(t, err)
		equals(t, true, added)
		added, err = s.ZAdd("s1", 3.4, "noot")
		ok(t, err)
		equals(t, true, added)
		added, err = s.ZAdd("s1", 3.5, "noot")
		ok(t, err)
		equals(t, false, added)

		members, err := s.ZMembers("s1")
		ok(t, err)
		equals(t, []string{"noot", "aap"}, members)
	}

	// Error cases
	{
		// Wrong type of key
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)

		_, err = redis.Int(c.Do("ZADD", "str", 1.0, "hi"))
		assert(t, err != nil, "ZADD error")
		_, err = redis.String(c.Do("ZADD"))
		assert(t, err != nil, "ZADD error")
		_, err = redis.String(c.Do("ZADD", "set"))
		assert(t, err != nil, "ZADD error")
		_, err = redis.String(c.Do("ZADD", "set", 1.0))
		assert(t, err != nil, "ZADD error")
		_, err = redis.String(c.Do("ZADD", "set", 1.0, "foo", 1.0)) // odd
		assert(t, err != nil, "ZADD error")

		_, err = redis.Int(c.Do("ZRANK", "str"))
		assert(t, err != nil, "ZRANK error")
		_, err = redis.String(c.Do("ZRANK"))
		assert(t, err != nil, "ZRANK error")
		_, err = redis.String(c.Do("ZRANK", "set", "spurious"))
		assert(t, err != nil, "ZRANK error")

		_, err = redis.Int(c.Do("ZCARD", "str"))
		assert(t, err != nil, "ZCARD error")
		_, err = redis.String(c.Do("ZCARD"))
		assert(t, err != nil, "ZCARD error")
		_, err = redis.String(c.Do("ZCARD", "set", "spurious"))
		assert(t, err != nil, "ZCARD error")
	}

}
