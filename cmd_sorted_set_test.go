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

// Test ZRANGE
func TestSortedSetRange(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")
	s.ZAdd("z", 3, "three")
	s.ZAdd("z", 3, "drei")
	s.ZAdd("z", math.Inf(+1), "inf")

	{
		b, err := redis.Strings(c.Do("ZRANGE", "z", 0, -1))
		ok(t, err)
		equals(t, []string{"one", "two", "zwei", "drei", "three", "inf"}, b)
	}
	{
		b, err := redis.Strings(c.Do("ZRANGE", "z", 0, 1))
		ok(t, err)
		equals(t, []string{"one", "two"}, b)
	}
	{
		b, err := redis.Strings(c.Do("ZRANGE", "z", -1, -1))
		ok(t, err)
		equals(t, []string{"inf"}, b)
	}

	// weird cases.
	{
		b, err := redis.Strings(c.Do("ZRANGE", "z", -100, -100))
		ok(t, err)
		equals(t, []string{}, b)
	}
	{
		b, err := redis.Strings(c.Do("ZRANGE", "z", 100, 400))
		ok(t, err)
		equals(t, []string{}, b)
	}
	// Nonexistent key
	{
		b, err := redis.Strings(c.Do("ZRANGE", "nosuch", 1, 4))
		ok(t, err)
		equals(t, []string{}, b)
	}

	// With scores
	{
		b, err := redis.Strings(c.Do("ZRANGE", "z", 1, 2, "WITHSCORES"))
		ok(t, err)
		equals(t, []string{"two", "2", "zwei", "2"}, b)
	}
	// INF in WITHSCORES
	{
		b, err := redis.Strings(c.Do("ZRANGE", "z", 4, -1, "WITHSCORES"))
		ok(t, err)
		equals(t, []string{"three", "3", "inf", "inf"}, b)
	}

	// Error cases
	{
		_, err = redis.String(c.Do("ZRANGE"))
		assert(t, err != nil, "ZRANGE error")
		_, err = redis.String(c.Do("ZRANGE", "set"))
		assert(t, err != nil, "ZRANGE error")
		_, err = redis.String(c.Do("ZRANGE", "set", 1))
		assert(t, err != nil, "ZRANGE error")
		_, err = redis.String(c.Do("ZRANGE", "set", "noint", 1))
		assert(t, err != nil, "ZRANGE error")
		_, err = redis.String(c.Do("ZRANGE", "set", 1, "noint"))
		assert(t, err != nil, "ZRANGE error")
		_, err = redis.String(c.Do("ZRANGE", "set", 1, 2, "toomany"))
		assert(t, err != nil, "ZRANGE error")
		// Wrong type of key
		s.Set("str", "value")
		_, err = redis.Int(c.Do("ZRANGE", "str", 1, 2))
		assert(t, err != nil, "ZRANGE error")
	}
}

// Test ZREM
func TestSortedSetRem(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")

	// Simple delete
	{
		b, err := redis.Int(c.Do("ZREM", "z", "two", "zwei", "nosuch"))
		ok(t, err)
		equals(t, 2, b)
		assert(t, s.Exists("z"), "key is there")
	}
	// Delete the last member
	{
		b, err := redis.Int(c.Do("ZREM", "z", "one"))
		ok(t, err)
		equals(t, 1, b)
		assert(t, !s.Exists("z"), "key is gone")
	}
	// Nonexistent key
	{
		b, err := redis.Int(c.Do("ZREM", "nosuch", "member"))
		ok(t, err)
		equals(t, 0, b)
	}

	// Direct
	{
		s.ZAdd("z2", 1, "one")
		s.ZAdd("z2", 2, "two")
		s.ZAdd("z2", 2, "zwei")
		gone, err := s.ZRem("z2", "two")
		ok(t, err)
		assert(t, gone, "member gone")
		members, err := s.ZMembers("z2")
		ok(t, err)
		equals(t, []string{"one", "zwei"}, members)
	}

	// Error cases
	{
		_, err = redis.String(c.Do("ZREM"))
		assert(t, err != nil, "ZREM error")
		_, err = redis.String(c.Do("ZREM", "set"))
		assert(t, err != nil, "ZREM error")
		// Wrong type of key
		s.Set("str", "value")
		_, err = redis.Int(c.Do("ZREM", "str", "aap"))
		assert(t, err != nil, "ZREM error")
	}
}

// Test ZSCORE
func TestSortedSetScore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")

	// Simple case
	{
		b, err := redis.Float64(c.Do("ZSCORE", "z", "two"))
		ok(t, err)
		equals(t, 2.0, b)
	}
	// no such member
	{
		b, err := c.Do("ZSCORE", "z", "nosuch")
		ok(t, err)
		equals(t, nil, b)
	}
	// no such key
	{
		b, err := c.Do("ZSCORE", "nosuch", "nosuch")
		ok(t, err)
		equals(t, nil, b)
	}

	// Direct
	{
		s.ZAdd("z2", 1, "one")
		s.ZAdd("z2", 2, "two")
		score, err := s.ZScore("z2", "two")
		ok(t, err)
		equals(t, 2.0, score)
	}

	// Error cases
	{
		_, err = redis.String(c.Do("ZSCORE"))
		assert(t, err != nil, "ZSCORE error")
		_, err = redis.String(c.Do("ZSCORE", "key"))
		assert(t, err != nil, "ZSCORE error")
		_, err = redis.String(c.Do("ZSCORE", "too", "many", "arguments"))
		assert(t, err != nil, "ZSCORE error")
		// Wrong type of key
		s.Set("str", "value")
		_, err = redis.Int(c.Do("ZSCORE", "str", "aap"))
		assert(t, err != nil, "ZSCORE error")
	}
}
