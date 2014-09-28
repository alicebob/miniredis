package miniredis

import (
	"math"
	"testing"

	"github.com/garyburd/redigo/redis"
)

// Test ZADD / ZCARD / ZRANK / ZREVRANK.
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

		m, err = redis.Int(c.Do("ZREVRANK", "z", "one"))
		ok(t, err)
		equals(t, 2, m)
		m, err = redis.Int(c.Do("ZREVRANK", "z", "three"))
		ok(t, err)
		equals(t, 0, m)
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

		_, err = redis.String(c.Do("ZDEVRANK"))
		assert(t, err != nil, "ZDEVRANK error")

		_, err = redis.Int(c.Do("ZCARD", "str"))
		assert(t, err != nil, "ZCARD error")
		_, err = redis.String(c.Do("ZCARD"))
		assert(t, err != nil, "ZCARD error")
		_, err = redis.String(c.Do("ZCARD", "set", "spurious"))
		assert(t, err != nil, "ZCARD error")
	}

}

// Test ZRANGE and ZREVRANGE
func TestSortedSetRange(t *testing.T) {
	// ZREVRANGE is the same code as ZRANGE
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

		b, err = redis.Strings(c.Do("ZREVRANGE", "z", 0, -1))
		ok(t, err)
		equals(t, []string{"inf", "three", "drei", "zwei", "two", "one"}, b)
	}
	{
		b, err := redis.Strings(c.Do("ZRANGE", "z", 0, 1))
		ok(t, err)
		equals(t, []string{"one", "two"}, b)

		b, err = redis.Strings(c.Do("ZREVRANGE", "z", 0, 1))
		ok(t, err)
		equals(t, []string{"inf", "three"}, b)
	}
	{
		b, err := redis.Strings(c.Do("ZRANGE", "z", -1, -1))
		ok(t, err)
		equals(t, []string{"inf"}, b)

		b, err = redis.Strings(c.Do("ZREVRANGE", "z", -1, -1))
		ok(t, err)
		equals(t, []string{"one"}, b)
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

		b, err = redis.Strings(c.Do("ZREVRANGE", "z", 1, 2, "WITHSCORES"))
		ok(t, err)
		equals(t, []string{"three", "3", "drei", "3"}, b)
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
		_, err = redis.String(c.Do("ZREVRANGE"))
		assert(t, err != nil, "ZREVRANGE error")
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

// Test ZRANGEBYSCORE,  ZREVRANGEBYSCORE, and ZCOUNT
func TestSortedSetRangeByScore(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.ZAdd("z", -273.15, "zero kelvin")
	s.ZAdd("z", -4, "minusfour")
	s.ZAdd("z", 1, "one")
	s.ZAdd("z", 2, "two")
	s.ZAdd("z", 2, "zwei")
	s.ZAdd("z", 3, "three")
	s.ZAdd("z", 3, "drei")
	s.ZAdd("z", math.Inf(+1), "inf")

	// Normal cases
	{
		b, err := redis.Strings(c.Do("ZRANGEBYSCORE", "z", "-inf", "inf"))
		ok(t, err)
		equals(t, []string{"zero kelvin", "minusfour", "one", "two", "zwei", "drei", "three", "inf"}, b)

		b, err = redis.Strings(c.Do("ZREVRANGEBYSCORE", "z", "inf", "-inf"))
		ok(t, err)
		equals(t, []string{"inf", "three", "drei", "zwei", "two", "one", "minusfour", "zero kelvin"}, b)

		i, err := redis.Int(c.Do("ZCOUNT", "z", "-inf", "inf"))
		ok(t, err)
		equals(t, 8, i)
	}
	{
		b, err := redis.Strings(c.Do("ZRANGEBYSCORE", "z", "2", "3"))
		ok(t, err)
		equals(t, []string{"two", "zwei", "drei", "three"}, b)

		b, err = redis.Strings(c.Do("ZREVRANGEBYSCORE", "z", "3", "2"))
		ok(t, err)
		equals(t, []string{"three", "drei", "zwei", "two"}, b)

		i, err := redis.Int(c.Do("ZCOUNT", "z", "2", "3"))
		ok(t, err)
		equals(t, 4, i)
	}
	// Exclusive min
	{
		b, err := redis.Strings(c.Do("ZRANGEBYSCORE", "z", "(2", "3"))
		ok(t, err)
		equals(t, []string{"drei", "three"}, b)

		i, err := redis.Int(c.Do("ZCOUNT", "z", "(2", "3"))
		ok(t, err)
		equals(t, 2, i)
	}
	// Exclusive max
	{
		b, err := redis.Strings(c.Do("ZRANGEBYSCORE", "z", "2", "(3"))
		ok(t, err)
		equals(t, []string{"two", "zwei"}, b)
	}
	// Exclusive both
	{
		b, err := redis.Strings(c.Do("ZRANGEBYSCORE", "z", "(2", "(3"))
		ok(t, err)
		equals(t, []string{}, b)
	}
	// Wrong ranges
	{
		b, err := redis.Strings(c.Do("ZRANGEBYSCORE", "z", "+inf", "-inf"))
		ok(t, err)
		equals(t, []string{}, b)

		b, err = redis.Strings(c.Do("ZREVRANGEBYSCORE", "z", "-inf", "+inf"))
		ok(t, err)
		equals(t, []string{}, b)
	}

	// No such key
	{
		b, err := redis.Strings(c.Do("ZRANGEBYSCORE", "nosuch", "-inf", "inf"))
		ok(t, err)
		equals(t, []string{}, b)
	}

	// With scores
	{
		b, err := redis.Strings(c.Do("ZRANGEBYSCORE", "z", "(1", 2, "WITHSCORES"))
		ok(t, err)
		equals(t, []string{"two", "2", "zwei", "2"}, b)
	}

	// With LIMIT
	// (note, this is SQL like logic, not the redis RANGE logic)
	{
		b, err := redis.Strings(c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", 1, 2))
		ok(t, err)
		equals(t, []string{"minusfour", "one"}, b)

		b, err = redis.Strings(c.Do("ZREVRANGEBYSCORE", "z", "inf", "-inf", "LIMIT", 1, 2))
		ok(t, err)
		equals(t, []string{"three", "drei"}, b)

		b, err = redis.Strings(c.Do("ZRANGEBYSCORE", "z", "1", "inf", "LIMIT", 1, 2000))
		ok(t, err)
		equals(t, []string{"two", "zwei", "drei", "three", "inf"}, b)

		b, err = redis.Strings(c.Do("ZREVRANGEBYSCORE", "z", "inf", "1", "LIMIT", 1, 2000))
		ok(t, err)
		equals(t, []string{"three", "drei", "zwei", "two", "one"}, b)

		// Negative start limit. No go.
		b, err = redis.Strings(c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", -1, 2))
		ok(t, err)
		equals(t, []string{}, b)

		// Negative end limit. Is fine but ignored.
		b, err = redis.Strings(c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", 1, -2))
		ok(t, err)
		equals(t, []string{"minusfour", "one", "two", "zwei", "drei", "three", "inf"}, b)
	}
	// Everything
	{
		b, err := redis.Strings(c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "WITHSCORES", "LIMIT", 1, 2))
		ok(t, err)
		equals(t, []string{"minusfour", "-4", "one", "1"}, b)

		b, err = redis.Strings(c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", 1, 2, "WITHSCORES"))
		ok(t, err)
		equals(t, []string{"minusfour", "-4", "one", "1"}, b)
	}

	// Error cases
	{
		_, err = redis.String(c.Do("ZRANGEBYSCORE"))
		assert(t, err != nil, "ZRANGEBYSCORE error")
		_, err = redis.String(c.Do("ZRANGEBYSCORE", "set"))
		assert(t, err != nil, "ZRANGEBYSCORE error")
		_, err = redis.String(c.Do("ZRANGEBYSCORE", "set", 1))
		assert(t, err != nil, "ZRANGEBYSCORE error")
		_, err = redis.String(c.Do("ZRANGEBYSCORE", "set", "nofloat", 1))
		assert(t, err != nil, "ZRANGEBYSCORE error")
		_, err = redis.String(c.Do("ZRANGEBYSCORE", "set", 1, "nofloat"))
		assert(t, err != nil, "ZRANGEBYSCORE error")
		_, err = redis.String(c.Do("ZRANGEBYSCORE", "set", 1, 2, "toomany"))
		assert(t, err != nil, "ZRANGEBYSCORE error")
		_, err = redis.String(c.Do("ZRANGEBYSCORE", "set", "[1", 2, "toomany"))
		assert(t, err != nil, "ZRANGEBYSCORE error")
		_, err = redis.String(c.Do("ZRANGEBYSCORE", "set", 1, "[2", "toomany"))
		assert(t, err != nil, "ZRANGEBYSCORE error")
		_, err = redis.String(c.Do("ZRANGEBYSCORE", "set", "[1", 2, "LIMIT", "noint", 1))
		assert(t, err != nil, "ZRANGEBYSCORE error")
		_, err = redis.String(c.Do("ZRANGEBYSCORE", "set", "[1", 2, "LIMIT", 1, "noint"))
		assert(t, err != nil, "ZRANGEBYSCORE error")
		// Wrong type of key
		s.Set("str", "value")
		_, err = redis.Int(c.Do("ZRANGEBYSCORE", "str", 1, 2))
		assert(t, err != nil, "ZRANGEBYSCORE error")

		_, err = redis.String(c.Do("ZREVRANGEBYSCORE"))
		assert(t, err != nil, "ZREVRANGEBYSCORE error")

		_, err = redis.String(c.Do("ZCOUNT"))
		assert(t, err != nil, "ZCOUNT error")
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

// Test ZREMRANGEBYLEX
func TestSortedSetRemRangeByLex(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.ZAdd("z", 12, "zero kelvin")
	s.ZAdd("z", 12, "minusfour")
	s.ZAdd("z", 12, "one")
	s.ZAdd("z", 12, "oneone")
	s.ZAdd("z", 12, "two")
	s.ZAdd("z", 12, "zwei")
	s.ZAdd("z", 12, "three")
	s.ZAdd("z", 12, "drei")
	s.ZAdd("z", 12, "inf")

	// Inclusive range
	{
		b, err := redis.Int(c.Do("ZREMRANGEBYLEX", "z", "[o", "[three"))
		ok(t, err)
		equals(t, 3, b)

		members, err := s.ZMembers("z")
		ok(t, err)
		equals(t,
			[]string{"drei", "inf", "minusfour", "two", "zero kelvin", "zwei"},
			members,
		)
	}

	// Wrong ranges
	{
		b, err := redis.Int(c.Do("ZREMRANGEBYLEX", "z", "+", "(z"))
		ok(t, err)
		equals(t, 0, b)
	}

	// No such key
	{
		b, err := redis.Int(c.Do("ZREMRANGEBYLEX", "nosuch", "-", "+"))
		ok(t, err)
		equals(t, 0, b)
	}

	// Error cases
	{
		_, err = c.Do("ZREMRANGEBYLEX")
		assert(t, err != nil, "ZREMRANGEBYLEX error")
		_, err = c.Do("ZREMRANGEBYLEX", "set")
		assert(t, err != nil, "ZREMRANGEBYLEX error")
		_, err = c.Do("ZREMRANGEBYLEX", "set", "1", "[a")
		assert(t, err != nil, "ZREMRANGEBYLEX error")
		_, err = c.Do("ZREMRANGEBYLEX", "set", "[a", "1")
		assert(t, err != nil, "ZREMRANGEBYLEX error")
		_, err = c.Do("ZREMRANGEBYLEX", "set", "[a", "!a")
		assert(t, err != nil, "ZREMRANGEBYLEX error")
		_, err = c.Do("ZREMRANGEBYLEX", "set", "-", "+", "toomany")
		assert(t, err != nil, "ZREMRANGEBYLEX error")
		// Wrong type of key
		s.Set("str", "value")
		_, err = c.Do("ZREMRANGEBYLEX", "str", "-", "+")
		assert(t, err != nil, "ZREMRANGEBYLEX error")
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

// Test ZRANGEBYLEX, ZLEXCOUNT
func TestSortedSetRangeByLex(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.ZAdd("z", 12, "zero kelvin")
	s.ZAdd("z", 12, "minusfour")
	s.ZAdd("z", 12, "one")
	s.ZAdd("z", 12, "oneone")
	s.ZAdd("z", 12, "two")
	s.ZAdd("z", 12, "zwei")
	s.ZAdd("z", 12, "three")
	s.ZAdd("z", 12, "drei")
	s.ZAdd("z", 12, "inf")

	// Normal cases
	{
		b, err := redis.Strings(c.Do("ZRANGEBYLEX", "z", "-", "+"))
		ok(t, err)
		equals(t, []string{"drei", "inf", "minusfour", "one", "oneone", "three", "two", "zero kelvin", "zwei"}, b)

		i, err := redis.Int(c.Do("ZLEXCOUNT", "z", "-", "+"))
		ok(t, err)
		equals(t, 9, i)
	}
	// Inclusive range
	{
		b, err := redis.Strings(c.Do("ZRANGEBYLEX", "z", "[o", "[three"))
		ok(t, err)
		equals(t, []string{"one", "oneone", "three"}, b)

		i, err := redis.Int(c.Do("ZLEXCOUNT", "z", "[o", "[three"))
		ok(t, err)
		equals(t, 3, i)
	}
	// Inclusive range
	{
		b, err := redis.Strings(c.Do("ZRANGEBYLEX", "z", "(o", "(z"))
		ok(t, err)
		equals(t, []string{"one", "oneone", "three", "two"}, b)

		i, err := redis.Int(c.Do("ZLEXCOUNT", "z", "(o", "(z"))
		ok(t, err)
		equals(t, 4, i)
	}
	// Wrong ranges
	{
		b, err := redis.Strings(c.Do("ZRANGEBYLEX", "z", "+", "(z"))
		ok(t, err)
		equals(t, []string{}, b)

		b, err = redis.Strings(c.Do("ZRANGEBYLEX", "z", "(a", "-"))
		ok(t, err)
		equals(t, []string{}, b)

		b, err = redis.Strings(c.Do("ZRANGEBYLEX", "z", "(z", "(a"))
		ok(t, err)
		equals(t, []string{}, b)

		i, err := redis.Int(c.Do("ZLEXCOUNT", "z", "(z", "(z"))
		ok(t, err)
		equals(t, 0, i)
	}

	// No such key
	{
		b, err := redis.Strings(c.Do("ZRANGEBYLEX", "nosuch", "-", "+"))
		ok(t, err)
		equals(t, []string{}, b)

		i, err := redis.Int(c.Do("ZLEXCOUNT", "nosuch", "-", "+"))
		ok(t, err)
		equals(t, 0, i)
	}

	// With LIMIT
	// (note, this is SQL like logic, not the redis RANGE logic)
	{
		b, err := redis.Strings(c.Do("ZRANGEBYLEX", "z", "-", "+", "LIMIT", 1, 2))
		ok(t, err)
		equals(t, []string{"inf", "minusfour"}, b)

		// Negative start limit. No go.
		b, err = redis.Strings(c.Do("ZRANGEBYLEX", "z", "-", "+", "LIMIT", -1, 2))
		ok(t, err)
		equals(t, []string{}, b)

		// Negative end limit. Is fine but ignored.
		b, err = redis.Strings(c.Do("ZRANGEBYLEX", "z", "-", "+", "LIMIT", 1, -2))
		ok(t, err)
		equals(t, []string{"inf", "minusfour", "one", "oneone", "three", "two", "zero kelvin", "zwei"}, b)
	}

	// Error cases
	{
		_, err = c.Do("ZRANGEBYLEX")
		assert(t, err != nil, "ZRANGEBYLEX error")
		_, err = c.Do("ZRANGEBYLEX", "set")
		assert(t, err != nil, "ZRANGEBYLEX error")
		_, err = c.Do("ZRANGEBYLEX", "set", "1", "[a")
		assert(t, err != nil, "ZRANGEBYLEX error")
		_, err = c.Do("ZRANGEBYLEX", "set", "[a", "1")
		assert(t, err != nil, "ZRANGEBYLEX error")
		_, err = c.Do("ZRANGEBYLEX", "set", "[a", "!a")
		assert(t, err != nil, "ZRANGEBYLEX error")
		_, err = c.Do("ZRANGEBYLEX", "set", "-", "+", "toomany")
		assert(t, err != nil, "ZRANGEBYLEX error")
		_, err = c.Do("ZRANGEBYLEX", "set", "[1", "(1", "LIMIT", "noint", 1)
		assert(t, err != nil, "ZRANGEBYLEX error")
		_, err = c.Do("ZRANGEBYLEX", "set", "[1", "(1", "LIMIT", 1, "noint")
		assert(t, err != nil, "ZRANGEBYLEX error")
		// Wrong type of key
		s.Set("str", "value")
		_, err = c.Do("ZRANGEBYLEX", "str", "-", "+")
		assert(t, err != nil, "ZRANGEBYLEX error")

		_, err = c.Do("ZLEXCOUNT")
		assert(t, err != nil, "ZLEXCOUNT error")
		_, err = c.Do("ZLEXCOUNT", "k")
		assert(t, err != nil, "ZLEXCOUNT error")
		_, err = c.Do("ZLEXCOUNT", "k", "[a", "a")
		assert(t, err != nil, "ZLEXCOUNT error")
		_, err = c.Do("ZLEXCOUNT", "k", "a", "(a")
		assert(t, err != nil, "ZLEXCOUNT error")
		_, err = c.Do("ZLEXCOUNT", "k", "(a", "(a", "toomany")
		assert(t, err != nil, "ZLEXCOUNT error")
	}
}

// Test ZINCRBY
func TestSortedSetIncrby(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Normal cases
	{
		// New key
		b, err := redis.Float64(c.Do("ZINCRBY", "z", 1, "member"))
		ok(t, err)
		equals(t, 1.0, b)

		// Existing key
		b, err = redis.Float64(c.Do("ZINCRBY", "z", 2.5, "member"))
		ok(t, err)
		equals(t, 3.5, b)

		// New member
		b, err = redis.Float64(c.Do("ZINCRBY", "z", 1, "othermember"))
		ok(t, err)
		equals(t, 1.0, b)
	}

	// Error cases
	{
		_, err = redis.String(c.Do("ZINCRBY"))
		assert(t, err != nil, "ZINCRBY error")
		_, err = redis.String(c.Do("ZINCRBY", "set"))
		assert(t, err != nil, "ZINCRBY error")
		_, err = redis.String(c.Do("ZINCRBY", "set", "nofloat", "a"))
		assert(t, err != nil, "ZINCRBY error")
		_, err = redis.String(c.Do("ZINCRBY", "set", 1.0, "too", "many"))
		assert(t, err != nil, "ZINCRBY error")
		// Wrong type of key
		s.Set("str", "value")
		_, err = c.Do("ZINCRBY", "str", 1.0, "member")
		assert(t, err != nil, "ZINCRBY error")
	}
}
