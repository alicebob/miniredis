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
