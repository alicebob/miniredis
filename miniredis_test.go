package miniredis

import (
	"testing"

	"github.com/garyburd/redigo/redis"
)

// Test starting/stopping a server
func TestServer(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	_, err = c.Do("PING")
	ok(t, err)

	// A single client
	equals(t, 1, s.CurrentConnectionCount())
	equals(t, 1, s.TotalConnectionCount())
	equals(t, 1, s.CommandCount())
	_, err = c.Do("PING")
	ok(t, err)
	equals(t, 2, s.CommandCount())
}

func TestMultipleServers(t *testing.T) {
	s1, err := Run()
	ok(t, err)
	s2, err := Run()
	ok(t, err)
	if s1.Addr() == s2.Addr() {
		t.Fatal("Non-unique addresses", s1.Addr(), s2.Addr())
	}

	s2.Close()
	s1.Close()
	// Closing multiple times is fine
	go s1.Close()
	go s1.Close()
	s1.Close()
}

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

// Test Hash.
func TestHash(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	{
		b, err := redis.Int(c.Do("HSET", "aap", "noot", "mies"))
		ok(t, err)
		equals(t, 1, b) // New field.
	}

	{
		v, err := redis.String(c.Do("HGET", "aap", "noot"))
		ok(t, err)
		equals(t, "mies", v)
		equals(t, "mies", s.HGet("aap", "noot"))
	}

	{
		b, err := redis.Int(c.Do("HSET", "aap", "noot", "mies"))
		ok(t, err)
		equals(t, 0, b) // Existing field.
	}

	// Wrong type of key
	{
		_, err := redis.String(c.Do("SET", "foo", "bar"))
		ok(t, err)
		_, err = redis.Int(c.Do("HSET", "foo", "noot", "mies"))
		assert(t, err != nil, "HSET error")
	}

	// hash exists, key doesn't.
	{
		b, err := c.Do("HGET", "aap", "nosuch")
		ok(t, err)
		equals(t, nil, b)
	}

	// hash doesn't exists.
	{
		b, err := c.Do("HGET", "nosuch", "nosuch")
		ok(t, err)
		equals(t, nil, b)
		equals(t, "", s.HGet("nosuch", "nosuch"))
	}

	// HGET on wrong type
	{
		_, err := redis.Int(c.Do("HGET", "aap"))
		assert(t, err != nil, "HGET error")
	}

	// Direct HSet()
	{
		s.HSet("wim", "zus", "jet")
		v, err := redis.String(c.Do("HGET", "wim", "zus"))
		ok(t, err)
		equals(t, "jet", v)
	}
}
