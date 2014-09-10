package miniredis

import (
	"testing"

	"github.com/garyburd/redigo/redis"
)

func TestMulti(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Do accept MULTI, but use it as a no-op
	r, err := redis.String(c.Do("MULTI"))
	ok(t, err)
	equals(t, "OK", r)
}

func TestExec(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Exec without MULTI.
	_, err = c.Do("EXEC")
	assert(t, err != nil, "do EXEC error")
}

func TestDiscard(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// DISCARD without MULTI.
	_, err = c.Do("DISCARD")
	assert(t, err != nil, "do DISCARD error")
}

// Test simple multi/exec block.
func TestSimpleTransaction(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	b, err := redis.String(c.Do("MULTI"))
	ok(t, err)
	equals(t, "OK", b)

	b, err = redis.String(c.Do("SET", "aap", 1))
	ok(t, err)
	equals(t, "QUEUED", b)

	// Not set yet.
	equals(t, "", s.Get("aap"))

	v, err := redis.Values(c.Do("EXEC"))
	ok(t, err)
	equals(t, 1, len(redis.Args(v)))
	equals(t, "OK", v[0])

	// SET should be back to normal mode
	b, err = redis.String(c.Do("SET", "aap", 1))
	ok(t, err)
	equals(t, "OK", b)
}

func TestDiscardTransaction(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.Set("aap", "noot")

	b, err := redis.String(c.Do("MULTI"))
	ok(t, err)
	equals(t, "OK", b)

	b, err = redis.String(c.Do("SET", "aap", "mies"))
	ok(t, err)
	equals(t, "QUEUED", b)

	// Not committed
	equals(t, "noot", s.Get("aap"))

	v, err := redis.String(c.Do("DISCARD"))
	ok(t, err)
	equals(t, "OK", v)

	// TX didn't get executed
	equals(t, "noot", s.Get("aap"))
}

func TestTxQueueErr(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	b, err := redis.String(c.Do("MULTI"))
	ok(t, err)
	equals(t, "OK", b)

	b, err = redis.String(c.Do("SET", "aap", "mies"))
	ok(t, err)
	equals(t, "QUEUED", b)

	// That's an error!
	_, err = redis.String(c.Do("SET", "aap"))
	assert(t, err != nil, "do SET error")

	// Thisone is ok again
	b, err = redis.String(c.Do("SET", "noot", "vuur"))
	ok(t, err)
	equals(t, "QUEUED", b)

	_, err = redis.String(c.Do("EXEC"))
	assert(t, err != nil, "do EXEC error")

	// Didn't get EXECed
	equals(t, "", s.Get("aap"))
}
