package miniredis

import (
	"testing"

	"github.com/garyburd/redigo/redis"
)

func TestAuth(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// We accept all AUTH
	_, err = c.Do("AUTH", "foo", "bar")
	ok(t, err)
}

func TestEcho(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	r, err := redis.String(c.Do("ECHO", "hello\nworld"))
	ok(t, err)
	equals(t, "hello\nworld", r)
}
