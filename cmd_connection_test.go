package miniredis

import (
	"testing"

	"github.com/gomodule/redigo/redis"

	"github.com/alicebob/miniredis/v2/client"
)

func TestAuth(t *testing.T) {
	t.Run("default user", func(t *testing.T) {
		s, err := Run()
		ok(t, err)
		defer s.Close()
		c, err := redis.Dial("tcp", s.Addr())
		ok(t, err)

		_, err = c.Do("AUTH", "foo", "bar", "baz")
		mustFail(t, err, "ERR syntax error")

		s.RequireAuth("nocomment")
		_, err = c.Do("PING", "foo", "bar")
		mustFail(t, err, "NOAUTH Authentication required.")

		_, err = c.Do("AUTH", "wrongpasswd")
		mustFail(t, err, "WRONGPASS invalid username-password pair")

		_, err = c.Do("AUTH", "nocomment")
		ok(t, err)

		_, err = c.Do("PING")
		ok(t, err)
	})

	t.Run("another user", func(t *testing.T) {
		s, err := Run()
		ok(t, err)
		defer s.Close()
		c, err := redis.Dial("tcp", s.Addr())
		ok(t, err)

		s.RequireUserAuth("hello", "world")
		_, err = c.Do("PING", "foo", "bar")
		mustFail(t, err, "NOAUTH Authentication required.")

		_, err = c.Do("AUTH", "hello", "wrongpasswd")
		mustFail(t, err, "WRONGPASS invalid username-password pair")

		_, err = c.Do("AUTH", "goodbye", "world")
		mustFail(t, err, "WRONGPASS invalid username-password pair")

		_, err = c.Do("AUTH", "hello", "world")
		ok(t, err)

		_, err = c.Do("PING")
		ok(t, err)
	})

	t.Run("error cases", func(t *testing.T) {
		s, err := Run()
		ok(t, err)
		defer s.Close()
		c, err := redis.Dial("tcp", s.Addr())
		ok(t, err)

		_, err = c.Do("AUTH")
		mustFail(t, err, "ERR wrong number of arguments for 'auth' command")

		_, err = c.Do("AUTH", "foo", "bar", "baz")
		mustFail(t, err, "ERR syntax error")
	})
}

func TestPing(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := client.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("no args", func(t *testing.T) {
		res, err := c.Do("PING")
		ok(t, err)
		equals(t, "+PONG\r\n", res)
	})

	t.Run("args", func(t *testing.T) {
		res, err := c.Do("PING", "hi")
		ok(t, err)
		equals(t, "$2\r\nhi\r\n", res)
	})

	t.Run("error", func(t *testing.T) {
		res, err := c.Do("PING", "foo", "bar")
		ok(t, err)
		equals(t, "-ERR wrong number of arguments for 'ping' command\r\n", res)
	})
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

func TestSelect(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	_, err = redis.String(c.Do("SET", "foo", "bar"))
	ok(t, err)

	_, err = redis.String(c.Do("SELECT", "5"))
	ok(t, err)

	_, err = redis.String(c.Do("SET", "foo", "baz"))
	ok(t, err)

	// Direct access.
	got, err := s.Get("foo")
	ok(t, err)
	equals(t, "bar", got)
	s.Select(5)
	got, err = s.Get("foo")
	ok(t, err)
	equals(t, "baz", got)

	// Another connection should have its own idea of the db:
	c2, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	v, err := redis.String(c2.Do("GET", "foo"))
	ok(t, err)
	equals(t, "bar", v)
}

func TestSwapdb(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	_, err = redis.String(c.Do("SET", "foo", "bar"))
	ok(t, err)

	_, err = redis.String(c.Do("SELECT", "5"))
	ok(t, err)

	_, err = redis.String(c.Do("SET", "foo", "baz"))
	ok(t, err)

	res, err := redis.String(c.Do("SWAPDB", "0", "5"))
	ok(t, err)
	equals(t, "OK", res)

	got, err := s.Get("foo")
	ok(t, err)
	equals(t, "baz", got)
	s.Select(5)
	got, err = s.Get("foo")
	ok(t, err)
	equals(t, "bar", got)

	// Another connection should have its own idea of the db:
	c2, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	v, err := redis.String(c2.Do("GET", "foo"))
	ok(t, err)
	equals(t, "baz", v)

	// errors
	{
		_, err := redis.String(c.Do("SWAPDB"))
		mustFail(t, err, errWrongNumber("SWAPDB"))

		_, err = redis.String(c.Do("SWAPDB", 1, 2, 3))
		mustFail(t, err, errWrongNumber("SWAPDB"))

		_, err = redis.String(c.Do("SWAPDB", "foo", 2))
		mustFail(t, err, "ERR invalid first DB index")

		_, err = redis.String(c.Do("SWAPDB", 1, "bar"))
		mustFail(t, err, "ERR invalid second DB index")

		_, err = redis.String(c.Do("SWAPDB", "foo", "bar"))
		mustFail(t, err, "ERR invalid first DB index")

		_, err = redis.String(c.Do("SWAPDB", -1, 2))
		mustFail(t, err, "ERR DB index is out of range")

		_, err = redis.String(c.Do("SWAPDB", 1, -2))
		mustFail(t, err, "ERR DB index is out of range")
	}
}

func TestQuit(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	v, err := redis.String(c.Do("QUIT"))
	ok(t, err)
	equals(t, "OK", v)

	v, err = redis.String(c.Do("PING"))
	assert(t, err != nil, "QUIT closed the client")
	equals(t, "", v)
}

func TestSetError(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	r, err := redis.String(c.Do("PING"))
	ok(t, err)
	equals(t, "PONG", r)

	s.SetError("LOADING Redis is loading the dataset in memory")
	_, err = c.Do("PING", "hi")
	mustFail(t, err, "LOADING Redis is loading the dataset in memory")

	s.SetError("")
	r, err = redis.String(c.Do("PING"))
	ok(t, err)
	equals(t, "PONG", r)
}
