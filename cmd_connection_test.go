package miniredis

import (
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

func TestAuth(t *testing.T) {
	t.Run("default user", func(t *testing.T) {
		s, err := Run()
		ok(t, err)
		defer s.Close()
		c, err := proto.Dial(s.Addr())
		ok(t, err)
		defer c.Close()

		mustDo(t, c,
			"AUTH", "foo", "bar", "baz",
			proto.Error("ERR syntax error"),
		)

		s.RequireAuth("nocomment")
		mustDo(t, c,
			"PING", "foo", "bar",
			proto.Error("NOAUTH Authentication required."),
		)
		mustDo(t, c,
			"AUTH", "wrongpasswd",
			proto.Error("WRONGPASS invalid username-password pair"),
		)
		mustDo(t, c,
			"AUTH", "nocomment",
			proto.Inline("OK"),
		)
		mustDo(t, c,
			"PING",
			proto.Inline("PONG"),
		)
	})

	t.Run("another user", func(t *testing.T) {
		s, err := Run()
		ok(t, err)
		defer s.Close()
		c, err := proto.Dial(s.Addr())
		ok(t, err)
		defer c.Close()

		s.RequireUserAuth("hello", "world")
		mustDo(t, c,
			"PING", "foo", "bar",
			proto.Error("NOAUTH Authentication required."),
		)
		mustDo(t, c,
			"AUTH", "hello", "wrongpasswd",
			proto.Error("WRONGPASS invalid username-password pair"),
		)
		mustDo(t, c,
			"AUTH", "goodbye", "world",
			proto.Error("WRONGPASS invalid username-password pair"),
		)
		mustDo(t, c,
			"AUTH", "hello", "world",
			proto.Inline("OK"),
		)
		mustDo(t, c,
			"PING",
			proto.Inline("PONG"),
		)
	})

	t.Run("error cases", func(t *testing.T) {
		s, err := Run()
		ok(t, err)
		defer s.Close()
		c, err := proto.Dial(s.Addr())
		ok(t, err)
		defer c.Close()

		mustDo(t, c,
			"AUTH",
			proto.Error("ERR wrong number of arguments for 'auth' command"),
		)

		mustDo(t, c,
			"AUTH", "foo", "bar", "baz",
			proto.Error("ERR syntax error"),
		)
	})
}

func TestPing(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("no args", func(t *testing.T) {
		mustDo(t, c,
			"PING",
			proto.Inline("PONG"),
		)
	})

	t.Run("args", func(t *testing.T) {
		mustDo(t, c,
			"PING", "hi",
			proto.String("hi"),
		)
	})

	t.Run("error", func(t *testing.T) {
		mustDo(t, c,
			"PING", "foo", "bar",
			proto.Error(errWrongNumber("ping")),
		)
	})
}

func TestEcho(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"ECHO", "hello\nworld",
		proto.String("hello\nworld"),
	)

	mustDo(t, c,
		"ECHO",
		proto.Error(errWrongNumber("echo")),
	)
}

func TestSelect(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustOK(t, c, "SET", "foo", "bar")
	mustOK(t, c, "SELECT", "5")
	mustOK(t, c, "SET", "foo", "baz")

	t.Run("direct access", func(t *testing.T) {
		got, err := s.Get("foo")
		ok(t, err)
		equals(t, "bar", got)

		s.Select(5)
		got, err = s.Get("foo")
		ok(t, err)
		equals(t, "baz", got)
	})

	// Another connection should have its own idea of the selected db:
	c2, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c2.Close()
	mustDo(t, c2,
		"GET", "foo",
		proto.String("bar"),
	)
}

func TestSwapdb(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustOK(t, c, "SET", "foo", "bar")
	mustOK(t, c, "SELECT", "5")
	mustOK(t, c, "SET", "foo", "baz")
	mustOK(t, c, "SWAPDB", "0", "5")

	t.Run("direct", func(t *testing.T) {
		got, err := s.Get("foo")
		ok(t, err)
		equals(t, "baz", got)
		s.Select(5)
		got, err = s.Get("foo")
		ok(t, err)
		equals(t, "bar", got)
	})

	t.Run("another connection", func(t *testing.T) {
		c2, err := proto.Dial(s.Addr())
		ok(t, err)
		defer c2.Close()
		mustDo(t, c2,
			"GET", "foo",
			proto.String("baz"),
		)
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"SWAPDB",
			proto.Error(errWrongNumber("SWAPDB")),
		)
		mustDo(t, c,
			"SWAPDB", "1", "2", "3",
			proto.Error(errWrongNumber("SWAPDB")),
		)
		mustDo(t, c,
			"SWAPDB", "foo", "2",
			proto.Error("ERR invalid first DB index"),
		)
		mustDo(t, c,
			"SWAPDB", "1", "bar",
			proto.Error("ERR invalid second DB index"),
		)
		mustDo(t, c,
			"SWAPDB", "foo", "bar",
			proto.Error("ERR invalid first DB index"),
		)
		mustDo(t, c,
			"SWAPDB", "-1", "2",
			proto.Error("ERR DB index is out of range"),
		)
		mustDo(t, c,
			"SWAPDB", "1", "-2",
			proto.Error("ERR DB index is out of range"),
		)
	})
}

func TestQuit(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustOK(t, c, "QUIT")

	res, err := c.Do("PING")
	assert(t, err != nil, "QUIT closed the client")
	equals(t, "", res)
}

func TestSetError(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	mustDo(t, c,
		"PING",
		proto.Inline("PONG"),
	)

	s.SetError("LOADING Redis is loading the dataset in memory")
	mustDo(t, c,
		"ECHO",
		proto.Error("LOADING Redis is loading the dataset in memory"),
	)

	s.SetError("")
	mustDo(t, c,
		"PING",
		proto.Inline("PONG"),
	)
}
