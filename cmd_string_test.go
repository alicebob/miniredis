package miniredis

import (
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/proto"
)

// Test simple GET/SET keys
func TestString(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// SET command
	mustOK(t, c,
		"SET", "foo", "bar",
	)

	// GET command
	mustDo(t, c,
		"GET", "foo",
		proto.String("bar"),
	)

	// Query server directly.
	{
		got, err := s.Get("foo")
		ok(t, err)
		equals(t, "bar", got)
	}

	// Use Set directly
	{
		ok(t, s.Set("aap", "noot"))
		s.CheckGet(t, "aap", "noot")
		mustDo(t, c,
			"GET", "aap",
			proto.String("noot"),
		)
		s.CheckGet(t, "aap", "noot")
		// Re-set.
		ok(t, s.Set("aap", "noot2"))
	}

	// non-existing key
	mustNil(t, c,
		"GET", "reallynosuchkey",
	)

	t.Run("errors", func(t *testing.T) {
		must1(t, c,
			"HSET", "wim", "zus", "jet",
		)
		mustDo(t, c,
			"GET", "wim",
			proto.Error(msgWrongType),
		)
	})
}

func TestSet(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("basic", func(t *testing.T) {
		// Simple case
		mustOK(t, c,
			"SET", "aap", "noot",
		)

		// Overwrite other types.
		s.HSet("wim", "teun", "vuur")
		mustOK(t, c,
			"SET", "wim", "gijs",
		)
		s.CheckGet(t, "wim", "gijs")
	})

	t.Run("NX", func(t *testing.T) {
		// new key
		mustOK(t, c,
			"SET", "mies", "toon", "NX",
		)
		// now existing key
		mustNil(t, c,
			"SET", "mies", "toon", "NX",
		)
		// lowercase NX is no problem
		mustNil(t, c,
			"SET", "mies", "toon", "nx",
		)
	})

	// XX argument - only set if exists
	t.Run("XX", func(t *testing.T) {
		// new key, no go
		mustNil(t, c,
			"SET", "one", "two", "XX",
		)

		s.Set("one", "three")

		mustOK(t, c,
			"SET", "one", "two", "XX",
		)
		s.CheckGet(t, "one", "two")

		// XX with another key type
		s.HSet("eleven", "twelve", "thirteen")
		mustOK(t, c,
			"SET", "eleven", "fourteen", "XX",
		)
		s.CheckGet(t, "eleven", "fourteen")
	})

	t.Run("EX PX", func(t *testing.T) {
		// EX or PX argument. TTL values.
		mustOK(t, c,
			"SET", "one", "two", "EX", "1299",
		)
		s.CheckGet(t, "one", "two")
		equals(t, time.Second*1299, s.TTL("one"))

		mustOK(t, c,
			"SET", "three", "four", "PX", "8888",
		)
		s.CheckGet(t, "three", "four")
		equals(t, time.Millisecond*8888, s.TTL("three"))

		mustDo(t, c,
			"SET", "one", "two", "EX", "notimestamp",
			proto.Error(msgInvalidInt),
		)

		mustDo(t, c,
			"SET", "one", "two", "EX",
			proto.Error(msgInvalidInt),
		)

		mustDo(t, c,
			"SET", "aap", "noot", "EX", "0",
			proto.Error("ERR invalid expire time in set"),
		)
		mustDo(t, c,
			"SET", "aap", "noot", "EX", "-100",
			proto.Error("ERR invalid expire time in set"),
		)
	})

	t.Run("KEEPTTL", func(t *testing.T) {
		s.Set("foo", "bar")
		s.SetTTL("foo", time.Second*1337)
		mustOK(t, c,
			"SET", "foo", "baz", "KEEPTTL",
		)
		s.CheckGet(t, "foo", "baz")
		equals(t, time.Second*1337, s.TTL("foo"))
	})

	t.Run("GET", func(t *testing.T) {
		mustNil(t, c,
			"SET", "dino", "bar", "GET",
		)
		mustDo(t, c,
			"SET", "dino", "bal", "GET",
			proto.String("bar"),
		)
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"SET", "one", "two", "FOO",
			proto.Error(msgSyntaxError),
		)
	})
}

func TestMget(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.Set("zus", "jet")
	s.Set("teun", "vuur")
	s.Set("gijs", "lam")
	s.Set("kees", "bok")

	mustDo(t, c,
		"MGET", "zus", "nosuch", "kees",
		proto.Array(proto.String("jet"), proto.Nil, proto.String("bok")),
	)

	// Wrong key type returns nil
	{
		s.HSet("aap", "foo", "bar")
		mustDo(t, c,
			"MGET", "aap",
			proto.Array(proto.Nil),
		)
	}
}

func TestMset(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		mustOK(t, c,
			"MSET", "zus", "jet", "teun", "vuur", "gijs", "lam",
		)
		s.CheckGet(t, "zus", "jet")
		s.CheckGet(t, "teun", "vuur")
		s.CheckGet(t, "gijs", "lam")
	}

	// Other types are overwritten
	{
		s.HSet("aap", "foo", "bar")
		mustOK(t, c,
			"MSET", "aap", "jet",
		)
		s.CheckGet(t, "aap", "jet")
	}

	// Odd argument list is not OK
	mustDo(t, c,
		"MSET", "zus", "jet", "teun",
		proto.Error("ERR wrong number of arguments for MSET"),
	)

	// TTL is cleared
	{
		s.Set("foo", "bar")
		s.HSet("aap", "foo", "bar") // even for weird keys.
		s.SetTTL("aap", time.Second*999)
		s.SetTTL("foo", time.Second*999)
		mustOK(t, c,
			"MSET", "aap", "noot", "foo", "baz",
		)
		equals(t, time.Duration(0), s.TTL("aap"))
		equals(t, time.Duration(0), s.TTL("foo"))
	}
}

func TestSetex(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Usual case
	{
		mustOK(t, c,
			"SETEX", "aap", "1234", "noot",
		)
		s.CheckGet(t, "aap", "noot")
		equals(t, time.Second*1234, s.TTL("aap"))
	}

	// Same thing
	mustOK(t, c,
		"SETEX", "aap", "1234", "noot",
	)

	// Error cases
	{
		mustDo(t, c,
			"SETEX", "aap", "nottl", "noot",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"SETEX", "aap",
			proto.Error(errWrongNumber("setex")),
		)
		mustDo(t, c,
			"SETEX", "aap", "12",
			proto.Error(errWrongNumber("setex")),
		)
		mustDo(t, c,
			"SETEX", "aap", "12", "noot", "toomuch",
			proto.Error(errWrongNumber("setex")),
		)
		mustDo(t, c,
			"SETEX", "aap", "0", "noot",
			proto.Error("ERR invalid expire time in setex"),
		)
		mustDo(t, c,
			"SETEX", "aap", "-10", "noot",
			proto.Error("ERR invalid expire time in setex"),
		)
	}
}

func TestPsetex(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Usual case
	{
		mustOK(t, c,
			"PSETEX", "aap", "1234", "noot",
		)
		s.CheckGet(t, "aap", "noot")
		equals(t, time.Millisecond*1234, s.TTL("aap"))
	}

	// Same thing
	mustOK(t, c,
		"PSETEX", "aap", "1234", "noot",
	)

	// Error cases
	{
		mustDo(t, c,
			"PSETEX", "aap", "nottl", "noot",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"PSETEX", "aap",
			proto.Error(errWrongNumber("psetex")),
		)
		mustDo(t, c,
			"PSETEX", "aap", "12",
			proto.Error(errWrongNumber("psetex")),
		)
		mustDo(t, c,
			"PSETEX", "aap", "12", "noot", "toomuch",
			proto.Error(errWrongNumber("psetex")),
		)
		mustDo(t, c,
			"PSETEX", "aap", "0", "noot",
			proto.Error("ERR invalid expire time in psetex"),
		)
		mustDo(t, c,
			"PSETEX", "aap", "-10", "noot",
			proto.Error("ERR invalid expire time in psetex"),
		)
	}
}

func TestSetnx(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Existing key
	{
		s.Set("foo", "bar")
		must0(t, c,
			"SETNX", "foo", "not bar",
		)
		s.CheckGet(t, "foo", "bar")
	}

	// New key
	{
		must1(t, c,
			"SETNX", "notfoo", "also not bar",
		)
		s.CheckGet(t, "notfoo", "also not bar")
	}

	// Existing key of a different type
	{
		s.HSet("foo", "bar", "baz")
		must0(t, c,
			"SETNX", "foo", "not bar",
		)
		equals(t, "hash", s.Type("foo"))
		_, err = s.Get("foo")
		equals(t, ErrWrongType, err)
		equals(t, "baz", s.HGet("foo", "bar"))
	}
}

func TestIncr(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Existing key
	{
		s.Set("foo", "12")
		mustDo(t, c,
			"INCR", "foo",
			proto.Int(13),
		)
		s.CheckGet(t, "foo", "13")
	}

	// Existing key, not an integer
	{
		s.Set("foo", "noint")
		mustDo(t, c,
			"INCR", "foo",
			proto.Error(msgInvalidInt),
		)
	}

	// New key
	{
		must1(t, c,
			"INCR", "bar",
		)
		s.CheckGet(t, "bar", "1")
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"INCR", "wrong",
			proto.Error(msgWrongType),
		)
	}

	// Direct usage
	{
		i, err := s.Incr("count", 1)
		ok(t, err)
		equals(t, 1, i)
		i, err = s.Incr("count", 1)
		ok(t, err)
		equals(t, 2, i)
		_, err = s.Incr("wrong", 1)
		assert(t, err != nil, "do s.Incr error")
	}

	// Wrong usage
	{
		mustDo(t, c,
			"INCR",
			proto.Error(errWrongNumber("incr")),
		)
		mustDo(t, c,
			"INCR", "new", "key",
			proto.Error(errWrongNumber("incr")),
		)
	}
}

func TestIncrBy(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Existing key
	{
		s.Set("foo", "12")
		mustDo(t, c,
			"INCRBY", "foo", "400",
			proto.Int(412),
		)
		s.CheckGet(t, "foo", "412")
	}

	// Existing key, not an integer
	{
		s.Set("foo", "noint")
		mustDo(t, c,
			"INCRBY", "foo", "400",
			proto.Error(msgInvalidInt),
		)
	}

	// New key
	{
		mustDo(t, c,
			"INCRBY", "bar", "4000",
			proto.Int(4000),
		)
		s.CheckGet(t, "bar", "4000")
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"INCRBY", "wrong", "400",
			proto.Error(msgWrongType),
		)
	}

	// Amount not an integer
	mustDo(t, c,
		"INCRBY", "key", "noint",
		proto.Error(msgInvalidInt),
	)

	// Wrong usage
	{
		mustDo(t, c,
			"INCRBY",
			proto.Error(errWrongNumber("incrby")),
		)
		mustDo(t, c,
			"INCRBY", "another", "new", "key",
			proto.Error(errWrongNumber("incrby")),
		)
	}
}

func TestIncrbyfloat(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Existing key
	{
		s.Set("foo", "12")
		mustDo(t, c,
			"INCRBYFLOAT", "foo", "400.12",
			proto.String("412.12"),
		)
		s.CheckGet(t, "foo", "412.12")
	}

	// Existing key, not a number
	{
		s.Set("foo", "noint")
		mustDo(t, c,
			"INCRBYFLOAT", "foo", "400",
			proto.Error(msgInvalidFloat),
		)
	}

	// New key
	{
		mustDo(t, c,
			"INCRBYFLOAT", "bar", "40.33",
			proto.String("40.33"),
		)
		s.CheckGet(t, "bar", "40.33")
	}

	// Direct usage
	{
		s.Set("foo", "500.1")
		f, err := s.Incrfloat("foo", 12)
		ok(t, err)
		equals(t, 512.1, f)
		s.CheckGet(t, "foo", "512.1")

		s.HSet("wrong", "aap", "noot")
		_, err = s.Incrfloat("wrong", 12)
		assert(t, err != nil, "do s.Incrfloat() error")
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"INCRBYFLOAT", "wrong", "400",
			proto.Error(msgWrongType),
		)
	}

	// Amount not a number
	mustDo(t, c,
		"INCRBYFLOAT", "key", "noint",
		proto.Error(msgInvalidFloat),
	)

	// Wrong usage
	{
		mustDo(t, c,
			"INCRBYFLOAT",
			proto.Error(errWrongNumber("incrbyfloat")),
		)
		mustDo(t, c,
			"INCRBYFLOAT", "another", "new", "key",
			proto.Error(errWrongNumber("incrbyfloat")),
		)
	}
}

func TestDecrBy(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Existing key
	{
		s.Set("foo", "12")
		mustDo(t, c,
			"DECRBY", "foo", "400",
			proto.Int(-388),
		)
		s.CheckGet(t, "foo", "-388")
	}

	// Existing key, not an integer
	{
		s.Set("foo", "noint")
		mustDo(t, c,
			"DECRBY", "foo", "400",
			proto.Error(msgInvalidInt),
		)
	}

	// New key
	{
		mustDo(t, c,
			"DECRBY", "bar", "4000",
			proto.Int(-4000),
		)
		s.CheckGet(t, "bar", "-4000")
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"DECRBY", "wrong", "400",
			proto.Error(msgWrongType),
		)
	}

	// Amount not an integer
	mustDo(t, c,
		"DECRBY", "key", "noint",
		proto.Error(msgInvalidInt),
	)

	// Wrong usage
	{
		mustDo(t, c,
			"DECRBY",
			proto.Error(errWrongNumber("decrby")),
		)
		mustDo(t, c,
			"DECRBY", "another", "new", "key",
			proto.Error(errWrongNumber("decrby")),
		)
	}
}

func TestDecr(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Existing key
	{
		s.Set("foo", "12")
		mustDo(t, c,
			"DECR", "foo",
			proto.Int(11),
		)
		s.CheckGet(t, "foo", "11")
	}

	// Existing key, not an integer
	{
		s.Set("foo", "noint")
		mustDo(t, c,
			"DECR", "foo",
			proto.Error(msgInvalidInt),
		)
	}

	// New key
	{
		mustDo(t, c,
			"DECR", "bar",
			proto.Int(-1),
		)
		s.CheckGet(t, "bar", "-1")
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"DECR", "wrong",
			proto.Error(msgWrongType),
		)
	}

	// Wrong usage
	{
		mustDo(t, c,
			"DECR",
			proto.Error(errWrongNumber("decr")),
		)
		mustDo(t, c,
			"DECR", "new", "key",
			proto.Error(errWrongNumber("decr")),
		)
	}

	// Direct one works
	{
		s.Set("aap", "400")
		s.Incr("aap", +42)
		s.CheckGet(t, "aap", "442")
	}
}

func TestGetSet(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Existing key
	{
		s.Set("foo", "bar")
		mustDo(t, c,
			"GETSET", "foo", "baz",
			proto.String("bar"),
		)
		s.CheckGet(t, "foo", "baz")
	}

	// New key
	{
		mustNil(t, c,
			"GETSET", "bar", "bak",
		)
		s.CheckGet(t, "bar", "bak")
	}

	// TTL needs to be cleared
	{
		s.Set("one", "two")
		s.SetTTL("one", time.Second*1234)
		mustDo(t, c,
			"GETSET", "one", "three",
			proto.String("two"),
		)
		s.CheckGet(t, "bar", "bak")
		equals(t, time.Duration(0), s.TTL("one"))
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"GETSET", "wrong", "key",
			proto.Error(msgWrongType),
		)
	}

	// Wrong usage
	{
		mustDo(t, c,
			"GETSET",
			proto.Error(errWrongNumber("getset")),
		)
		mustDo(t, c,
			"GETSET", "spurious", "arguments", "here",
			proto.Error(errWrongNumber("getset")),
		)
	}
}

func TestStrlen(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Existing key
	{
		s.Set("foo", "bar!")
		mustDo(t, c,
			"STRLEN", "foo",
			proto.Int(4),
		)
	}

	// New key
	{
		must0(t, c,
			"STRLEN", "nosuch",
		)
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"STRLEN", "wrong",
			proto.Error(msgWrongType),
		)
	}

	// Wrong usage
	{
		mustDo(t, c,
			"STRLEN",
			proto.Error(errWrongNumber("strlen")),
		)
		mustDo(t, c,
			"STRLEN", "spurious", "arguments",
			proto.Error(errWrongNumber("strlen")),
		)
	}
}

func TestAppend(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Existing key
	{
		s.Set("foo", "bar!")
		mustDo(t, c,
			"APPEND", "foo", "morebar",
			proto.Int(11),
		)
	}

	// New key
	mustDo(t, c,
		"APPEND", "bar", "was empty",
		proto.Int(9),
	)

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"APPEND", "wrong", "type",
			proto.Error(msgWrongType),
		)
	}

	// Wrong usage
	{
		mustDo(t, c,
			"APPEND",
			proto.Error(errWrongNumber("append")),
		)
		mustDo(t, c,
			"APPEND", "missing",
			proto.Error(errWrongNumber("append")),
		)
		mustDo(t, c,
			"APPEND", "spurious", "arguments", "!",
			proto.Error(errWrongNumber("append")),
		)
	}
}

func TestGetrange(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		s.Set("foo", "abcdefg")
		test := func(s, e int, res string) {
			t.Helper()
			mustDo(t, c,
				"GETRANGE", "foo", strconv.Itoa(s), strconv.Itoa(e),
				proto.String(res),
			)
		}
		test(0, 0, "a")
		test(0, 3, "abcd")
		test(0, 7, "abcdefg")
		test(0, 100, "abcdefg")
		test(1, 2, "bc")
		test(1, 100, "bcdefg")
		test(-4, -2, "def")
		test(0, -1, "abcdefg")
		test(0, -2, "abcdef")
		test(0, -100, "a") // Redis is funny
		test(-2, 2, "")
	}

	// New key
	mustDo(t, c,
		"GETRANGE", "bar", "0", "4",
		proto.String(""),
	)

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"GETRANGE", "wrong", "0", "0",
			proto.Error(msgWrongType),
		)
	}

	// Wrong usage
	{
		mustDo(t, c,
			"GETRANGE",
			proto.Error(errWrongNumber("getrange")),
		)
		mustDo(t, c,
			"GETRANGE", "missing",
			proto.Error(errWrongNumber("getrange")),
		)
		mustDo(t, c,
			"GETRANGE", "many", "spurious", "arguments", "!",
			proto.Error(errWrongNumber("getrange")),
		)
		mustDo(t, c,
			"GETRANGE", "many", "noint", "12",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"GETRANGE", "many", "12", "noint",
			proto.Error(msgInvalidInt),
		)
	}
}

func TestSetrange(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Simple case
	{
		s.Set("foo", "abcdefg")
		mustDo(t, c,
			"SETRANGE", "foo", "1", "bar",
			proto.Int(7),
		)
		s.CheckGet(t, "foo", "abarefg")
	}
	// Non existing key
	{
		mustDo(t, c,
			"SETRANGE", "nosuch", "3", "bar",
			proto.Int(6),
		)
		s.CheckGet(t, "nosuch", "\x00\x00\x00bar")
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"SETRANGE", "wrong", "0", "aap",
			proto.Error(msgWrongType),
		)
	}

	// Wrong usage
	{
		mustDo(t, c,
			"SETRANGE",
			proto.Error(errWrongNumber("setrange")),
		)
		mustDo(t, c,
			"SETRANGE", "missing",
			proto.Error(errWrongNumber("setrange")),
		)
		mustDo(t, c,
			"SETRANGE", "missing", "1",
			proto.Error(errWrongNumber("setrange")),
		)
		mustDo(t, c,
			"SETRANGE", "key", "noint", "",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"SETRANGE", "key", "-1", "",
			proto.Error("ERR offset is out of range"),
		)
		mustDo(t, c,
			"SETRANGE", "many", "12", "keys", "here",
			proto.Error(errWrongNumber("setrange")),
		)
	}
}

func TestBitcount(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		s.Set("countme", "a") // 'a' is 0x1100001
		mustDo(t, c,
			"BITCOUNT", "countme",
			proto.Int(3),
		)

		s.Set("countme", "aaaaa") // 'a' is 0x1100001
		mustDo(t, c,
			"BITCOUNT", "countme",
			proto.Int(3*5),
		)
	}
	// Non-existing
	must0(t, c,
		"BITCOUNT", "nosuch",
	)

	{
		// a: 0x1100001 - 3
		// b: 0x1100010 - 3
		// c: 0x1100011 - 4
		// d: 0x1100100 - 3
		s.Set("foo", "abcd")
		test := func(s, e, res int) {
			t.Helper()
			mustDo(t, c,
				"BITCOUNT", "foo", strconv.Itoa(s), strconv.Itoa(e),
				proto.Int(res),
			)
		}
		test(0, 0, 3)  // "a"
		test(0, 3, 13) // "abcd"
		test(2, -2, 4) // "c"
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"BITCOUNT", "wrong",
			proto.Error(msgWrongType),
		)
	}

	// Wrong usage
	{
		mustDo(t, c,
			"BITCOUNT",
			proto.Error(errWrongNumber("bitcount")),
		)
		mustDo(t, c,
			"BITCOUNT", "many", "spurious", "arguments", "!",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"BITCOUNT", "many", "noint", "12",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"BITCOUNT", "many", "12", "noint",
			proto.Error(msgInvalidInt),
		)
	}
}

func TestBitop(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		and := func(a, b byte) byte { return a & b }
		equals(t, []byte("`"), sliceBinOp(and, []byte("a"), []byte("b")))
		equals(t, []byte("`\000\000"), sliceBinOp(and, []byte("aaa"), []byte("b")))
		equals(t, []byte("`\000\000"), sliceBinOp(and, []byte("a"), []byte("bbb")))
		equals(t, []byte("``\000"), sliceBinOp(and, []byte("aa"), []byte("bbb")))
	}

	// Single char AND
	{
		s.Set("a", "a") // 'a' is 0x1100001
		s.Set("b", "b") // 'b' is 0x1100010
		mustDo(t, c,
			"BITOP", "AND", "bitand", "a", "b",
			proto.Int(1), // Length of the longest key
		)
		s.CheckGet(t, "bitand", "`")
	}
	// Multi char AND
	{
		s.Set("a", "aa")   // 'a' is 0x1100001
		s.Set("b", "bbbb") // 'b' is 0x1100010
		mustDo(t, c,
			"BITOP", "AND", "bitand", "a", "b",
			proto.Int(4), // Length of the longest key
		)
		s.CheckGet(t, "bitand", "``\000\000")
	}

	// Multi char OR
	{
		s.Set("a", "aa")   // 'a' is 0x1100001
		s.Set("b", "bbbb") // 'b' is 0x1100010
		mustDo(t, c,
			"BITOP", "OR", "bitor", "a", "b",
			proto.Int(4),
		)
		s.CheckGet(t, "bitor", "ccbb")
	}

	// Multi char XOR
	{
		s.Set("a", "aa")   // 'a' is 0x1100001
		s.Set("b", "bbbb") // 'b' is 0x1100010
		mustDo(t, c,
			"BITOP", "XOR", "bitxor", "a", "b",
			proto.Int(4),
		)
		s.CheckGet(t, "bitxor", "\x03\x03bb")
	}

	// Guess who's NOT like the other ops?
	{
		s.Set("a", "aa") // 'a' is 0x1100001
		mustDo(t, c,
			"BITOP", "NOT", "not", "a",
			proto.Int(2),
		)
		s.CheckGet(t, "not", "\x9e\x9e")
	}

	// Single argument. Works, just an roundabout copy.
	{
		s.Set("a", "a") // 'a' is 0x1100001
		mustDo(t, c,
			"BITOP", "AND", "copy", "a",
			proto.Int(1),
		)
		s.CheckGet(t, "copy", "a")
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"BITOP", "AND", "wrong",
			proto.Error(errWrongNumber("bitop")),
		)
	}

	// Wrong usage
	{
		mustDo(t, c,
			"BITOP",
			proto.Error(errWrongNumber("bitop")),
		)
		mustDo(t, c,
			"BITOP", "AND",
			proto.Error(errWrongNumber("bitop")),
		)
		mustDo(t, c,
			"BITOP", "WHAT",
			proto.Error(errWrongNumber("bitop")),
		)
		mustDo(t, c,
			"BITOP", "NOT",
			proto.Error(errWrongNumber("bitop")),
		)
		mustDo(t, c,
			"BITOP", "NOT", "foo", "bar", "baz",
			proto.Error("ERR BITOP NOT must be called with a single source key."),
		)
	}
}

func TestBitpos(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("basic", func(t *testing.T) {
		s.Set("findme", "\xff\xf0\x00")
		mustDo(t, c,
			"BITPOS", "findme", "0",
			proto.Int(12),
		)
		mustDo(t, c,
			"BITPOS", "findme", "0", "1",
			proto.Int(12),
		)
		mustDo(t, c,
			"BITPOS", "findme", "0", "1", "1",
			proto.Int(12),
		)

		must0(t, c,
			"BITPOS", "findme", "1",
		)
		mustDo(t, c,
			"BITPOS", "findme", "1", "1",
			proto.Int(8),
		)
		mustDo(t, c,
			"BITPOS", "findme", "1", "1", "2",
			proto.Int(8),
		)

		mustDo(t, c,
			"BITPOS", "findme", "1", "10000",
			proto.Int(-1),
		)
	})

	t.Run("substrings", func(t *testing.T) {
		s.Set("bin", string([]rune{rune(0b0000_0000), rune(0b0010_0000), rune(0b0001_0000)}))
		mustDo(t, c, "BITPOS", "bin", "1",
			proto.Int(10))
		mustDo(t, c, "BITPOS", "bin", "1", "1",
			proto.Int(10))
		mustDo(t, c, "BITPOS", "bin", "1", "1", "2",
			proto.Int(10))
		mustDo(t, c, "BITPOS", "bin", "1", "2", "2",
			proto.Int(19))
		mustDo(t, c, "BITPOS", "bin", "1", "0", "0",
			proto.Int(-1))
		mustDo(t, c, "BITPOS", "bin", "1", "0", "-1",
			proto.Int(10))
		mustDo(t, c, "BITPOS", "bin", "1", "0", "-2",
			proto.Int(10))
		mustDo(t, c, "BITPOS", "bin", "1", "0", "-3",
			proto.Int(-1))
		mustDo(t, c, "BITPOS", "bin", "0", "0", "-999",
			proto.Int(0))
		mustDo(t, c, "BITPOS", "bin", "1", "-1",
			proto.Int(19))
		mustDo(t, c, "BITPOS", "bin", "1", "-1", "-1",
			proto.Int(19))
		mustDo(t, c, "BITPOS", "bin", "1", "-1", "2",
			proto.Int(19))
		mustDo(t, c, "BITPOS", "bin", "1", "-2",
			proto.Int(10))
	})

	t.Run("only zeros", func(t *testing.T) {
		s.Set("zero", "\x00\x00")
		mustDo(t, c,
			"BITPOS", "zero", "1",
			proto.Int(-1),
		)
		must0(t, c,
			"BITPOS", "zero", "0",
		)

		// -end is ok
		mustDo(t, c,
			"BITPOS", "zero", "0", "0", "-100",
			proto.Int(0),
		)
	})

	t.Run("only ones", func(t *testing.T) {
		s.Set("one", "\xff\xff")
		mustDo(t, c,
			"BITPOS", "one", "1",
			proto.Int(0),
		)
		mustDo(t, c,
			"BITPOS", "one", "1", "1",
			proto.Int(8),
		)
		mustDo(t, c,
			"BITPOS", "one", "1", "2",
			proto.Int(-1),
		)
		mustDo(t, c,
			"BITPOS", "one", "0",
			proto.Int(16), // Special case
		)
		mustDo(t, c,
			"BITPOS", "one", "0", "1",
			proto.Int(16), // Special case
		)
		mustDo(t, c,
			"BITPOS", "one", "0", "0", "1",
			proto.Int(-1), // Counter the special case
		)
	})

	t.Run("non-existing", func(t *testing.T) {
		mustDo(t, c, "BITPOS", "nosuch", "1",
			proto.Int(-1),
		)
		mustDo(t, c, "BITPOS", "nosuch", "1", "0",
			proto.Int(-1),
		)
		mustDo(t, c, "BITPOS", "nosuch", "0",
			proto.Int(0),
		)
		mustDo(t, c, "BITPOS", "nosuch", "0", "0",
			proto.Int(0),
		)
	})

	t.Run("empty string", func(t *testing.T) {
		s.Set("empty", "")
		mustDo(t, c,
			"BITPOS", "empty", "1",
			proto.Int(-1),
		)
		mustDo(t, c,
			"BITPOS", "empty", "0",
			proto.Int(-1),
		)
		mustDo(t, c,
			"BITPOS", "empty", "0", "0",
			proto.Int(-1),
		)
		mustDo(t, c,
			"BITPOS", "empty", "0", "0", "0",
			proto.Int(-1),
		)
		mustDo(t, c,
			"BITPOS", "empty", "0", "0", "-1",
			proto.Int(-1),
		)
	})

	t.Run("wrong type", func(t *testing.T) {
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"BITPOS", "wrong", "1",
			proto.Error(msgWrongType),
		)
	})

	t.Run("wrong usage", func(t *testing.T) {
		mustDo(t, c,
			"BITPOS",
			proto.Error(errWrongNumber("bitpos")),
		)
		mustDo(t, c,
			"BITPOS", "many", "noint",
			proto.Error(msgInvalidInt),
		)
		mustDo(t, c,
			"BITPOS", "many",
			proto.Error(errWrongNumber("bitpos")),
		)
	})
}

func TestGetbit(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		s.Set("findme", "\x08")
		must0(t, c,
			"GETBIT", "findme", "0",
		)
		must1(t, c,
			"GETBIT", "findme", "4",
		)
		must0(t, c,
			"GETBIT", "findme", "5",
		)
	}

	// Non-existing
	{
		must0(t, c,
			"GETBIT", "nosuch", "1",
		)
		must0(t, c,
			"GETBIT", "nosuch", "1000",
		)
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"GETBIT", "wrong", "1",
			proto.Error(msgWrongType),
		)
	}

	// Wrong usage
	{
		mustDo(t, c,
			"GETBIT", "foo",
			proto.Error(errWrongNumber("getbit")),
		)
		mustDo(t, c,
			"GETBIT", "spurious", "arguments", "!",
			proto.Error(errWrongNumber("getbit")),
		)
		mustDo(t, c,
			"GETBIT", "many", "noint",
			proto.Error("ERR bit offset is not an integer or out of range"),
		)
	}
}

func TestSetbit(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		s.Set("findme", "\x08")
		must1(t, c,
			"SETBIT", "findme", "4", "0",
		)
		s.CheckGet(t, "findme", "\x00")

		must0(t, c,
			"SETBIT", "findme", "4", "1",
		)
		s.CheckGet(t, "findme", "\x08")
	}

	// Non-existing
	{
		must0(t, c,
			"SETBIT", "nosuch", "0", "1",
		)
		s.CheckGet(t, "nosuch", "\x80")
	}

	// Too short
	{
		s.Set("short", "\x00\x00")
		must0(t, c,
			"SETBIT", "short", "24", "0",
		)
		s.CheckGet(t, "short", "\x00\x00\x00\x00")
		must0(t, c,
			"SETBIT", "short", "32", "1",
		)
		s.CheckGet(t, "short", "\x00\x00\x00\x00\x80")
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		mustDo(t, c,
			"SETBIT", "wrong", "0", "1",
			proto.Error(msgWrongType),
		)
	}

	// Wrong usage
	{
		mustDo(t, c,
			"SETBIT", "foo",
			proto.Error(errWrongNumber("setbit")),
		)
		mustDo(t, c,
			"SETBIT", "spurious", "arguments", "!",
			proto.Error("ERR bit offset is not an integer or out of range"),
		)
		mustDo(t, c,
			"SETBIT", "many", "noint", "1",
			proto.Error("ERR bit offset is not an integer or out of range"),
		)
		mustDo(t, c,
			"SETBIT", "many", "1", "noint",
			proto.Error("ERR bit is not an integer or out of range"),
		)
		mustDo(t, c,
			"SETBIT", "many", "-3", "0",
			proto.Error("ERR bit offset is not an integer or out of range"),
		)
		mustDo(t, c,
			"SETBIT", "many", "3", "2",
			proto.Error("ERR bit is not an integer or out of range"),
		)
	}
}

func TestMsetnx(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		must1(t, c,
			"MSETNX", "aap", "noot", "mies", "vuur",
		)
		s.CheckGet(t, "aap", "noot")
		s.CheckGet(t, "mies", "vuur")
	}

	// A key exists.
	{
		must0(t, c,
			"MSETNX", "noaap", "noot", "mies", "vuur!",
		)
		equals(t, false, s.Exists("noaap"))
		s.CheckGet(t, "aap", "noot")
		s.CheckGet(t, "mies", "vuur")
	}

	// Other type of existing key
	{
		s.HSet("one", "two", "three")
		must0(t, c,
			"MSETNX", "one", "two", "three", "four!",
		)
		equals(t, false, s.Exists("three"))
	}

	// Wrong usage
	{
		mustDo(t, c,
			"MSETNX", "foo",
			proto.Error(errWrongNumber("msetnx")),
		)
		mustDo(t, c,
			"MSETNX", "odd", "arguments", "!",
			proto.Error("ERR wrong number of arguments for MSET"),
		)
		mustDo(t, c,
			"MSETNX",
			proto.Error(errWrongNumber("msetnx")),
		)
	}
}
