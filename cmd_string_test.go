package miniredis

import (
	"testing"

	"github.com/garyburd/redigo/redis"
)

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

	// GET a non-existing key. Should be nil.
	{
		b, err := c.Do("GET", "reallynosuchkey")
		ok(t, err)
		equals(t, nil, b)
	}

	// Wrong usage.
	{
		_, err := c.Do("HSET", "wim", "zus", "jet")
		ok(t, err)
		_, err = c.Do("GET", "wim")
		assert(t, err != nil, "no GET error")
	}
}

func TestSet(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Simple case
	{
		v, err := redis.String(c.Do("SET", "aap", "noot"))
		ok(t, err)
		equals(t, "OK", v)
	}

	// Overwrite other types.
	{
		s.HSet("wim", "teun", "vuur")
		v, err := redis.String(c.Do("SET", "wim", "gijs"))
		ok(t, err)
		equals(t, "OK", v)
		equals(t, "gijs", s.Get("wim"))
	}

	// NX argument
	{
		// new key
		v, err := redis.String(c.Do("SET", "mies", "toon", "NX"))
		ok(t, err)
		equals(t, "OK", v)
		// now existing key
		nx, err := c.Do("SET", "mies", "toon", "NX")
		ok(t, err)
		equals(t, nil, nx)
		// lowercase NX is no problem
		nx, err = c.Do("SET", "mies", "toon", "nx")
		ok(t, err)
		equals(t, nil, nx)
	}

	// XX argument - only set if exists
	{
		// new key, no go
		v, err := c.Do("SET", "one", "two", "XX")
		ok(t, err)
		equals(t, nil, v)

		s.Set("one", "three")

		v, err = c.Do("SET", "one", "two", "XX")
		ok(t, err)
		equals(t, "OK", v)
		equals(t, "two", s.Get("one"))

		// XX with another key type
		s.HSet("eleven", "twelve", "thirteen")
		h, err := redis.String(c.Do("SET", "eleven", "fourteen", "XX"))
		ok(t, err)
		equals(t, "OK", h)
		equals(t, "fourteen", s.Get("eleven"))
	}

	// EX or PX argument. Expire values.
	{
		v, err := c.Do("SET", "one", "two", "EX", 1299)
		ok(t, err)
		equals(t, "OK", v)
		equals(t, "two", s.Get("one"))
		equals(t, 1299, s.Expire("one"))

		v, err = c.Do("SET", "three", "four", "PX", 8888)
		ok(t, err)
		equals(t, "OK", v)
		equals(t, "four", s.Get("three"))
		equals(t, 8888, s.Expire("three"))

		_, err = c.Do("SET", "one", "two", "EX", "notimestamp")
		assert(t, err != nil, "no SET error on invalid EX")

		_, err = c.Do("SET", "one", "two", "EX")
		assert(t, err != nil, "no SET error on missing EX argument")
	}

	// Invalid argument
	{
		_, err := c.Do("SET", "one", "two", "FOO")
		assert(t, err != nil, "no SET error")
	}
}

func TestMget(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.Set("zus", "jet")
	s.Set("teun", "vuur")
	s.Set("gijs", "lam")
	s.Set("kees", "bok")
	{
		v, err := redis.Values(c.Do("MGET", "zus", "nosuch", "kees"))
		ok(t, err)
		equals(t, 3, len(v))
		equals(t, "jet", string(v[0].([]byte)))
		equals(t, nil, v[1])
		equals(t, "bok", string(v[2].([]byte)))
	}

	// Wrong key type returns nil
	{
		s.HSet("aap", "foo", "bar")
		v, err := redis.Values(c.Do("MGET", "aap"))
		ok(t, err)
		equals(t, 1, len(v))
		equals(t, nil, v[0])
	}
}

func TestMset(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	{
		v, err := redis.String(c.Do("MSET", "zus", "jet", "teun", "vuur", "gijs", "lam"))
		ok(t, err)
		equals(t, "OK", v)
		equals(t, "jet", s.Get("zus"))
		equals(t, "vuur", s.Get("teun"))
		equals(t, "lam", s.Get("gijs"))
	}

	// Other types are overwritten
	{
		s.HSet("aap", "foo", "bar")
		v, err := redis.String(c.Do("MSET", "aap", "jet"))
		ok(t, err)
		equals(t, "OK", v)
		equals(t, "jet", s.Get("aap"))
	}

	// Odd argument list is not OK
	{
		_, err := redis.String(c.Do("MSET", "zus", "jet", "teun"))
		assert(t, err != nil, "No MSET error")
	}

	// TTL is cleared
	{
		s.Set("foo", "bar")
		s.HSet("aap", "foo", "bar") // even for weird keys.
		s.SetExpire("aap", 999)
		s.SetExpire("foo", 999)
		v, err := redis.String(c.Do("MSET", "aap", "noot", "foo", "baz"))
		ok(t, err)
		equals(t, "OK", v)
		equals(t, 0, s.Expire("aap"))
		equals(t, 0, s.Expire("foo"))
	}
}

func TestSetex(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Usual case
	{
		v, err := redis.String(c.Do("SETEX", "aap", 1234, "noot"))
		ok(t, err)
		equals(t, "OK", v)
		equals(t, "noot", s.Get("aap"))
		equals(t, 1234, s.Expire("aap"))
	}

	// Same thing
	{
		_, err := redis.String(c.Do("SETEX", "aap", "1234", "noot"))
		ok(t, err)
	}

	// Invalid TTL
	{
		_, err := redis.String(c.Do("SETEX", "aap", "nottl", "noot"))
		assert(t, err != nil, "no SETEX error")
	}
}

func TestSetnx(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Existing key
	{
		s.Set("foo", "bar")
		v, err := redis.Int(c.Do("SETNX", "foo", "not bar"))
		ok(t, err)
		equals(t, 0, v)
		equals(t, "bar", s.Get("foo"))
	}

	// New key
	{
		v, err := redis.Int(c.Do("SETNX", "notfoo", "also not bar"))
		ok(t, err)
		equals(t, 1, v)
		equals(t, "also not bar", s.Get("notfoo"))
	}

	// Existing key of a different type
	{
		s.HSet("foo", "bar", "baz")
		v, err := redis.Int(c.Do("SETNX", "foo", "not bar"))
		ok(t, err)
		equals(t, 0, v)
		equals(t, "bar", s.Get("foo"))

	}
}

func TestIncr(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Existing key
	{
		s.Set("foo", "12")
		v, err := redis.Int(c.Do("INCR", "foo"))
		ok(t, err)
		equals(t, 13, v)
		equals(t, "13", s.Get("foo"))
	}

	// Existing key, not an integer
	{
		s.Set("foo", "noint")
		_, err := redis.Int(c.Do("INCR", "foo"))
		assert(t, err != nil, "do INCR error")
	}

	// New key
	{
		v, err := redis.Int(c.Do("INCR", "bar"))
		ok(t, err)
		equals(t, 1, v)
		equals(t, "1", s.Get("bar"))
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		_, err := redis.Int(c.Do("INCR", "wrong"))
		assert(t, err != nil, "do INCR error")
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("INCR"))
		assert(t, err != nil, "do INCR error")
		_, err = redis.Int(c.Do("INCR", "new", "key"))
		assert(t, err != nil, "do INCR error")
	}
}

func TestIncrBy(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Existing key
	{
		s.Set("foo", "12")
		v, err := redis.Int(c.Do("INCRBY", "foo", "400"))
		ok(t, err)
		equals(t, 412, v)
		equals(t, "412", s.Get("foo"))
	}

	// Existing key, not an integer
	{
		s.Set("foo", "noint")
		_, err := redis.Int(c.Do("INCRBY", "foo", "400"))
		assert(t, err != nil, "do INCRBY error")
	}

	// New key
	{
		v, err := redis.Int(c.Do("INCRBY", "bar", "4000"))
		ok(t, err)
		equals(t, 4000, v)
		equals(t, "4000", s.Get("bar"))
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		_, err := redis.Int(c.Do("INCRBY", "wrong", "400"))
		assert(t, err != nil, "do INCRBY error")
	}

	// Amount not an interger
	{
		_, err := redis.Int(c.Do("INCRBY", "key", "noint"))
		assert(t, err != nil, "do INCRBY error")
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("INCRBY"))
		assert(t, err != nil, "do INCRBY error")
		_, err = redis.Int(c.Do("INCRBY", "another", "new", "key"))
		assert(t, err != nil, "do INCRBY error")
	}
}

func TestDecrBy(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Existing key
	{
		s.Set("foo", "12")
		v, err := redis.Int(c.Do("DECRBY", "foo", "400"))
		ok(t, err)
		equals(t, -388, v)
		equals(t, "-388", s.Get("foo"))
	}

	// Existing key, not an integer
	{
		s.Set("foo", "noint")
		_, err := redis.Int(c.Do("DECRBY", "foo", "400"))
		assert(t, err != nil, "do DECRBY error")
	}

	// New key
	{
		v, err := redis.Int(c.Do("DECRBY", "bar", "4000"))
		ok(t, err)
		equals(t, -4000, v)
		equals(t, "-4000", s.Get("bar"))
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		_, err := redis.Int(c.Do("DECRBY", "wrong", "400"))
		assert(t, err != nil, "do DECRBY error")
	}

	// Amount not an interger
	{
		_, err := redis.Int(c.Do("DECRBY", "key", "noint"))
		assert(t, err != nil, "do DECRBY error")
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("DECRBY"))
		assert(t, err != nil, "do DECRBY error")
		_, err = redis.Int(c.Do("DECRBY", "another", "new", "key"))
		assert(t, err != nil, "do DECRBY error")
	}
}

func TestDecr(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Existing key
	{
		s.Set("foo", "12")
		v, err := redis.Int(c.Do("DECR", "foo"))
		ok(t, err)
		equals(t, 11, v)
		equals(t, "11", s.Get("foo"))
	}

	// Existing key, not an integer
	{
		s.Set("foo", "noint")
		_, err := redis.Int(c.Do("DECR", "foo"))
		assert(t, err != nil, "do DECR error")
	}

	// New key
	{
		v, err := redis.Int(c.Do("DECR", "bar"))
		ok(t, err)
		equals(t, -1, v)
		equals(t, "-1", s.Get("bar"))
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		_, err := redis.Int(c.Do("DECR", "wrong"))
		assert(t, err != nil, "do DECR error")
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("DECR"))
		assert(t, err != nil, "do DECR error")
		_, err = redis.Int(c.Do("DECR", "new", "key"))
		assert(t, err != nil, "do DECR error")
	}

	// Direct one works
	{
		s.Set("aap", "400")
		s.Incr("aap", +42)
		equals(t, "442", s.Get("aap"))
	}
}

func TestGetSet(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Existing key
	{
		s.Set("foo", "bar")
		v, err := redis.String(c.Do("GETSET", "foo", "baz"))
		ok(t, err)
		equals(t, "bar", v)
		equals(t, "baz", s.Get("foo"))
	}

	// New key
	{
		v, err := c.Do("GETSET", "bar", "bak")
		ok(t, err)
		equals(t, nil, v)
		equals(t, "bak", s.Get("bar"))
	}

	// TTL needs to be cleared
	{
		s.Set("one", "two")
		s.SetExpire("one", 1234)
		v, err := redis.String(c.Do("GETSET", "one", "three"))
		ok(t, err)
		equals(t, "two", v)
		equals(t, "bak", s.Get("bar"))
		equals(t, 0, s.Expire("one"))
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		_, err := redis.Int(c.Do("GETSET", "wrong", "key"))
		assert(t, err != nil, "do GETSET error")
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("GETSET"))
		assert(t, err != nil, "do GETSET error")
		_, err = redis.Int(c.Do("GETSET", "spurious", "arguments", "here"))
		assert(t, err != nil, "do GETSET error")
	}
}

func TestStrlen(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Existing key
	{
		s.Set("foo", "bar!")
		v, err := redis.Int(c.Do("STRLEN", "foo"))
		ok(t, err)
		equals(t, 4, v)
	}

	// New key
	{
		v, err := redis.Int(c.Do("STRLEN", "nosuch"))
		ok(t, err)
		equals(t, 0, v)
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		_, err := redis.Int(c.Do("STRLEN", "wrong"))
		assert(t, err != nil, "do STRLEN error")
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("STRLEN"))
		assert(t, err != nil, "do STRLEN error")
		_, err = redis.Int(c.Do("STRLEN", "spurious", "arguments"))
		assert(t, err != nil, "do STRLEN error")
	}
}

func TestAppend(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Existing key
	{
		s.Set("foo", "bar!")
		v, err := redis.Int(c.Do("APPEND", "foo", "morebar"))
		ok(t, err)
		equals(t, 11, v)
	}

	// New key
	{
		v, err := redis.Int(c.Do("APPEND", "bar", "was empty"))
		ok(t, err)
		equals(t, 9, v)
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		_, err := redis.Int(c.Do("APPEND", "wrong", "type"))
		assert(t, err != nil, "do APPEND error")
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("APPEND"))
		assert(t, err != nil, "do APPEND error")
		_, err = redis.Int(c.Do("APPEND", "missing"))
		assert(t, err != nil, "do APPEND error")
		_, err = redis.Int(c.Do("APPEND", "spurious", "arguments", "!"))
		assert(t, err != nil, "do APPEND error")
	}
}

func TestGetrange(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	{
		s.Set("foo", "abcdefg")
		type tc struct {
			s   int
			e   int
			res string
		}
		for _, p := range []tc{
			{0, 0, "a"},
			{0, 3, "abcd"},
			{0, 100, "abcdefg"},
			{1, 2, "bc"},
			{1, 100, "bcdefg"},
			{0, -1, "abcdefg"},
			{0, -2, "abcdef"},
			{0, -100, ""},
		} {
			{

				v, err := redis.String(c.Do("GETRANGE", "foo", p.s, p.e))
				ok(t, err)
				equals(t, p.res, v)
			}
		}
	}

	// New key
	{
		v, err := redis.String(c.Do("GETRANGE", "bar", 0, 4))
		ok(t, err)
		equals(t, "", v)
	}

	// Wrong type of existing key
	{
		s.HSet("wrong", "aap", "noot")
		_, err := redis.Int(c.Do("GETRANGE", "wrong", 0, 0))
		assert(t, err != nil, "do APPEND error")
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("GETRANGE"))
		assert(t, err != nil, "do GETRANGE error")
		_, err = redis.Int(c.Do("GETRANGE", "missing"))
		assert(t, err != nil, "do GETRANGE error")
		_, err = redis.Int(c.Do("GETRANGE", "many", "spurious", "arguments", "!"))
		assert(t, err != nil, "do GETRANGE error")
		_, err = redis.Int(c.Do("GETRANGE", "many", "noint", 12))
		assert(t, err != nil, "do GETRANGE error")
		_, err = redis.Int(c.Do("GETRANGE", "many", 12, "noint"))
		assert(t, err != nil, "do GETRANGE error")
	}
}
