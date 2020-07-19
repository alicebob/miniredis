package miniredis

import (
	"strconv"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"

	"github.com/alicebob/miniredis/v2/proto"
)

// Test EXPIRE. Keys with an expiration are called volatile in Redis parlance.
func TestTTL(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Not volatile yet
	{
		equals(t, time.Duration(0), s.TTL("foo"))
		mustDo(t, c,
			"TTL", "foo",
			proto.Int(-2),
		)
	}

	// Set something
	{
		mustOK(t, c, "SET", "foo", "bar")
		// key exists, but no Expire set yet
		mustDo(t, c,
			"TTL", "foo",
			proto.Int(-1),
		)
		must1(t, c, "EXPIRE", "foo", "1200") // EXPIRE returns 1 on success
		mustDo(t, c,
			"TTL", "foo",
			proto.Int(1200),
		)
	}

	// A SET resets the expire.
	{
		mustOK(t, c, "SET", "foo", "bar")
		mustDo(t, c,
			"TTL", "foo",
			proto.Int(-1),
		)
	}

	// Set a non-existing key
	{
		must0(t, c, "EXPIRE", "nokey", "1200") // EXPIRE returns 0 on failure
	}

	// Remove an expire
	{

		// No key yet
		must0(t, c, "PERSIST", "exkey")

		mustOK(t, c, "SET", "exkey", "bar")

		// No timeout yet
		must0(t, c, "PERSIST", "exkey")

		must1(t, c, "EXPIRE", "exkey", "1200")

		// All fine now
		must1(t, c, "PERSIST", "exkey")

		// No TTL left
		mustDo(t, c,
			"TTL", "exkey",
			proto.Int(-1),
		)
	}

	// Hash key works fine, too
	{
		must1(t, c, "HSET", "wim", "zus", "jet")
		must1(t, c, "EXPIRE", "wim", "1234")
		mustDo(t, c,
			"EXPIRE", "wim", "1234",
			proto.Int(1),
		)
	}

	{
		mustOK(t, c, "SET", "wim", "zus")
		must1(t, c, "EXPIRE", "wim", "-1200")
		equals(t, false, s.Exists("wim"))
	}
}

func TestExpireat(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Not volatile yet
	{
		equals(t, time.Duration(0), s.TTL("foo"))
		mustDo(t, c,
			"TTL", "foo",
			proto.Int(-2),
		)
	}

	// Set something
	{
		mustOK(t, c, "SET", "foo", "bar")
		// Key exists, but no ttl set.
		mustDo(t, c,
			"TTL", "foo",
			proto.Int(-1),
		)

		now := 1234567890
		s.SetTime(time.Unix(int64(now), 0))
		must1(t, c, "EXPIREAT", "foo", strconv.Itoa(now+100)) // EXPIREAT returns 1 on success.

		equals(t, 100*time.Second, s.TTL("foo"))
		equals(t, 100*time.Second, s.TTL("foo"))
		mustDo(t, c, "TTL", "foo", proto.Int(100))
	}
}

func TestTouch(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Set something
	t.Run("basic", func(t *testing.T) {
		s.SetTime(time.Unix(1234567890, 0))
		mustOK(t, c, "SET", "foo", "bar", "EX", "100")
		mustOK(t, c, "SET", "baz", "qux", "EX", "100")

		// Touch one key
		must1(t, c, "TOUCH", "baz")

		// Touch multiple keys, "nay" doesn't exist
		mustDo(t, c,
			"TOUCH", "foo", "baz", "nay",
			proto.Int(2),
		)
	})

	t.Run("failure cases", func(t *testing.T) {
		mustDo(t, c,
			"TOUCH",
			proto.Error("ERR wrong number of arguments for 'touch' command"),
		)
	})

	t.Run("TTL unchanged", func(t *testing.T) {
		mustOK(t, c, "SET", "foo", "bar", "EX", "100")

		s.FastForward(time.Second * 99)
		equals(t, time.Second, s.TTL("foo"))

		must1(t, c, "TOUCH", "baz")
		equals(t, time.Second, s.TTL("foo"))
	})
}

func TestPexpireat(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Not volatile yet
	{
		equals(t, time.Duration(0), s.TTL("foo"))
		mustDo(t, c,
			"TTL", "foo",
			proto.Int(-2),
		)
	}

	// Set something
	{
		mustOK(t, c, "SET", "foo", "bar")
		// Key exists, but no ttl set.
		mustDo(t, c,
			"PTTL", "foo",
			proto.Int(-1),
		)

		now := 1234567890
		s.SetTime(time.Unix(int64(now), 0))
		must1(t, c, "PEXPIREAT", "foo", strconv.Itoa(now*1000+100)) // PEXPIREAT returns 1 on success.

		equals(t, 100*time.Millisecond, s.TTL("foo"))
		mustDo(t, c,
			"PTTL", "foo",
			proto.Int(100),
		)
	}
}

func TestPexpire(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("key exists", func(t *testing.T) {
		ok(t, s.Set("foo", "bar"))
		must1(t, c, "PEXPIRE", "foo", "12")

		mustDo(t, c,
			"PTTL", "foo",
			proto.Int(12),
		)
		equals(t, 12*time.Millisecond, s.TTL("foo"))
	})

	t.Run("no such key", func(t *testing.T) {
		must0(t, c, "PEXPIRE", "nosuch", "12")
		mustDo(t, c,
			"PTTL", "nosuch",
			proto.Int(-2),
		)
	})

	t.Run("no expire", func(t *testing.T) {
		s.Set("aap", "noot")
		mustDo(t, c,
			"PTTL", "aap",
			proto.Int(-1),
		)
	})
}

func TestDel(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("simple", func(t *testing.T) {
		s.Set("foo", "bar")
		s.HSet("aap", "noot", "mies")
		s.Set("one", "two")
		s.SetTTL("one", time.Second*1234)
		s.Set("three", "four")
		mustDo(t, c,
			"DEL", "one", "aap", "nosuch",
			proto.Int(2),
		)
		equals(t, time.Duration(0), s.TTL("one"))
	})

	t.Run("failure cases", func(t *testing.T) {
		mustDo(t, c,
			"DEL",
			proto.Error("ERR wrong number of arguments for 'del' command"),
		)
	})

	t.Run("direct", func(t *testing.T) {
		s.Set("foo", "bar")
		s.Del("foo")
		got, err := s.Get("foo")
		equals(t, ErrKeyNotFound, err)
		equals(t, "", got)
	})
}

func TestUnlink(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("simple", func(t *testing.T) {
		s.Set("foo", "bar")
		s.HSet("aap", "noot", "mies")
		s.Set("one", "two")
		s.SetTTL("one", time.Second*1234)
		s.Set("three", "four")
		mustDo(t, c,
			"UNLINK", "one", "aap", "nosuch",
			proto.Int(2),
		)
		equals(t, time.Duration(0), s.TTL("one"))
	})

	t.Run("direct", func(t *testing.T) {
		s.Set("foo", "bar")
		s.Unlink("foo")
		got, err := s.Get("foo")
		equals(t, ErrKeyNotFound, err)
		equals(t, "", got)
	})
}

func TestType(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.Set("foo", "bar!")
	t.Run("string", func(t *testing.T) {
		mustDo(t, c,
			"TYPE", "foo",
			proto.Inline("string"),
		)
	})

	s.HSet("aap", "noot", "mies")
	t.Run("hash", func(t *testing.T) {
		mustDo(t, c,
			"TYPE", "aap",
			proto.Inline("hash"),
		)
	})

	t.Run("no such key", func(t *testing.T) {
		mustDo(t, c,
			"TYPE", "nosuch",
			proto.Inline("none"),
		)
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"TYPE",
			proto.Error("usage error"),
		)
		mustDo(t, c,
			"TYPE", "spurious", "arguments",
			proto.Error("usage error"),
		)
	})

	t.Run("direct", func(t *testing.T) {
		equals(t, "hash", s.Type("aap"))
		equals(t, "", s.Type("nokey"))
	})
}

func TestExists(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("string", func(t *testing.T) {
		s.Set("foo", "bar!")
		must1(t, c, "EXISTS", "foo")
	})

	t.Run("hash", func(t *testing.T) {
		s.HSet("aap", "noot", "mies")
		must1(t, c, "EXISTS", "aap")
	})

	t.Run("multiple keys", func(t *testing.T) {
		mustDo(t, c,
			"EXISTS", "foo", "aap",
			proto.Int(2),
		)

		mustDo(t, c,
			"EXISTS", "foo", "noot", "aap",
			proto.Int(2),
		)
	})

	t.Run("nosuch keys", func(t *testing.T) {
		must0(t, c, "EXISTS", "nosuch")
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"EXISTS",
			proto.Error(errWrongNumber("exists")),
		)
	})

	t.Run("direct", func(t *testing.T) {
		equals(t, true, s.Exists("aap"))
		equals(t, false, s.Exists("nokey"))
	})
}

func TestMove(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// No problem.
	{
		s.Set("foo", "bar!")
		v, err := redis.Int(c.Do("MOVE", "foo", 1))
		ok(t, err)
		equals(t, 1, v)
	}

	// Src key doesn't exists.
	{
		v, err := redis.Int(c.Do("MOVE", "nosuch", 1))
		ok(t, err)
		equals(t, 0, v)
	}

	// Target key already exists.
	{
		s.DB(0).Set("two", "orig")
		s.DB(1).Set("two", "taken")
		v, err := redis.Int(c.Do("MOVE", "two", 1))
		ok(t, err)
		equals(t, 0, v)
		s.CheckGet(t, "two", "orig")
	}

	// TTL is also moved
	{
		s.DB(0).Set("one", "two")
		s.DB(0).SetTTL("one", time.Second*4242)
		v, err := redis.Int(c.Do("MOVE", "one", 1))
		ok(t, err)
		equals(t, 1, v)
		equals(t, s.DB(1).TTL("one"), time.Second*4242)
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("MOVE"))
		assert(t, err != nil, "do MOVE error")
		_, err = redis.Int(c.Do("MOVE", "foo"))
		assert(t, err != nil, "do MOVE error")
		_, err = redis.Int(c.Do("MOVE", "foo", "noint"))
		assert(t, err != nil, "do MOVE error")
		_, err = redis.Int(c.Do("MOVE", "foo", 2, "toomany"))
		assert(t, err != nil, "do MOVE error")
	}
}

func TestKeys(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.Set("foo", "bar!")
	s.Set("foobar", "bar!")
	s.Set("barfoo", "bar!")
	s.Set("fooooo", "bar!")

	{
		v, err := redis.Strings(c.Do("KEYS", "foo"))
		ok(t, err)
		equals(t, []string{"foo"}, v)
	}

	// simple '*'
	{
		v, err := redis.Strings(c.Do("KEYS", "foo*"))
		ok(t, err)
		equals(t, []string{"foo", "foobar", "fooooo"}, v)
	}
	// simple '?'
	{
		v, err := redis.Strings(c.Do("KEYS", "fo?"))
		ok(t, err)
		equals(t, []string{"foo"}, v)
	}

	// Don't die on never-matching pattern.
	{
		v, err := redis.Strings(c.Do("KEYS", `f\`))
		ok(t, err)
		equals(t, []string{}, v)
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("KEYS"))
		assert(t, err != nil, "do KEYS error")
		_, err = redis.Int(c.Do("KEYS", "foo", "noint"))
		assert(t, err != nil, "do KEYS error")
	}
}

func TestRandom(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Empty db.
	{
		v, err := c.Do("RANDOMKEY")
		ok(t, err)
		equals(t, nil, v)
	}

	s.Set("one", "bar!")
	s.Set("two", "bar!")
	s.Set("three", "bar!")

	// No idea which key will be returned.
	{
		v, err := redis.String(c.Do("RANDOMKEY"))
		ok(t, err)
		assert(t, v == "one" || v == "two" || v == "three", "RANDOMKEY looks sane")
	}

	// Wrong usage
	{
		_, err = redis.Int(c.Do("RANDOMKEY", "spurious"))
		assert(t, err != nil, "do RANDOMKEY error")
	}
}

func TestRename(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Non-existing key
	{
		_, err := redis.Int(c.Do("RENAME", "nosuch", "to"))
		assert(t, err != nil, "do RENAME error")
	}

	// Same key
	{
		_, err := redis.Int(c.Do("RENAME", "from", "from"))
		assert(t, err != nil, "do RENAME error")
	}

	// Move a string key
	{
		s.Set("from", "value")
		str, err := redis.String(c.Do("RENAME", "from", "to"))
		ok(t, err)
		equals(t, "OK", str)
		equals(t, false, s.Exists("from"))
		equals(t, true, s.Exists("to"))
		s.CheckGet(t, "to", "value")
		_, ok := s.dbs[0].ttl["to"]
		equals(t, ok, false)
	}

	// Move a hash key
	{
		s.HSet("from", "key", "value")
		str, err := redis.String(c.Do("RENAME", "from", "to"))
		ok(t, err)
		equals(t, "OK", str)
		equals(t, false, s.Exists("from"))
		equals(t, true, s.Exists("to"))
		equals(t, "value", s.HGet("to", "key"))
		_, ok := s.dbs[0].ttl["to"]
		equals(t, ok, false)
	}

	// Ensure dest ttl nil if source does not have a ttl

	{
		s.Set("TTLfrom", "value")
		s.Set("TTLto", "value")
		s.SetTTL("TTLto", time.Second*99999)
		equals(t, time.Second*99999, s.TTL("TTLto"))
		str, err := redis.String(c.Do("RENAME", "TTLfrom", "TTLto"))
		ok(t, err)
		equals(t, "OK", str)
		_, ok := s.dbs[0].ttl["TTLto"]
		equals(t, ok, false)
	}

	// Move over something which exists
	{
		s.Set("from", "string value")
		s.HSet("to", "key", "value")
		s.SetTTL("from", time.Second*999999)

		str, err := redis.String(c.Do("RENAME", "from", "to"))
		ok(t, err)
		equals(t, "OK", str)
		equals(t, false, s.Exists("from"))
		equals(t, true, s.Exists("to"))
		s.CheckGet(t, "to", "string value")
		equals(t, time.Duration(0), s.TTL("from"))
		equals(t, time.Second*999999, s.TTL("to"))
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("RENAME"))
		assert(t, err != nil, "do RENAME error")
		_, err = redis.Int(c.Do("RENAME", "too few"))
		assert(t, err != nil, "do RENAME error")
		_, err = redis.Int(c.Do("RENAME", "some", "spurious", "arguments"))
		assert(t, err != nil, "do RENAME error")
	}
}

func TestScan(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// We cheat with scan. It always returns everything.

	s.Set("key", "value")

	// No problem
	{
		res, err := redis.Values(c.Do("SCAN", 0))
		ok(t, err)
		equals(t, 2, len(res))

		var c int
		var keys []string
		_, err = redis.Scan(res, &c, &keys)
		ok(t, err)
		equals(t, 0, c)
		equals(t, []string{"key"}, keys)
	}

	// Invalid cursor
	{
		res, err := redis.Values(c.Do("SCAN", 42))
		ok(t, err)
		equals(t, 2, len(res))

		var c int
		var keys []string
		_, err = redis.Scan(res, &c, &keys)
		ok(t, err)
		equals(t, 0, c)
		equals(t, []string(nil), keys)
	}

	// COUNT (ignored)
	{
		res, err := redis.Values(c.Do("SCAN", 0, "COUNT", 200))
		ok(t, err)
		equals(t, 2, len(res))

		var c int
		var keys []string
		_, err = redis.Scan(res, &c, &keys)
		ok(t, err)
		equals(t, 0, c)
		equals(t, []string{"key"}, keys)
	}

	// MATCH
	{
		s.Set("aap", "noot")
		s.Set("mies", "wim")
		res, err := redis.Values(c.Do("SCAN", 0, "MATCH", "mi*"))
		ok(t, err)
		equals(t, 2, len(res))

		var c int
		var keys []string
		_, err = redis.Scan(res, &c, &keys)
		ok(t, err)
		equals(t, 0, c)
		equals(t, []string{"mies"}, keys)
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("SCAN"))
		assert(t, err != nil, "do SCAN error")
		_, err = redis.Int(c.Do("SCAN", "noint"))
		assert(t, err != nil, "do SCAN error")
		_, err = redis.Int(c.Do("SCAN", 1, "MATCH"))
		assert(t, err != nil, "do SCAN error")
		_, err = redis.Int(c.Do("SCAN", 1, "COUNT"))
		assert(t, err != nil, "do SCAN error")
		_, err = redis.Int(c.Do("SCAN", 1, "COUNT", "noint"))
		assert(t, err != nil, "do SCAN error")
	}
}

func TestRenamenx(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Non-existing key
	{
		_, err := redis.Int(c.Do("RENAMENX", "nosuch", "to"))
		assert(t, err != nil, "do RENAMENX error")
	}

	// Same key
	{
		_, err := redis.Int(c.Do("RENAMENX", "from", "from"))
		assert(t, err != nil, "do RENAMENX error")
	}

	// Move a string key
	{
		s.Set("from", "value")
		n, err := redis.Int(c.Do("RENAMENX", "from", "to"))
		ok(t, err)
		equals(t, 1, n)
		equals(t, false, s.Exists("from"))
		equals(t, true, s.Exists("to"))
		s.CheckGet(t, "to", "value")
	}

	// Move over something which exists
	{
		s.Set("from", "string value")
		s.Set("to", "value")

		n, err := redis.Int(c.Do("RENAMENX", "from", "to"))
		ok(t, err)
		equals(t, 0, n)
		equals(t, true, s.Exists("from"))
		equals(t, true, s.Exists("to"))
		s.CheckGet(t, "from", "string value")
		s.CheckGet(t, "to", "value")
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("RENAMENX"))
		assert(t, err != nil, "do RENAMENX error")
		_, err = redis.Int(c.Do("RENAMENX", "too few"))
		assert(t, err != nil, "do RENAMENX error")
		_, err = redis.Int(c.Do("RENAMENX", "some", "spurious", "arguments"))
		assert(t, err != nil, "do RENAMENX error")
	}
}
