package miniredis

import (
	"strconv"
	"testing"
	"time"

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
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// No problem.
	{
		s.Set("foo", "bar!")
		must1(t, c, "MOVE", "foo", "1")
	}

	// Src key doesn't exists.
	{
		must0(t, c, "MOVE", "nosuch", "1")
	}

	// Target key already exists.
	{
		s.DB(0).Set("two", "orig")
		s.DB(1).Set("two", "taken")
		must0(t, c, "MOVE", "two", "1")
		s.CheckGet(t, "two", "orig")
	}

	// TTL is also moved
	{
		s.DB(0).Set("one", "two")
		s.DB(0).SetTTL("one", time.Second*4242)
		must1(t, c, "MOVE", "one", "1")
		equals(t, s.DB(1).TTL("one"), time.Second*4242)
	}

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"MOVE",
			proto.Error(errWrongNumber("move")),
		)
		mustDo(t, c,
			"MOVE", "foo",
			proto.Error(errWrongNumber("move")),
		)
		mustDo(t, c,
			"MOVE", "foo", "noint",
			proto.Error("ERR source and destination objects are the same"),
		)
		mustDo(t, c,
			"MOVE", "foo", "2", "toomany",
			proto.Error(errWrongNumber("move")),
		)
	})
}

func TestKeys(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.Set("foo", "bar!")
	s.Set("foobar", "bar!")
	s.Set("barfoo", "bar!")
	s.Set("fooooo", "bar!")

	mustDo(t, c,
		"KEYS", "foo",
		proto.Strings("foo"),
	)

	// simple '*'
	mustDo(t, c,
		"KEYS", "foo*",
		proto.Strings("foo", "foobar", "fooooo"),
	)

	// simple '?'
	mustDo(t, c,
		"KEYS", "fo?",
		proto.Strings("foo"),
	)

	// Don't die on never-matching pattern.
	mustDo(t, c,
		"KEYS", `f\`,
		proto.Strings(),
	)

	t.Run("error", func(t *testing.T) {
		mustDo(t, c,
			"KEYS",
			proto.Error(errWrongNumber("keys")),
		)
		mustDo(t, c,
			"KEYS", "foo", "noint",
			proto.Error(errWrongNumber("keys")),
		)
	})
}

func TestRandom(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Empty db.
	mustNil(t, c, "RANDOMKEY")

	s.Set("one", "bar!")
	s.Set("two", "bar!")
	s.Set("three", "bar!")

	// No idea which key will be returned.
	{
		v, err := c.Do("RANDOMKEY")
		ok(t, err)
		assert(t, v == proto.String("one") || v == proto.String("two") || v == proto.String("three"), "RANDOMKEY looks sane")
	}

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"RANDOMKEY", "spurious",
			proto.Error(errWrongNumber("randomkey")),
		)
	})
}

func TestRename(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Non-existing key
	mustDo(t, c,
		"RENAME", "nosuch", "to",
		proto.Error("ERR no such key"),
	)

	// Same key
	mustDo(t, c,
		"RENAME", "from", "from",
		proto.Error("ERR no such key"),
	)

	t.Run("string key", func(t *testing.T) {
		s.Set("from", "value")
		mustOK(t, c, "RENAME", "from", "to")
		equals(t, false, s.Exists("from"))
		equals(t, true, s.Exists("to"))
		s.CheckGet(t, "to", "value")
		_, ok := s.dbs[0].ttl["to"]
		equals(t, ok, false)
	})

	t.Run("hash key", func(t *testing.T) {
		s.HSet("from", "key", "value")
		mustOK(t, c, "RENAME", "from", "to")
		equals(t, false, s.Exists("from"))
		equals(t, true, s.Exists("to"))
		equals(t, "value", s.HGet("to", "key"))
		_, ok := s.dbs[0].ttl["to"]
		equals(t, ok, false)
	})

	t.Run("ttl", func(t *testing.T) {
		s.Set("TTLfrom", "value")
		s.Set("TTLto", "value")
		s.SetTTL("TTLto", time.Second*99999)
		equals(t, time.Second*99999, s.TTL("TTLto"))
		mustOK(t, c, "RENAME", "TTLfrom", "TTLto")
		_, ok := s.dbs[0].ttl["TTLto"]
		equals(t, ok, false)
	})

	t.Run("overwrite", func(t *testing.T) {
		s.Set("from", "string value")
		s.HSet("to", "key", "value")
		s.SetTTL("from", time.Second*999999)

		mustOK(t, c, "RENAME", "from", "to")
		equals(t, false, s.Exists("from"))
		equals(t, true, s.Exists("to"))
		s.CheckGet(t, "to", "string value")
		equals(t, time.Duration(0), s.TTL("from"))
		equals(t, time.Second*999999, s.TTL("to"))
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"RENAME",
			proto.Error(errWrongNumber("rename")),
		)
		mustDo(t, c,
			"RENAME", "too few",
			proto.Error(errWrongNumber("rename")),
		)
		mustDo(t, c,
			"RENAME", "some", "spurious", "arguments",
			proto.Error(errWrongNumber("rename")),
		)
	})
}

func TestScan(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// We cheat with scan. It always returns everything.

	s.Set("key", "value")

	t.Run("no problem", func(t *testing.T) {
		mustDo(t, c,
			"SCAN", "0",
			proto.Array(
				proto.String("0"),
				proto.Array(
					proto.String("key"),
				),
			),
		)
	})

	t.Run("invalid cursor", func(t *testing.T) {
		mustDo(t, c,
			"SCAN", "42",
			proto.Array(
				proto.String("0"),
				proto.Array(),
			),
		)
	})

	t.Run("count (ignored)", func(t *testing.T) {
		mustDo(t, c,
			"SCAN", "0", "COUNT", "200",
			proto.Array(
				proto.String("0"),
				proto.Array(
					proto.String("key"),
				),
			),
		)
	})

	t.Run("match", func(t *testing.T) {
		s.Set("aap", "noot")
		s.Set("mies", "wim")

		mustDo(t, c,
			"SCAN", "0", "MATCH", "mi*",
			proto.Array(
				proto.String("0"),
				proto.Array(
					proto.String("mies"),
				),
			),
		)
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"SCAN",
			proto.Error(errWrongNumber("scan")),
		)
		mustDo(t, c,
			"SCAN", "noint",
			proto.Error("ERR invalid cursor"),
		)
		mustDo(t, c,
			"SCAN", "1", "MATCH",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"SCAN", "1", "COUNT",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"SCAN", "1", "COUNT", "noint",
			proto.Error("ERR value is not an integer or out of range"),
		)
	})
}

func TestRenamenx(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Non-existing key
	mustDo(t, c,
		"RENAMENX", "nosuch", "to",
		proto.Error("ERR no such key"),
	)

	t.Run("same key", func(t *testing.T) {
		s.Set("akey", "value")
		must0(t, c,
			"RENAMENX", "akey", "akey",
		)
	})

	// Move a string key
	t.Run("string key", func(t *testing.T) {
		s.Set("from", "value")
		must1(t, c, "RENAMENX", "from", "to")
		equals(t, false, s.Exists("from"))
		equals(t, true, s.Exists("to"))
		s.CheckGet(t, "to", "value")
	})

	t.Run("existing key", func(t *testing.T) {
		s.Set("from", "string value")
		s.Set("to", "value")

		must0(t, c, "RENAMENX", "from", "to")
		equals(t, true, s.Exists("from"))
		equals(t, true, s.Exists("to"))
		s.CheckGet(t, "from", "string value")
		s.CheckGet(t, "to", "value")
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"RENAME",
			proto.Error(errWrongNumber("rename")),
		)
		mustDo(t, c,
			"RENAME", "too few",
			proto.Error(errWrongNumber("rename")),
		)
		mustDo(t, c,
			"RENAME", "some", "spurious", "arguments",
			proto.Error(errWrongNumber("rename")),
		)
	})
}

func TestCopy(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("basic", func(t *testing.T) {
		s.Set("key1", "value")
		// should return 1 after a successful copy operation:
		must1(t, c, "COPY", "key1", "key2")
		s.CheckGet(t, "key2", "value")
		equals(t, "string", s.Type("key2"))
	})

	// should return 0 when trying to copy a nonexistent key:
	t.Run("nonexistent key", func(t *testing.T) {
		must0(t, c, "COPY", "nosuch", "to")
	})

	// should return 0 when trying to overwrite an existing key:
	t.Run("existing key", func(t *testing.T) {
		s.Set("existingkey", "value")
		s.Set("newkey", "newvalue")
		must0(t, c, "COPY", "newkey", "existingkey")
		// existing key value should remain unchanged:
		s.CheckGet(t, "existingkey", "value")
	})

	t.Run("destination db", func(t *testing.T) {
		s.Set("akey1", "value")
		must1(t, c, "COPY", "akey1", "akey2", "DB", "2")
		s.Select(2)
		s.CheckGet(t, "akey2", "value")
		equals(t, "string", s.Type("akey2"))
	})
	s.Select(0)

	t.Run("replace", func(t *testing.T) {
		s.Set("rkey1", "value")
		s.Set("rkey2", "another")
		must1(t, c, "COPY", "rkey1", "rkey2", "REPLACE")
		s.CheckGet(t, "rkey2", "value")
		equals(t, "string", s.Type("rkey2"))
	})

	t.Run("direct", func(t *testing.T) {
		s.Set("d1", "value")
		ok(t, s.Copy(0, "d1", 0, "d2"))
		equals(t, "string", s.Type("d2"))
		s.CheckGet(t, "d2", "value")
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c, "COPY",
			proto.Error(errWrongNumber("copy")),
		)
		mustDo(t, c, "COPY", "foo",
			proto.Error(errWrongNumber("copy")),
		)
		mustDo(t, c, "COPY", "foo", "bar", "baz",
			proto.Error(msgSyntaxError),
		)
	})
}
