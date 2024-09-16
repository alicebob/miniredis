package miniredis

import (
	"sort"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/proto"
)

func TestHash(t *testing.T) {
	s, c := runWithClient(t)

	must1(t, c, "HSET", "aap", "noot", "mies")

	t.Run("basic", func(t *testing.T) {
		mustDo(t, c,
			"HGET", "aap", "noot",
			proto.String("mies"),
		)
		equals(t, "mies", s.HGet("aap", "noot"))

		// Existing field.
		must0(t, c, "HSET", "aap", "noot", "mies")

		// Multiple fields.
		mustDo(t, c,
			"HSET", "aaa", "bbb", "cc", "ddd", "ee",
			proto.Int(2),
		)

		mustDo(t, c,
			"HGET", "aaa", "bbb",
			proto.String("cc"),
		)
		equals(t, "cc", s.HGet("aaa", "bbb"))
		mustDo(t, c,
			"HGET", "aaa", "ddd",
			proto.String("ee"),
		)
		equals(t, "ee", s.HGet("aaa", "ddd"))
	})

	t.Run("wrong key type", func(t *testing.T) {
		mustOK(t, c, "SET", "foo", "bar")
		mustDo(t, c,
			"HSET", "foo", "noot", "mies",
			proto.Error("WRONGTYPE Operation against a key holding the wrong kind of value"),
		)
	})

	t.Run("unmatched pairs", func(t *testing.T) {
		mustDo(t, c,
			"HSET", "a", "b", "c", "d",
			proto.Error(errWrongNumber("hset")),
		)
	})

	t.Run("no such key", func(t *testing.T) {
		mustNil(t, c, "HGET", "aap", "nosuch")
	})

	t.Run("no such hash", func(t *testing.T) {
		mustNil(t, c, "HGET", "nosuch", "nosuch")
		equals(t, "", s.HGet("nosuch", "nosuch"))
	})

	t.Run("wrong type", func(t *testing.T) {
		mustDo(t, c,
			"HGET", "aap",
			proto.Error("ERR wrong number of arguments for 'hget' command"),
		)
	})

	t.Run("direct HSet()", func(t *testing.T) {
		s.HSet("wim", "zus", "jet")
		mustDo(t, c,
			"HGET", "wim", "zus",
			proto.String("jet"),
		)

		s.HSet("xxx", "yyy", "a", "zzz", "b")
		mustDo(t, c,
			"HGET", "xxx", "yyy",
			proto.String("a"),
		)
		mustDo(t, c,
			"HGET", "xxx", "zzz",
			proto.String("b"),
		)
	})
}

func TestHashSetNX(t *testing.T) {
	s, c := runWithClient(t)

	// New Hash
	must1(t, c, "HSETNX", "wim", "zus", "jet")

	must0(t, c, "HSETNX", "wim", "zus", "jet")

	// Just a new key
	must1(t, c, "HSETNX", "wim", "aap", "noot")

	// Wrong key type
	s.Set("foo", "bar")
	mustDo(t, c,
		"HSETNX", "foo", "nosuch", "nosuch",
		proto.Error("WRONGTYPE Operation against a key holding the wrong kind of value"),
	)
}

func TestHashMSet(t *testing.T) {
	s, c := runWithClient(t)

	// New Hash
	{
		mustOK(t, c, "HMSET", "hash", "wim", "zus", "jet", "vuur")

		equals(t, "zus", s.HGet("hash", "wim"))
		equals(t, "vuur", s.HGet("hash", "jet"))
	}

	// Doesn't touch ttl.
	{
		s.SetTTL("hash", time.Second*999)
		mustOK(t, c, "HMSET", "hash", "gijs", "lam")
		equals(t, time.Second*999, s.TTL("hash"))
	}

	{
		// Wrong key type
		s.Set("str", "value")
		mustDo(t, c, "HMSET", "str", "key", "value", proto.Error("WRONGTYPE Operation against a key holding the wrong kind of value"))

		// Usage error
		mustDo(t, c, "HMSET", "str", proto.Error(errWrongNumber("hmset")))
		mustDo(t, c, "HMSET", "str", "odd", proto.Error(errWrongNumber("hmset")))
		mustDo(t, c, "HMSET", "str", "key", "value", "odd", proto.Error(errWrongNumber("hmset")))
	}
}

func TestHashDel(t *testing.T) {
	s, c := runWithClient(t)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")
	mustDo(t, c, "HDEL", "wim", "zus", "gijs", proto.Int(2))

	must0(t, c, "HDEL", "wim", "nosuch")

	// Deleting all makes the key disappear
	mustDo(t, c, "HDEL", "wim", "teun", "kees", proto.Int(2))
	assert(t, !s.Exists("wim"), "no more wim key")

	// Key doesn't exists.
	must0(t, c, "HDEL", "nosuch", "nosuch")

	// Wrong key type
	s.Set("foo", "bar")
	mustDo(t, c, "HDEL", "foo", "nosuch", proto.Error(msgWrongType))

	// Direct HDel()
	s.HSet("aap", "noot", "mies")
	s.HDel("aap", "noot")
	equals(t, "", s.HGet("aap", "noot"))
}

func TestHashExists(t *testing.T) {
	s, c := runWithClient(t)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	must1(t, c, "HEXISTS", "wim", "zus")
	must0(t, c, "HEXISTS", "wim", "nosuch")
	must0(t, c, "HEXISTS", "nosuch", "nosuch")

	// Wrong key type
	s.Set("foo", "bar")
	mustDo(t, c,
		"HEXISTS", "foo", "nosuch",
		proto.Error(msgWrongType),
	)
}

func TestHashGetall(t *testing.T) {
	s, c := runWithClient(t)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")
	mustDo(t, c,
		"HGETALL", "wim",
		proto.Strings(
			"gijs", "lam",
			"kees", "bok",
			"teun", "vuur",
			"zus", "jet",
		),
	)

	mustDo(t, c, "HGETALL", "nosuch",
		proto.Strings(),
	)

	// Wrong key type
	s.Set("foo", "bar")
	mustDo(t, c, "HGETALL", "foo",
		proto.Error(msgWrongType),
	)

	useRESP3(t, c)
	t.Run("RESP3", func(t *testing.T) {
		mustDo(t, c,
			"HGETALL", "wim",
			proto.StringMap(
				"gijs", "lam",
				"kees", "bok",
				"teun", "vuur",
				"zus", "jet",
			),
		)
		mustDo(t, c, "HGETALL", "nosuch",
			proto.StringMap(),
		)
	})
}

func TestHashKeys(t *testing.T) {
	s, c := runWithClient(t)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")
	mustDo(t, c,
		"HKEYS", "wim",
		proto.Strings(
			"gijs",
			"kees",
			"teun",
			"zus",
		),
	)

	t.Run("direct", func(t *testing.T) {
		direct, err := s.HKeys("wim")
		ok(t, err)
		equals(t, []string{
			"gijs",
			"kees",
			"teun",
			"zus",
		}, direct)
		_, err = s.HKeys("nosuch")
		equals(t, err, ErrKeyNotFound)
	})

	mustDo(t, c, "HKEYS", "nosuch", proto.Strings())

	// Wrong key type
	s.Set("foo", "bar")
	mustDo(t, c, "HKEYS", "foo", proto.Error(msgWrongType))
}

func TestHashValues(t *testing.T) {
	s, c := runWithClient(t)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")
	mustDo(t, c, "HVALS", "wim",
		proto.Strings(
			"bok",
			"jet",
			"lam",
			"vuur",
		),
	)

	mustDo(t, c, "HVALS", "nosuch", proto.Strings())

	// Wrong key type
	s.Set("foo", "bar")
	mustDo(t, c, "HVALS", "foo", proto.Error(msgWrongType))
}

func TestHashLen(t *testing.T) {
	s, c := runWithClient(t)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")
	mustDo(t, c, "HLEN", "wim", proto.Int(4))

	must0(t, c, "HLEN", "nosuch")

	// Wrong key type
	s.Set("foo", "bar")
	mustDo(t, c, "HLEN", "foo", proto.Error(msgWrongType))
}

func TestHashMget(t *testing.T) {
	s, c := runWithClient(t)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")
	mustDo(t, c,
		"HMGET", "wim", "zus", "nosuch", "kees",
		proto.Array(
			proto.String("jet"),
			proto.Nil,
			proto.String("bok"),
		),
	)

	mustDo(t, c,
		"HMGET", "nosuch", "zus", "kees",
		proto.Array(
			proto.Nil,
			proto.Nil,
		),
	)

	// Wrong key type
	s.Set("foo", "bar")
	mustDo(t, c,
		"HMGET", "foo", "bar",
		proto.Error(msgWrongType),
	)
}

func TestHashIncrby(t *testing.T) {
	s, c := runWithClient(t)

	// New key
	must1(t, c, "HINCRBY", "hash", "field", "1")

	// Existing key
	mustDo(t, c,
		"HINCRBY", "hash", "field", "100",
		proto.Int(101),
	)

	// Minus works.
	mustDo(t, c,
		"HINCRBY", "hash", "field", "-12",
		proto.Int(101-12),
	)

	t.Run("direct", func(t *testing.T) {
		s.HIncr("hash", "field", -3)
		equals(t, "86", s.HGet("hash", "field"))
	})

	t.Run("errors", func(t *testing.T) {
		// Wrong key type
		s.Set("str", "cake")
		mustDo(t, c,
			"HINCRBY", "str", "case", "4",
			proto.Error(msgWrongType),
		)

		mustDo(t, c,
			"HINCRBY", "str", "case", "foo",
			proto.Error("ERR value is not an integer or out of range"),
		)

		mustDo(t, c,
			"HINCRBY", "str",
			proto.Error(errWrongNumber("hincrby")),
		)
	})
}

func TestHashIncrbyfloat(t *testing.T) {
	s, c := runWithClient(t)

	// Existing key
	{
		s.HSet("hash", "field", "12")
		mustDo(t, c,
			"HINCRBYFLOAT", "hash", "field", "400.12",
			proto.String("412.12"),
		)
		equals(t, "412.12", s.HGet("hash", "field"))
	}

	// Existing key, not a number
	{
		s.HSet("hash", "field", "noint")
		mustDo(t, c,
			"HINCRBYFLOAT", "hash", "field", "400",
			proto.Error("ERR value is not a valid float"),
		)
	}

	// New key
	{
		mustDo(t, c,
			"HINCRBYFLOAT", "hash", "newfield", "40.33",
			proto.String("40.33"),
		)
		equals(t, "40.33", s.HGet("hash", "newfield"))
	}

	t.Run("direct", func(t *testing.T) {
		s.HSet("hash", "field", "500.1")
		f, err := s.HIncrfloat("hash", "field", 12)
		ok(t, err)
		equals(t, 512.1, f)
		equals(t, "512.1", s.HGet("hash", "field"))
	})

	t.Run("errors", func(t *testing.T) {
		s.Set("wrong", "type")
		mustDo(t, c,
			"HINCRBYFLOAT", "wrong", "type", "400",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"HINCRBYFLOAT",
			proto.Error(errWrongNumber("hincrbyfloat")),
		)
		mustDo(t, c,
			"HINCRBYFLOAT", "wrong",
			proto.Error(errWrongNumber("hincrbyfloat")),
		)
		mustDo(t, c,
			"HINCRBYFLOAT", "wrong", "value",
			proto.Error(errWrongNumber("hincrbyfloat")),
		)
		mustDo(t, c,
			"HINCRBYFLOAT", "wrong", "value", "noint",
			proto.Error("ERR value is not a valid float"),
		)
		mustDo(t, c,
			"HINCRBYFLOAT", "foo", "bar", "12", "tomanye",
			proto.Error(errWrongNumber("hincrbyfloat")),
		)
	})
}

func TestHscan(t *testing.T) {
	s, c := runWithClient(t)

	// We cheat with hscan. It always returns everything.

	s.HSet("h", "field1", "value1")
	s.HSet("h", "field2", "value2")

	// No problem
	mustDo(t, c,
		"HSCAN", "h", "0",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("field1"),
				proto.String("value1"),
				proto.String("field2"),
				proto.String("value2"),
			),
		),
	)

	// Invalid cursor
	mustDo(t, c,
		"HSCAN", "h", "42",
		proto.Array(
			proto.String("0"),
			proto.Array(),
		),
	)

	// COUNT (ignored)
	mustDo(t, c,
		"HSCAN", "h", "0", "COUNT", "200",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("field1"),
				proto.String("value1"),
				proto.String("field2"),
				proto.String("value2"),
			),
		),
	)

	// MATCH
	s.HSet("h", "aap", "a")
	s.HSet("h", "noot", "b")
	s.HSet("h", "mies", "m")
	mustDo(t, c,
		"HSCAN", "h", "0", "MATCH", "mi*",
		proto.Array(
			proto.String("0"),
			proto.Array(
				proto.String("mies"),
				proto.String("m"),
			),
		),
	)

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"HSCAN",
			proto.Error(errWrongNumber("hscan")),
		)
		mustDo(t, c,
			"HSCAN", "set",
			proto.Error(errWrongNumber("hscan")),
		)
		mustDo(t, c,
			"HSCAN", "set", "noint",
			proto.Error("ERR invalid cursor"),
		)
		mustDo(t, c,
			"HSCAN", "set", "1", "MATCH",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"HSCAN", "set", "1", "COUNT",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"HSCAN", "set", "1", "COUNT", "noint",
			proto.Error("ERR value is not an integer or out of range"),
		)
	})
}

func TestHstrlen(t *testing.T) {
	s, c := runWithClient(t)

	t.Run("basic", func(t *testing.T) {
		s.HSet("myhash", "foo", "bar")
		mustDo(t, c,
			"HSTRLEN", "myhash", "foo",
			proto.Int(3),
		)
	})

	t.Run("no such key", func(t *testing.T) {
		s.HSet("myhash", "foo", "bar")
		must0(t, c,
			"HSTRLEN", "myhash", "nosuch",
		)
	})

	t.Run("no such hash", func(t *testing.T) {
		s.HSet("myhash", "foo", "bar")
		must0(t, c,
			"HSTRLEN", "yourhash", "foo",
		)
	})

	t.Run("utf8", func(t *testing.T) {
		s.HSet("myhash", "snow", "☃☃☃")
		mustDo(t, c,
			"HSTRLEN", "myhash", "snow",
			proto.Int(9),
		)
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"HSTRLEN",
			proto.Error("ERR wrong number of arguments for 'hstrlen' command"),
		)

		mustDo(t, c,
			"HSTRLEN", "bar",
			proto.Error("ERR wrong number of arguments for 'hstrlen' command"),
		)

		mustDo(t, c,
			"HSTRLEN", "bar", "baz", "bak",
			proto.Error("ERR wrong number of arguments for 'hstrlen' command"),
		)

		s.Set("notahash", "bar")
		mustDo(t, c,
			"HSTRLEN", "notahash", "bar",
			proto.Error("WRONGTYPE Operation against a key holding the wrong kind of value"),
		)
	})
}

func TestHashRandField(t *testing.T) {
	s, c := runWithClient(t)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")

	{
		v, err := c.Do("HRANDFIELD", "wim", "1")
		ok(t, err)
		assert(t, v == proto.Strings("zus") || v == proto.Strings("teun") || v == proto.Strings("gijs") || v == proto.Strings("kees"), "HRANDFIELD looks sane")
	}

	{
		v, err := c.Do("HRANDFIELD", "wim", "1", "WITHVALUES")
		ok(t, err)
		st, err := proto.Parse(v)
		ok(t, err)
		li := st.([]interface{})
		keys := make([]string, len(li))
		for i, v := range li {
			keys[i] = v.(string)
		}

		assert(t, len(keys) == 2, "HRANDFIELD looks sane")
		assert(t, keys[0] == "zus" || keys[0] == "teun" || keys[0] == "gijs" || keys[0] == "kees", "HRANDFIELD looks sane")
		assert(t, keys[1] == "jet" || keys[1] == "vuur" || keys[1] == "lam" || keys[1] == "bok", "HRANDFIELD looks sane")
	}

	{
		v, err := c.Do("HRANDFIELD", "wim", "4")
		ok(t, err)
		st, err := proto.Parse(v)
		ok(t, err)
		li := st.([]interface{})
		keys := make([]string, len(li))
		for i, v := range li {
			keys[i] = v.(string)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		assert(t, len(keys) == 4, "HRANDFIELD looks sane")
		assert(t, keys[0] == "gijs", "HRANDFIELD looks sane")
		assert(t, keys[1] == "kees", "HRANDFIELD looks sane")
		assert(t, keys[2] == "teun", "HRANDFIELD looks sane")
		assert(t, keys[3] == "zus", "HRANDFIELD looks sane")
	}

	{
		v, err := c.Do("HRANDFIELD", "wim", "5")
		ok(t, err)
		st, err := proto.Parse(v)
		ok(t, err)
		li := st.([]interface{})
		keys := make([]string, len(li))
		for i, v := range li {
			keys[i] = v.(string)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		assert(t, len(keys) == 4, "HRANDFIELD looks sane")
		assert(t, keys[0] == "gijs", "HRANDFIELD looks sane")
		assert(t, keys[1] == "kees", "HRANDFIELD looks sane")
		assert(t, keys[2] == "teun", "HRANDFIELD looks sane")
		assert(t, keys[3] == "zus", "HRANDFIELD looks sane")
	}

	{
		v, err := c.Do("HRANDFIELD", "wim", "-5")
		ok(t, err)
		st, err := proto.Parse(v)
		ok(t, err)
		li := st.([]interface{})
		keys := make([]string, len(li))
		for i, v := range li {
			keys[i] = v.(string)
		}

		keyMap := make(map[string]bool)
		for _, key := range keys {
			keyMap[key] = true
		}
		assert(t, len(keys) == 5, "HRANDFIELD looks sane")
		assert(t, len(keyMap) <= 4, "HRANDFIELD looks sane")
	}

	// Wrong key type
	mustDo(t, c,
		"HRANDFIELD", "wim", "zus",
		proto.Error(msgInvalidInt),
	)
}

func TestHashHexpire(t *testing.T) {
	s, c := runWithClient(t)

	must1(t, c, "HSET", "aap", "noot", "mies")
	must1(t, c, "HEXPIRE", "aap", "30", "FIELDS", "1", "noot")

	s.FastForward(time.Second * 29)
	equals(t, time.Second, s.dbs[0].hashTtls["aap"]["noot"])

	s.FastForward(time.Second)
	_, exists := s.dbs[0].hashTtls["aap"]["noot"]
	assert(t, !exists, "ttl still exists for field")
	_, exists = s.dbs[0].hashKeys["aap"]["noot"]
	assert(t, !exists, "field still exists")
}
