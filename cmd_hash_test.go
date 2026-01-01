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

func TestParseHExpireArgs(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		want        hexpireOpts
		wantErr     string
		description string
	}{
		{
			name: "basic usage",
			args: []string{"mykey", "300", "FIELDS", "2", "field1", "field2"},
			want: hexpireOpts{
				key:    "mykey",
				ttl:    300,
				fields: []string{"field1", "field2"},
			},
			wantErr:     "",
			description: "Basic HEXPIRE with key, ttl, and fields",
		},
		{
			name: "with NX option",
			args: []string{"mykey", "300", "NX", "FIELDS", "1", "field1"},
			want: hexpireOpts{
				key:    "mykey",
				ttl:    300,
				nx:     true,
				fields: []string{"field1"},
			},
			wantErr:     "",
			description: "HEXPIRE with NX flag",
		},
		{
			name: "with XX option",
			args: []string{"mykey", "300", "XX", "FIELDS", "1", "field1"},
			want: hexpireOpts{
				key:    "mykey",
				ttl:    300,
				xx:     true,
				fields: []string{"field1"},
			},
			wantErr:     "",
			description: "HEXPIRE with XX flag",
		},
		{
			name: "with GT option",
			args: []string{"mykey", "300", "GT", "FIELDS", "1", "field1"},
			want: hexpireOpts{
				key:    "mykey",
				ttl:    300,
				gt:     true,
				fields: []string{"field1"},
			},
			wantErr:     "",
			description: "HEXPIRE with GT flag",
		},
		{
			name: "with LT option",
			args: []string{"mykey", "300", "LT", "FIELDS", "1", "field1"},
			want: hexpireOpts{
				key:    "mykey",
				ttl:    300,
				lt:     true,
				fields: []string{"field1"},
			},
			wantErr:     "",
			description: "HEXPIRE with LT flag",
		},
		{
			name: "multiple options",
			args: []string{"mykey", "300", "XX", "GT", "FIELDS", "3", "f1", "f2", "f3"},
			want: hexpireOpts{
				key:    "mykey",
				ttl:    300,
				xx:     true,
				gt:     true,
				fields: []string{"f1", "f2", "f3"},
			},
			wantErr:     "",
			description: "HEXPIRE with multiple options",
		},
		{
			name:        "invalid TTL",
			args:        []string{"mykey", "invalid", "FIELDS", "1", "field1"},
			want:        hexpireOpts{},
			wantErr:     msgInvalidInt,
			description: "Invalid TTL value should return error",
		},
		{
			name: "missing FIELDS keyword",
			args: []string{"mykey", "300"},
			want: hexpireOpts{
				key: "mykey",
				ttl: 300,
			},
			wantErr:     "",
			description: "Missing FIELDS is OK - validation happens at command level",
		},
		{
			name:        "invalid numFields",
			args:        []string{"mykey", "300", "FIELDS", "invalid", "field1"},
			want:        hexpireOpts{},
			wantErr:     msgNumFieldsInvalid,
			description: "Invalid numFields should return error",
		},
		{
			name:        "zero numFields",
			args:        []string{"mykey", "300", "FIELDS", "0"},
			want:        hexpireOpts{},
			wantErr:     msgNumFieldsInvalid,
			description: "Zero numFields should return error",
		},
		{
			name:        "negative numFields",
			args:        []string{"mykey", "300", "FIELDS", "-1"},
			want:        hexpireOpts{},
			wantErr:     msgNumFieldsInvalid,
			description: "Negative numFields should return error",
		},
		{
			name:        "not enough fields provided",
			args:        []string{"mykey", "300", "FIELDS", "3", "field1"},
			want:        hexpireOpts{},
			wantErr:     msgNumFieldsParameter,
			description: "Not enough fields provided should return error",
		},
		{
			name:        "GT and LT together",
			args:        []string{"mykey", "300", "GT", "LT", "FIELDS", "1", "field1"},
			want:        hexpireOpts{},
			wantErr:     msgGTandLT,
			description: "GT and LT together should return error",
		},
		{
			name:        "NX and XX together",
			args:        []string{"mykey", "300", "NX", "XX", "FIELDS", "1", "field1"},
			want:        hexpireOpts{},
			wantErr:     msgNXandXXGTLT,
			description: "NX and XX together should return error",
		},
		{
			name:        "NX and GT together",
			args:        []string{"mykey", "300", "NX", "GT", "FIELDS", "1", "field1"},
			want:        hexpireOpts{},
			wantErr:     msgNXandXXGTLT,
			description: "NX and GT together should return error",
		},
		{
			name:        "NX and LT together",
			args:        []string{"mykey", "300", "NX", "LT", "FIELDS", "1", "field1"},
			want:        hexpireOpts{},
			wantErr:     msgNXandXXGTLT,
			description: "NX and LT together should return error",
		},
		{
			name: "case insensitive options",
			args: []string{"mykey", "300", "nx", "fields", "1", "field1"},
			want: hexpireOpts{
				key:    "mykey",
				ttl:    300,
				nx:     true,
				fields: []string{"field1"},
			},
			wantErr:     "",
			description: "Options should be case insensitive",
		},
		{
			name: "multiple fields",
			args: []string{"mykey", "300", "FIELDS", "5", "f1", "f2", "f3", "f4", "f5"},
			want: hexpireOpts{
				key:    "mykey",
				ttl:    300,
				fields: []string{"f1", "f2", "f3", "f4", "f5"},
			},
			wantErr:     "",
			description: "Should handle multiple fields correctly",
		},
		{
			name: "negative TTL",
			args: []string{"mykey", "-1", "FIELDS", "1", "field1"},
			want: hexpireOpts{
				key:    "mykey",
				ttl:    -1,
				fields: []string{"field1"},
			},
			wantErr:     "",
			description: "Negative TTL should be accepted (for immediate expiration)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := parseHExpireArgs(tt.args)

			// Check error
			if tt.wantErr != "" {
				if gotErr == "" {
					t.Errorf("parseHExpireArgs() error = %q, wantErr containing %q", gotErr, tt.wantErr)
					return
				}
				// Check if the error contains the expected message
				if !contains(gotErr, tt.wantErr) {
					t.Errorf("parseHExpireArgs() error = %q, wantErr containing %q", gotErr, tt.wantErr)
				}
				return
			}

			if gotErr != "" {
				t.Errorf("parseHExpireArgs() unexpected error = %q", gotErr)
				return
			}

			// Check result
			if got.key != tt.want.key {
				t.Errorf("parseHExpireArgs() key = %q, want %q", got.key, tt.want.key)
			}
			if got.ttl != tt.want.ttl {
				t.Errorf("parseHExpireArgs() ttl = %d, want %d", got.ttl, tt.want.ttl)
			}
			if got.nx != tt.want.nx {
				t.Errorf("parseHExpireArgs() nx = %v, want %v", got.nx, tt.want.nx)
			}
			if got.xx != tt.want.xx {
				t.Errorf("parseHExpireArgs() xx = %v, want %v", got.xx, tt.want.xx)
			}
			if got.gt != tt.want.gt {
				t.Errorf("parseHExpireArgs() gt = %v, want %v", got.gt, tt.want.gt)
			}
			if got.lt != tt.want.lt {
				t.Errorf("parseHExpireArgs() lt = %v, want %v", got.lt, tt.want.lt)
			}
			if len(got.fields) != len(tt.want.fields) {
				t.Errorf("parseHExpireArgs() fields length = %d, want %d", len(got.fields), len(tt.want.fields))
			} else {
				for i := range got.fields {
					if got.fields[i] != tt.want.fields[i] {
						t.Errorf("parseHExpireArgs() fields[%d] = %q, want %q", i, got.fields[i], tt.want.fields[i])
					}
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestHexpire(t *testing.T) {
	s, c := runWithClient(t)

	t.Run("basic expiration", func(t *testing.T) {
		must1(t, c, "HSET", "myhash", "field1", "value1")
		mustDo(t, c,
			"HEXPIRE", "myhash", "10", "FIELDS", "1", "field1",
			proto.Ints(1),
		)
	})

	t.Run("expire multiple fields", func(t *testing.T) {
		mustDo(t, c, "HSET", "myhash2", "field1", "value1", "field2", "value2", proto.Int(2))
		mustDo(t, c,
			"HEXPIRE", "myhash2", "20", "FIELDS", "2", "field1", "field2",
			proto.Ints(1, 1),
		)
	})

	t.Run("expire non-existent field", func(t *testing.T) {
		must1(t, c, "HSET", "myhash3", "field1", "value1")
		mustDo(t, c,
			"HEXPIRE", "myhash3", "10", "FIELDS", "1", "nonexistent",
			proto.Ints(-2),
		)
	})

	t.Run("expire on non-existent key", func(t *testing.T) {
		mustDo(t, c,
			"HEXPIRE", "nokey", "10", "FIELDS", "1", "field1",
			proto.Ints(-2),
		)
	})

	t.Run("NX option - set only when no expiration", func(t *testing.T) {
		must1(t, c, "HSET", "hash2", "f1", "v1")

		// First time should succeed (no expiration set)
		mustDo(t, c,
			"HEXPIRE", "hash2", "10", "NX", "FIELDS", "1", "f1",
			proto.Ints(1),
		)

		// Second time should fail (expiration already set)
		mustDo(t, c,
			"HEXPIRE", "hash2", "20", "NX", "FIELDS", "1", "f1",
			proto.Ints(0),
		)
	})

	t.Run("XX option - set only when expiration exists", func(t *testing.T) {
		must1(t, c, "HSET", "hash3", "f1", "v1")

		// First time should fail (no expiration set)
		mustDo(t, c,
			"HEXPIRE", "hash3", "10", "XX", "FIELDS", "1", "f1",
			proto.Ints(0),
		)

		// Set expiration first
		mustDo(t, c,
			"HEXPIRE", "hash3", "10", "FIELDS", "1", "f1",
			proto.Ints(1),
		)

		// Now XX should succeed
		mustDo(t, c,
			"HEXPIRE", "hash3", "20", "XX", "FIELDS", "1", "f1",
			proto.Ints(1),
		)
	})

	t.Run("GT option - set only when new expiration is greater", func(t *testing.T) {
		must1(t, c, "HSET", "hash4", "f1", "v1")

		// Set initial expiration
		mustDo(t, c,
			"HEXPIRE", "hash4", "10", "FIELDS", "1", "f1",
			proto.Ints(1),
		)

		// Try to set lower expiration with GT - should fail
		mustDo(t, c,
			"HEXPIRE", "hash4", "5", "GT", "FIELDS", "1", "f1",
			proto.Ints(0),
		)

		// Set higher expiration with GT - should succeed
		mustDo(t, c,
			"HEXPIRE", "hash4", "20", "GT", "FIELDS", "1", "f1",
			proto.Ints(1),
		)
	})

	t.Run("LT option - set only when new expiration is less", func(t *testing.T) {
		must1(t, c, "HSET", "hash5", "f1", "v1")

		// Set initial expiration
		mustDo(t, c,
			"HEXPIRE", "hash5", "20", "FIELDS", "1", "f1",
			proto.Ints(1),
		)

		// Try to set higher expiration with LT - should fail
		mustDo(t, c,
			"HEXPIRE", "hash5", "30", "LT", "FIELDS", "1", "f1",
			proto.Ints(0),
		)

		// Set lower expiration with LT - should succeed
		mustDo(t, c,
			"HEXPIRE", "hash5", "10", "LT", "FIELDS", "1", "f1",
			proto.Ints(1),
		)
	})

	t.Run("field expiration actually expires", func(t *testing.T) {
		mustDo(t, c, "HSET", "hash6", "f1", "v1", "f2", "v2", proto.Int(2))

		// Set very short expiration
		mustDo(t, c,
			"HEXPIRE", "hash6", "1", "FIELDS", "1", "f1",
			proto.Ints(1),
		)

		// Field should exist now
		mustDo(t, c,
			"HGET", "hash6", "f1",
			proto.String("v1"),
		)

		// Fast forward past expiration
		s.FastForward(2 * time.Second)

		// Field should be gone
		mustDo(t, c,
			"HGET", "hash6", "f1",
			proto.Nil,
		)

		// But other field should still exist
		mustDo(t, c,
			"HGET", "hash6", "f2",
			proto.String("v2"),
		)
	})

	t.Run("all fields expired removes hash", func(t *testing.T) {
		must1(t, c, "HSET", "hash7", "f1", "v1")

		// Set very short expiration
		mustDo(t, c,
			"HEXPIRE", "hash7", "1", "FIELDS", "1", "f1",
			proto.Ints(1),
		)

		// Hash should exist
		mustDo(t, c,
			"EXISTS", "hash7",
			proto.Int(1),
		)

		// Fast forward past expiration
		s.FastForward(2 * time.Second)

		// Hash should be gone
		mustDo(t, c,
			"EXISTS", "hash7",
			proto.Int(0),
		)
	})

	t.Run("error cases", func(t *testing.T) {
		mustOK(t, c, "SET", "stringkey", "value")

		// Wrong number of arguments
		mustDo(t, c,
			"HEXPIRE", "myhash",
			proto.Error(errWrongNumber("hexpire")),
		)

		// Wrong type
		mustDo(t, c,
			"HEXPIRE", "stringkey", "10", "FIELDS", "1", "field1",
			proto.Error(msgWrongType),
		)

		// Invalid TTL
		mustDo(t, c,
			"HEXPIRE", "myhash", "notanumber", "FIELDS", "1", "field1",
			proto.Error(msgInvalidInt),
		)

		// Invalid numFields
		mustDo(t, c,
			"HEXPIRE", "myhash", "10", "FIELDS", "notanumber", "field1",
			proto.Error(msgNumFieldsInvalid),
		)

		// Zero numFields - needs at least one dummy field to pass atLeast(5) check
		mustDo(t, c,
			"HEXPIRE", "myhash", "10", "FIELDS", "0", "dummy",
			proto.Error(msgNumFieldsInvalid),
		)

		// Not enough fields
		mustDo(t, c,
			"HEXPIRE", "myhash", "10", "FIELDS", "2", "field1",
			proto.Error(msgNumFieldsParameter),
		)

		// GT and LT together
		mustDo(t, c,
			"HEXPIRE", "myhash", "10", "GT", "LT", "FIELDS", "1", "field1",
			proto.Error(msgGTandLT),
		)

		// NX and XX together
		mustDo(t, c,
			"HEXPIRE", "myhash", "10", "NX", "XX", "FIELDS", "1", "field1",
			proto.Error(msgNXandXXGTLT),
		)
	})

	t.Run("negative TTL for immediate expiration", func(t *testing.T) {
		mustDo(t, c, "HSET", "hash8", "f1", "v1", "f2", "v2", proto.Int(2))

		// Set negative expiration (immediate expiration)
		mustDo(t, c,
			"HEXPIRE", "hash8", "-1", "FIELDS", "1", "f1",
			proto.Ints(1),
		)

		// Fast forward a tiny bit
		s.FastForward(100 * time.Millisecond)

		// Field should be gone
		mustDo(t, c,
			"HGET", "hash8", "f1",
			proto.Nil,
		)
	})

	t.Run("case insensitive options", func(t *testing.T) {
		must1(t, c, "HSET", "hash9", "f1", "v1")

		mustDo(t, c,
			"HEXPIRE", "hash9", "10", "nx", "fields", "1", "f1",
			proto.Ints(1),
		)
	})

	t.Run("TTL is actually stored in hashTTLs map", func(t *testing.T) {
		must1(t, c, "HSET", "hash10", "field1", "value1")

		// Set TTL
		mustDo(t, c,
			"HEXPIRE", "hash10", "300", "FIELDS", "1", "field1",
			proto.Ints(1),
		)

		// Verify TTL is stored in the internal map
		// Note: s.DB(0) internally handles locking
		fieldTTLs, ok := s.DB(0).hashTTLs["hash10"]
		if !ok {
			t.Fatal("hashTTLs map not created for key")
		}
		ttl, ok := fieldTTLs["field1"]
		if !ok {
			t.Fatal("TTL not set for field1")
		}
		expectedTTL := 300 * time.Second
		if ttl != expectedTTL {
			t.Errorf("TTL mismatch: got %v, want %v", ttl, expectedTTL)
		}

		// Set another field's TTL
		must1(t, c, "HSET", "hash10", "field2", "value2")
		mustDo(t, c,
			"HEXPIRE", "hash10", "600", "FIELDS", "1", "field2",
			proto.Ints(1),
		)

		// Verify both TTLs are stored
		fieldTTLs = s.DB(0).hashTTLs["hash10"]
		if len(fieldTTLs) != 2 {
			t.Errorf("Expected 2 field TTLs, got %d", len(fieldTTLs))
		}
		ttl1 := fieldTTLs["field1"]
		ttl2 := fieldTTLs["field2"]
		if ttl1 != 300*time.Second {
			t.Errorf("field1 TTL mismatch: got %v, want %v", ttl1, 300*time.Second)
		}
		if ttl2 != 600*time.Second {
			t.Errorf("field2 TTL mismatch: got %v, want %v", ttl2, 600*time.Second)
		}
	})
}

func TestCheckHashFieldTTL(t *testing.T) {
	s := NewMiniRedis()
	defer s.Close()

	t.Run("no TTLs set - no-op", func(t *testing.T) {
		s.HSet("hash1", "field1", "value1")
		s.HSet("hash1", "field2", "value2")

		// Call checkHashFieldTTL with no TTLs set
		s.DB(0).checkHashFieldTTL("hash1", 5*time.Second)

		// Fields should still exist
		equals(t, "value1", s.HGet("hash1", "field1"))
		equals(t, "value2", s.HGet("hash1", "field2"))
	})

	t.Run("key not in hashTTLs map - no-op", func(t *testing.T) {
		s.HSet("hash2", "field1", "value1")

		// Call checkHashFieldTTL for a key not in hashTTLs
		s.DB(0).checkHashFieldTTL("hash2", 5*time.Second)

		// Field should still exist
		equals(t, "value1", s.HGet("hash2", "field1"))
	})

	t.Run("TTL decrements correctly", func(t *testing.T) {
		s.HSet("hash3", "field1", "value1")

		// Manually set TTL
		db := s.DB(0)
		db.hashTTLs["hash3"] = map[string]time.Duration{
			"field1": 10 * time.Second,
		}

		// Decrement by 3 seconds
		db.checkHashFieldTTL("hash3", 3*time.Second)

		// TTL should be 7 seconds now
		equals(t, 7*time.Second, db.hashTTLs["hash3"]["field1"])
		equals(t, "value1", s.HGet("hash3", "field1"))
	})

	t.Run("field expires when TTL reaches zero", func(t *testing.T) {
		s.HSet("hash4", "field1", "value1")
		s.HSet("hash4", "field2", "value2")

		// Set TTL that will expire
		db := s.DB(0)
		db.hashTTLs["hash4"] = map[string]time.Duration{
			"field1": 2 * time.Second,
		}

		// Decrement past zero
		db.checkHashFieldTTL("hash4", 3*time.Second)

		// field1 should be deleted
		equals(t, "", s.HGet("hash4", "field1"))
		// field2 should still exist
		equals(t, "value2", s.HGet("hash4", "field2"))
		// TTL entry should be removed
		_, exists := db.hashTTLs["hash4"]["field1"]
		equals(t, false, exists)
	})

	t.Run("multiple fields with different TTLs", func(t *testing.T) {
		s.HSet("hash5", "field1", "value1")
		s.HSet("hash5", "field2", "value2")
		s.HSet("hash5", "field3", "value3")

		db := s.DB(0)
		db.hashTTLs["hash5"] = map[string]time.Duration{
			"field1": 2 * time.Second,
			"field2": 5 * time.Second,
			"field3": 10 * time.Second,
		}

		// Decrement by 3 seconds
		db.checkHashFieldTTL("hash5", 3*time.Second)

		// field1 should be deleted (2-3 = -1 <= 0)
		equals(t, "", s.HGet("hash5", "field1"))
		// field2 should still exist with 2 seconds left
		equals(t, "value2", s.HGet("hash5", "field2"))
		equals(t, 2*time.Second, db.hashTTLs["hash5"]["field2"])
		// field3 should still exist with 7 seconds left
		equals(t, "value3", s.HGet("hash5", "field3"))
		equals(t, 7*time.Second, db.hashTTLs["hash5"]["field3"])
	})

	t.Run("hash deleted when all fields expire", func(t *testing.T) {
		s.HSet("hash6", "field1", "value1")
		s.HSet("hash6", "field2", "value2")

		db := s.DB(0)
		db.hashTTLs["hash6"] = map[string]time.Duration{
			"field1": 2 * time.Second,
			"field2": 3 * time.Second,
		}

		// Decrement past all TTLs
		db.checkHashFieldTTL("hash6", 5*time.Second)

		// Both fields should be deleted
		equals(t, "", s.HGet("hash6", "field1"))
		equals(t, "", s.HGet("hash6", "field2"))

		// Hash key should not exist
		assert(t, !s.Exists("hash6"), "hash6 should be deleted")
	})

	t.Run("hash not deleted when some fields remain", func(t *testing.T) {
		s.HSet("hash7", "field1", "value1")
		s.HSet("hash7", "field2", "value2")

		db := s.DB(0)
		db.hashTTLs["hash7"] = map[string]time.Duration{
			"field1": 2 * time.Second,
			// field2 has no TTL
		}

		// Decrement past field1's TTL
		db.checkHashFieldTTL("hash7", 3*time.Second)

		// field1 should be deleted
		equals(t, "", s.HGet("hash7", "field1"))
		// field2 should still exist (no TTL)
		equals(t, "value2", s.HGet("hash7", "field2"))

		// Hash key should still exist
		assert(t, s.Exists("hash7"), "hash7 should still exist")
	})

	t.Run("negative TTL causes immediate expiration", func(t *testing.T) {
		s.HSet("hash8", "field1", "value1")

		db := s.DB(0)
		db.hashTTLs["hash8"] = map[string]time.Duration{
			"field1": -1 * time.Second,
		}

		// Any decrement should trigger deletion
		db.checkHashFieldTTL("hash8", 1*time.Millisecond)

		// field should be deleted
		equals(t, "", s.HGet("hash8", "field1"))
		assert(t, !s.Exists("hash8"), "hash8 should be deleted")
	})
}
