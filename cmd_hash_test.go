package miniredis

import (
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"

	"github.com/alicebob/miniredis/v2/proto"
)

func TestHash(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

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
			proto.Error("ERR wrong number of arguments for HMSET"),
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
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

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
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

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
		mustDo(t, c, "HMSET", "str", "key", "value", "odd", proto.Error("ERR wrong number of arguments for HMSET"))
	}
}

func TestHashDel(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

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
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

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
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

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
}

func TestHashKeys(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

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
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

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
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

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
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")
	v, err := redis.Values(c.Do("HMGET", "wim", "zus", "nosuch", "kees"))
	ok(t, err)
	equals(t, 3, len(v))
	equals(t, "jet", string(v[0].([]byte)))
	equals(t, nil, v[1])
	equals(t, "bok", string(v[2].([]byte)))

	v, err = redis.Values(c.Do("HMGET", "nosuch", "zus", "kees"))
	ok(t, err)
	equals(t, 2, len(v))
	equals(t, nil, v[0])
	equals(t, nil, v[1])

	// Wrong key type
	s.Set("foo", "bar")
	_, err = redis.Int(c.Do("HMGET", "foo", "bar"))
	assert(t, err != nil, "no HMGET error")
}

func TestHashIncrby(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// New key
	{
		v, err := redis.Int(c.Do("HINCRBY", "hash", "field", 1))
		ok(t, err)
		equals(t, 1, v)
	}

	// Existing key
	{
		v, err := redis.Int(c.Do("HINCRBY", "hash", "field", 100))
		ok(t, err)
		equals(t, 101, v)
	}

	// Minus works.
	{
		v, err := redis.Int(c.Do("HINCRBY", "hash", "field", -12))
		ok(t, err)
		equals(t, 101-12, v)
	}

	// Direct usage
	s.HIncr("hash", "field", -3)
	equals(t, "86", s.HGet("hash", "field"))

	// Error cases.
	{
		// Wrong key type
		s.Set("str", "cake")
		_, err = redis.Values(c.Do("HINCRBY", "str", "case", 4))
		assert(t, err != nil, "no HINCRBY error")

		_, err = redis.Values(c.Do("HINCRBY", "str", "case", "foo"))
		assert(t, err != nil, "no HINCRBY error")

		_, err = redis.Values(c.Do("HINCRBY", "str"))
		assert(t, err != nil, "no HINCRBY error")
	}
}

func TestHashIncrbyfloat(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// Existing key
	{
		s.HSet("hash", "field", "12")
		v, err := redis.Float64(c.Do("HINCRBYFLOAT", "hash", "field", "400.12"))
		ok(t, err)
		equals(t, 412.12, v)
		equals(t, "412.12", s.HGet("hash", "field"))
	}

	// Existing key, not a number
	{
		s.HSet("hash", "field", "noint")
		_, err := redis.Float64(c.Do("HINCRBYFLOAT", "hash", "field", "400"))
		assert(t, err != nil, "do HINCRBYFLOAT error")
	}

	// New key
	{
		v, err := redis.Float64(c.Do("HINCRBYFLOAT", "hash", "newfield", "40.33"))
		ok(t, err)
		equals(t, 40.33, v)
		equals(t, "40.33", s.HGet("hash", "newfield"))
	}

	// Direct usage
	{
		s.HSet("hash", "field", "500.1")
		f, err := s.HIncrfloat("hash", "field", 12)
		ok(t, err)
		equals(t, 512.1, f)
		equals(t, "512.1", s.HGet("hash", "field"))
	}

	// Wrong type of existing key
	{
		s.Set("wrong", "type")
		_, err := redis.Int(c.Do("HINCRBYFLOAT", "wrong", "type", "400"))
		assert(t, err != nil, "do HINCRBYFLOAT error")
	}

	// Wrong usage
	{
		_, err := redis.Int(c.Do("HINCRBYFLOAT"))
		assert(t, err != nil, "do HINCRBYFLOAT error")
		_, err = redis.Int(c.Do("HINCRBYFLOAT", "wrong"))
		assert(t, err != nil, "do HINCRBYFLOAT error")
		_, err = redis.Int(c.Do("HINCRBYFLOAT", "wrong", "value"))
		assert(t, err != nil, "do HINCRBYFLOAT error")
		_, err = redis.Int(c.Do("HINCRBYFLOAT", "wrong", "value", "noint"))
		assert(t, err != nil, "do HINCRBYFLOAT error")
		_, err = redis.Int(c.Do("HINCRBYFLOAT", "foo", "bar", 12, "tomanye"))
		assert(t, err != nil, "do HINCRBYFLOAT error")
	}
}

func TestHscan(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// We cheat with hscan. It always returns everything.

	s.HSet("h", "field1", "value1")
	s.HSet("h", "field2", "value2")

	// No problem
	{
		res, err := redis.Values(c.Do("HSCAN", "h", 0))
		ok(t, err)
		equals(t, 2, len(res))

		var c int
		var keys []string
		_, err = redis.Scan(res, &c, &keys)
		ok(t, err)
		equals(t, 0, c)
		equals(t, []string{"field1", "value1", "field2", "value2"}, keys)
	}

	// Invalid cursor
	{
		res, err := redis.Values(c.Do("HSCAN", "h", 42))
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
		res, err := redis.Values(c.Do("HSCAN", "h", 0, "COUNT", 200))
		ok(t, err)
		equals(t, 2, len(res))

		var c int
		var keys []string
		_, err = redis.Scan(res, &c, &keys)
		ok(t, err)
		equals(t, 0, c)
		equals(t, []string{"field1", "value1", "field2", "value2"}, keys)
	}

	// MATCH
	{
		s.HSet("h", "aap", "a")
		s.HSet("h", "noot", "b")
		s.HSet("h", "mies", "m")
		res, err := redis.Values(c.Do("HSCAN", "h", 0, "MATCH", "mi*"))
		ok(t, err)
		equals(t, 2, len(res))

		var c int
		var keys []string
		_, err = redis.Scan(res, &c, &keys)
		ok(t, err)
		equals(t, 0, c)
		equals(t, []string{"mies", "m"}, keys)
	}

	t.Run("errors", func(t *testing.T) {
		_, err := redis.Int(c.Do("HSCAN"))
		mustFail(t, err, "ERR wrong number of arguments for 'hscan' command")

		_, err = redis.Int(c.Do("HSCAN", "set"))
		mustFail(t, err, "ERR wrong number of arguments for 'hscan' command")

		_, err = redis.Int(c.Do("HSCAN", "set", "noint"))
		mustFail(t, err, "ERR invalid cursor")

		_, err = redis.Int(c.Do("HSCAN", "set", 1, "MATCH"))
		mustFail(t, err, "ERR syntax error")

		_, err = redis.Int(c.Do("HSCAN", "set", 1, "COUNT"))
		mustFail(t, err, "ERR syntax error")

		_, err = redis.Int(c.Do("HSCAN", "set", 1, "COUNT", "noint"))
		mustFail(t, err, "ERR value is not an integer or out of range")
	})
}

func TestHstrlen(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	t.Run("basic", func(t *testing.T) {
		s.HSet("myhash", "foo", "bar")
		n, err := redis.Int(c.Do("HSTRLEN", "myhash", "foo"))
		ok(t, err)
		equals(t, 3, n)
	})

	t.Run("no such key", func(t *testing.T) {
		s.HSet("myhash", "foo", "bar")
		n, err := redis.Int(c.Do("HSTRLEN", "myhash", "nosuch"))
		ok(t, err)
		equals(t, 0, n)
	})

	t.Run("no such hash", func(t *testing.T) {
		s.HSet("myhash", "foo", "bar")
		n, err := redis.Int(c.Do("HSTRLEN", "yourhash", "foo"))
		ok(t, err)
		equals(t, 0, n)
	})

	t.Run("utf8", func(t *testing.T) {
		s.HSet("myhash", "snow", "☃☃☃")
		n, err := redis.Int(c.Do("HSTRLEN", "myhash", "snow"))
		ok(t, err)
		equals(t, 9, n)
	})

	t.Run("errors", func(t *testing.T) {
		_, err := redis.Int(c.Do("HSTRLEN"))
		mustFail(t, err, "ERR wrong number of arguments for 'hstrlen' command")

		_, err = redis.Int(c.Do("HSTRLEN", "bar"))
		mustFail(t, err, "ERR wrong number of arguments for 'hstrlen' command")

		_, err = redis.Int(c.Do("HSTRLEN", "bar", "baz", "bak"))
		mustFail(t, err, "ERR wrong number of arguments for 'hstrlen' command")

		s.Set("notahash", "bar")
		_, err = redis.Int(c.Do("HSTRLEN", "notahash", "bar"))
		mustFail(t, err, "WRONGTYPE Operation against a key holding the wrong kind of value")
	})
}
