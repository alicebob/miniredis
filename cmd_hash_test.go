package miniredis

import (
	"sort"
	"testing"

	"github.com/garyburd/redigo/redis"
)

// Test Hash.
func TestHash(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	{
		b, err := redis.Int(c.Do("HSET", "aap", "noot", "mies"))
		ok(t, err)
		equals(t, 1, b) // New field.
	}

	{
		v, err := redis.String(c.Do("HGET", "aap", "noot"))
		ok(t, err)
		equals(t, "mies", v)
		equals(t, "mies", s.HGet("aap", "noot"))
	}

	{
		b, err := redis.Int(c.Do("HSET", "aap", "noot", "mies"))
		ok(t, err)
		equals(t, 0, b) // Existing field.
	}

	// Wrong type of key
	{
		_, err := redis.String(c.Do("SET", "foo", "bar"))
		ok(t, err)
		_, err = redis.Int(c.Do("HSET", "foo", "noot", "mies"))
		assert(t, err != nil, "HSET error")
	}

	// hash exists, key doesn't.
	{
		b, err := c.Do("HGET", "aap", "nosuch")
		ok(t, err)
		equals(t, nil, b)
	}

	// hash doesn't exists.
	{
		b, err := c.Do("HGET", "nosuch", "nosuch")
		ok(t, err)
		equals(t, nil, b)
		equals(t, "", s.HGet("nosuch", "nosuch"))
	}

	// HGET on wrong type
	{
		_, err := redis.Int(c.Do("HGET", "aap"))
		assert(t, err != nil, "HGET error")
	}

	// Direct HSet()
	{
		s.HSet("wim", "zus", "jet")
		v, err := redis.String(c.Do("HGET", "wim", "zus"))
		ok(t, err)
		equals(t, "jet", v)
	}
}

func TestHashSetNX(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	// New Hash
	v, err := redis.Int(c.Do("HSETNX", "wim", "zus", "jet"))
	ok(t, err)
	equals(t, 1, v)

	v, err = redis.Int(c.Do("HSETNX", "wim", "zus", "jet"))
	ok(t, err)
	equals(t, 0, v)

	// Just a new key
	v, err = redis.Int(c.Do("HSETNX", "wim", "aap", "noot"))
	ok(t, err)
	equals(t, 1, v)

	// Wrong key type
	s.Set("foo", "bar")
	_, err = redis.Int(c.Do("HSETNX", "foo", "nosuch", "nosuch"))
	assert(t, err != nil, "no HSETNX error")
}

func TestHashDel(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")
	v, err := redis.Int(c.Do("HDEL", "wim", "zus", "gijs"))
	ok(t, err)
	equals(t, 2, v)

	v, err = redis.Int(c.Do("HDEL", "wim", "nosuch"))
	ok(t, err)
	equals(t, 0, v)

	// Key doesn't exists.
	v, err = redis.Int(c.Do("HDEL", "nosuch", "nosuch"))
	ok(t, err)
	equals(t, 0, v)

	// Wrong key type
	s.Set("foo", "bar")
	_, err = redis.Int(c.Do("HDEL", "foo", "nosuch"))
	assert(t, err != nil, "no HDEL error")

	// Direct HDel()
	s.HSet("aap", "noot", "mies")
	s.HDel("aap", "noot")
	equals(t, "", s.HGet("aap", "noot"))
}

func TestHashExists(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	v, err := redis.Int(c.Do("HEXISTS", "wim", "zus"))
	ok(t, err)
	equals(t, 1, v)

	v, err = redis.Int(c.Do("HEXISTS", "wim", "nosuch"))
	ok(t, err)
	equals(t, 0, v)

	v, err = redis.Int(c.Do("HEXISTS", "nosuch", "nosuch"))
	ok(t, err)
	equals(t, 0, v)

	// Wrong key type
	s.Set("foo", "bar")
	_, err = redis.Int(c.Do("HEXISTS", "foo", "nosuch"))
	assert(t, err != nil, "no HDEL error")
}

func TestHashGetall(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")
	v, err := redis.Strings(c.Do("HGETALL", "wim"))
	ok(t, err)
	equals(t, 8, len(v))
	d := map[string]string{}
	for len(v) > 0 {
		d[v[0]] = v[1]
		v = v[2:]
	}
	equals(t, map[string]string{
		"zus":  "jet",
		"teun": "vuur",
		"gijs": "lam",
		"kees": "bok",
	}, d)

	v, err = redis.Strings(c.Do("HGETALL", "nosuch"))
	ok(t, err)
	equals(t, 0, len(v))

	// Wrong key type
	s.Set("foo", "bar")
	_, err = redis.Int(c.Do("HGETALL", "foo"))
	assert(t, err != nil, "no HGETALL error")
}

func TestHashKeys(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")
	v, err := redis.Strings(c.Do("HKEYS", "wim"))
	ok(t, err)
	equals(t, 4, len(v))
	sort.Strings(v)
	equals(t, []string{
		"gijs",
		"kees",
		"teun",
		"zus",
	}, v)

	// Direct command, while we're at it
	direct := s.HKeys("wim")
	sort.Strings(direct)
	equals(t, []string{
		"gijs",
		"kees",
		"teun",
		"zus",
	}, direct)
	equals(t, []string{}, s.HKeys("nosuch"))

	v, err = redis.Strings(c.Do("HKEYS", "nosuch"))
	ok(t, err)
	equals(t, 0, len(v))

	// Wrong key type
	s.Set("foo", "bar")
	_, err = redis.Int(c.Do("HKEYS", "foo"))
	assert(t, err != nil, "no HKEYS error")
}

func TestHashValues(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")
	v, err := redis.Strings(c.Do("HVALS", "wim"))
	ok(t, err)
	equals(t, 4, len(v))
	sort.Strings(v)
	equals(t, []string{
		"bok",
		"jet",
		"lam",
		"vuur",
	}, v)

	v, err = redis.Strings(c.Do("HVALS", "nosuch"))
	ok(t, err)
	equals(t, 0, len(v))

	// Wrong key type
	s.Set("foo", "bar")
	_, err = redis.Int(c.Do("HVALS", "foo"))
	assert(t, err != nil, "no HVALS error")
}

func TestHashLen(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	s.HSet("wim", "zus", "jet")
	s.HSet("wim", "teun", "vuur")
	s.HSet("wim", "gijs", "lam")
	s.HSet("wim", "kees", "bok")
	v, err := redis.Int(c.Do("HLEN", "wim"))
	ok(t, err)
	equals(t, 4, v)

	v, err = redis.Int(c.Do("HLEN", "nosuch"))
	ok(t, err)
	equals(t, 0, v)

	// Wrong key type
	s.Set("foo", "bar")
	_, err = redis.Int(c.Do("HLEN", "foo"))
	assert(t, err != nil, "no HLEN error")
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
