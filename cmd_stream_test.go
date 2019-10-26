package miniredis

import (
	"fmt"
	"math"
	"regexp"
	"testing"

	"github.com/gomodule/redigo/redis"
)

// Test XADD / XLEN
func TestStream(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	res, err := redis.String(c.Do("XADD", "s", "1234567-89", "one", "1", "two", "2"))
	ok(t, err)
	equals(t, "1234567-89", res)

	count, err := redis.Int(c.Do("XLEN", "s"))
	ok(t, err)
	equals(t, 1, count)

	t.Run("TYPE", func(t *testing.T) {
		s, err := redis.String(c.Do("TYPE", "s"))
		ok(t, err)
		equals(t, "stream", s)
	})

	t.Run("direct usage", func(t *testing.T) {
		_, err := s.XAdd("s1", "0-0", map[string]string{"name": "foo"})
		assert(t, err != nil, "XAdd error")

		id, err := s.XAdd("s1", "12345-67", map[string]string{"name": "bar"})
		ok(t, err)
		equals(t, "12345-67", id)

		id, err = s.XAdd("s1", "12345-0", map[string]string{"name": "foo"})
		ok(t, err)

		id, err = s.XAdd("s1", "*", map[string]string{"name": "baz"})
		ok(t, err)
		exp := `\d+-0`
		matched, err := regexp.MatchString(exp, id)
		ok(t, err)
		assert(t, matched, "expected: %#v got: %#v", exp, id)

		stream, err := s.Stream("s1")
		ok(t, err)
		equals(t, 3, len(stream))
		equals(t, map[string]string{"name": "bar"}, stream[1]["12345-67"])
	})
}

// Test XADD
func TestStreamAdd(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("XADD", func(t *testing.T) {
		res, err := redis.String(c.Do("XADD", "s", "123456", "one", "11", "two", "22"))
		ok(t, err)
		equals(t, "123456-0", res)

		res, err = redis.String(c.Do("XADD", "s", "*", "one", "1", "two", "2"))
		ok(t, err)
		exp := `\d+-0`
		matched, err := regexp.MatchString(exp, res)
		ok(t, err)
		assert(t, matched, "expected: %#v got: %#v", exp, res)

		k := fmt.Sprintf("%d-0", uint64(math.MaxUint64-100))
		res, err = redis.String(c.Do("XADD", "s", k, "one", "11", "two", "22"))
		ok(t, err)
		equals(t, k, res)

		res, err = redis.String(c.Do("XADD", "s", "*", "one", "111", "two", "222"))
		ok(t, err)
		equals(t, fmt.Sprintf("%d-1", uint64(math.MaxUint64-100)), res)
	})

	t.Run("error cases", func(t *testing.T) {
		// Wrong type of key
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)

		_, err = s.XAdd("str", "*", map[string]string{"hi": "1"})
		mustFail(t, err, msgWrongType)

		_, err = redis.String(c.Do("XADD", "str", "*", "hi", "1"))
		mustFail(t, err, msgWrongType)
		_, err = redis.String(c.Do("XADD"))
		assert(t, err != nil, "XADD error")
		_, err = redis.String(c.Do("XADD", "s"))
		assert(t, err != nil, "XADD error")
		_, err = redis.String(c.Do("XADD", "s", "*"))
		assert(t, err != nil, "XADD error")
		_, err = redis.String(c.Do("XADD", "s", "*", "key")) // odd
		assert(t, err != nil, "XADD error")
		_, err = redis.String(c.Do("XADD", "s", "MAXLEN", "~", "1000", "*", "key")) // MAXLEN
		assert(t, err != nil, "XADD error")

		_, err = redis.String(c.Do("XADD", "s", "a-b", "one", "111", "two", "222")) // invalid id format
		assert(t, err != nil, "XADD error")
		_, err = redis.String(c.Do("XADD", "s", "0-0", "one", "111", "two", "222")) // invalid id format
		assert(t, err != nil, "XADD error")
		_, err = redis.String(c.Do("XADD", "s", "1234567-89", "one", "111", "two", "222")) // invalid id value
		assert(t, err != nil, "XADD error")
		_, err = redis.String(c.Do("XADD", "s", fmt.Sprintf("%d-0", uint64(math.MaxUint64-100)), "one", "111", "two", "222")) // invalid id value
		assert(t, err != nil, "XADD error")
	})
}

// Test XLEN
func TestStreamLen(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	_, err = redis.String(c.Do("XADD", "s", "*", "one", "1", "two", "2"))
	ok(t, err)
	_, err = redis.String(c.Do("XADD", "s", "*", "one", "11", "two", "22"))
	ok(t, err)

	t.Run("XLEN", func(t *testing.T) {
		count, err := redis.Int(c.Do("XLEN", "s"))
		ok(t, err)
		equals(t, 2, count)

		count, err = redis.Int(c.Do("XLEN", "s3"))
		ok(t, err)
		equals(t, 0, count)
	})

	t.Run("error cases", func(t *testing.T) {
		// Wrong type of key
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)

		_, err = redis.Int(c.Do("XLEN"))
		mustFail(t, err, errWrongNumber("xlen"))

		_, err = redis.Int(c.Do("XLEN", "str"))
		mustFail(t, err, msgWrongType)
	})
}
