package miniredis

import (
	"fmt"
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Test XADD / XLEN / XRANGE
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

	now := time.Date(2001, 1, 1, 4, 4, 5, 4_000_000, time.UTC)
	s.SetTime(now)

	t.Run("direct usage", func(t *testing.T) {
		_, err := s.XAdd("s1", "0-0", []string{"name", "foo"})
		mustFail(t, err, errInvalidStreamValue.Error())

		id, err := s.XAdd("s1", "12345-67", []string{"name", "bar"})
		ok(t, err)
		equals(t, "12345-67", id)

		_, err = s.XAdd("s1", "12345-0", []string{"name", "foo"})
		mustFail(t, err, errInvalidStreamValue.Error())

		id, err = s.XAdd("s1", "*", []string{"name", "baz"})
		ok(t, err)
		equals(t, "978321845004-0", id)

		stream, err := s.Stream("s1")
		ok(t, err)
		equals(t, 2, len(stream))
		equals(t, StreamEntry{
			ID:     "12345-67",
			Values: []string{"name", "bar"},
		}, stream[0])
		equals(t, StreamEntry{
			ID:     "978321845004-0",
			Values: []string{"name", "baz"},
		}, stream[1])
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

	t.Run("XADD SetTime", func(t *testing.T) {
		now := time.Date(2001, 1, 1, 4, 4, 5, 4_000_000, time.UTC)
		s.SetTime(now)
		id, err := redis.String(c.Do("XADD", "now", "*", "one", "1"))
		ok(t, err)
		equals(t, "978321845004-0", id)

		id, err = redis.String(c.Do("XADD", "now", "*", "two", "2"))
		ok(t, err)
		equals(t, "978321845004-1", id)
	})

	t.Run("XADD MAXLEN", func(t *testing.T) {
		now := time.Date(2001, 1, 1, 4, 4, 5, 4_000_000, time.UTC)
		s.SetTime(now)

		for i := 0; i < 100; i++ {
			_, err := redis.String(c.Do("XADD", "nowy", "MAXLEN", "10", "*", "one", "1"))
			ok(t, err)
			nowy, _ := s.Stream("nowy")
			assert(t, len(nowy) <= 10, "deleted entries")
		}
		nowy, _ := s.Stream("nowy")
		equals(t, 10, len(nowy))

		for i := 0; i < 100; i++ {
			_, err := redis.String(c.Do("XADD", "nowz", "MAXLEN", "~", "10", "*", "one", "1"))
			ok(t, err)
			nowz, _ := s.Stream("nowz")
			assert(t, len(nowz) <= 10, "deleted entries")
		}
		nowz, _ := s.Stream("nowz")
		equals(t, 10, len(nowz))
	})

	t.Run("error cases", func(t *testing.T) {
		// Wrong type of key
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)
		_, err = s.XAdd("str", "*", []string{"hi", "1"})
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
		_, err = redis.String(c.Do("XADD", "s", "MAXLEN", "!!!", "1000", "*", "key"))
		assert(t, err != nil, "XADD error")
		_, err = redis.String(c.Do("XADD", "s", "MAXLEN", "~", "thousand", "*", "key"))
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

// Test XRANGE / XREVRANGE
func TestStreamRange(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	_, err = redis.String(c.Do("XADD", "planets", "0-1", "name", "Mercury", "greek-god", "Hermes", "idx", "1"))
	ok(t, err)
	_, err = redis.String(c.Do("XADD", "planets", "1-0", "name", "Venus", "greek-god", "Aphrodite", "idx", "2"))
	ok(t, err)
	_, err = redis.String(c.Do("XADD", "planets", "2-1", "name", "Earth", "greek-god", "", "idx", "3"))
	ok(t, err)
	_, err = redis.String(c.Do("XADD", "planets", "3-0", "greek-god", "Ares", "name", "Mars", "idx", "4"))
	ok(t, err)
	_, err = redis.String(c.Do("XADD", "planets", "4-1", "name", "Jupiter", "greek-god", "Dias", "idx", "5"))
	ok(t, err)

	t.Run("XRANGE", func(t *testing.T) {
		res, err := redis.Values(c.Do("XRANGE", "planets", "1", "+"))
		ok(t, err)
		equals(t, 4, len(res))

		item := res[1].([]interface{})
		id := string(item[0].([]byte))

		vals := item[1].([]interface{})
		field := string(vals[0].([]byte))
		value := string(vals[1].([]byte))

		equals(t, "2-1", id)
		equals(t, "name", field)
		equals(t, "Earth", value)

		res, err = redis.Values(c.Do("XREVRANGE", "planets", "3", "1"))
		ok(t, err)
		equals(t, 3, len(res))

		item = res[2].([]interface{})
		id = string(item[0].([]byte))

		vals = item[1].([]interface{})
		field = string(vals[0].([]byte))
		value = string(vals[1].([]byte))

		equals(t, "1-0", id)
		equals(t, "name", field)
		equals(t, "Venus", value)
	})

	t.Run("error cases", func(t *testing.T) {
		// Wrong type of key
		_, err := redis.String(c.Do("SET", "str", "value"))
		ok(t, err)

		_, err = redis.Int(c.Do("XRANGE", "str", "-", "+"))
		mustFail(t, err, msgWrongType)

		_, err = redis.Int(c.Do("XRANGE", "str", "-", "+"))
		mustFail(t, err, msgWrongType)

		_, err = redis.Int(c.Do("XRANGE"))
		mustFail(t, err, errWrongNumber("xrange"))
		_, err = redis.Int(c.Do("XRANGE", "foo"))
		mustFail(t, err, errWrongNumber("xrange"))
		_, err = redis.Int(c.Do("XRANGE", "foo", 1))
		mustFail(t, err, errWrongNumber("xrange"))
		_, err = redis.Int(c.Do("XRANGE", "foo", 2, 3, "toomany"))
		mustFail(t, err, msgSyntaxError)
		_, err = c.Do("XRANGE", "foo", 2, 3, "COUNT", "noint")
		mustFail(t, err, msgInvalidInt)
		_, err = c.Do("XRANGE", "foo", 2, 3, "COUNT", 1, "toomany")
		mustFail(t, err, msgSyntaxError)
		_, err = c.Do("XRANGE", "foo", "-", "noint")
		mustFail(t, err, msgInvalidStreamID)
	})
}
