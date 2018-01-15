package miniredis

import (
	"testing"

	"github.com/garyburd/redigo/redis"
)

func TestEval(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	{
		b, err := redis.Int(c.Do("EVAL", "return 42", 0))
		ok(t, err)
		equals(t, 42, b)
	}

	{
		b, err := c.Do("EVAL", "return {KEYS[1], ARGV[1]}", 1, "key1", "key2")
		ok(t, err)
		equals(t, []interface{}{"key1", "key2"}, b)
	}

	{
		b, err := c.Do("EVAL", "return {ARGV[1]}", 0, "key1")
		ok(t, err)
		equals(t, []interface{}{"key1"}, b)
	}

	// Invalid args
	_, err = c.Do("EVAL", 42, 0)
	assert(t, err != nil, "no EVAL error")

	_, err = c.Do("EVAL", "return 42")
	mustFail(t, err, errWrongNumber("eval"))

	_, err = c.Do("EVAL", "return 42", 1)
	mustFail(t, err, msgInvalidKeysNumber)

	_, err = c.Do("EVAL", "return 42", -1)
	mustFail(t, err, msgNegativeKeysNumber)

	_, err = c.Do("EVAL", "return 42", "letter")
	mustFail(t, err, msgInvalidInt)

	_, err = c.Do("EVAL", "[", 0)
	assert(t, err != nil, "no EVAL error")
}

func TestCmdEvalReplyConversion(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	cases := map[string]struct {
		script   string
		args     []interface{}
		expected interface{}
	}{
		"Return nil": {
			script: "",
			args: []interface{}{
				0,
			},
		},
		"Return boolean true": {
			script: "return true",
			args: []interface{}{
				0,
			},
			expected: int64(1),
		},
		"Return boolean false": {
			script: "return false",
			args: []interface{}{
				0,
			},
			expected: int64(0),
		},
		"Return single number": {
			script: "return 10",
			args: []interface{}{
				0,
			},
			expected: int64(10),
		},
		"Return single float": {
			script: "return 12.345",
			args: []interface{}{
				0,
			},
			expected: int64(12),
		},
		"Return multiple number": {
			script: "return 10, 20",
			args: []interface{}{
				0,
			},
			expected: int64(10),
		},
		"Return single string": {
			script: "return 'test'",
			args: []interface{}{
				0,
			},
			expected: "test",
		},
		"Return multiple string": {
			script: "return 'test1', 'test2'",
			args: []interface{}{
				0,
			},
			expected: "test1",
		},
		"Return single table multiple integer": {
			script: "return {10, 20}",
			args: []interface{}{
				0,
			},
			expected: []interface{}{
				int64(10),
				int64(20),
			},
		},
		"Return single table multiple string": {
			script: "return {'test1', 'test2'}",
			args: []interface{}{
				0,
			},
			expected: []interface{}{
				"test1",
				"test2",
			},
		},
		"Return nested table": {
			script: "return {10, 20, {30, 40}}",
			args: []interface{}{
				0,
			},
			expected: []interface{}{
				int64(10),
				int64(20),
				[]interface{}{
					int64(30),
					int64(40),
				},
			},
		},
		"Return combination table": {
			script: "return {10, 20, {30, 'test', true, 40}, false}",
			args: []interface{}{
				0,
			},
			expected: []interface{}{
				int64(10),
				int64(20),
				[]interface{}{
					int64(30),
					"test",
					int64(1),
					int64(40),
				},
				int64(0),
			},
		},
		"KEYS and ARGV": {
			script: "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
			args: []interface{}{
				2,
				"key1",
				"key2",
				"first",
				"second",
			},
			expected: []interface{}{
				"key1",
				"key2",
				"first",
				"second",
			},
		},
	}

	for id, tc := range cases {
		reply, err := c.Do("EVAL", append([]interface{}{tc.script}, tc.args...)...)
		if err != nil {
			t.Errorf("%v: Unexpected error: %v", id, err)
			continue
		}
		equals(t, tc.expected, reply)
	}
}

func TestCmdEvalResponse(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	{
		v, err := c.Do("EVAL", "return redis.call('set','foo','bar')", 0)
		ok(t, err)
		equals(t, "OK", v)
	}

	{
		v, err := c.Do("EVAL", "return redis.call('get','foo')", 0)
		ok(t, err)
		equals(t, "bar", v)
	}

	{
		v, err := c.Do("EVAL", "return redis.call('HMSET', 'mkey', 'foo','bar','foo1','bar1')", 0)
		ok(t, err)
		equals(t, "OK", v)
	}

	{
		v, err := c.Do("EVAL", "return redis.call('HGETALL','mkey')", 0)
		ok(t, err)
		equals(t, []interface{}{"foo", "bar", "foo1", "bar1"}, v)
	}

	{
		v, err := c.Do("EVAL", "return redis.call('HMGET','mkey', 'foo1')", 0)
		ok(t, err)
		equals(t, []interface{}{"bar1"}, v)
	}

	{
		v, err := c.Do("EVAL", "return redis.call('HMGET','mkey', 'foo')", 0)
		ok(t, err)
		equals(t, []interface{}{"bar"}, v)
	}

	{
		v, err := c.Do("EVAL", "return redis.call('HMGET','mkey', 'bad', 'key')", 0)
		ok(t, err)
		equals(t, []interface{}{nil, nil}, v)
	}
}

func TestCmdScript(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	// SCRIPT LOAD
	{
		v, err := redis.Strings(c.Do("SCRIPT", "LOAD", "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", "return redis.call('set','foo','bar')"))
		ok(t, err)
		equals(t, []string{"a42059b356c875f0717db19a51f6aaca9ae659ea", "2fa2b029f72572e803ff55a09b1282699aecae6a"}, v)
	}

	// SCRIPT EXISTS
	{
		v, err := redis.Int64s(c.Do("SCRIPT", "exists", "a42059b356c875f0717db19a51f6aaca9ae659ea", "2fa2b029f72572e803ff55a09b1282699aecae6a", "invalid sha"))
		ok(t, err)
		equals(t, []int64{1, 1, 0}, v)
	}

	// SCRIPT FLUSH
	{
		v, err := redis.String(c.Do("SCRIPT", "flush"))
		ok(t, err)
		equals(t, "OK", v)
	}
}

func TestCmdScriptAndEvalsha(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	// SCRIPT LOAD
	{
		v, err := redis.Strings(c.Do("SCRIPT", "LOAD", "redis.call('set', KEYS[1], ARGV[1])\n return redis.call('get', KEYS[1]) "))
		ok(t, err)
		equals(t, []string{"054a13c20b748da2922a5f37f144342de21b8650"}, v)
	}

	// TEST EVALSHA
	{
		v, err := c.Do("EVALSHA", "054a13c20b748da2922a5f37f144342de21b8650", 1, "test_key", "test_value")
		ok(t, err)
		equals(t, "test_value", v)
	}

}
