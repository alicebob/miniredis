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
	defer c.Close()

	{
		b, err := redis.Int(c.Do("EVAL", "return 42", 0))
		ok(t, err)
		equals(t, 42, b)
	}

	{
		b, err := redis.Strings(c.Do("EVAL", "return {KEYS[1], ARGV[1]}", 1, "key1", "key2"))
		ok(t, err)
		equals(t, []string{"key1", "key2"}, b)
	}

	{
		b, err := redis.Strings(c.Do("EVAL", "return {ARGV[1]}", 0, "key1"))
		ok(t, err)
		equals(t, []string{"key1"}, b)
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

func TestEvalCall(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	_, err = c.Do("EVAL", "redis.call()", "0")
	assert(t, err != nil, "no EVAL error")

	_, err = c.Do("EVAL", "redis.call({})", "0")
	assert(t, err != nil, "no EVAL error")

	_, err = c.Do("EVAL", "redis.call(1)", "0")
	assert(t, err != nil, "no EVAL error")
}

func TestScript(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	var (
		script1sha = "a42059b356c875f0717db19a51f6aaca9ae659ea"
		script2sha = "1fa00e76656cc152ad327c13fe365858fd7be306" // "return 42"
	)
	{
		v, err := redis.String(c.Do("SCRIPT", "LOAD", "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"))
		ok(t, err)
		equals(t, script1sha, v)
	}

	{
		v, err := redis.String(c.Do("SCRIPT", "LOAD", "return 42"))
		ok(t, err)
		equals(t, script2sha, v)
	}

	{
		v, err := redis.Int64s(c.Do("SCRIPT", "EXISTS", script1sha, script2sha, "invalid sha"))
		ok(t, err)
		equals(t, []int64{1, 1, 0}, v)
	}

	{
		v, err := redis.String(c.Do("SCRIPT", "FLUSH"))
		ok(t, err)
		equals(t, "OK", v)
	}

	{
		v, err := redis.Int64s(c.Do("SCRIPT", "EXISTS", script1sha))
		ok(t, err)
		equals(t, []int64{0}, v)
	}

	{
		v, err := redis.Int64s(c.Do("SCRIPT", "EXISTS"))
		ok(t, err)
		equals(t, []int64{}, v)
	}

	_, err = c.Do("SCRIPT")
	mustFail(t, err, errWrongNumber("script"))

	_, err = c.Do("SCRIPT", "LOAD")
	mustFail(t, err, msgScriptUsage)

	_, err = c.Do("SCRIPT", "LOAD", "return 42", "FOO")
	mustFail(t, err, msgScriptUsage)

	_, err = c.Do("SCRIPT", "LOAD", "[")
	assert(t, err != nil, "no SCRIPT lOAD error")

	_, err = c.Do("SCRIPT", "FLUSH", "1")
	mustFail(t, err, msgScriptUsage)

	_, err = c.Do("SCRIPT", "FOO")
	mustFail(t, err, msgScriptUsage)
}

func TestEvalsha(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	script1sha := "bfbf458525d6a0b19200bfd6db3af481156b367b"
	{
		v, err := redis.String(c.Do("SCRIPT", "LOAD", "return {KEYS[1],ARGV[1]}"))
		ok(t, err)
		equals(t, script1sha, v)
	}

	{
		b, err := redis.Strings(c.Do("EVALSHA", script1sha, 1, "key1", "key2"))
		ok(t, err)
		equals(t, []string{"key1", "key2"}, b)
	}

	_, err = c.Do("EVALSHA")
	mustFail(t, err, errWrongNumber("evalsha"))

	_, err = c.Do("EVALSHA", "foo")
	mustFail(t, err, errWrongNumber("evalsha"))

	_, err = c.Do("EVALSHA", "foo", 0)
	mustFail(t, err, msgNoScriptFound)

	_, err = c.Do("EVALSHA", script1sha, script1sha)
	mustFail(t, err, msgInvalidInt)

	_, err = c.Do("EVALSHA", script1sha, -1)
	mustFail(t, err, msgNegativeKeysNumber)

	_, err = c.Do("EVALSHA", script1sha, 1)
	mustFail(t, err, msgInvalidKeysNumber)

	_, err = c.Do("EVALSHA", "foo", 1, "bar")
	mustFail(t, err, msgNoScriptFound)
}

func TestCmdEvalReply(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

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
			expected: []byte("test"),
		},
		"Return multiple string": {
			script: "return 'test1', 'test2'",
			args: []interface{}{
				0,
			},
			expected: []byte("test1"),
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
				[]byte("test1"),
				[]byte("test2"),
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
					[]byte("test"),
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
				[]byte("key1"),
				[]byte("key2"),
				[]byte("first"),
				[]byte("second"),
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

	{
		_, err := c.Do("EVAL", `return {err="broken"}`, 0)
		mustFail(t, err, "broken")

		_, err = c.Do("EVAL", `return redis.error_reply("broken")`, 0)
		mustFail(t, err, "broken")
	}

	{
		v, err := redis.String(c.Do("EVAL", `return {ok="good"}`, 0))
		ok(t, err)
		equals(t, "good", v)

		v, err = redis.String(c.Do("EVAL", `return redis.status_reply("good")`, 0))
		ok(t, err)
		equals(t, "good", v)
	}

	_, err = c.Do("EVAL", `return redis.error_reply()`, 0)
	assert(t, err != nil, "no EVAL error")

	_, err = c.Do("EVAL", `return redis.error_reply(1)`, 0)
	assert(t, err != nil, "no EVAL error")

	_, err = c.Do("EVAL", `return redis.status_reply()`, 0)
	assert(t, err != nil, "no EVAL error")

	_, err = c.Do("EVAL", `return redis.status_reply(1)`, 0)
	assert(t, err != nil, "no EVAL error")
}

func TestCmdEvalResponse(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	{
		v, err := redis.String(c.Do("EVAL", "return redis.call('set','foo','bar')", 0))
		ok(t, err)
		equals(t, "OK", v)
	}

	{
		v, err := redis.String(c.Do("EVAL", "return redis.call('get','foo')", 0))
		ok(t, err)
		equals(t, "bar", v)
	}

	{
		v, err := redis.String(c.Do("EVAL", "return redis.call('HMSET', 'mkey', 'foo','bar','foo1','bar1')", 0))
		ok(t, err)
		equals(t, "OK", v)
	}

	{
		v, err := redis.Strings(c.Do("EVAL", "return redis.call('HGETALL','mkey')", 0))
		ok(t, err)
		equals(t, []string{"foo", "bar", "foo1", "bar1"}, v)
	}

	{
		v, err := redis.Strings(c.Do("EVAL", "return redis.call('HMGET','mkey', 'foo1')", 0))
		ok(t, err)
		equals(t, []string{"bar1"}, v)
	}

	{
		v, err := redis.Strings(c.Do("EVAL", "return redis.call('HMGET','mkey', 'foo')", 0))
		ok(t, err)
		equals(t, []string{"bar"}, v)
	}

	{
		v, err := c.Do("EVAL", "return redis.call('HMGET','mkey', 'bad', 'key')", 0)
		ok(t, err)
		equals(t, []interface{}{nil, nil}, v)
	}
}
