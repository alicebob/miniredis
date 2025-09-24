package miniredis

import (
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

func TestEval(t *testing.T) {
	_, c := runWithClient(t)

	mustDo(t, c,
		"EVAL", "return 42", "0",
		proto.Int(42),
	)

	mustDo(t, c,
		"EVAL", "return {KEYS[1], ARGV[1]}", "1", "key1", "key2",
		proto.Strings("key1", "key2"),
	)

	mustDo(t, c,
		"EVAL", "return {ARGV[1]}", "0", "key1",
		proto.Strings("key1"),
	)

	// Invalid args
	mustDo(t, c,
		"EVAL", "42", "0",
		proto.Error("ERR Error compiling script (new function): <string> line:1(column:2) near '42':   syntax error "),
	)

	mustDo(t, c,
		"EVAL", "return 42",
		proto.Error(errWrongNumber("eval")),
	)

	mustDo(t, c,
		"EVAL", "return 42", "1",
		proto.Error(msgInvalidKeysNumber),
	)

	mustDo(t, c,
		"EVAL", "return 42", "-1",
		proto.Error(msgNegativeKeysNumber),
	)

	mustDo(t, c,
		"EVAL", "return 42", "letter",
		proto.Error(msgInvalidInt),
	)

	mustDo(t, c,
		"EVAL", "[", "0",
		proto.Error("ERR Error compiling script (new function): <string> line:1(column:1) near '[':   syntax error "),
	)

	mustDo(t, c,
		"EVAL", "os.exit(42)",
		proto.Error(errWrongNumber("eval")),
	)

	mustDo(t, c,
		"EVAL", `return string.gsub("foo", "o", "a")`,
		proto.Error(errWrongNumber("eval")),
	)

	mustContain(t, c,
		"EVAL", "return someGlobal", "0",
		"Script attempted to access nonexistent global variable 'someGlobal'",
	)

	mustContain(t, c,
		"EVAL", "someGlobal = 5", "0",
		"Script attempted to create global variable 'someGlobal'",
	)

	t.Run("bigger float value", func(t *testing.T) {
		must0(t, c,
			"EVAL", "return redis.call('expire','foo', 999999)", "0",
		)
		must0(t, c,
			"EVAL", "return redis.call('expire','foo',1000000)", "0",
		)
	})
}

func TestEvalCall(t *testing.T) {
	_, c := runWithClient(t)

	mustContain(t, c,
		"EVAL", "redis.call()", "0",
		"Error compiling script",
	)

	mustContain(t, c,
		"EVAL", "redis.call({})", "0",
		"Error compiling script",
	)

	mustContain(t, c,
		"EVAL", "redis.call(1)", "0",
		"Error compiling script",
	)
}

func TestScript(t *testing.T) {
	_, c := runWithClient(t)

	var (
		script1sha = "a42059b356c875f0717db19a51f6aaca9ae659ea"
		script2sha = "1fa00e76656cc152ad327c13fe365858fd7be306" // "return 42"
	)
	mustDo(t, c,
		"SCRIPT", "LOAD", "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
		proto.String(script1sha),
	)

	mustDo(t, c,
		"SCRIPT", "LOAD", "return 42",
		proto.String(script2sha),
	)

	mustDo(t, c,
		"SCRIPT", "EXISTS", script1sha, script2sha, "invalid sha",
		proto.Array(proto.Int(1), proto.Int(1), proto.Int(0)),
	)

	mustOK(t, c, "SCRIPT", "FLUSH")
	mustOK(t, c, "SCRIPT", "FLUSH", "async")
	mustOK(t, c, "SCRIPT", "FLUSH", "sync")

	mustDo(t, c,
		"SCRIPT", "EXISTS", script1sha,
		proto.Array(proto.Int(0)),
	)

	mustDo(t, c,
		"SCRIPT", "EXISTS",
		proto.Error(errWrongNumber("script|exists")),
	)

	mustDo(t, c,
		"SCRIPT",
		proto.Error(errWrongNumber("script")),
	)

	mustDo(t, c,
		"SCRIPT", "LOAD",
		proto.Error("ERR unknown subcommand or wrong number of arguments for 'LOAD'. Try SCRIPT HELP."),
	)

	mustDo(t, c,
		"SCRIPT", "LOAD", "return 42", "FOO",
		proto.Error("ERR unknown subcommand or wrong number of arguments for 'LOAD'. Try SCRIPT HELP."),
	)

	mustContain(t, c,
		"SCRIPT", "LOAD", "[",
		"Error compiling script",
	)

	mustDo(t, c,
		"SCRIPT", "FLUSH", "1",
		proto.Error("ERR SCRIPT FLUSH only support SYNC|ASYNC option"),
	)

	mustDo(t, c,
		"SCRIPT", "FOO",
		proto.Error("ERR unknown subcommand 'FOO'. Try SCRIPT HELP."),
	)
}

func TestCJSON(t *testing.T) {
	_, c := runWithClient(t)

	mustDo(t, c,
		"EVAL", `return cjson.decode('{"id":"foo"}')['id']`, "0",
		proto.String("foo"),
	)
	mustDo(t, c,
		"EVAL", `return cjson.encode({foo=42})`, "0",
		proto.String(`{"foo":42}`),
	)

	mustContain(t, c,
		"EVAL", `redis.encode()`, "0",
		"Error compiling script",
	)
	mustContain(t, c,
		"EVAL", `redis.encode("1", "2")`, "0",
		"Error compiling script",
	)
	mustContain(t, c,
		"EVAL", `redis.decode()`, "0",
		"Error compiling script",
	)
	mustContain(t, c,
		"EVAL", `redis.decode("{")`, "0",
		"Error compiling script",
	)
	mustContain(t, c,
		"EVAL", `redis.decode("1", "2")`, "0",
		"Error compiling script",
	)
}

func TestLog(t *testing.T) {
	_, c := runWithClient(t)
	mustNil(t, c,
		"EVAL", "redis.log(redis.LOG_NOTICE, 'hello')", "0")
}

func TestSha1Hex(t *testing.T) {
	_, c := runWithClient(t)

	test1 := func(val string, want string) {
		t.Helper()
		mustDo(t, c,
			"EVAL", "return redis.sha1hex(ARGV[1])", "0", val,
			proto.String(want),
		)
	}
	test1("foo", "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33")
	test1("bar", "62cdb7020ff920e5aa642c3d4066950dd1f01f4d")
	test1("0", "b6589fc6ab0dc82cf12099d1c2d40ab994e8410c")

	test2 := func(eval, want string) {
		t.Helper()
		mustDo(t, c,
			"EVAL", eval, "0",
			proto.String(want),
		)
	}
	test2("return redis.sha1hex({})", "da39a3ee5e6b4b0d3255bfef95601890afd80709")
	test2("return redis.sha1hex(nil)", "da39a3ee5e6b4b0d3255bfef95601890afd80709")
	test2("return redis.sha1hex(42)", "92cfceb39d57d914ed8b14d0e37643de0797ae56")

	mustContain(t, c,
		"EVAL", "redis.sha1hex()", "0",
		"wrong number of arguments",
	)
}

func TestEvalsha(t *testing.T) {
	_, c := runWithClient(t)

	script1sha := "bfbf458525d6a0b19200bfd6db3af481156b367b"
	mustDo(t, c,
		"SCRIPT", "LOAD", "return {KEYS[1],ARGV[1]}",
		proto.String(script1sha),
	)
	mustDo(t, c,
		"EVALSHA", script1sha, "1", "key1", "key2",
		proto.Strings("key1", "key2"),
	)

	mustDo(t, c,
		"EVALSHA",
		proto.Error(errWrongNumber("evalsha")),
	)

	mustDo(t, c,
		"EVALSHA", "foo",
		proto.Error(errWrongNumber("evalsha")),
	)

	mustDo(t, c,
		"EVALSHA", "foo", "0",
		proto.Error(msgNoScriptFound),
	)

	mustDo(t, c,
		"EVALSHA", script1sha, script1sha,
		proto.Error(msgInvalidInt),
	)

	mustDo(t, c,
		"EVALSHA", script1sha, "-1",
		proto.Error(msgNegativeKeysNumber),
	)

	mustDo(t, c,
		"EVALSHA", script1sha, "1",
		proto.Error(msgInvalidKeysNumber),
	)

	mustDo(t, c,
		"EVALSHA", "foo", "1", "bar",
		proto.Error(msgNoScriptFound),
	)
}

func TestCmdEvalReply(t *testing.T) {
	_, c := runWithClient(t)

	// return nil
	mustNil(t, c,
		"EVAL", "", "0",
	)
	// return boolean true
	must1(t, c,
		"EVAL", "return true", "0",
	)
	// return boolean false
	mustNil(t, c,
		"EVAL", "return false", "0",
	)
	// return single number
	mustDo(t, c,
		"EVAL", "return 10", "0",
		proto.Int(10),
	)
	// return single float
	mustDo(t, c,
		"EVAL", "return 12.345", "0",
		proto.Int(12),
	)
	// return multiple numbers
	mustDo(t, c,
		"EVAL", "return 10, 20", "0",
		proto.Int(10),
	)
	// return single string
	mustDo(t, c,
		"EVAL", "return 'test'", "0",
		proto.String("test"),
	)
	// return multiple strings
	mustDo(t, c,
		"EVAL", "return 'test1', 'test2'", "0",
		proto.String("test1"),
	)
	// return single table multiple integer
	mustDo(t, c,
		"EVAL", "return {10, 20}", "0",
		proto.Array(
			proto.Int(10),
			proto.Int(20),
		),
	)
	// return single table multiple string
	mustDo(t, c,
		"EVAL", "return {'test1', 'test2'}", "0",
		proto.Strings("test1", "test2"),
	)
	// return nested table
	mustDo(t, c,
		"EVAL", "return {10, 20, {30, 40}}", "0",
		proto.Array(
			proto.Int(10),
			proto.Int(20),
			proto.Ints(30, 40),
		),
	)
	// return combination table
	mustDo(t, c,
		"EVAL", "return {10, 20, {30, 'test', true, 40}, false}", "0",
		proto.Array(
			proto.Int(10),
			proto.Int(20),
			proto.Array(
				proto.Int(30),
				proto.String("test"),
				proto.Int(1),
				proto.Int(40),
			),
			proto.Nil,
		),
	)
	// KEYS and ARGV
	mustDo(t, c,
		"EVAL", "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
		"2", "key1", "key2", "first", "second",
		proto.Strings(
			"key1",
			"key2",
			"first",
			"second",
		),
	)

	mustOK(t, c,
		"EVAL", `return redis.call("XGROUP", "CREATE", KEYS[1], ARGV[1], "$", "MKSTREAM")`,
		"1", "stream", "group",
	)
	mustDo(t, c,
		"EVAL", `return redis.call("XPENDING", KEYS[1], ARGV[1], "-", "+", 1, ARGV[2])`,
		"1", "stream", "group", "consumer",
		proto.Array(),
	)

	mustDo(t, c,
		"EVAL", `return {err="broken"}`, "0",
		proto.Error("broken"),
	)

	mustDo(t, c,
		"EVAL", `return redis.error_reply("broken")`, "0",
		proto.Error("ERR broken"),
	)

	mustDo(t, c,
		"EVAL", `return {ok="good"}`, "0",
		proto.Inline("good"),
	)

	mustDo(t, c,
		"EVAL", `return redis.status_reply("good")`, "0",
		proto.Inline("good"),
	)

	mustContain(t, c,
		"EVAL", `return redis.error_reply()`, "0",
		"wrong number or type of arguments",
	)

	mustContain(t, c,
		"EVAL", `return redis.error_reply(1)`, "0",
		"wrong number or type of arguments",
	)

	mustContain(t, c,
		"EVAL", `return redis.status_reply()`, "0",
		"wrong number or type of arguments",
	)

	mustContain(t, c,
		"EVAL", `return redis.status_reply(1)`, "0",
		"wrong number or type of arguments",
	)
}

func TestCmdEvalResponse(t *testing.T) {
	_, c := runWithClient(t)

	mustOK(t, c,
		"EVAL", "return redis.call('set','foo','bar')", "0",
	)

	mustDo(t, c,
		"EVAL", "return redis.call('get','foo')", "0",
		proto.String("bar"),
	)
	mustNil(t, c,
		"EVAL", "return redis.call('get','nosuch')", "0",
	)

	mustOK(t, c,
		"EVAL", "return redis.call('HMSET', 'mkey', 'foo','bar','foo1','bar1')", "0",
	)

	mustDo(t, c,
		"EVAL", "return redis.call('HGETALL','mkey')", "0",
		proto.Strings("foo", "bar", "foo1", "bar1"),
	)

	mustDo(t, c,
		"EVAL", "return redis.call('HMGET','mkey', 'foo1')", "0",
		proto.Strings("bar1"),
	)

	mustDo(t, c,
		"EVAL", "return redis.call('HMGET','mkey', 'foo')", "0",
		proto.Strings("bar"),
	)

	mustDo(t, c,
		"EVAL", "return redis.call('HMGET','mkey', 'bad', 'key')", "0",
		proto.Array(proto.Nil, proto.Nil),
	)
}

func TestCmdEvalAuth(t *testing.T) {
	s, c := runWithClient(t)

	eval := "return redis.call('set','foo','bar')"

	s.RequireAuth("123password")

	mustDo(t, c,
		"EVAL", eval, "0",
		proto.Error("NOAUTH Authentication required."),
	)

	mustOK(t, c,
		"AUTH", "123password",
	)

	mustOK(t, c,
		"EVAL", eval, "0",
	)
}

func TestLuaReplicate(t *testing.T) {
	_, c := runWithClient(t)

	t.Run("replicate_commands", func(t *testing.T) {
		mustNil(t, c,
			"EVAL", "redis.replicate_commands()", "0",
		)
	})

	t.Run("set_repl", func(t *testing.T) {
		mustNil(t, c,
			"EVAL", "redis.set_repl(redis.REPL_NONE)", "0",
		)
	})
}

func TestLuaTX(t *testing.T) {
	_, c := runWithClient(t)

	t.Run("eval", func(t *testing.T) {
		mustOK(t, c,
			"MULTI",
		)
		mustDo(t, c,
			"EVAL", "return {ARGV[1]}", "0", "key1",
			proto.Inline("QUEUED"),
		)
		mustDo(t, c,
			"EXEC",
			proto.Array(
				proto.Strings("key1"), // EVAL
			),
		)
	})

	t.Run("evalsha", func(t *testing.T) {
		script1sha := "bfbf458525d6a0b19200bfd6db3af481156b367b"
		mustOK(t, c,
			"MULTI",
		)
		mustDo(t, c,
			"SCRIPT", "LOAD", "return {KEYS[1],ARGV[1]}",
			proto.Inline("QUEUED"),
		)
		mustDo(t, c,
			"EVALSHA", script1sha, "1", "key1", "key2",
			proto.Inline("QUEUED"),
		)
		mustDo(t, c,
			"EXEC",
			proto.Array(
				proto.String(script1sha),      // SCRIPT
				proto.Strings("key1", "key2"), // EVALSHA
			),
		)
	})

	t.Run("compile", func(t *testing.T) {
		// compiling is done inside the transaction
		mustOK(t, c,
			"SET", "foo", "12",
		)

		mustOK(t, c,
			"MULTI",
		)
		mustDo(t, c,
			"SCRIPT", "LOAD", "foobar",
			proto.Inline("QUEUED"),
		)
		mustDo(t, c,
			"GET", "foo",
			proto.Inline("QUEUED"),
		)
		mustDo(t, c,
			"EXEC",
			proto.Array(
				proto.Error("ERR Error compiling script (new function): user_script at EOF:   parse error "),
				proto.String("12"),
			),
		)
	})

	t.Run("misc", func(t *testing.T) {
		// misc SCRIPT subcommands
		mustOK(t, c,
			"MULTI",
		)
		mustDo(t, c,
			"SCRIPT", "EXISTS", "123",
			proto.Inline("QUEUED"),
		)
		mustDo(t, c,
			"SCRIPT", "FLUSH",
			proto.Inline("QUEUED"),
		)
		mustDo(t, c,
			"EXEC",
			proto.Array(
				proto.Ints(0),
				proto.Inline("OK"),
			),
		)
	})
}

func TestEvalRo(t *testing.T) {
	_, c := runWithClient(t)

	t.Run("read-only command", func(t *testing.T) {
		mustOK(t, c,
			"SET", "readonly", "foo",
		)

		// Test EVALRO with read-only command (should work)
		mustDo(t, c,
			"EVAL_RO", "return redis.call('GET', KEYS[1])", "1", "readonly",
			proto.String("foo"),
		)
	})

	t.Run("write command", func(t *testing.T) {
		// Test EVALRO with write command (should fail)
		mustContain(t, c,
			"EVAL_RO", "return redis.call('SET', KEYS[1], ARGV[1])", "1", "key1", "value1",
			"Write commands are not allowed in read-only scripts",
		)
	})
}

func TestEvalshaRo(t *testing.T) {
	_, c := runWithClient(t)

	// First load a read-only script
	script := "return redis.call('GET', KEYS[1])"
	t.Run("read-only script", func(t *testing.T) {
		mustDo(t, c,
			"SCRIPT", "LOAD", script,
			proto.String("d3c21d0c2b9ca22f82737626a27bcaf5d288f99f"),
		)

		mustOK(t, c,
			"SET", "readonly", "foo",
		)

		// Test EVALSHA_RO with read-only command (should work)
		mustDo(t, c,
			"EVALSHA_RO", "d3c21d0c2b9ca22f82737626a27bcaf5d288f99f", "1", "readonly",
			proto.String("foo"),
		)

	})

	t.Run("write script", func(t *testing.T) {
		// Load a write script
		writeScript := "return redis.call('SET', KEYS[1], ARGV[1])"
		mustDo(t, c,
			"SCRIPT", "LOAD", writeScript,
			proto.String("d8f2fad9f8e86a53d2a6ebd960b33c4972cacc37"),
		)

		// Test EVALSHA_RO with write command (should fail)
		mustContain(t, c,
			"EVALSHA_RO", "d8f2fad9f8e86a53d2a6ebd960b33c4972cacc37", "1", "key1", "value1",
			"Write commands are not allowed in read-only scripts",
		)
	})
}

func TestEvalRoWriteCommandWithPcall(t *testing.T) {
	_, c := runWithClient(t)

	t.Run("return error", func(t *testing.T) {
		// Test EVAL with pcall and write command (should fail)
		mustContain(t, c,
			"EVAL_RO", "return redis.pcall('FAKECOMMAND', KEYS[1], ARGV[1])", "1", "key1", "value1",
			"Unknown Redis command called from script",
		)
	})

	t.Run("extra work after error", func(t *testing.T) {
		script := `
local err = redis.pcall('FAKECOMMAND', KEYS[1], ARGV[1]);
local res = "pcall:" .. err['err'];
return res;
`
		// Test EVAL with pcall and write command (should fail)
		mustContain(t, c,
			"EVAL_RO", script, "1", "key1", "value1",
			"pcall:ERR Unknown Redis command called from script",
		)
	})

	t.Run("write command in read-only script", func(t *testing.T) {
		// Test EVALRO with pcall and write command (should fail)
		mustContain(t, c,
			"EVAL_RO", "return redis.pcall('SET', KEYS[1], ARGV[1])", "1", "key1", "value1",
			"Write commands are not allowed in read-only scripts",
		)
	})
}

func TestEvalWithPcall(t *testing.T) {
	_, c := runWithClient(t)

	t.Run("return error", func(t *testing.T) {
		// Test EVAL with pcall and write command (should fail)
		mustContain(t, c,
			"EVAL", "return redis.pcall('FAKECOMMAND', KEYS[1], ARGV[1])", "1", "key1", "value1",
			"Unknown Redis command called from script",
		)
	})

	t.Run("continue after error", func(t *testing.T) {
		script := `
local err = redis.pcall('FAKECOMMAND', KEYS[1], ARGV[1]);
local res = "pcall:" .. err['err'];
return res;
`
		// Test EVAL with pcall and write command (should fail)
		mustContain(t, c,
			"EVAL", script, "1", "foo", "value1",
			"pcall:ERR Unknown Redis command called from script",
		)
	})
}
