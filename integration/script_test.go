//go:build int
// +build int

package main

import (
	"testing"
)

func TestScript(t *testing.T) {
	t.Run("EVAL", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("EVAL", "return 42", "0")
			c.Do("EVAL", "", "0")
			c.Do("EVAL", "return 42", "1", "foo")
			c.Do("EVAL", "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", "2", "key1", "key2", "first", "second")
			c.Do("EVAL", "return {ARGV[1]}", "0", "first")
			c.Do("EVAL", "return {ARGV[1]}", "0", "first\nwith\nnewlines!\r\r\n\t!")
			c.Do("EVAL", "return redis.call('GET', 'nosuch')==false", "0")
			c.Do("EVAL", "return redis.call('GET', 'nosuch')==nil", "0")
			c.Do("EVAL", "local a = redis.call('MGET', 'bar'); return a[1] == false", "0")
			c.Do("EVAL", "local a = redis.call('MGET', 'bar'); return a[1] == nil", "0")
			c.Do("EVAL", "return redis.call('ZRANGE', 'q', 0, -1)", "0")
			c.Do("EVAL", "return redis.call('LPOP', 'foo')", "0")

			// failure cases
			c.Error("wrong number", "EVAL")
			c.Error("wrong number", "EVAL", "return 42")
			c.Error("wrong number", "EVAL", "[")
			c.Error("not an integer", "EVAL", "return 42", "return 43")
			c.Error("greater", "EVAL", "return 42", "1")
			c.Error("negative", "EVAL", "return 42", "-1")
			c.Error("wrong number", "EVAL", "42")
		})
	})

	t.Run("SCRIPT", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("SCRIPT", "LOAD", "return 42")
			c.Do("SCRIPT", "LOAD", "return 42")
			c.Do("SCRIPT", "LOAD", "return 43")

			c.Do("SCRIPT", "EXISTS", "1fa00e76656cc152ad327c13fe365858fd7be306")
			c.Do("SCRIPT", "EXISTS", "0", "1fa00e76656cc152ad327c13fe365858fd7be306")
			c.Do("SCRIPT", "EXISTS", "0")
			c.Do("SCRIPT", "EXISTS")

			c.Do("SCRIPT", "FLUSH")
			c.Do("SCRIPT", "EXISTS", "1fa00e76656cc152ad327c13fe365858fd7be306")
			c.Do("SCRIPT", "FLUSH", "ASYNC")
			c.Do("SCRIPT", "FLUSH", "SyNc")

			c.Error("wrong number", "SCRIPT")
			c.Error("wrong number", "SCRIPT", "LOAD", "return 42", "return 42")
			c.DoLoosely("SCRIPT", "LOAD", "]")
			c.Error("wrong number", "SCRIPT", "LOAD", "]", "foo")
			c.Error("wrong number", "SCRIPT", "LOAD")
			c.Error("only support", "SCRIPT", "FLUSH", "foo")
			c.Error("only support", "SCRIPT", "FLUSH", "ASYNC", "foo")
			c.Error("wrong number", "SCRIPT", "FOO")
		})
	})

	t.Run("EVALSHA", func(t *testing.T) {
		sha1 := "1fa00e76656cc152ad327c13fe365858fd7be306" // "return 42"
		sha2 := "bfbf458525d6a0b19200bfd6db3af481156b367b" // keys[1], argv[1]

		testRaw(t, func(c *client) {
			c.Do("SCRIPT", "LOAD", "return 42")
			c.Do("SCRIPT", "LOAD", "return {KEYS[1],ARGV[1]}")
			c.Do("EVALSHA", sha1, "0")
			c.Do("EVALSHA", sha2, "0")
			c.Do("EVALSHA", sha2, "0", "foo")
			c.Do("EVALSHA", sha2, "1", "foo")
			c.Do("EVALSHA", sha2, "1", "foo", "bar")
			c.Do("EVALSHA", sha2, "1", "foo", "bar", "baz")

			c.Do("SCRIPT", "FLUSH")
			c.Error("Please use EVAL", "EVALSHA", sha1, "0")

			c.Do("SCRIPT", "LOAD", "return 42")
			c.Error("wrong number", "EVALSHA", sha1)
			c.Error("wrong number", "EVALSHA")
			c.Error("wrong number", "EVALSHA", "nosuch")
			c.Error("Please use EVAL", "EVALSHA", "nosuch", "0")
		})
	})

	t.Run("combined", func(t *testing.T) {
		sha1 := "1fa00e76656cc152ad327c13fe365858fd7be306" // "return 42"

		testRaw(t, func(c *client) {
			// EVAL stores the script
			c.Do("EVAL", "return 42", "0")
			c.Do("SCRIPT", "EXISTS", sha1)
			c.Do("EVALSHA", sha1, "0")

			// doesn't store the script on syntax error
			c.Error("compiling", "EVAL", "return '<-syntax error", "0")
			c.Do("SCRIPT", "EXISTS", "015cb4913729c68a7209188bbdee1b1ca19358bf")
			c.Error("NOSCRIPT", "EVALSHA", "015cb4913729c68a7209188bbdee1b1ca19358bf", "0")

			// does store the script on arg errors
			c.Do("SCRIPT", "FLUSH")
			c.Error("not an int", "EVAL", "return 42", "notanumber")
			c.Do("SCRIPT", "EXISTS", sha1)
			c.Error("NOSCRIPT", "EVALSHA", sha1, "0")
		})
	})
}

func TestLua(t *testing.T) {
	// basic datatype things
	datatypes := func(c *client) {
		c.Do("EVAL", "", "0")
		c.Do("EVAL", "return 42", "0")
		c.Do("EVAL", "return 42, 43", "0")
		c.Do("EVAL", "return true", "0")
		c.Do("EVAL", "return 'foo'", "0")
		c.Do("EVAL", "return 3.1415", "0")
		c.Do("EVAL", "return 3.9999", "0")
		c.Do("EVAL", "return {1,'foo'}", "0")
		c.Do("EVAL", "return {1,'foo',nil,'foo'}", "0")
		c.Do("EVAL", "return 3.9999+3", "0")
		c.Do("EVAL", "return 3.99+0.0001", "0")
		c.Do("EVAL", "return 3.9999+0.201", "0")
		c.Do("EVAL", "return {{1}}", "0")
		c.Do("EVAL", "return {1,{1,{1,'bar'}}}", "0")
		c.Do("EVAL", "return nil", "0")
	}
	testRaw(t, datatypes)
	testRESP3(t, datatypes)

	// special returns
	testRaw(t, func(c *client) {
		c.Error("oops", "EVAL", "return {err = 'oops'}", "0")
		c.Do("EVAL", "return {1,{err = 'oops'}}", "0")
		c.Error("oops", "EVAL", "return redis.error_reply('oops')", "0")
		c.Do("EVAL", "return {1,redis.error_reply('oops')}", "0")
		c.Error("oops", "EVAL", "return {err = 'oops', noerr = true}", "0") // doc error?
		c.Error("oops", "EVAL", "return {1, 2, err = 'oops'}", "0")         // doc error?

		c.Do("EVAL", "return {ok = 'great'}", "0")
		c.Do("EVAL", "return {1,{ok = 'great'}}", "0")
		c.Do("EVAL", "return redis.status_reply('great')", "0")
		c.Do("EVAL", "return {1,redis.status_reply('great')}", "0")
		c.Do("EVAL", "return {ok = 'great', notok = 'yes'}", "0")       // doc error?
		c.Do("EVAL", "return {1, 2, ok = 'great', notok = 'yes'}", "0") // doc error?

		c.Error("type of arguments", "EVAL", "return redis.error_reply(1)", "0")
		c.Error("type of arguments", "EVAL", "return redis.error_reply()", "0")
		c.Error("type of arguments", "EVAL", "return redis.error_reply(redis.error_reply('foo'))", "0")
		c.Error("type of arguments", "EVAL", "return redis.status_reply(1)", "0")
		c.Error("type of arguments", "EVAL", "return redis.status_reply()", "0")
		c.Error("type of arguments", "EVAL", "return redis.status_reply(redis.status_reply('foo'))", "0")
	})

	// state inside lua
	testRaw(t, func(c *client) {
		c.Do("EVAL", "redis.call('SELECT', 3); redis.call('SET', 'foo', 'bar')", "0")
		c.Do("GET", "foo")
		c.Do("SELECT", "3")
		c.Do("GET", "foo")
	})

	// lua env
	testRaw(t, func(c *client) {
		// c.Do("EVAL", "print(1)", "0")
		c.Do("EVAL", `return string.format('%q', "pretty string")`, "0")
		c.Error("Script attempted to access nonexistent global variable", "EVAL", "os.clock()", "0")
		c.Error("Error", "EVAL", "os.exit(42)", "0")
		c.Do("EVAL", "return table.concat({1,2,3})", "0")
		c.Do("EVAL", "return math.abs(-42)", "0")
		c.Error("Script attempted to access nonexistent global variable", "EVAL", `return utf8.len("hello world")`, "0")
		c.Error("Error", "EVAL", `require("utf8")`, "0")
		c.Do("EVAL", `return coroutine.running()`, "0")
	})

	// sha1hex
	testRaw(t, func(c *client) {
		c.Do("EVAL", `return redis.sha1hex("foo")`, "0")
		c.Do("SET", "bar", "32")
		c.Do("EVAL", `return redis.sha1hex(KEYS["bar"])`, "0")
		c.Do("EVAL", `return redis.sha1hex(KEYS[1])`, "1", "bar")
		c.Do("EVAL", `return redis.sha1hex(nil)`, "0")
		c.Do("EVAL", `return redis.sha1hex(42)`, "0")
		c.Do("EVAL", `return redis.sha1hex({})`, "0")
		c.Do("EVAL", `return redis.sha1hex(KEYS[1])`, "0")
		c.Error(
			"wrong number of arguments",
			"EVAL", `return redis.sha1hex()`, "0",
		)
		c.Error(
			"wrong number of arguments",
			"EVAL", `return redis.sha1hex(1, 2)`, "0",
		)
	})

	// cjson module
	testRaw(t, func(c *client) {
		c.Do("EVAL", `return cjson.decode('{"id":"foo"}')['id']`, "0")
		// c.Do("SET", "foo", `{"value":42}`)
		// c.Do("EVAL", `return KEYS[1]`, 1, "foo")
		// c.Do("EVAL", `return cjson.decode(KEYS[1])['value']`, 1, "foo")
		c.Do("EVAL", `return cjson.decode(ARGV[1])['value']`, "0", `{"value":"42"}`)
		c.Do("EVAL", `return redis.call("SET", "enc", cjson.encode({["foo"]="bar"}))`, "0")
		c.Do("EVAL", `return redis.call("SET", "enc", cjson.encode({["foo"]={["foo"]=42}}))`, "0")
		c.Do("GET", "enc")

		c.Error(
			"bad argument #1 to ",
			"EVAL", `return cjson.encode()`, "0",
		)
		c.Error(
			"bad argument #1 to ",
			"EVAL", `return cjson.encode(1, 2)`, "0",
		)
		c.Error(
			"bad argument #1 to ",
			"EVAL", `return cjson.decode()`, "0",
		)
		c.Error(
			"bad argument #1 to ",
			"EVAL", `return cjson.decode(1, 2)`, "0",
		)
	})

	// selected DB gets passed on to lua
	testRaw(t, func(c *client) {
		c.Do("SELECT", "3")
		c.Do("EVAL", "redis.call('SET', 'foo', 'bar')", "0")
		c.Do("GET", "foo")
		c.Do("SELECT", "0")
		c.Do("GET", "foo")
	})
}

func TestLuaCall(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "1")
		c.Do("EVAL", `local foo = redis.call("GET", "foo"); redis.call("SET", "foo", foo+1)`, "0")
		c.Do("GET", "foo")
		c.Do("EVAL", `return redis.call("GET", "foo")`, "0")
		c.Do("EVAL", `return redis.call("SET", "foo", 42)`, "0")
		c.Do("EVAL", `redis.log(redis.LOG_NOTICE, "hello")`, "0")
		c.Do("EVAL", `local res = redis.call("GET", "foo"); return res['ok']`, "0")
	})

	testRaw(t, func(c *client) {
		script := `
			local result = redis.call('SET', 'mykey', 'myvalue', 'NX');
			return result['ok'];
		`
		c.Do("EVAL", script, "0")
	})

	// datatype errors
	testRaw(t, func(c *client) {
		c.Error(
			"Please specify at least one argument for redis.call()",
			"EVAL", `redis.call()`, "0",
		)
		c.Error(
			"Lua redis() command arguments must be strings or integers",
			"EVAL", `redis.call({})`, "0",
		)
		c.Error(
			"Unknown Redis command called from Lua script",
			"EVAL", `redis.call(1)`, "0",
		)
		c.Error(
			"Unknown Redis command called from Lua script",
			"EVAL", `redis.call("1")`, "0",
		)
		c.Error(
			"Lua redis() command arguments must be strings or integers",
			"EVAL", `redis.call("ECHO", true)`, "0",
		)
		c.Error(
			"Lua redis() command arguments must be strings or integers",
			"EVAL", `redis.call("ECHO", false)`, "0",
		)
		c.Error(
			"Lua redis() command arguments must be strings or integers",
			"EVAL", `redis.call("ECHO", nil)`, "0",
		)
		c.Error(
			"Lua redis() command arguments must be strings or integers",
			"EVAL", `redis.call("HELLO", {})`, "0",
		)
		// c.Error("Error", "EVAL", `redis.call("HELLO", 1)`, "0")
		// c.Error("Redis command", "EVAL", `redis.call("HELLO", 3.14)`, "0")
		c.Error(
			"Lua redis() command arguments must be strings or integers",
			"EVAL", `redis.call("GET", {})`, "0",
		)
	})

	// call() errors
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "1")

		c.Error("rong number of arg", "EVAL", `redis.call("HGET", "foo")`, "0")
		c.Do("GET", "foo")
		c.Error("rong number of arg", "EVAL", `local foo = redis.call("HGET", "foo"); redis.call("SET", "res", foo)`, "0")
		c.Do("GET", "foo")
		c.Do("GET", "res")
		c.Error("WRONGTYPE", "EVAL", `local foo = redis.call("HGET", "foo", "bar"); redis.call("SET", "res", foo)`, "0")
		c.Do("GET", "foo")
		c.Do("GET", "res")
	})

	// pcall() errors
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "1")
		c.Error(
			"Lua redis() command arguments must be strings or integers",
			"EVAL", `local foo = redis.pcall("HGET", "foo"); redis.call("SET", "res", foo)`, "0",
		)
		c.Do("GET", "foo")
		c.Do("GET", "res")
		c.Error(
			"Lua redis() command arguments must be strings or integers",
			"EVAL", `local foo = redis.pcall("HGET", "foo", "bar"); redis.call("SET", "res", foo)`, "0",
		)
		c.Do("GET", "foo")
		c.Do("GET", "res")
	})

	// call() with non-allowed commands
	testRaw(t, func(c *client) {
		c.Do("SET", "foo", "1")

		c.Error(
			"This Redis command is not allowed from scripts",
			"EVAL", `redis.call("MULTI")`, "0",
		)
		c.Error(
			"This Redis command is not allowed from scripts",
			"EVAL", `redis.call("EXEC")`, "0",
		)
		c.Error(
			"This Redis command is not allowed from scripts",
			"EVAL", `redis.call("EVAL", "redis.call(\"GET\", \"foo\")", 0)`, "0",
		)
		c.Error(
			"This Redis command is not allowed from scripts",
			"EVAL", `redis.call("SCRIPT", "LOAD", "return 42")`, "0",
		)
		c.Error(
			"This Redis command is not allowed from scripts",
			"EVAL", `redis.call("EVALSHA", "123", "0")`, "0",
		)
		c.Error(
			"This Redis command is not allowed from scripts",
			"EVAL", `redis.call("AUTH", "foobar")`, "0",
		)
		c.Error(
			"This Redis command is not allowed from scripts",
			"EVAL", `redis.call("WATCH", "foobar")`, "0",
		)
		c.Error(
			"This Redis command is not allowed from scripts",
			"EVAL", `redis.call("SUBSCRIBE", "foo")`, "0",
		)
		c.Error(
			"This Redis command is not allowed from scripts",
			"EVAL", `redis.call("UNSUBSCRIBE", "foo")`, "0",
		)
		c.Error(
			"This Redis command is not allowed from scripts",
			"EVAL", `redis.call("PSUBSCRIBE", "foo")`, "0",
		)
		c.Error(
			"This Redis command is not allowed from scripts",
			"EVAL", `redis.call("PUNSUBSCRIBE", "foo")`, "0",
		)
		c.Do("EVAL", `redis.pcall("EXEC")`, "0")
		c.Do("GET", "foo")
	})
}

func TestScriptNoAuth(t *testing.T) {
	testAuth(t,
		"supersecret",
		func(c *client) {
			c.Error("Authentication required", "EVAL", `redis.call("ECHO", "foo")`, "0")
			c.Do("AUTH", "supersecret")
			c.Do("EVAL", `redis.call("ECHO", "foo")`, "0")
		},
	)
}

func TestScriptReplicate(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do(
			"EVAL", `redis.replicate_commands();`, "0",
		)
	})
}

func TestScriptTx(t *testing.T) {
	sha2 := "bfbf458525d6a0b19200bfd6db3af481156b367b" // keys[1], argv[1]

	testRaw(t, func(c *client) {
		c.Do("SCRIPT", "LOAD", "return {KEYS[1],ARGV[1]}")
		c.Do("MULTI")
		c.Do("EVALSHA", sha2, "0")
		c.Do("EXEC")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("SCRIPT", "LOAD", "return {KEYS[1],ARGV[1]}")
		c.Do("EVALSHA", sha2, "0")
		c.Do("EXEC")
	})

	testRaw(t, func(c *client) {
		c.Do("MULTI")
		c.Do("SCRIPT", "LOAD", "return {")
		c.Do("SCRIPT", "FOO")
		c.Do("EVALSHA", "aaaa", "0")
		c.DoLoosely("EXEC")
	})
}
