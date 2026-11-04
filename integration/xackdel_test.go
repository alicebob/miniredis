package main

import "testing"

func TestXackdel(t *testing.T) {
	skip(t)
	testRaw(t, exerciseXackdel)
}

func exerciseXackdel(c *client) {
	c.Do("XACKDEL", "missing", "group", "IDS", "1", "1-0")
	c.Do("XACKDEL", "missing", "group", "IDS", "1", "invalid")
	c.Do("SET", "xackdel-string", "value")
	c.Error("wrong kind", "XACKDEL", "xackdel-string", "group", "IDS", "1", "1-0")
	for _, policy := range []string{"KEEPREF", "DELREF", "ACKED"} {
		key := "xackdel-" + policy
		c.Do("XADD", key, "1-0", "name", "one")
		c.Do("XADD", key, "2-0", "name", "two")
		c.Do("XGROUP", "CREATE", key, "a", "0")
		c.Do("XGROUP", "CREATE", key, "b", "0")
		c.Do("XREADGROUP", "GROUP", "a", "alice", "STREAMS", key, ">")
		c.Do("XREADGROUP", "GROUP", "b", "bob", "STREAMS", key, ">")
		c.Do("XACKDEL", key, "a", policy, "IDS", "2", "1-0", "3-0")
		c.Do("XPENDING", key, "a")
		c.Do("XPENDING", key, "b")
		c.Do("XRANGE", key, "-", "+")
		c.Do("XACKDEL", key, "b", policy, "IDS", "1", "1-0")
		c.Do("XRANGE", key, "-", "+")
	}
	c.Do("XADD", "xackdel-unread", "1-0", "f", "v")
	c.Do("XGROUP", "CREATE", "xackdel-unread", "a", "0")
	c.Do("XGROUP", "CREATE", "xackdel-unread", "b", "0")
	c.Do("XREADGROUP", "GROUP", "a", "alice", "STREAMS", "xackdel-unread", ">")
	c.Error("stream ID", "XACKDEL", "xackdel-unread", "a", "IDS", "2", "1-0", "invalid")
	c.Do("XPENDING", "xackdel-unread", "a")
	c.Do("XACKDEL", "xackdel-unread", "a", "ACKED", "IDS", "1", "1-0")
	c.Do("XRANGE", "xackdel-unread", "-", "+")
	c.Do("XREADGROUP", "GROUP", "b", "bob", "STREAMS", "xackdel-unread", ">")
	c.Do("XACKDEL", "xackdel-unread", "b", "ACKED", "IDS", "2", "1-0", "1-0")
	c.Do("XRANGE", "xackdel-unread", "-", "+")
	c.Error("stream ID", "XACKDEL", "xackdel-unread", "a", "IDS", "1", "invalid")
	c.Error("positive integer", "XACKDEL", "missing", "group", "IDS", "-1", "1-0")
	c.Error("must match", "XACKDEL", "missing", "group", "IDS", "2", "1-0")
	c.Error("syntax", "XACKDEL", "missing", "group", "invalid", "IDS", "1", "1-0")
}
