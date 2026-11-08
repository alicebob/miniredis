package miniredis

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/proto"
	"github.com/alicebob/miniredis/v2/size"
)

// Test DBSIZE, FLUSHDB, and FLUSHALL.
func TestCmdServer(t *testing.T) {
	s, c := runWithClient(t)

	// Set something
	{
		s.Set("aap", "niet")
		s.Set("roos", "vuur")
		s.DB(1).Set("noot", "mies")
	}

	{
		mustDo(t, c,
			"DBSIZE",
			proto.Int(2),
		)

		mustOK(t, c,
			"FLUSHDB",
		)
		must0(t, c,
			"DBSIZE",
		)

		mustOK(t, c,
			"SELECT", "1",
		)

		must1(t, c,
			"DBSIZE",
		)

		mustOK(t, c,
			"FLUSHALL",
		)

		must0(t, c,
			"DBSIZE",
		)

		mustOK(t, c,
			"SELECT", "4",
		)

		must0(t, c,
			"DBSIZE",
		)
	}

	{
		mustOK(t, c,
			"FLUSHDB", "ASYNC",
		)

		mustOK(t, c,
			"FLUSHALL", "ASYNC",
		)
	}

	{
		mustDo(t, c,
			"DBSIZE", "FOO",
			proto.Error(errWrongNumber("dbsize")),
		)

		mustDo(t, c,
			"FLUSHDB", "FOO",
			proto.Error("ERR syntax error"),
		)

		mustDo(t, c,
			"FLUSHDB", "ASYNC", "FOO",
			proto.Error("ERR syntax error"),
		)

		mustDo(t, c,
			"FLUSHALL", "FOO",
			proto.Error("ERR syntax error"),
		)

		mustDo(t, c,
			"FLUSHALL", "ASYNC", "FOO",
			proto.Error("ERR syntax error"),
		)

		mustDo(t, c,
			"FLUSHALL", "ASYNC", "ASYNC",
			proto.Error("ERR syntax error"),
		)
	}
}

// Test TIME
func TestCmdServerTime(t *testing.T) {
	s, c := runWithClient(t)

	_, err := c.Do("TIME")
	ok(t, err)

	s.SetTime(time.Unix(100, 123456789))
	mustDo(t, c,
		"TIME",
		proto.Strings("100", "123456"),
	)

	mustDo(t, c,
		"TIME", "FOO",
		proto.Error(errWrongNumber("time")),
	)
}

// Test Memory Usage
func TestCmdServerMemoryUsage(t *testing.T) {
	_, c := runWithClient(t)

	c.Do("SET", "foo", "bar")
	mustDo(t, c,
		"PFADD", "h", "aap", "noot", "mies",
		proto.Int(1),
	)

	// Intended only for having metrics not to be 1:1 Redis.
	// The exact size is platform dependent, so assert the string
	// value via `size.Of` and only require the HLL to be larger.
	mustDo(t, c,
		"MEMORY", "USAGE", "foo",
		proto.Int(size.Of("bar")),
	)

	res, err := c.Do("MEMORY", "USAGE", "h")
	ok(t, err)
	// c.Do returns a raw RESP integer, e.g. ":124\r\n".
	hll, err := strconv.Atoi(strings.TrimPrefix(strings.TrimSpace(res), ":"))
	ok(t, err)
	if hll <= size.Of("bar") {
		t.Errorf("MEMORY USAGE h = %v, want > %v", hll, size.Of("bar"))
	}
}

func TestCmdServerMemoryUsageLowerCase(t *testing.T) {
	_, c := runWithClient(t)

	c.Do("SET", "foo", "bar")
	mustDo(t, c,
		"memory", "usage", "foo",
		proto.Int(size.Of("bar")),
	)
}
