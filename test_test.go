package miniredis

import (
	"reflect"
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	tb.Helper()
	if !condition {
		tb.Errorf(msg, v...)
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Errorf("unexpected error: %s", err.Error())
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	tb.Helper()
	if !reflect.DeepEqual(exp, act) {
		tb.Errorf("expected: %#v got: %#v", exp, act)
	}
}

// mustFail compares the error strings
func mustFail(tb testing.TB, err error, want string) {
	tb.Helper()
	if err == nil {
		tb.Errorf("expected an error, but got a nil")
		return
	}

	if have := err.Error(); have != want {
		tb.Errorf("have %q, want %q", have, want)
	}
}

// execute a Do(args[,-1]...), which needs to be the same as the last arg.
func mustDo(tb testing.TB, c *proto.Client, args ...string) {
	tb.Helper()
	args, want := args[:len(args)-1], args[len(args)-1]

	res, err := c.Do(args...)
	ok(tb, err)
	equals(tb, want, res)
}

// mustOK is a mustDo() which expects an "OK" response
func mustOK(tb testing.TB, c *proto.Client, args ...string) {
	tb.Helper()
	mustDo(tb, c, append(args, proto.Inline("OK"))...)
}

// mustNil is a mustDo() which expects a nil response
func mustNil(tb testing.TB, c *proto.Client, args ...string) {
	tb.Helper()
	mustDo(tb, c, append(args, proto.Nil)...)
}

// must0 is a mustDo() which expects a `0` response
func must0(tb testing.TB, c *proto.Client, args ...string) {
	tb.Helper()
	mustDo(tb, c, append(args, proto.Int(0))...)
}

// must1 is a mustDo() which expects a `1` response
func must1(tb testing.TB, c *proto.Client, args ...string) {
	tb.Helper()
	mustDo(tb, c, append(args, proto.Int(1))...)
}

// execute a Read()
func mustRead(tb testing.TB, c *proto.Client, want string) {
	tb.Helper()
	res, err := c.Read()
	ok(tb, err)
	equals(tb, want, res)
}
