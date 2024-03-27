package miniredis

import (
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

func TestMulti(t *testing.T) {
	_, c := runWithClient(t)

	// Do accept MULTI, but use it as a no-op
	mustOK(t, c,
		"MULTI",
	)
}

func TestExec(t *testing.T) {
	_, c := runWithClient(t)

	// Exec without MULTI.
	mustDo(t, c,
		"EXEC",
		proto.Error("ERR EXEC without MULTI"),
	)
}

func TestDiscard(t *testing.T) {
	_, c := runWithClient(t)

	// DISCARD without MULTI.
	mustDo(t, c,
		"DISCARD",
		proto.Error("ERR DISCARD without MULTI"),
	)
}

func TestWatch(t *testing.T) {
	_, c := runWithClient(t)

	// Simple WATCH
	mustOK(t, c,
		"WATCH", "foo",
	)

	// Can't do WATCH in a MULTI
	{
		mustOK(t, c,
			"MULTI",
		)
		mustDo(t, c,
			"WATCH", "foo",
			proto.Error("ERR WATCH in MULTI"),
		)
	}
}

// Test simple multi/exec block.
func TestSimpleTransaction(t *testing.T) {
	s, c := runWithClient(t)

	mustOK(t, c,
		"MULTI",
	)

	mustDo(t, c,
		"SET", "aap", "1",
		proto.Inline("QUEUED"),
	)

	// Not set yet.
	equals(t, false, s.Exists("aap"))

	mustDo(t, c,
		"EXEC",
		proto.Array(proto.Inline("OK")),
	)

	// SET should be back to normal mode
	mustOK(t, c,
		"SET", "aap", "1",
	)
}

func TestDiscardTransaction(t *testing.T) {
	s, c := runWithClient(t)

	s.Set("aap", "noot")

	mustOK(t, c,
		"MULTI",
	)

	mustDo(t, c,
		"SET", "aap", "mies",
		proto.Inline("QUEUED"),
	)

	// Not committed
	s.CheckGet(t, "aap", "noot")

	mustOK(t, c,
		"DISCARD",
	)

	// TX didn't get executed
	s.CheckGet(t, "aap", "noot")
}

func TestTxQueueErr(t *testing.T) {
	s, c := runWithClient(t)

	mustOK(t, c,
		"MULTI",
	)

	mustDo(t, c,
		"SET", "aap", "mies",
		proto.Inline("QUEUED"),
	)

	// That's an error!
	mustDo(t, c,
		"SET", "aap",
		proto.Error(errWrongNumber("set")),
	)

	// Thisone is ok again
	mustDo(t, c,
		"SET", "noot", "vuur",
		proto.Inline("QUEUED"),
	)

	mustDo(t, c,
		"EXEC",
		proto.Error("EXECABORT Transaction discarded because of previous errors."),
	)

	// Didn't get EXECed
	equals(t, false, s.Exists("aap"))
}

func TestTxWatch(t *testing.T) {
	// Watch with no error.
	s, c := runWithClient(t)

	s.Set("one", "two")
	mustOK(t, c,
		"WATCH", "one",
	)

	mustOK(t, c,
		"MULTI",
	)

	mustDo(t, c,
		"GET", "one",
		proto.Inline("QUEUED"),
	)

	mustDo(t, c,
		"EXEC",
		proto.Strings("two"),
	)
}

func TestTxWatchErr(t *testing.T) {
	// Watch with en error.
	s, c := runWithClient(t)
	c2, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c2.Close()

	s.Set("one", "two")
	mustOK(t, c,
		"WATCH", "one",
		proto.String(""),
	)

	// Here comes client 2
	mustOK(t, c2, "SET", "one", "three")

	mustOK(t, c,
		"MULTI",
	)

	mustDo(t, c,
		"GET", "one",
		proto.Inline("QUEUED"),
	)

	mustNilList(t, c,
		"EXEC",
	)

	// It did get updated, and we're not in a transaction anymore.
	mustDo(t, c,
		"GET", "one",
		proto.String("three"),
	)
}

func TestUnwatch(t *testing.T) {
	s, c := runWithClient(t)
	c2, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c2.Close()

	s.Set("one", "two")
	mustOK(t, c,
		"WATCH", "one",
	)

	mustOK(t, c,
		"UNWATCH",
	)

	// Here comes client 2
	mustOK(t, c2,
		"SET", "one", "three",
	)

	mustOK(t, c,
		"MULTI",
	)

	mustDo(t, c,
		"SET", "one", "four",
		proto.Inline("QUEUED"),
	)

	mustDo(t, c,
		"EXEC",
		proto.Array(proto.Inline("OK")),
	)

	// It did get updated by our TX
	mustDo(t, c,
		"GET", "one",
		proto.String("four"),
	)
}
