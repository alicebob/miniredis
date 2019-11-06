package miniredis

import (
	"testing"
)

func TestStreamID(t *testing.T) {
	test := func(a, b string, want int) {
		if have := streamCmp(a, b); have != want {
			t.Errorf("cmp(%q, %q) have %d, want %d", a, b, have, want)
		}
	}
	test("1-1", "2-1", -1)
	test("1-1", "1-1", 0)
	test("1-1", "0-1", 1)
	test("1-1", "1-2", -1)
	test("1-1", "1-1", 0)
	test("1-1", "1-0", 1)
}

func TestFormatStreamID(t *testing.T) {
	if have, _ := formatStreamID("1-1"); have != "1-1" {
		t.Errorf("have %q, want %q", have, "1-1")
	}
	if have, _ := formatStreamID("1"); have != "1-0" {
		t.Errorf("have %q, want %q", have, "1-0")
	}
	if have, _ := formatStreamID("1-002"); have != "1-2" {
		t.Errorf("have %q, want %q", have, "1-2")
	}
	if _, err := formatStreamID("1-foo"); err != errInvalidEntryID {
		t.Errorf("have %s, want %s", err, errInvalidEntryID)
	}
	if _, err := formatStreamID("foo"); err != errInvalidEntryID {
		t.Errorf("have %s, want %s", err, errInvalidEntryID)
	}
}
