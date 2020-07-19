package proto

import (
	"testing"
)

func TestTypes(t *testing.T) {
	test := func(have, want string) {
		t.Helper()
		if have != want {
			t.Errorf("have %q, want %q", have, want)
		}
	}

	test(String(""), "$0\r\n\r\n")
	test(String("foo"), "$3\r\nfoo\r\n")

	test(Inline("Hi"), "+Hi\r\n")

	test(Error("ERR wrong"), "-ERR wrong\r\n")

	test(Array(Inline("hi"), Inline("ho")), "*2\r\n+hi\r\n+ho\r\n")

	test(Strings("hi", "ho"), "*2\r\n$2\r\nhi\r\n$2\r\nho\r\n")
}
