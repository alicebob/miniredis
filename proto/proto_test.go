package proto

import (
	"bufio"
	"reflect"
	"strings"
	"testing"
)

func TestWrite(t *testing.T) {
	test := func(want string, cmd ...string) {
		t.Helper()
		buf := &strings.Builder{}
		if err := Write(buf, cmd); err != nil {
			t.Errorf("write: %s", err)
			return
		}
		have := buf.String()
		if have != want {
			t.Errorf("have %q, want %q", have, want)
		}
	}

	test("*0\r\n")
	test("*1\r\n$0\r\n\r\n", "")
	test("*1\r\n$4\r\nPING\r\n", "PING")
	test("*3\r\n$4\r\nPING\r\n$1\r\na\r\n$1\r\nb\r\n", "PING", "a", "b")
}

// https://github.com/antirez/RESP3/blob/master/spec.md
func TestRead(t *testing.T) {
	test := func(t *testing.T, payload string) {
		t.Helper()

		r := bufio.NewReader(strings.NewReader(payload + "+ping\r\n"))
		cmd, err := Read(r)
		if err != nil {
			t.Errorf("read: %s", err)
			return
		}
		if cmd != payload {
			t.Errorf("have %q, want %q", cmd, payload)
			return
		}

		// should not have eaten bytes for the next command
		peek, err := r.Peek(7)
		if err != nil {
			t.Errorf("peek: %s", err)
			return
		}
		if have, want := string(peek), "+ping\r\n"; have != want {
			t.Errorf("have %q, want %q", have, want)
		}
	}

	t.Run("blob strings", func(t *testing.T) {
		test(t, "$11\r\nhello world\r\n")
		test(t, "$0\r\n\r\n")
	})

	t.Run("simple strings", func(t *testing.T) {
		test(t, "+abc\r\n")
		test(t, "+\r\n")
	})

	t.Run("simple errors", func(t *testing.T) {
		test(t, "-ERR wrong\r\n")
	})

	t.Run("numbers", func(t *testing.T) {
		test(t, ":10\r\n")
	})

	t.Run("array", func(t *testing.T) {
		test(t, "*0\r\n")
		test(t, "*1\r\n-foo\r\n")
		test(t, "*2\r\n-foo\r\n$3\r\nfoo\r\n")
		test(t, "*-1\r\n")
	})

	t.Run("nil", func(t *testing.T) {
		test(t, "$-1\r\n")
	})
}

func TestReadArray(t *testing.T) {
	have, err := ReadArray(Strings("foo", "bar"))
	if err != nil {
		t.Errorf("read: %s", err)
	}
	if want := []string{String("foo"), String("bar")}; !reflect.DeepEqual(have, want) {
		t.Errorf("have %q, want %q", have, want)
	}
}

func TestReadString(t *testing.T) {
	have, err := ReadString(String("foo"))
	if err != nil {
		t.Errorf("read: %s", err)
	}
	if want := "foo"; have != want {
		t.Errorf("have %q, want %q", have, want)
	}
}

func TestReadStrings(t *testing.T) {
	have, err := ReadStrings(Strings("foo", "bar"))
	if err != nil {
		t.Errorf("read: %s", err)
	}
	if want := []string{"foo", "bar"}; !reflect.DeepEqual(have, want) {
		t.Errorf("have %q, want %q", have, want)
	}
}

func TestParse(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		have, err := Parse(Int(42))
		if err != nil {
			t.Errorf("read: %s", err)
		}
		if want := 42; !reflect.DeepEqual(have, want) {
			t.Errorf("have %q, want %q", have, want)
		}
	})

	t.Run("inline", func(t *testing.T) {
		have, err := Parse(Inline("foo"))
		if err != nil {
			t.Errorf("read: %s", err)
		}
		if want := "foo"; !reflect.DeepEqual(have, want) {
			t.Errorf("have %q, want %q", have, want)
		}
	})

	t.Run("string", func(t *testing.T) {
		have, err := Parse(String("foo"))
		if err != nil {
			t.Errorf("read: %s", err)
		}
		if want := "foo"; !reflect.DeepEqual(have, want) {
			t.Errorf("have %q, want %q", have, want)
		}
	})

	t.Run("strings", func(t *testing.T) {
		have, err := Parse(Strings("foo", "bar"))
		if err != nil {
			t.Errorf("read: %s", err)
		}
		if want := []interface{}{"foo", "bar"}; !reflect.DeepEqual(have, want) {
			t.Errorf("have %q, want %q", have, want)
		}
	})
}
