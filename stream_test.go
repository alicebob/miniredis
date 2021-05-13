package miniredis

import (
	"testing"
	"time"
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

func TestStreamKey(t *testing.T) {
	now := time.Now()

	t.Run("add", func(t *testing.T) {
		s := newStreamKey()
		id, err := s.add("123-123", []string{"k", "v"}, now)
		ok(t, err)
		equalStr(t, "123-123", id)
		equals(t, 1, len(s.entries))
	})

	t.Run("after", func(t *testing.T) {
		s := newStreamKey()
		s.add("123-123", []string{"k", "v"}, now)
		s.add("123-128", []string{"k", "v"}, now)
		s.add("123-129", []string{"k", "v"}, now)

		equals(t, 3, len(s.after("0")))
		equals(t, 3, len(s.after("123-122")))
		equals(t, 2, len(s.after("123-123")))
		equals(t, 2, len(s.after("123-124")))
		equals(t, 1, len(s.after("123-128")))
		equals(t, 0, len(s.after("123-129")))
		equals(t, 0, len(s.after("999-999")))
	})

	t.Run("get", func(t *testing.T) {
		s := newStreamKey()
		s.add("123-123", []string{"k", "v"}, now)
		s.add("123-128", []string{"k", "w"}, now)
		s.add("123-129", []string{"k", "y"}, now)

		i, entry := s.get("0")
		equals(t, 0, i)
		equals(t, (*StreamEntry)(nil), entry)

		i, entry = s.get("123-123")
		equals(t, 0, i)
		equalStr(t, "123-123", entry.ID)

		i, entry = s.get("123-124")
		equals(t, 0, i)
		equals(t, (*StreamEntry)(nil), entry)

		i, entry = s.get("123-129")
		equals(t, 2, i)
		equalStr(t, "123-129", entry.ID)

		i, entry = s.get("999-999")
		equals(t, 0, i)
		equals(t, (*StreamEntry)(nil), entry)
	})

	t.Run("delete", func(t *testing.T) {
		s := newStreamKey()
		s.add("123-123", []string{"k", "v"}, now)
		s.add("123-124", []string{"k", "v"}, now)
		s.add("123-125", []string{"k", "v"}, now)
		equals(t, 3, len(s.entries))
		n, err := s.delete([]string{"123-124"})
		ok(t, err)
		equals(t, 1, n)
		equals(t, 2, len(s.entries))

		n, err = s.delete([]string{"9-9"})
		ok(t, err)
		equals(t, 0, n)

		n, err = s.delete([]string{"999-999"})
		ok(t, err)
		equals(t, 0, n)

		equals(t, 2, len(s.entries))
	})
}

func TestStreamKeyGroup(t *testing.T) {
	now := time.Now()
	s := newStreamKey()
	_, err := s.add("123-123", []string{"k", "v"}, now)
	ok(t, err)

	ok(t, s.createGroup("mygroup", "$"))
	g := s.groups["mygroup"]

	{
		s.add("999-1", []string{"k", "v"}, now)
		ls := g.readGroup(now, "consumer1", ">", 999)
		equals(t, 1, len(ls))
	}

	{
		s.add("999-2", []string{"k", "v"}, now)
		s.add("999-3", []string{"k", "v"}, now)
		ls := g.readGroup(now, "consumer1", ">", 1)
		equals(t, 1, len(ls))
		equalStr(t, "999-2", ls[0].ID)
	}

	// re-read unacked messages
	{
		ls := g.readGroup(now, "consumer1", "0-0", 999)
		equals(t, 2, len(ls))
		equalStr(t, "999-1", ls[0].ID)
		equalStr(t, "999-2", ls[1].ID)
	}

	// ack
	{
		n, err := g.ack([]string{"999-2"})
		ok(t, err)
		equals(t, 1, n)
		ls := g.readGroup(now, "consumer1", "0-0", 999)
		equals(t, 1, len(ls))
		equalStr(t, "999-1", ls[0].ID)
	}

	t.Run("invalid acks", func(t *testing.T) {
		n, err := g.ack([]string{"99999-0"})
		ok(t, err)
		equals(t, 0, n)
	})

	t.Run("delete last ID", func(t *testing.T) {
		s := newStreamKey()
		s.add("123-123", []string{"k", "v"}, now)
		ok(t, s.createGroup("mygroup", "$"))
		g := s.groups["mygroup"]
		_, err := s.delete([]string{"123-123"}) // !
		ok(t, err)

		ls := g.readGroup(now, "consumer1", ">", 999)
		equals(t, 0, len(ls))
	})
}
