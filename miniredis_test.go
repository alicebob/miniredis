package miniredis

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/proto"
	"github.com/alicebob/miniredis/v2/server"
)

// Test starting/stopping a server
func TestServer(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()
	mustDo(t, c, "PING", proto.Inline("PONG"))

	// A single client
	equals(t, 1, s.CurrentConnectionCount())
	equals(t, 1, s.TotalConnectionCount())
	equals(t, 1, s.CommandCount())
	mustDo(t, c, "PING", proto.Inline("PONG"))
	equals(t, 2, s.CommandCount())
}

func TestMultipleServers(t *testing.T) {
	s1, err := Run()
	ok(t, err)
	s2, err := Run()
	ok(t, err)
	if s1.Addr() == s2.Addr() {
		t.Fatal("Non-unique addresses", s1.Addr(), s2.Addr())
	}

	s2.Close()
	s1.Close()
	// Closing multiple times is fine
	go s1.Close()
	go s1.Close()
	s1.Close()
}

func TestRestart(t *testing.T) {
	s, err := Run()
	ok(t, err)
	addr := s.Addr()

	s.Set("color", "red")

	s.Close()
	err = s.Restart()
	ok(t, err)
	if have, want := s.Addr(), addr; have != want {
		t.Fatalf("have: %s, want: %s", have, want)
	}

	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()
	mustDo(t, c, "PING", proto.Inline("PONG"))

	mustDo(t, c,
		"GET", "color",
		proto.String("red"),
	)
}

// Test a custom addr
func TestAddr(t *testing.T) {
	m := NewMiniRedis()
	err := m.StartAddr("127.0.0.1:7887")
	ok(t, err)
	defer m.Close()

	c, err := proto.Dial("127.0.0.1:7887")
	ok(t, err)
	defer c.Close()
	mustDo(t, c, "PING", proto.Inline("PONG"))
}

func TestDump(t *testing.T) {
	s, err := Run()
	ok(t, err)
	s.Set("aap", "noot")
	s.Set("vuur", "mies")
	s.HSet("ahash", "aap", "noot")
	s.HSet("ahash", "vuur", "mies")
	if have, want := s.Dump(), `- aap
   "noot"
- ahash
   aap: "noot"
   vuur: "mies"
- vuur
   "mies"
`; have != want {
		t.Errorf("have: %q, want: %q", have, want)
	}

	// Tricky whitespace
	s.Select(1)
	s.Set("whitespace", "foo\nbar\tbaz!")
	if have, want := s.Dump(), `- whitespace
   "foo\nbar\tbaz!"
`; have != want {
		t.Errorf("have: %q, want: %q", have, want)
	}

	// Long key
	s.Select(2)
	s.Set("long", "This is a rather long key, with some fox jumping over a fence or something.")
	s.Set("countonme", "0123456789012345678901234567890123456789012345678901234567890123456789")
	s.HSet("hlong", "long", "This is another rather long key, with some fox jumping over a fence or something.")
	if have, want := s.Dump(), `- countonme
   "01234567890123456789012345678901234567890123456789012"...(70)
- hlong
   long: "This is another rather long key, with some fox jumpin"...(81)
- long
   "This is a rather long key, with some fox jumping over"...(75)
`; have != want {
		t.Errorf("have: %q, want: %q", have, want)
	}
}

func TestDumpList(t *testing.T) {
	s, err := Run()
	ok(t, err)
	s.Push("elements", "earth")
	s.Push("elements", "wind")
	s.Push("elements", "fire")
	if have, want := s.Dump(), `- elements
   "earth"
   "wind"
   "fire"
`; have != want {
		t.Errorf("have: %q, want: %q", have, want)
	}
}

func TestDumpSet(t *testing.T) {
	s, err := Run()
	ok(t, err)
	s.SetAdd("elements", "earth")
	s.SetAdd("elements", "wind")
	s.SetAdd("elements", "fire")
	if have, want := s.Dump(), `- elements
   "earth"
   "fire"
   "wind"
`; have != want {
		t.Errorf("have: %q, want: %q", have, want)
	}
}

func TestDumpSortedSet(t *testing.T) {
	s, err := Run()
	ok(t, err)
	s.ZAdd("elements", 2.0, "wind")
	s.ZAdd("elements", 3.0, "earth")
	s.ZAdd("elements", 1.0, "fire")
	if have, want := s.Dump(), `- elements
   1.000000: "fire"
   2.000000: "wind"
   3.000000: "earth"
`; have != want {
		t.Errorf("have: %q, want: %q", have, want)
	}
}

func TestDumpStream(t *testing.T) {
	s, err := Run()
	ok(t, err)
	s.XAdd("elements", "0-1", []string{"name", "earth"})
	s.XAdd("elements", "123456789-0", []string{"name", "wind"})
	s.XAdd("elements", "123456789-1", []string{"name", "fire"})
	if have, want := s.Dump(), `- elements
   0-1
      "name": "earth"
   123456789-0
      "name": "wind"
   123456789-1
      "name": "fire"
`; have != want {
		t.Errorf("have: %q, want: %q", have, want)
	}

	s.XAdd("elements", "*", []string{"name", "Leeloo"})
	fullHave := s.Dump()
	have := strings.Split(fullHave, "\n")[8]
	want := `      "name": "Leeloo"`
	if have != want {
		t.Errorf("have: %q, want: %q", have, want)
	}
}

func TestKeysAndFlush(t *testing.T) {
	s, err := Run()
	ok(t, err)
	s.Set("aap", "noot")
	s.Set("vuur", "mies")
	s.Set("muur", "oom")
	s.HSet("hash", "key", "value")
	equals(t, []string{"aap", "hash", "muur", "vuur"}, s.Keys())

	s.Select(1)
	s.Set("1aap", "1noot")
	equals(t, []string{"1aap"}, s.Keys())

	s.Select(0)
	s.FlushDB()
	equals(t, []string{}, s.Keys())
	s.Select(1)
	equals(t, []string{"1aap"}, s.Keys())

	s.Select(0)
	s.FlushAll()
	equals(t, []string{}, s.Keys())
	s.Select(1)
	equals(t, []string{}, s.Keys())
}

func TestExpireWithFastForward(t *testing.T) {
	s, err := Run()
	ok(t, err)

	s.Set("aap", "noot")
	s.Set("noot", "aap")
	s.SetTTL("aap", 10*time.Second)

	s.FastForward(5 * time.Second)
	equals(t, 2, len(s.Keys()))

	s.FastForward(5 * time.Second)
	equals(t, 1, len(s.Keys()))
}

/*
we don't have the redis client anymore
func TestPool(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	pool := &redis.Pool{
		MaxIdle:     1,
		IdleTimeout: 5 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", s.Addr())
		},
	}
	c := pool.Get()
	c.Close()
}
*/

func TestMiniredis_isValidCMD(t *testing.T) {
	testCases := []struct {
		name         string
		isAuthorized bool
		isInPUBSUB   bool
		wantResult   bool
	}{
		{
			name:         "Client is not authorized, no PUBSUB mode",
			isAuthorized: false,
			isInPUBSUB:   false,
			wantResult:   false,
		},
		{
			name:         "Client is not authorized, PUBSUB mode",
			isAuthorized: false,
			isInPUBSUB:   true,
			wantResult:   false,
		},
		{
			name:         "Client is authorized, PUBSUB mode",
			isAuthorized: true,
			isInPUBSUB:   true,
			wantResult:   false,
		},
		{
			name:         "Client is authorized, no PUBSUB mode",
			isAuthorized: true,
			isInPUBSUB:   false,
			wantResult:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := &Miniredis{
				passwords: map[string]string{
					"example_username": "example_password",
				},
			}
			c := server.NewPeer(bufio.NewWriter(&bytes.Buffer{}))
			c.Ctx = &connCtx{
				authenticated: tc.isAuthorized,
			}
			if tc.isInPUBSUB {
				c.Ctx = &connCtx{
					authenticated: tc.isAuthorized,
					subscriber:    newSubscriber(),
				}
			}

			assert(t, tc.wantResult == m.isValidCMD(c, "example_cmd"), "fail")
		})
	}
}
