package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

const (
	errWrongNumberOfArgs = "ERR Wrong number of args"
)

func Test(t *testing.T) {
	s, err := NewServer(":0")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if have := s.Addr().Port; have <= 0 {
		t.Fatalf("have %v, want > 0", have)
	}

	s.Register("PING", func(c *Peer, cmd string, args []string) {
		c.WriteInline("PONG")
	})
	s.Register("ECHO", func(c *Peer, cmd string, args []string) {
		if len(args) != 1 {
			c.WriteError(errWrongNumberOfArgs)
			return
		}
		c.WriteBulk(args[0])
	})
	s.Register("dWaRfS", func(c *Peer, cmd string, args []string) {
		if len(args) != 0 {
			c.WriteError(errWrongNumberOfArgs)
			return
		}
		c.WriteLen(7)
		c.WriteBulk("Blick")
		c.WriteBulk("Flick")
		c.WriteBulk("Glick")
		c.WriteBulk("Plick")
		c.WriteBulk("Quee")
		c.WriteBulk("Snick")
		c.WriteBulk("Whick")
	})
	s.Register("PLUS", func(c *Peer, cmd string, args []string) {
		if len(args) != 2 {
			c.WriteError(errWrongNumberOfArgs)
			return
		}
		a, err := strconv.Atoi(args[0])
		if err != nil {
			c.WriteError(fmt.Sprintf("ERR not an int: %q", args[0]))
			return
		}
		b, err := strconv.Atoi(args[1])
		if err != nil {
			c.WriteError(fmt.Sprintf("ERR not an int: %q", args[1]))
			return
		}
		c.WriteInt(a + b)
	})
	s.Register("NULL", func(c *Peer, cmd string, args []string) {
		c.WriteNull()
	})

	c, err := proto.Dial(s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	{
		res, err := c.Do("PING")
		if err != nil {
			t.Fatal(err)
		}
		if have, want := res, proto.Inline("PONG"); have != want {
			t.Errorf("have: %s, want: %s", have, want)
		}
	}

	{
		res, err := c.Do("NOSUCH")
		if err != nil {
			t.Fatal(err)
		}
		if have, want := res, proto.Error("ERR unknown command `NOSUCH`, with args beginning with: "); have != want {
			t.Errorf("have: %s, want: %s", have, want)
		}
	}

	{
		res, err := c.Do("pInG")
		if err != nil {
			t.Fatal(err)
		}
		if have, want := res, proto.Inline("PONG"); have != want {
			t.Errorf("have: %s, want: %s", have, want)
		}
	}

	{
		echo, err := c.Do("ECHO", "hello\nworld")
		if err != nil {
			t.Fatal(err)
		}
		if have, want := echo, proto.String("hello\nworld"); have != want {
			t.Errorf("have: %s, want: %s", have, want)
		}
	}

	{
		res, err := c.Do("ECHO")
		if err != nil {
			t.Fatal(err)
		}
		if have, want := res, proto.Error(errWrongNumberOfArgs); have != want {
			t.Errorf("have: %s, want: %s", have, want)
		}
	}

	{
		dwarfs, err := c.Do("dwaRFS")
		if err != nil {
			t.Fatal(err)
		}
		if have, want := dwarfs, proto.Strings("Blick",
			"Flick",
			"Glick",
			"Plick",
			"Quee",
			"Snick",
			"Whick",
		); have != want {
			t.Errorf("have: %s, want: %s", have, want)
		}
	}

	{
		res, err := c.Do("NULL")
		if err != nil {
			t.Fatal(err)
		}
		if have, want := res, proto.Nil; have != want {
			t.Errorf("have: %s, want: %s", have, want)
		}
	}

	{
		bigPayload := strings.Repeat("X", 1<<24)
		echo, err := c.Do("ECHO", bigPayload)
		if err != nil {
			t.Fatal(err)
		}
		if have, want := echo, proto.String(bigPayload); have != want {
			t.Errorf("have: %s, want: %s", have, want)
		}
	}
}

func testServerTLS(t *testing.T) *tls.Config {
	cert, err := tls.LoadX509KeyPair("../testdata/server.crt", "../testdata/server.key")
	if err != nil {
		t.Fatal(err)
	}

	cp := x509.NewCertPool()
	rootca, err := ioutil.ReadFile("../testdata/client.crt")
	if err != nil {
		t.Fatal(err)
	}
	if !cp.AppendCertsFromPEM(rootca) {
		t.Fatal("client cert err")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   "Server",
		ClientCAs:    cp,
	}
}

func testClientTLS(t *testing.T) *tls.Config {
	cert, err := tls.LoadX509KeyPair("../testdata/client.crt", "../testdata/client.key")
	if err != nil {
		t.Fatal(err)
	}
	cp := x509.NewCertPool()
	rootca, err := ioutil.ReadFile("../testdata/server.crt")
	if err != nil {
		t.Fatal(err)
	}
	if !cp.AppendCertsFromPEM(rootca) {
		t.Fatal("server cert err")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ServerName:   "Server",
		RootCAs:      cp,
	}
}

func TestTLS(t *testing.T) {
	s, err := NewServerTLS("127.0.0.1:0", testServerTLS(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if have := s.Addr().Port; have <= 0 {
		t.Fatalf("have %v, want > 0", have)
	}

	s.Register("PING", func(c *Peer, cmd string, args []string) {
		c.WriteInline("PONG")
	})

	c, err := proto.DialTLS(s.Addr().String(), testClientTLS(t))
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Do("PING")
	if err != nil {
		t.Fatal(err)
	}
	if have, want := res, proto.Inline("PONG"); have != want {
		t.Errorf("have: %s, want: %s", have, want)
	}
}

func TestFormatFloat(t *testing.T) {
	eq := func(t *testing.T, want string, n float64) {
		t.Helper()
		have := formatFloat(n)
		if have != want {
			t.Errorf("have: %s, want: %s", have, want)
		}
	}

	eq(t, "1", 1.0)
	eq(t, "0.0001", 0.0001) // checked
	eq(t, "1.1", 1.1)
	eq(t, "1.01", 1.01)
	eq(t, "1.001", 1.001)
	eq(t, "1.0001", 1.0001)
	eq(t, "2.5339988685347402e-65", 0.000000000000000000000000000000000000000000000000000000000000000025339988685347402)
	eq(t, "2.5339988685347402e-65", 2.5339988685347402e-65)
	eq(t, "3479099956230698", 3479099956230698)
	eq(t, "3.479099956230698e+7", 34790999.56230698123123123)

	eq(t, "1.2", 1.2)
	eq(t, "2.4", 2*1.2)
	eq(t, "3.6", 3*1.2)
	eq(t, "4.8", 4*1.2)
	a := 1.2
	eq(t, "1.2", a)
	a += 1.2
	eq(t, "2.4", a)
	a += 1.2
	eq(t, "3.5999999999999996", a)
	a += 1.2
	eq(t, "4.8", a)
}
