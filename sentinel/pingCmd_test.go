package sentinel

import (
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/matryer/is"

	"github.com/gomodule/redigo/redis"
)

func TestPing(t *testing.T) {
	is := is.New(t)

	m, err := miniredis.Run()
	is.NoErr(err)
	s, err := Run(m)
	is.NoErr(err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	is.NoErr(err)

	// PING command
	{
		v, err := redis.String(c.Do("PING"))
		t.Logf("PING returned: %v", v)
		is.NoErr(err)
		is.True(v == "PONG")
	}
}
