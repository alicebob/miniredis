package sentinel

import (
	"testing"

	"github.com/matryer/is"

	"github.com/gomodule/redigo/redis"
)

func TestPing(t *testing.T) {
	is := is.New(t)

	s, err := Run()
	is.NoErr(err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	is.NoErr(err)

	// SET command
	{
		v, err := redis.String(c.Do("PING"))
		is.NoErr(err)
		is.True(v == "PONG")
	}
}
