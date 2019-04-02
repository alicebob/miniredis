package sentinel

import (
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/matryer/is"

	"github.com/gomodule/redigo/redis"
)

func TestNewSentinel(t *testing.T) {
	is := is.New(t)
	m, err := miniredis.Run()
	is.NoErr(err)
	defer m.Close()

	s := NewSentinel(WithMaster(m), WithReplicas([]*miniredis.Miniredis{m}))
	is.Equal(s.Master(), m)      // make sure the master is correctly set
	is.Equal(s.Replicas()[0], m) // make sure the replicas are correctly set

	err = s.Start()
	is.NoErr(err)
	defer s.Close()

	c, err := redis.Dial("tcp", s.Addr())
	is.NoErr(err)

	// PING command
	{
		v, err := redis.String(c.Do("PING"))
		is.NoErr(err)
		is.True(v == "PONG")
	}

}
