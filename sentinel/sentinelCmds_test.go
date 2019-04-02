package sentinel

import (
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/matryer/is"

	"github.com/gomodule/redigo/redis"
)

func TestSentinelCmds(t *testing.T) {
	is := is.New(t)
	m, err := miniredis.Run()
	is.NoErr(err)
	defer m.Close()

	s := NewSentinel(m, WithReplicas([]*miniredis.Miniredis{m}))
	is.Equal(s.Master(), m)      // make sure the master is correctly set
	is.Equal(s.Replicas()[0], m) // make sure the replicas are correctly set

	err = s.Start()
	is.NoErr(err)
	defer s.Close()

	c, err := redis.Dial("tcp", s.Addr())
	is.NoErr(err)

	// MASTERS command
	{
		// results is an []interfaces which points to [][]strings
		results, err := c.Do("SENTINEL", "MASTERS")
		is.NoErr(err)
		info, err := redis.Strings(results.([]interface{})[0], nil)
		t.Logf("%v", info)
		for _, v := range info {
			t.Logf("%v", v)
		}
	}

}
