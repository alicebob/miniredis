package miniredis

import (
	"fmt"
	"testing"

	"github.com/gomodule/redigo/redis"
)

// Test CLUSTER SLOTS.
func TestClusterSlots(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)

	{
		v, err := redis.Values(c.Do("CLUSTER", "SLOTS"))
		ok(t, err)
		equals(t, 1, len(v))

		v2 := v[0].([]interface{})
		equals(t, 3, len(v2))

		v3 := v2[2].([]interface{})
		equals(t, 3, len(v3))

		addr := fmt.Sprintf("%s:%d", string(v3[0].([]uint8)), v3[1].(int64))
		equals(t, s.Addr(), addr)
	}
}
