package miniredis

import (
	"testing"

	"github.com/gomodule/redigo/redis"
)

// Test GEOADD / GEORADIUS
func TestGeo(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	{
		_, err := c.Do("GEOADD", "Sicily", 13.361389, 38.115556, "Palermo")
		ok(t, err)
		_, err = c.Do("GEOADD", "Sicily", 15.087269, 37.502669, "Catania")
		ok(t, err)

		res, err := redis.Values(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km", "WITHDIST", "WITHCOORD"))
		ok(t, err)
		equals(t, 2, len(res))
		for _, loc := range res {
			item := loc.([]interface{})
			var (
				name, _     = redis.String(item[0], nil)
				distance, _ = redis.Float64(item[1], nil)
				coord, _    = redis.Float64s(item[2], nil)
			)
			if name != "Catania" && name != "Palermo" {
				t.Errorf("unexpected name %q", name)
			}
			if distance == 0.00 {
				t.Errorf("distance shouldn't be empty")
			}
			equals(t, 2, len(coord))
			if coord[0] == 0.00 || coord[1] == 0.00 {
				t.Errorf("latitude/longitude shouldn't be empty")
			}
		}
	}
}
