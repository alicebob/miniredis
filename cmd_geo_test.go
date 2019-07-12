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

		// GEORADIUS + WITHDIST + WITHCOORD
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

		// GEORADIUS + WITHCOORD
		res, err = redis.Values(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km", "WITHCOORD"))
		ok(t, err)
		equals(t, 2, len(res))
		for _, loc := range res {
			item := loc.([]interface{})
			var (
				name, _  = redis.String(item[0], nil)
				coord, _ = redis.Float64s(item[1], nil)
			)
			equals(t, 2, len(item))
			if name != "Catania" && name != "Palermo" {
				t.Errorf("unexpected name %q", name)
			}
			equals(t, 2, len(coord))
			if coord[0] == 0.00 || coord[1] == 0.00 {
				t.Errorf("latitude/longitude shouldn't be empty")
			}
		}

		// GEORADIUS + WITHDIST
		res, err = redis.Values(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km", "WITHDIST"))
		ok(t, err)
		equals(t, 2, len(res))
		for _, loc := range res {
			item := loc.([]interface{})
			var (
				name, _     = redis.String(item[0], nil)
				distance, _ = redis.Float64(item[1], nil)
			)
			equals(t, 2, len(item))
			if name != "Catania" && name != "Palermo" {
				t.Errorf("unexpected name %q", name)
			}
			if distance == 0.00 {
				t.Errorf("distance shouldn't be empty")
			}
		}

		// No optional parameters
		res, err = redis.Values(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km"))
		ok(t, err)
		equals(t, 2, len(res))
		for _, loc := range res {
			item := loc.([]interface{})
			var (
				name, _ = redis.String(item[0], nil)
			)
			equals(t, 1, len(item))
			if name != "Catania" && name != "Palermo" {
				t.Errorf("unexpected name %q", name)
			}
		}

		// Too small radius
		res, err = redis.Values(c.Do("GEORADIUS", "Sicily", 15, 37, 1, "km"))
		ok(t, err)
		equals(t, 0, len(res))

		// Wrong coords
		res, err = redis.Values(c.Do("GEORADIUS", "Sicily", 80, 80, 200, "km"))
		ok(t, err)
		equals(t, 0, len(res))

		// Wrong map key
		res, err = redis.Values(c.Do("GEORADIUS", "Capri", 15, 37, 200, "km"))
		ok(t, err)
		equals(t, 0, len(res))

		// Unsupported/unknown distance unit
		res, err = redis.Values(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "mm"))
		if err == nil {
			t.Error("Expected error for unsupported distance unit")
		}
		equals(t, 0, len(res))

		// Wrong parameter type
		res, err = redis.Values(c.Do("GEORADIUS", "Sicily", "abc", "def", "ghi", "m"))
		if err == nil {
			t.Error("Expected error for wrong parameter type")
		}
		equals(t, 0, len(res))

		// Negative coords
		res, err = redis.Values(c.Do("GEORADIUS", "Sicily", -15, -37, 200, "m"))
		if err == nil {
			t.Error("Expected error for negative coords")
		}
		equals(t, 0, len(res))

		// Negative radius
		res, err = redis.Values(c.Do("GEORADIUS", "Sicily", 15, 37, -200, "m"))
		if err == nil {
			t.Error("Expected error for negative radius")
		}
		equals(t, 0, len(res))
	}
}
