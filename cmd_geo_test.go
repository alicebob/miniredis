package miniredis

import (
	"testing"

	"github.com/gomodule/redigo/redis"
)

func TestGeoadd(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("ok", func(t *testing.T) {
		_, err := c.Do("GEOADD", "Sicily", 13.361389, 38.115556, "Palermo")
		ok(t, err)
		_, err = c.Do("GEOADD", "Sicily", 15.087269, 37.502669, "Catania")
		ok(t, err)
	})

	t.Run("failure cases", func(t *testing.T) {
		_, err = c.Do("GEOADD", "broken", -190.0, 10.0, "hi")
		mustFail(t, err, "ERR invalid longitude,latitude pair -190.000000,10.000000")
		_, err = c.Do("GEOADD", "broken", 190.0, 10.0, "hi")
		mustFail(t, err, "ERR invalid longitude,latitude pair 190.000000,10.000000")
		_, err := c.Do("GEOADD", "broken", 10.0, -86.0, "hi")
		mustFail(t, err, "ERR invalid longitude,latitude pair 10.000000,-86.000000")
		_, err = c.Do("GEOADD", "broken", 10.0, 86.0, "hi")
		mustFail(t, err, "ERR invalid longitude,latitude pair 10.000000,86.000000")

		_, err = c.Do("GEOADD", "broken", "notafloat", 10.0, "hi")
		mustFail(t, err, "ERR value is not a valid float")
		_, err = c.Do("GEOADD", "broken", 10.0, "notafloat", "hi")
		mustFail(t, err, "ERR value is not a valid float")
	})
}

func TestGeopos(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	_, err = c.Do("GEOADD", "Sicily", 13.361389, 38.115556, "Palermo")
	ok(t, err)

	t.Run("ok", func(t *testing.T) {
		pos, err := redis.Positions(c.Do("GEOPOS", "Sicily", "Palermo"))
		ok(t, err)
		equals(t, 1, len(pos))
		equals(t, [2]float64{13.36139, 38.11556}, *pos[0])
	})

	t.Run("no location", func(t *testing.T) {
		pos, err := redis.Positions(c.Do("GEOPOS", "Sicily", "Corleone"))
		ok(t, err)
		equals(t, []*[2]float64{nil}, pos)
	})

	t.Run("failure cases", func(t *testing.T) {
		_, err = c.Do("GEOPOS")
		mustFail(t, err, "ERR wrong number of arguments for 'geopos' command")
		_, err = c.Do("SET", "foo", "bar")
		ok(t, err)
		_, err = c.Do("GEOPOS", "foo")
		mustFail(t, err, msgWrongType)
	})
}

// Test GEOADD / GEORADIUS / GEORADIUS_RO
func TestGeo(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := redis.Dial("tcp", s.Addr())
	ok(t, err)
	defer c.Close()

	_, err = c.Do("GEOADD", "Sicily", 13.361389, 38.115556, "Palermo")
	ok(t, err)
	_, err = c.Do("GEOADD", "Sicily", 15.087269, 37.502669, "Catania")
	ok(t, err)

	t.Run("WITHDIST WITHCOORD", func(t *testing.T) {
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
	})

	t.Run("WITHCOORD", func(t *testing.T) {
		res, err := redis.Values(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km", "WITHCOORD"))
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
	})

	t.Run("WITHDIST", func(t *testing.T) {
		// in km
		res, err := redis.Values(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km", "WITHDIST"))
		ok(t, err)
		equals(t, 2, len(res))
		var (
			name1, name2 string
			dist1, dist2 float64
		)
		leftover, err := redis.Scan(res[0].([]interface{}), &name1, &dist1)
		ok(t, err)
		equals(t, 0, len(leftover))
		equals(t, "Palermo", name1)
		equals(t, 190.4426, dist1) // in km
		_, err = redis.Scan(res[1].([]interface{}), &name2, &dist2)
		ok(t, err)
		equals(t, "Catania", name2)
		equals(t, 56.4412, dist2)

		// in meter
		res, err = redis.Values(c.Do("GEORADIUS", "Sicily", 15, 37, 200000, "m", "WITHDIST"))
		ok(t, err)
		equals(t, 2, len(res))
		distance, err := redis.Float64(res[0].([]interface{})[1], nil)
		ok(t, err)
		equals(t, 190442.5579, distance) // in meter
	})

	t.Run("ASC DESC", func(t *testing.T) {
		asc, err := redis.Strings(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km", "ASC"))
		ok(t, err)
		equals(t, []string{"Catania", "Palermo"}, asc)

		desc, err := redis.Strings(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km", "DESC"))
		ok(t, err)
		equals(t, []string{"Palermo", "Catania"}, desc)
	})

	t.Run("COUNT", func(t *testing.T) {
		count1, err := redis.Strings(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km", "ASC", "COUNT", 1))
		ok(t, err)
		equals(t, []string{"Catania"}, count1)

		count99, err := redis.Strings(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km", "ASC", "COUNT", 99))
		ok(t, err)
		equals(t, []string{"Catania", "Palermo"}, count99)

		_, err = c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km", "COUNT")
		mustFail(t, err, "ERR syntax error")

		_, err = c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km", "COUNT", "notanumber")
		mustFail(t, err, msgInvalidInt)

		_, err = c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km", "COUNT", -12)
		mustFail(t, err, "ERR COUNT must be > 0")
	})

	t.Run("no args", func(t *testing.T) {
		res, err := redis.Strings(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "km"))
		ok(t, err)
		equals(t, 2, len(res))
		equals(t, []string{"Palermo", "Catania"}, res)

		// Too small radius
		res, err = redis.Strings(c.Do("GEORADIUS", "Sicily", 15, 37, 1, "km"))
		ok(t, err)
		equals(t, 0, len(res))

		// Wrong coords
		res, err = redis.Strings(c.Do("GEORADIUS", "Sicily", 80, 80, 200, "km"))
		ok(t, err)
		equals(t, 0, len(res))

		// Wrong map key
		res, err = redis.Strings(c.Do("GEORADIUS", "Capri", 15, 37, 200, "km"))
		ok(t, err)
		equals(t, 0, len(res))

		// Unsupported/unknown distance unit
		res, err = redis.Strings(c.Do("GEORADIUS", "Sicily", 15, 37, 200, "mm"))
		mustFail(t, err, "ERR wrong number of arguments for 'georadius' command")
		equals(t, 0, len(res))

		// Wrong parameter type
		res, err = redis.Strings(c.Do("GEORADIUS", "Sicily", "abc", "def", "ghi", "m"))
		mustFail(t, err, "ERR wrong number of arguments for 'georadius' command")
		equals(t, 0, len(res))
	})

	t.Run("GEORADIUS_RO", func(t *testing.T) {
		asc, err := redis.Strings(c.Do("GEORADIUS_RO", "Sicily", 15, 37, 200, "km", "ASC"))
		ok(t, err)
		equals(t, []string{"Catania", "Palermo"}, asc)

		_, err = c.Do("GEORADIUS_RO", "Sicily", 15, 37, 200, "km", "STORE", "foo")
		mustFail(t, err, "ERR syntax error")

		_, err = c.Do("GEORADIUS_RO", "Sicily", 15, 37, 200, "km", "STOREDIST", "foo")
		mustFail(t, err, "ERR syntax error")
	})
}
