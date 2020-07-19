package miniredis

import (
	"testing"

	"github.com/gomodule/redigo/redis"

	"github.com/alicebob/miniredis/v2/proto"
)

func TestGeoadd(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("ok", func(t *testing.T) {
		must1(t, c, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo")
		must1(t, c, "GEOADD", "Sicily", "15.087269", "37.502669", "Catania")
	})

	t.Run("failure cases", func(t *testing.T) {
		mustDo(t, c,
			"GEOADD", "broken", "-190.0", "10.0", "hi",
			proto.Error("ERR invalid longitude,latitude pair -190.000000,10.000000"),
		)
		mustDo(t, c,
			"GEOADD", "broken", "190.0", "10.0", "hi",
			proto.Error("ERR invalid longitude,latitude pair 190.000000,10.000000"),
		)
		mustDo(t, c,
			"GEOADD", "broken", "10.0", "-86.0", "hi",
			proto.Error("ERR invalid longitude,latitude pair 10.000000,-86.000000"),
		)
		mustDo(t, c,
			"GEOADD", "broken", "10.0", "86.0", "hi",
			proto.Error("ERR invalid longitude,latitude pair 10.000000,86.000000"),
		)

		mustDo(t, c,
			"GEOADD", "broken", "notafloat", "10.0", "hi",
			proto.Error("ERR value is not a valid float"),
		)
		mustDo(t, c,
			"GEOADD", "broken", "10.0", "notafloat", "hi",
			proto.Error("ERR value is not a valid float"),
		)
	})
}

func TestGeopos(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	must1(t, c, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo")

	t.Run("ok", func(t *testing.T) {
		mustDo(t, c,
			"GEOPOS", "Sicily", "Palermo",
			proto.Array(
				proto.Strings("13.361389", "38.115556"),
			),
		)
	})

	t.Run("no location", func(t *testing.T) {
		mustDo(t, c,
			"GEOPOS", "Sicily", "Corleone",
			proto.Array(proto.Nil),
		)
	})

	t.Run("failure cases", func(t *testing.T) {
		mustDo(t, c,
			"GEOPOS",
			proto.Error(errWrongNumber("geopos")),
		)
		s.Set("foo", "bar")
		mustDo(t, c,
			"GEOPOS", "foo",
			proto.Error(msgWrongType),
		)
	})
}

// Test GEOADD / GEORADIUS / GEORADIUS_RO
func TestGeo(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	must1(t, c, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo")
	must1(t, c, "GEOADD", "Sicily", "15.087269", "37.502669", "Catania")

	t.Run("WITHDIST WITHCOORD", func(t *testing.T) {
		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST", "WITHCOORD",
			proto.Array(
				proto.Array(
					proto.String("Palermo"),
					proto.String("190.4424"),
					proto.Strings("13.361389", "38.115556"),
				),
				proto.Array(
					proto.String("Catania"),
					proto.String("56.4413"),
					proto.Strings("15.087267", "37.502668"),
				),
			),
		)
	})

	t.Run("WITHCOORD", func(t *testing.T) {
		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHCOORD",
			proto.Array(
				proto.Array(
					proto.String("Palermo"),
					proto.Strings("13.361389", "38.115556"),
				),
				proto.Array(
					proto.String("Catania"),
					proto.Strings("15.087267", "37.502668"),
				),
			),
		)
	})

	t.Run("WITHDIST", func(t *testing.T) {
		// in KM
		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST",
			proto.Array(
				proto.Strings("Palermo", "190.4424"),
				proto.Strings("Catania", "56.4413"),
			),
		)

		// in meter
		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200000", "m", "WITHDIST",
			proto.Array(
				proto.Strings("Palermo", "190442.4351"),
				proto.Strings("Catania", "56441.2660"),
			),
		)
	})

	t.Run("ASC DESC", func(t *testing.T) {
		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200", "km", "ASC",
			proto.Strings("Catania", "Palermo"),
		)

		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200", "km", "DESC",
			proto.Strings("Palermo", "Catania"),
		)
	})

	t.Run("COUNT", func(t *testing.T) {
		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200", "km", "ASC", "COUNT", "1",
			proto.Strings("Catania"),
		)

		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200", "km", "ASC", "COUNT", "99",
			proto.Strings("Catania", "Palermo"),
		)

		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT",
			proto.Error("ERR syntax error"),
		)

		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "notanumber",
			proto.Error(msgInvalidInt),
		)

		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "-12",
			proto.Error("ERR COUNT must be > 0"),
		)
	})

	t.Run("no args", func(t *testing.T) {
		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200", "km",
			proto.Strings("Palermo", "Catania"),
		)

		// Too small radius
		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "1", "km",
			proto.Array(),
		)

		// Wrong coords
		mustDo(t, c,
			"GEORADIUS", "Sicily", "80", "80", "200", "km",
			proto.Array(),
		)

		// Wrong map key
		mustDo(t, c,
			"GEORADIUS", "Capri", "15", "37", "200", "km",
			proto.Array(),
		)

		// Unsupported/unknown distance unit
		mustDo(t, c,
			"GEORADIUS", "Sicily", "15", "37", "200", "mm",
			proto.Error("ERR wrong number of arguments for 'georadius' command"),
		)

		// Wrong parameter type
		mustDo(t, c,
			"GEORADIUS", "Sicily", "abc", "def", "ghi", "m",
			proto.Error("ERR wrong number of arguments for 'georadius' command"),
		)
	})

	t.Run("GEORADIUS_RO", func(t *testing.T) {
		mustDo(t, c,
			"GEORADIUS_RO", "Sicily", "15", "37", "200", "km", "ASC",
			proto.Strings("Catania", "Palermo"),
		)

		mustDo(t, c,
			"GEORADIUS_RO", "Sicily", "15", "37", "200", "km", "STORE", "foo",
			proto.Error("ERR syntax error"),
		)

		mustDo(t, c,
			"GEORADIUS_RO", "Sicily", "15", "37", "200", "km", "STOREDIST", "foo",
			proto.Error("ERR syntax error"),
		)
	})
}

func TestGeodist(t *testing.T) {
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

	t.Run("no unit", func(t *testing.T) {
		d, err := redis.Float64(c.Do("GEODIST", "Sicily", "Palermo", "Catania"))
		ok(t, err)
		equals(t, 166274.1514, d)

		d, err = redis.Float64(c.Do("GEODIST", "Sicily", "Palermo", "Catania", "km"))
		ok(t, err)
		equals(t, 166.2742, d)
	})

	t.Run("no such key", func(t *testing.T) {
		n, err := c.Do("GEODIST", "nosuch", "nosuch", "nosuch")
		ok(t, err)
		equals(t, nil, n)

		n, err = c.Do("GEODIST", "Sicily", "Palermo", "nosuch")
		ok(t, err)
		equals(t, nil, n)

		n, err = c.Do("GEODIST", "Sicily", "nosuch", "Catania")
		ok(t, err)
		equals(t, nil, n)
	})

	t.Run("failure cases", func(t *testing.T) {
		_, err = c.Do("GEODIST")
		mustFail(t, err, "ERR wrong number of arguments for 'geodist' command")
		_, err = c.Do("GEODIST", "Sicily")
		mustFail(t, err, "ERR wrong number of arguments for 'geodist' command")
		_, err = c.Do("GEODIST", "Sicily", "Palermo")
		mustFail(t, err, "ERR wrong number of arguments for 'geodist' command")
		_, err = c.Do("GEODIST", "Sicily", "Palermo", "Catania", "miles")
		mustFail(t, err, "ERR unsupported unit provided. please use m, km, ft, mi")
		_, err = c.Do("GEODIST", "Sicily", "Palermo", "Catania", "m", "too many")
		mustFail(t, err, "ERR syntax error")

		_, err = c.Do("SET", "foo", "bar")
		ok(t, err)

		_, err = c.Do("GEODIST", "foo", "Palermo", "Catania")
		mustFail(t, err, "WRONGTYPE Operation against a key holding the wrong kind of value")
	})
}

// Test GEOADD / GEORADIUSBYMEMBER / GEORADIUSBYMEMBER_RO
func TestGeobymember(t *testing.T) {
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
		res, err := redis.Values(c.Do("GEORADIUSBYMEMBER", "Sicily", "Palermo", 200, "km", "WITHDIST", "WITHCOORD"))
		ok(t, err)
		equals(t, 2, len(res))
		for _, loc := range res {
			item := loc.([]interface{})
			var (
				name, _  = redis.String(item[0], nil)
				coord, _ = redis.Float64s(item[2], nil)
			)
			if name != "Catania" && name != "Palermo" {
				t.Errorf("unexpected name %q", name)
			}

			equals(t, 2, len(coord))
			if coord[0] == 0.00 || coord[1] == 0.00 {
				t.Errorf("latitude/longitude shouldn't be empty")
			}
		}
	})

	t.Run("WITHCOORD", func(t *testing.T) {
		res, err := redis.Values(c.Do("GEORADIUSBYMEMBER", "Sicily", "Palermo", 200, "km", "WITHCOORD"))
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
		res, err := redis.Values(c.Do("GEORADIUSBYMEMBER", "Sicily", "Palermo", 200, "km", "WITHDIST"))
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
		equals(t, 0.0, dist1) // in km
		_, err = redis.Scan(res[1].([]interface{}), &name2, &dist2)
		ok(t, err)
		equals(t, "Catania", name2)
		equals(t, 166.2742, dist2)

		// in meter
		res, err = redis.Values(c.Do("GEORADIUSBYMEMBER", "Sicily", "Catania", 200000, "m", "WITHDIST"))
		ok(t, err)
		equals(t, 2, len(res))
		distance, err := redis.Float64(res[0].([]interface{})[1], nil)
		ok(t, err)
		equals(t, 166274.1514, distance) // in meter
	})

	t.Run("ASC DESC", func(t *testing.T) {
		asc, err := redis.Strings(c.Do("GEORADIUSBYMEMBER", "Sicily", "Palermo", 200, "km", "ASC"))
		ok(t, err)
		equals(t, []string{"Palermo", "Catania"}, asc)

		asc2, err := redis.Strings(c.Do("GEORADIUSBYMEMBER", "Sicily", "Catania", 200, "km", "ASC"))
		ok(t, err)
		equals(t, []string{"Catania", "Palermo"}, asc2)

		desc, err := redis.Strings(c.Do("GEORADIUSBYMEMBER", "Sicily", "Palermo", 200, "km", "DESC"))
		ok(t, err)
		equals(t, []string{"Catania", "Palermo"}, desc)
	})

	t.Run("COUNT", func(t *testing.T) {
		count1, err := redis.Strings(c.Do("GEORADIUSBYMEMBER", "Sicily", "Palermo", 200, "km", "ASC", "COUNT", 1))
		ok(t, err)
		equals(t, []string{"Palermo"}, count1)

		count99, err := redis.Strings(c.Do("GEORADIUSBYMEMBER", "Sicily", "Palermo", 200, "km", "ASC", "COUNT", 99))
		ok(t, err)
		equals(t, []string{"Palermo", "Catania"}, count99)

		_, err = c.Do("GEORADIUSBYMEMBER", "Sicily", "Palermo", 200, "km", "COUNT")
		mustFail(t, err, "ERR syntax error")

		_, err = c.Do("GEORADIUSBYMEMBER", "Sicily", "Palermo", 200, "km", "COUNT", "notanumber")
		mustFail(t, err, msgInvalidInt)

		_, err = c.Do("GEORADIUSBYMEMBER", "Sicily", "Palermo", 200, "km", "COUNT", -12)
		mustFail(t, err, "ERR COUNT must be > 0")
	})

	t.Run("no args", func(t *testing.T) {
		res, err := redis.Strings(c.Do("GEORADIUSBYMEMBER", "Sicily", "Palermo", 200, "km"))
		ok(t, err)
		equals(t, 2, len(res))
		equals(t, []string{"Palermo", "Catania"}, res)

		// Wrong map key
		n, err := c.Do("GEORADIUSBYMEMBER", "Capri", "Palermo", 200, "km")
		ok(t, err)
		equals(t, nil, n)

		// Missing member
		res, err = redis.Strings(c.Do("GEORADIUSBYMEMBER", "Sicily", "nosuch", 200, "km"))
		mustFail(t, err, "ERR could not decode requested zset member")
		equals(t, 0, len(res))

		// Unsupported/unknown distance unit
		res, err = redis.Strings(c.Do("GEORADIUSBYMEMBER", "Sicily", "Palermo", 200, "mm"))
		mustFail(t, err, "ERR wrong number of arguments for 'georadiusbymember' command")
		equals(t, 0, len(res))

		// Wrong parameter type
		res, err = redis.Strings(c.Do("GEORADIUSBYMEMBER", "Sicily", "abc", "def", "ghi", "m"))
		mustFail(t, err, "ERR wrong number of arguments for 'georadiusbymember' command")
		equals(t, 0, len(res))
	})

	t.Run("GEORADIUSBYMEMBER_RO", func(t *testing.T) {
		asc, err := redis.Strings(c.Do("GEORADIUSBYMEMBER_RO", "Sicily", "Palermo", 200, "km", "ASC"))
		ok(t, err)
		equals(t, []string{"Palermo", "Catania"}, asc)

		_, err = c.Do("GEORADIUSBYMEMBER_RO", "Sicily", "Palermo", 200, "km", "STORE", "foo")
		mustFail(t, err, "ERR syntax error")

		_, err = c.Do("GEORADIUSBYMEMBER_RO", "Sicily", "Palermo", 200, "km", "STOREDIST", "foo")
		mustFail(t, err, "ERR syntax error")
	})
}
