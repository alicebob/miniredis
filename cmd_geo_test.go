package miniredis

import (
	"testing"

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
			proto.Array(proto.NilList),
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
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	must1(t, c, "GEOADD", "Sicily", "13.361389", "38.115556", "Palermo")
	must1(t, c, "GEOADD", "Sicily", "15.087269", "37.502669", "Catania")

	t.Run("no unit", func(t *testing.T) {
		mustDo(t, c,
			"GEODIST", "Sicily", "Palermo", "Catania",
			proto.String("166274.1514"),
		)
		mustDo(t, c,
			"GEODIST", "Sicily", "Palermo", "Catania", "km",
			proto.String("166.2742"),
		)
	})

	t.Run("no such key", func(t *testing.T) {
		mustNil(t, c, "GEODIST", "nosuch", "nosuch", "nosuch")
		mustNil(t, c, "GEODIST", "Sicily", "Palermo", "nosuch")
		mustNil(t, c, "GEODIST", "Sicily", "nosuch", "Catania")
	})

	t.Run("failure cases", func(t *testing.T) {
		mustDo(t, c,
			"GEODIST",
			proto.Error(errWrongNumber("geodist")),
		)
		mustDo(t, c, "GEODIST", "Sicily",
			proto.Error(errWrongNumber("geodist")),
		)
		mustDo(t, c, "GEODIST", "Sicily", "Palermo",
			proto.Error(errWrongNumber("geodist")),
		)
		mustDo(t, c,
			"GEODIST", "Sicily", "Palermo", "Catania", "miles",
			proto.Error("ERR unsupported unit provided. please use m, km, ft, mi"),
		)
		mustDo(t, c,
			"GEODIST", "Sicily", "Palermo", "Catania", "m", "too many",
			proto.Error("ERR syntax error"),
		)

		mustOK(t, c, "SET", "foo", "bar")
		mustDo(t, c,
			"GEODIST", "foo", "Palermo", "Catania",
			proto.Error("WRONGTYPE Operation against a key holding the wrong kind of value"),
		)
	})
}

// Test GEOADD / GEORADIUSBYMEMBER / GEORADIUSBYMEMBER_RO
func TestGeobymember(t *testing.T) {
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
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "WITHDIST", "WITHCOORD",
			proto.Array(
				proto.Array(proto.String("Palermo"), proto.String("0.0000"), proto.Strings("13.361389", "38.115556")),
				proto.Array(proto.String("Catania"), proto.String("166.2742"), proto.Strings("15.087267", "37.502668")),
			),
		)
	})

	t.Run("WITHCOORD", func(t *testing.T) {
		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "WITHCOORD",
			proto.Array(
				proto.Array(proto.String("Palermo"), proto.Strings("13.361389", "38.115556")),
				proto.Array(proto.String("Catania"), proto.Strings("15.087267", "37.502668")),
			),
		)
	})

	t.Run("WITHDIST", func(t *testing.T) {
		// in km
		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "WITHDIST",
			proto.Array(
				proto.Strings("Palermo", "0.0000"),
				proto.Strings("Catania", "166.2742"),
			),
		)

		// in meter
		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200000", "m", "WITHDIST",
			proto.Array(
				proto.Strings("Palermo", "0.0000"),
				proto.Strings("Catania", "166274.1514"), // in meter
			),
		)
	})

	t.Run("ASC DESC", func(t *testing.T) {
		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "ASC",
			proto.Strings("Palermo", "Catania"),
		)

		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Catania", "200", "km", "ASC",
			proto.Strings("Catania", "Palermo"),
		)

		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "DESC",
			proto.Strings("Catania", "Palermo"),
		)
	})

	t.Run("COUNT", func(t *testing.T) {
		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "ASC", "COUNT", "1",
			proto.Strings("Palermo"),
		)

		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "ASC", "COUNT", "99",
			proto.Strings("Palermo", "Catania"),
		)

		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "COUNT",
			proto.Error("ERR syntax error"),
		)

		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "COUNT", "notanumber",
			proto.Error(msgInvalidInt),
		)

		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "COUNT", "-12",
			proto.Error("ERR COUNT must be > 0"),
		)
	})

	t.Run("no args", func(t *testing.T) {
		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km",
			proto.Strings("Palermo", "Catania"),
		)

		// Wrong map key
		mustNil(t, c, "GEORADIUSBYMEMBER", "Capri", "Palermo", "200", "km")

		// Missing member
		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "nosuch", "200", "km",
			proto.Error("ERR could not decode requested zset member"),
		)

		// Unsupported/unknown distance unit
		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "mm",
			proto.Error("ERR wrong number of arguments for 'georadiusbymember' command"),
		)

		// Wrong parameter type
		mustDo(t, c,
			"GEORADIUSBYMEMBER", "Sicily", "abc", "def", "ghi", "m",
			proto.Error("ERR wrong number of arguments for 'georadiusbymember' command"),
		)
	})

	t.Run("GEORADIUSBYMEMBER_RO", func(t *testing.T) {
		mustDo(t, c,
			"GEORADIUSBYMEMBER_RO", "Sicily", "Palermo", "200", "km", "ASC",
			proto.Strings("Palermo", "Catania"),
		)

		mustDo(t, c,
			"GEORADIUSBYMEMBER_RO", "Sicily", "Palermo", "200", "km", "STORE", "foo",
			proto.Error("ERR syntax error"),
		)

		mustDo(t, c,
			"GEORADIUSBYMEMBER_RO", "Sicily", "Palermo", "200", "km", "STOREDIST", "foo",
			proto.Error("ERR syntax error"),
		)
	})
}
