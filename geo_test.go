package miniredis

import (
	"testing"
)

func TestGeolib(t *testing.T) {
	long := 13.36138933897018433
	lat := 38.11555639549629859
	v := toGeohash(long, lat)
	equals(t, v, uint64(3479099956230698))

	longBack, latBack := fromGeohash(uint64(float64(v)))
	equals(t, formatGeo(long), formatGeo(longBack))
	equals(t, formatGeo(lat), formatGeo(latBack))
}
