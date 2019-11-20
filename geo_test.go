package miniredis

import (
	"math"
	"testing"
)

func TestGeolib(t *testing.T) {
	long := 13.36138933897018433
	lat := 38.11555639549629859
	v := toGeohash(long, lat)
	equals(t, v, uint64(3479099956230698))

	longBack, latBack := fromGeohash(uint64(float64(v)))
	assert(t, math.Abs(long-longBack) < 0.000001, "long")
	assert(t, math.Abs(lat-latBack) < 0.000001, "lat")
}
