package fpconv

import (
	"math"
	"testing"
)

func TestFormatFloat(t *testing.T) {
	eq := func(t *testing.T, want string, n float64) {
		t.Helper()
		have := Dtoa(n)
		if have != want {
			t.Errorf("have: %s, want: %s", have, want)
		}
	}

	eq(t, "3.1415", 3.1415)
	eq(t, "0", 0.0)
	eq(t, "inf", math.Inf(1))
	eq(t, "-inf", math.Inf(-1))
	eq(t, "nan", math.NaN())
	eq(t, "1", 1.0)
	eq(t, "-1", -1.0)
	eq(t, "-1.1", -1.1)
	eq(t, "0.0001", 0.0001) // checked
	eq(t, "1.1", 1.1)
	eq(t, "1.01", 1.01)
	eq(t, "1.001", 1.001)
	eq(t, "1.0001", 1.0001)
	eq(t, "2.5339988685347402e-65", 0.000000000000000000000000000000000000000000000000000000000000000025339988685347402)
	eq(t, "2.5339988685347402e-65", 2.5339988685347402e-65)
	eq(t, "3479099956230698", 3479099956230698)
	eq(t, "3.479099956230698e+7", 34790999.56230698123123123)

	eq(t, "1.2", 1.2)
	eq(t, "2.4", 2*1.2)
	eq(t, "3.6", 3*1.2)
	eq(t, "4.8", 4*1.2)
	a := 1.2
	eq(t, "1.2", a)
	a += 1.2
	eq(t, "2.4", a)
	a += 1.2
	eq(t, "3.5999999999999996", a)
	a += 1.2
	eq(t, "4.8", a)
}
