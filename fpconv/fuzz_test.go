//go:build go1.18
// +build go1.18

package fpconv

import (
	"math"
	"strconv"
	"testing"
)

func FuzzDtoa(f *testing.F) {
	f.Add(-3.1415)
	f.Add(0.0)
	f.Add(2.5339988685347402e-65)
	f.Add(3.1415)
	f.Add(math.Inf(1))
	f.Add(math.Inf(-1))
	f.Add(math.NaN())

	f.Fuzz(func(t *testing.T, orig float64) {
		s := Dtoa(orig)
		if s == "" {
			t.Errorf("empty result")
		}
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Errorf("parse failed: %s", err)
		}
		if math.IsNaN(orig) {
			if !math.IsNaN(n) {
				t.Error("not NaN")
			}
			return
		}

		if math.IsNaN(n) {
			t.Error("got NaN")
		}
		if n != orig {
			t.Errorf("changed %f -> %f", n, orig)
		}
	})
}
