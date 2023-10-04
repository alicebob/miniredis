//go:build go1.16
// +build go1.16

package fpconv

import (
	"strconv"
	"testing"
)

func FuzzDtoa(f *testing.F) {
	f.Add(-3.1415)
	f.Add(0.0)
	f.Add(2.5339988685347402e-65)
	f.Add(3.1415)

	f.Fuzz(func(t *testing.T, orig float64) {
		s := Dtoa(orig)
		if s == "" {
			t.Errorf("empty result")
		}
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Errorf("parse failed: %s", err)
		}
		if n != orig {
			t.Errorf("changed %f -> %f", n, orig)
		}
	})
}
