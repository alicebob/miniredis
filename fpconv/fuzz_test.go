//go:build !go1.14

package fpconv

import (
	"strings"
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
		if strings.Count(s, ".") > 1 {
			t.Errorf("too many .s")
		}
		if strings.Count(s, "e") > 1 {
			t.Errorf("too many e's")
		}
	})
}
