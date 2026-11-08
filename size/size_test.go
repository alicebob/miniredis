package size

import (
	"testing"
)

func TestOf(t *testing.T) {
	t.Run("Array", func(t *testing.T) {
		// Arrays have no header, so the size is fully determined: 3 * 4 bytes.
		if got, want := Of([3]int32{1, 2, 3}), 12; got != want {
			t.Errorf("Of() = %v, want %v", got, want)
		}
	})

	t.Run("Slice", func(t *testing.T) {
		// The slice header size is platform dependent, so measure it from an
		// empty slice and assert only the fixed-size backing array contribution.
		base := Of([]int64{})
		if base <= 0 {
			t.Fatalf("slice header size should be positive, got %v", base)
		}
		if got, want := Of(make([]int64, 2, 5)), base+5*8; got != want {
			t.Errorf("Of() = %v, want %v", got, want)
		}
	})

	t.Run("String", func(t *testing.T) {
		base := Of("")
		if base <= 0 {
			t.Fatalf("string header size should be positive, got %v", base)
		}
		if got, want := Of("ABCdef"), base+6; got != want {
			t.Errorf("Of() = %v, want %v", got, want)
		}
	})

	t.Run("Map", func(t *testing.T) {
		base := Of(map[int64]string{})
		if base <= 0 {
			t.Fatalf("map header size should be positive, got %v", base)
		}
		got := Of(map[int64]string{0: "ABC", 1: "DEFG"})
		// Every entry must account for its key and value; any bucket overhead
		// only adds to that.
		min := base + Of(int64(0)) + Of("ABC") + Of(int64(1)) + Of("DEFG")
		if got <= min {
			t.Errorf("Of() = %v, want > %v", got, min)
		}
	})

	t.Run("Struct", func(t *testing.T) {
		// Structs with a single field (or identical field types) have no
		// padding, so Of must equal the sum of the fields.
		withSlice := struct {
			s []int64
		}{s: []int64{12345, 67890}}
		if got, want := Of(withSlice), Of([]int64{12345, 67890}); got != want {
			t.Errorf("Of(struct with slice) = %v, want %v", got, want)
		}

		withArray := struct {
			a [2]bool
		}{a: [2]bool{true, false}}
		if got, want := Of(withArray), Of([2]bool{true, false}); got != want {
			t.Errorf("Of(struct with array) = %v, want %v", got, want)
		}

		withStrings := struct {
			a, b string
		}{"ABC", "DEFG"}
		if got, want := Of(withStrings), Of("ABC")+Of("DEFG"); got != want {
			t.Errorf("Of(struct with strings) = %v, want %v", got, want)
		}

		nested := struct {
			inner struct {
				a, b string
			}
		}{
			inner: struct {
				a, b string
			}{"ABC", "DEFG"},
		}
		if got, want := Of(nested), Of("ABC")+Of("DEFG"); got != want {
			t.Errorf("Of(nested struct) = %v, want %v", got, want)
		}

		// Mixed field types can introduce padding; the result is never less
		// than the sum of the field sizes.
		mixed := struct {
			i int8
			s string
		}{i: 5, s: "abc"}
		if got, min := Of(mixed), Of(int8(5))+Of("abc"); got < min {
			t.Errorf("Of(struct with padding) = %v, want >= %v", got, min)
		}
	})
}
