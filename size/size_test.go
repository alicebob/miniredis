package size

import (
	"testing"
)

func TestOf(t *testing.T) {
	tests := []struct {
		name string
		v    interface{}
		want int
	}{
		{
			name: "Array",
			v:    [3]int32{1, 2, 3}, // 3 * 4  = 12
			want: 12,
		},
		{
			name: "Slice",
			v:    make([]int64, 2, 5), // 5 * 8 + 24 = 64
			want: 64,
		},
		{
			name: "String",
			v:    "ABCdef", // 6 + 16 = 22
			want: 22,
		},
		{
			name: "Map",
			// (8 + 3 + 16) + (8 + 4 + 16) = 55
			// 55 + 8 + 10.79 * 2 = 84
			v:    map[int64]string{0: "ABC", 1: "DEFG"},
			want: 84,
		},
		{
			name: "Struct",
			v: struct {
				slice     []int64
				array     [2]bool
				structure struct {
					i int8
					s string
				}
			}{
				slice: []int64{12345, 67890}, // 2 * 8 + 24 = 40
				array: [2]bool{true, false},  // 2 * 1 = 2
				structure: struct {
					i int8
					s string
				}{
					i: 5,     // 1
					s: "abc", // 3 * 1 + 16 = 19
				}, // 20 + 7 (padding) = 27
			}, // 40 + 2 + 27 = 69 + 6 (padding) = 75
			want: 75,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Of(tt.v); got != tt.want {
				t.Errorf("Of() = %v, want %v", got, tt.want)
			}
		})
	}
}
