package miniredis

import (
	hllLib "github.com/segmentio/go-hll"
	"github.com/spaolacci/murmur3"
)

type hll struct {
	inner hllLib.Hll
}

func newHll() *hll {
	inner, _ := hllLib.NewHll(hllLib.Settings{
		Log2m:             12,
		Regwidth:          6,
		ExplicitThreshold: 0,
		SparseEnabled:     true,
	})
	return &hll{
		inner: inner,
	}
}

// Add returns true if cardinality has been changed, or false otherwise.
func (h *hll) Add(item []byte) bool {
	prevCard := h.inner.Cardinality()
	h.inner.AddRaw(murmur3.Sum64(item))
	newCard := h.inner.Cardinality()
	return prevCard != newCard
}

func (h *hll) Count() int {
	return int(h.inner.Cardinality())
}

func (h *hll) Merge(other *hll) {
	h.inner.Union(other.inner)
}
