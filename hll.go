package miniredis

import (
	"github.com/axiomhq/hyperloglog"
	"github.com/spaolacci/murmur3"
)

type hll struct {
	inner *hyperloglog.Sketch
}

func newHll() *hll {
	return &hll{
		inner: hyperloglog.New14(),
	}
}

// Add returns true if cardinality has been changed, or false otherwise.
func (h *hll) Add(item []byte) bool {
	return h.inner.InsertHash(murmur3.Sum64(item))
}

func (h *hll) Count() int {
	return int(h.inner.Estimate())
}

func (h *hll) Merge(other *hll) {
	_ = h.inner.Merge(other.inner)
}

func (h *hll) Bytes() []byte {
	dataBytes, _ := h.inner.MarshalBinary()
	return dataBytes
}
