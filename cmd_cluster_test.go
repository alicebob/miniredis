package miniredis

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

// Test CLUSTER *.
func TestCluster(t *testing.T) {
	s, c := runWithClient(t)

	t.Run("slots", func(t *testing.T) {
		port, err := strconv.Atoi(s.Port())
		ok(t, err)
		mustDo(t, c,
			"CLUSTER", "SLOTS",
			proto.Array(
				proto.Array(
					proto.Int(0),
					proto.Int(16383),
					proto.Array(
						proto.String(s.Host()),
						proto.Int(port),
						proto.String("09dbe9720cda62f7865eabc5fd8857c5d2678366"),
					),
				),
			),
		)
	})

	t.Run("nodes", func(t *testing.T) {
		mustDo(t, c,
			"CLUSTER", "NODES",
			proto.String(fmt.Sprintf("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca %s:%s@%s myself,master - 0 0 1 connected 0-16383", s.Host(), s.Port(), s.Port())),
		)
	})

	t.Run("keyslot", func(t *testing.T) {
		mustDo(t, c,
			"CLUSTER", "keyslot", "{test_key}",
			proto.Int(163),
		)
	})
}
