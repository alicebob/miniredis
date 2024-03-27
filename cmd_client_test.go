package miniredis

import (
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

// Test CLIENT *.
func TestClient(t *testing.T) {
	t.Run("setname and getname", func(t *testing.T) {
		_, c := runWithClient(t)

		// Set the client name
		mustDo(t, c,
			"CLIENT", "SETNAME", "miniredis-tests",
			proto.Inline("OK"),
		)

		// Get the client name
		mustDo(t, c,
			"CLIENT", "GETNAME",
			proto.String("miniredis-tests"),
		)
	})

	t.Run("getname without setname", func(t *testing.T) {
		_, c := runWithClient(t)

		// Get the client name without setting it first
		mustDo(t, c,
			"CLIENT", "GETNAME",
			proto.Nil,
		)
	})
}
