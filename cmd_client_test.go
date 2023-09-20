package miniredis

import (
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

// Test CLIENT *.
func TestClient(t *testing.T) {
	t.Run("setname and getname", func(t *testing.T) {
		s := RunT(t)
		c, err := proto.Dial(s.Addr())
		ok(t, err)
		defer c.Close()

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
		s := RunT(t)
		c, err := proto.Dial(s.Addr())
		ok(t, err)
		defer c.Close()

		// Get the client name without setting it first
		mustDo(t, c,
			"CLIENT", "GETNAME",
			proto.Nil,
		)
	})
}
