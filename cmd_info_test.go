package miniredis

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/proto"
)

func TestMiniredis_cmdInfo(t *testing.T) {
	s := RunT(t)

	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("Invalid section name", func(t *testing.T) {
		mustDo(t, c,
			"INFO", "invalid_or_unsupported_section_name",
			proto.Error("section (invalid_or_unsupported_section_name) is not supported"),
		)
	})

	t.Run("No section name in args", func(t *testing.T) {
		mustDo(t, c,
			"INFO",
			proto.String("# Clients\nconnected_clients:1\r\n"),
		)
	})

	t.Run("Success", func(t *testing.T) {
		mustDo(t, c,
			"INFO", "clients",
			proto.String("# Clients\nconnected_clients:1\r\n"),
		)

		c2, err := proto.Dial(s.Addr())
		ok(t, err)
		mustDo(t, c2,
			"INFO", "clients",
			proto.String("# Clients\nconnected_clients:2\r\n"),
		)
		c2.Close()

		time.Sleep(10 * time.Millisecond)

		c3, err := proto.Dial(s.Addr())
		ok(t, err)
		defer c3.Close()
		mustDo(t, c3,
			"INFO", "clients",
			proto.String("# Clients\nconnected_clients:2\r\n"),
		)
	})
}
