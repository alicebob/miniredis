// Commands from http://redis.io/commands#connection

package miniredis

import (
	"github.com/bsm/redeo"
)

func commandsConnection(r *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("PING", func(out *redeo.Responder, _ *redeo.Request) error {
		out.WriteInlineString("PONG")
		return nil
	})
}
