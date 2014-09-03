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

	srv.HandleFunc("AUTH", func(out *redeo.Responder, _ *redeo.Request) error {
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("ECHO", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("Usage error")
			return nil
		}
		msg := r.Args[0]
		out.WriteString(msg)
		return nil
	})
}
