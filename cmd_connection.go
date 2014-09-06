// Commands from http://redis.io/commands#connection

package miniredis

import (
	"strconv"

	"github.com/bsm/redeo"
)

// Select sets the DB id for all direct commands.
func (m *Miniredis) Select(i int) {
	m.Lock()
	defer m.Unlock()
	m.clientDB = i
}

func commandsConnection(m *Miniredis, srv *redeo.Server) {
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
			out.WriteErrorString("usage error")
			return nil
		}
		msg := r.Args[0]
		out.WriteString(msg)
		return nil
	})

	srv.HandleFunc("SELECT", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("usage error")
			return nil
		}
		id, err := strconv.Atoi(r.Args[0])
		if err != nil {
			id = 0
		}

		m.Lock()
		defer m.Unlock()
		m.selectDB[r.Client().ID] = id
		out.WriteOK()
		return nil
	})
}
