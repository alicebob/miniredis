// Commands from http://redis.io/commands#connection

package miniredis

import (
	"strconv"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
)

func commandsConnection(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("AUTH", m.cmdAuth)
	srv.HandleFunc("ECHO", m.cmdEcho)
	srv.HandleFunc("PING", m.cmdPing)
	srv.HandleFunc("SELECT", m.cmdSelect)
	srv.HandleFunc("QUIT", m.cmdQuit)
}

// PING
func (m *Miniredis) cmdPing(out resp.ResponseWriter, r *resp.Command) {
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	out.AppendInlineString("PONG")
}

// AUTH
func (m *Miniredis) cmdAuth(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	pw := r.Arg(0).String()

	m.Lock()
	defer m.Unlock()
	if m.password == "" {
		out.AppendError("ERR Client sent AUTH, but no password is set")
		return
	}
	if m.password != pw {
		out.AppendError("ERR invalid password")
		return
	}

	setAuthenticated(redeo.GetClient(r.Context()))
	out.AppendOK()
}

// ECHO
func (m *Miniredis) cmdEcho(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	msg := r.Arg(0).String()
	out.AppendBulkString(msg)
}

// SELECT
func (m *Miniredis) cmdSelect(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	id, err := strconv.Atoi(r.Arg(0).String())
	if err != nil {
		id = 0
	}

	m.Lock()
	defer m.Unlock()

	ctx := getCtx(redeo.GetClient(r.Context()))
	ctx.selectedDB = id

	out.AppendOK()
}

// QUIT
func (m *Miniredis) cmdQuit(out resp.ResponseWriter, r *resp.Command) {
	// QUIT isn't transactionfied and accepts any arguments.
	out.AppendOK()
	redeo.GetClient(r.Context()).Close()
}
