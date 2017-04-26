// Commands from http://redis.io/commands#server

package miniredis

import (
	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
)

func commandsServer(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("DBSIZE", m.cmdDbsize)
	srv.HandleFunc("FLUSHALL", m.cmdFlushall)
	srv.HandleFunc("FLUSHDB", m.cmdFlushdb)
}

// DBSIZE
func (m *Miniredis) cmdDbsize(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() > 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		out.AppendInt(int64(len(db.keys)))
	})
}

// FLUSHALL
func (m *Miniredis) cmdFlushall(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() > 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		m.flushAll()
		out.AppendOK()
	})
}

// FLUSHDB
func (m *Miniredis) cmdFlushdb(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() > 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		m.db(ctx.selectedDB).flush()
		out.AppendOK()
	})
}
