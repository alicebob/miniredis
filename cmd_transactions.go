// Commands from http://redis.io/commands#transactions

package miniredis

import (
	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
)

// commandsTransaction handles MULTI &c.
func commandsTransaction(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("DISCARD", m.cmdDiscard)
	srv.HandleFunc("EXEC", m.cmdExec)
	srv.HandleFunc("MULTI", m.cmdMulti)
	srv.HandleFunc("UNWATCH", m.cmdUnwatch)
	srv.HandleFunc("WATCH", m.cmdWatch)
}

// MULTI
func (m *Miniredis) cmdMulti(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 0 {
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	ctx := getCtx(redeo.GetClient(r.Context()))

	if inTx(ctx) {
		out.AppendError("ERR MULTI calls can not be nested")
		return
	}

	startTx(ctx)

	out.AppendOK()
}

// EXEC
func (m *Miniredis) cmdExec(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	ctx := getCtx(redeo.GetClient(r.Context()))

	if !inTx(ctx) {
		out.AppendError("ERR EXEC without MULTI")
		return
	}

	if ctx.dirtyTransaction {
		out.AppendError("EXECABORT Transaction discarded because of previous errors.")
		return
	}

	m.Lock()
	defer m.Unlock()

	// Check WATCHed keys.
	for t, version := range ctx.watch {
		if m.db(t.db).keyVersion[t.key] > version {
			// Abort! Abort!
			stopTx(ctx)
			out.AppendArrayLen(0)
			return
		}
	}

	out.AppendArrayLen(len(ctx.transaction))
	for _, cb := range ctx.transaction {
		cb(out, ctx)
	}
	// wake up anyone who waits on anything.
	m.signal.Broadcast()

	stopTx(ctx)
}

// DISCARD
func (m *Miniredis) cmdDiscard(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	ctx := getCtx(redeo.GetClient(r.Context()))
	if !inTx(ctx) {
		out.AppendError("ERR DISCARD without MULTI")
		return
	}

	stopTx(ctx)
	out.AppendOK()
}

// WATCH
func (m *Miniredis) cmdWatch(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() == 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	ctx := getCtx(redeo.GetClient(r.Context()))
	if inTx(ctx) {
		out.AppendError("ERR WATCH in MULTI")
		return
	}

	m.Lock()
	defer m.Unlock()
	db := m.db(ctx.selectedDB)

	for _, key := range asString(r.Args()) {
		watch(db, ctx, key)
	}
	out.AppendOK()
}

// UNWATCH
func (m *Miniredis) cmdUnwatch(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	// Doesn't matter if UNWATCH is in a TX or not. Looks like a Redis bug to me.
	unwatch(getCtx(redeo.GetClient(r.Context())))

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		// Do nothing if it's called in a transaction.
		out.AppendOK()
	})
}
