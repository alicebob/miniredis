// Commands from http://redis.io/commands#transactions

package miniredis

import (
	"github.com/bsm/redeo"
)

func startTx(cl *redeo.Client) {
	if cl.Ctx == nil {
		cl.Ctx = &connCtx{}
	}
	ctx := cl.Ctx.(*connCtx)
	ctx.transaction = []txCmd{}
	ctx.transactionInvalid = false
}
func stopTx(cl *redeo.Client) {
	if cl.Ctx == nil {
		return
	}
	ctx := cl.Ctx.(*connCtx)
	ctx.transaction = nil
	unwatch(cl)
}

func inTx(cl *redeo.Client) bool {
	if cl.Ctx == nil {
		return false
	}
	ctx := cl.Ctx.(*connCtx)
	return ctx.transaction != nil
}

func addTxCmd(cl *redeo.Client, cb txCmd) {
	ctx := cl.Ctx.(*connCtx) // Will fail if we're not in a transaction.
	ctx.transaction = append(ctx.transaction, cb)
}

func invalidTx(cl *redeo.Client) bool {
	ctx := cl.Ctx.(*connCtx) // Will fail if we're not in a transaction.
	return ctx.transactionInvalid
}

func watch(db *redisDB, cl *redeo.Client, key string) {
	if cl.Ctx == nil {
		cl.Ctx = &connCtx{}
	}
	ctx := cl.Ctx.(*connCtx)
	if ctx.watch == nil {
		ctx.watch = map[dbKey]uint{}
	}
	ctx.watch[dbKey{db: db.id, key: key}] = db.keyVersion[key] // Can be 0.
}

func unwatch(cl *redeo.Client) {
	if cl.Ctx == nil {
		return
	}
	ctx := cl.Ctx.(*connCtx)
	ctx.watch = nil
}

// setTxInvalid can be called even when not in an tx. Is an no-op then.
func setTxInvalid(cl *redeo.Client) {
	if cl.Ctx == nil {
		// No transaction. Not relevant.
		return
	}
	ctx := cl.Ctx.(*connCtx)
	ctx.transactionInvalid = true
}

// commandsTransaction handles MULTI &c.
func commandsTransaction(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("MULTI", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 0 {
			out.WriteErrorString("ERR wrong number of arguments for 'multi' command")
			return nil
		}

		if inTx(r.Client()) {
			out.WriteErrorString("ERR MULTI calls can not be nested")
			return nil
		}

		startTx(r.Client())

		out.WriteOK()
		return nil
	})

	srv.HandleFunc("EXEC", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 0 {
			out.WriteErrorString("ERR wrong number of arguments for 'exec' command")
			return nil
		}

		if !inTx(r.Client()) {
			out.WriteErrorString("ERR EXEC without MULTI")
			return nil
		}

		if invalidTx(r.Client()) {
			out.WriteErrorString("EXECABORT Transaction discarded because of previous errors.")
			return nil
		}

		m.Lock()
		defer m.Unlock()

		ctx := r.Client().Ctx.(*connCtx)

		// Check WATCHed keys.
		for t, version := range ctx.watch {
			if m.db(t.db).keyVersion[t.key] > version {
				// Abort! Abort!
				stopTx(r.Client())
				out.WriteBulkLen(0)
				return nil
			}
		}

		out.WriteBulkLen(len(ctx.transaction))
		for _, cb := range ctx.transaction {
			cb(out, r.Client())
		}
		// We're done
		stopTx(r.Client())
		return nil
	})

	srv.HandleFunc("DISCARD", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 0 {
			out.WriteErrorString("ERR wrong number of arguments for 'exec' command")
			return nil
		}

		if !inTx(r.Client()) {
			out.WriteErrorString("ERR DISCARD without MULTI")
			return nil
		}

		stopTx(r.Client())
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("WATCH", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) == 0 {
			out.WriteErrorString("ERR wrong number of arguments for 'watch' command")
			return nil
		}

		if inTx(r.Client()) {
			out.WriteErrorString("ERR WATCH in MULTI")
			return nil
		}

		m.Lock()
		defer m.Unlock()
		db := m.dbFor(r.Client().Ctx)

		for _, key := range r.Args {
			watch(db, r.Client(), key)
		}
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("UNWATCH", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 0 {
			out.WriteErrorString("ERR wrong number of arguments for 'unwatch' command")
			return nil
		}

		// Doesn't matter if UNWATCH is in a TX or not. Looks like a Redis bug to me.

		m.Lock()
		defer m.Unlock()

		cb := func(out *redeo.Responder, cl *redeo.Client) {
			// Do nothing if it's called in a transaction.
			out.WriteOK()
		}
		if inTx(r.Client()) {
			addTxCmd(r.Client(), cb)
			out.WriteInlineString("QUEUED")
			return nil
		}
		unwatch(r.Client())
		out.WriteOK()
		return nil
	})
}
