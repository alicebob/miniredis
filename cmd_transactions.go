// Commands from http://redis.io/commands#transactions

package miniredis

import (
	"github.com/bsm/redeo"
)

func startTx(ctx *connCtx) {
	ctx.transaction = []txCmd{}
	ctx.dirtyTransaction = false
}

func stopTx(ctx *connCtx) {
	ctx.transaction = nil
	unwatch(ctx)
}

func inTx(ctx *connCtx) bool {
	return ctx.transaction != nil
}

func addTxCmd(ctx *connCtx, cb txCmd) {
	ctx.transaction = append(ctx.transaction, cb)
}

func dirtyTx(ctx *connCtx) bool {
	return ctx.dirtyTransaction
}

func watch(db *RedisDB, ctx *connCtx, key string) {
	if ctx.watch == nil {
		ctx.watch = map[dbKey]uint{}
	}
	ctx.watch[dbKey{db: db.id, key: key}] = db.keyVersion[key] // Can be 0.
}

func unwatch(ctx *connCtx) {
	ctx.watch = nil
}

// setDirty can be called even when not in an tx. Is an no-op then.
func setDirty(cl *redeo.Client) {
	if cl.Ctx == nil {
		// No transaction. Not relevant.
		return
	}
	getCtx(cl).dirtyTransaction = true
}

// commandsTransaction handles MULTI &c.
func commandsTransaction(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("MULTI", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 0 {
			out.WriteErrorString("ERR wrong number of arguments for 'multi' command")
			return nil
		}
		ctx := getCtx(r.Client())

		if inTx(ctx) {
			out.WriteErrorString("ERR MULTI calls can not be nested")
			return nil
		}

		startTx(ctx)

		out.WriteOK()
		return nil
	})

	srv.HandleFunc("EXEC", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 0 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'exec' command")
			return nil
		}

		ctx := getCtx(r.Client())

		if !inTx(ctx) {
			out.WriteErrorString("ERR EXEC without MULTI")
			return nil
		}

		if dirtyTx(ctx) {
			out.WriteErrorString("EXECABORT Transaction discarded because of previous errors.")
			return nil
		}

		m.Lock()
		defer m.Unlock()

		// Check WATCHed keys.
		for t, version := range ctx.watch {
			if m.db(t.db).keyVersion[t.key] > version {
				// Abort! Abort!
				stopTx(ctx)
				out.WriteBulkLen(0)
				return nil
			}
		}

		out.WriteBulkLen(len(ctx.transaction))
		for _, cb := range ctx.transaction {
			cb(out, ctx)
		}
		// We're done
		stopTx(ctx)
		return nil
	})

	srv.HandleFunc("DISCARD", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 0 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'discard' command")
			return nil
		}

		ctx := getCtx(r.Client())
		if !inTx(ctx) {
			out.WriteErrorString("ERR DISCARD without MULTI")
			return nil
		}

		stopTx(ctx)
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("WATCH", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) == 0 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'watch' command")
			return nil
		}

		ctx := getCtx(r.Client())
		if inTx(ctx) {
			out.WriteErrorString("ERR WATCH in MULTI")
			return nil
		}

		m.Lock()
		defer m.Unlock()
		db := m.db(ctx.selectedDB)

		for _, key := range r.Args {
			watch(db, ctx, key)
		}
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("UNWATCH", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 0 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'unwatch' command")
			return nil
		}

		// Doesn't matter if UNWATCH is in a TX or not. Looks like a Redis bug to me.
		unwatch(getCtx(r.Client()))

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			// Do nothing if it's called in a transaction.
			out.WriteOK()
		})
	})
}
