// Commands from http://redis.io/commands#hash

package miniredis

import (
	"strconv"
	"strings"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
)

// commandsHash handles all hash value operations.
func commandsHash(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("HDEL", m.cmdHdel)
	srv.HandleFunc("HEXISTS", m.cmdHexists)
	srv.HandleFunc("HGET", m.cmdHget)
	srv.HandleFunc("HGETALL", m.cmdHgetall)
	srv.HandleFunc("HINCRBY", m.cmdHincrby)
	srv.HandleFunc("HINCRBYFLOAT", m.cmdHincrbyfloat)
	srv.HandleFunc("HKEYS", m.cmdHkeys)
	srv.HandleFunc("HLEN", m.cmdHlen)
	srv.HandleFunc("HMGET", m.cmdHmget)
	srv.HandleFunc("HMSET", m.cmdHmset)
	srv.HandleFunc("HSET", m.cmdHset)
	srv.HandleFunc("HSETNX", m.cmdHsetnx)
	srv.HandleFunc("HVALS", m.cmdHvals)
	srv.HandleFunc("HSCAN", m.cmdHscan)
}

// HSET
func (m *Miniredis) cmdHset(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	var (
		key   = r.Arg(0).String()
		field = r.Arg(1).String()
		value = r.Arg(2).String()
	)

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "hash" {
			out.AppendError(msgWrongType)
			return
		}

		if db.hashSet(key, field, value) {
			out.AppendInt(0)
		} else {
			out.AppendInt(1)
		}
	})
}

// HSETNX
func (m *Miniredis) cmdHsetnx(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	var (
		key   = r.Arg(0).String()
		field = r.Arg(1).String()
		value = r.Arg(2).String()
	)

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "hash" {
			out.AppendError(msgWrongType)
			return
		}

		if _, ok := db.hashKeys[key]; !ok {
			db.hashKeys[key] = map[string]string{}
			db.keys[key] = "hash"
		}
		_, ok := db.hashKeys[key][field]
		if ok {
			out.AppendInt(0)
			return
		}
		db.hashKeys[key][field] = value
		db.keyVersion[key]++
		out.AppendInt(1)
	})
}

// HMSET
func (m *Miniredis) cmdHmset(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	args := asString(r.Args()[1:])
	if len(args)%2 != 0 {
		setDirty(redeo.GetClient(r.Context()))
		// non-default error message
		out.AppendError("ERR wrong number of arguments for HMSET")
		return
	}
	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "hash" {
			out.AppendError(msgWrongType)
			return
		}

		for len(args) > 0 {
			field := args[0]
			value := args[1]
			args = args[2:]

			db.hashSet(key, field, value)
		}
		out.AppendOK()
	})
}

// HGET
func (m *Miniredis) cmdHget(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	field := r.Arg(1).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			out.AppendNil()
			return
		}
		if t != "hash" {
			out.AppendError(msgWrongType)
			return
		}
		value, ok := db.hashKeys[key][field]
		if !ok {
			out.AppendNil()
			return
		}
		out.AppendBulkString(value)
	})
}

// HDEL
func (m *Miniredis) cmdHdel(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	fields := asString(r.Args()[1:])

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			// No key is zero deleted
			out.AppendInt(0)
			return
		}
		if t != "hash" {
			out.AppendError(msgWrongType)
			return
		}

		deleted := 0
		for _, f := range fields {
			_, ok := db.hashKeys[key][f]
			if !ok {
				continue
			}
			delete(db.hashKeys[key], f)
			deleted++
		}
		out.AppendInt(int64(deleted))

		// Nothing left. Remove the whole key.
		if len(db.hashKeys[key]) == 0 {
			db.del(key, true)
		}
	})
}

// HEXISTS
func (m *Miniredis) cmdHexists(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	field := r.Arg(1).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			out.AppendInt(0)
			return
		}
		if t != "hash" {
			out.AppendError(msgWrongType)
			return
		}

		if _, ok := db.hashKeys[key][field]; !ok {
			out.AppendInt(0)
			return
		}
		out.AppendInt(1)
	})
}

// HGETALL
func (m *Miniredis) cmdHgetall(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			out.AppendArrayLen(0)
			return
		}
		if t != "hash" {
			out.AppendError(msgWrongType)
			return
		}

		out.AppendArrayLen(len(db.hashKeys[key]) * 2)
		for _, k := range db.hashFields(key) {
			out.AppendBulkString(k)
			out.AppendBulkString(db.hashGet(key, k))
		}
	})
}

// HKEYS
func (m *Miniredis) cmdHkeys(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendArrayLen(0)
			return
		}
		if db.t(key) != "hash" {
			out.AppendError(msgWrongType)
			return
		}

		fields := db.hashFields(key)
		out.AppendArrayLen(len(fields))
		for _, f := range fields {
			out.AppendBulkString(f)
		}
	})
}

// HVALS
func (m *Miniredis) cmdHvals(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			out.AppendArrayLen(0)
			return
		}
		if t != "hash" {
			out.AppendError(msgWrongType)
			return
		}

		out.AppendArrayLen(len(db.hashKeys[key]))
		for _, v := range db.hashKeys[key] {
			out.AppendBulkString(v)
		}
	})
}

// HLEN
func (m *Miniredis) cmdHlen(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			out.AppendInt(0)
			return
		}
		if t != "hash" {
			out.AppendError(msgWrongType)
			return
		}

		out.AppendInt(int64(len(db.hashKeys[key])))
	})
}

// HMGET
func (m *Miniredis) cmdHmget(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	args := asString(r.Args()[1:])

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "hash" {
			out.AppendError(msgWrongType)
			return
		}

		f, ok := db.hashKeys[key]
		if !ok {
			f = map[string]string{}
		}

		out.AppendArrayLen(len(args))
		for _, k := range args {
			v, ok := f[k]
			if !ok {
				out.AppendNil()
				continue
			}
			out.AppendBulkString(v)
		}
	})
}

// HINCRBY
func (m *Miniredis) cmdHincrby(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	var (
		key        = r.Arg(0).String()
		field      = r.Arg(1).String()
		delta, err = strconv.Atoi(r.Arg(2).String())
	)
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "hash" {
			out.AppendError(msgWrongType)
			return
		}

		v, err := db.hashIncr(key, field, delta)
		if err != nil {
			out.AppendError(err.Error())
			return
		}
		out.AppendInt(int64(v))
	})
}

// HINCRBYFLOAT
func (m *Miniredis) cmdHincrbyfloat(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	var (
		key        = r.Arg(0).String()
		field      = r.Arg(1).String()
		delta, err = r.Arg(2).Float()
	)
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidFloat)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "hash" {
			out.AppendError(msgWrongType)
			return
		}

		v, err := db.hashIncrfloat(key, field, delta)
		if err != nil {
			out.AppendError(err.Error())
			return
		}
		out.AppendBulkString(formatFloat(v))
	})
}

// HSCAN
func (m *Miniredis) cmdHscan(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	cursor, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidCursor)
		return
	}
	// MATCH and COUNT options
	var withMatch bool
	var match string
	args := asString(r.Args()[2:])
	for len(args) > 0 {
		if strings.ToLower(args[0]) == "count" {
			if len(args) < 2 {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgSyntaxError)
				return
			}
			_, err := strconv.Atoi(args[1])
			if err != nil {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgInvalidInt)
				return
			}
			// We do nothing with count.
			args = args[2:]
			continue
		}
		if strings.ToLower(args[0]) == "match" {
			if len(args) < 2 {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgSyntaxError)
				return
			}
			withMatch = true
			match = args[1]
			args = args[2:]
			continue
		}
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgSyntaxError)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)
		// We return _all_ (matched) keys every time.

		if cursor != 0 {
			// Invalid cursor.
			out.AppendArrayLen(2)
			out.AppendBulkString("0") // no next cursor
			out.AppendArrayLen(0)     // no elements
			return
		}
		if db.exists(key) && db.t(key) != "hash" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		members := db.hashFields(key)
		if withMatch {
			members = matchKeys(members, match)
		}

		out.AppendArrayLen(2)
		out.AppendBulkString("0") // no next cursor
		// HSCAN gives key, values.
		out.AppendArrayLen(len(members) * 2)
		for _, k := range members {
			out.AppendBulkString(k)
			out.AppendBulkString(db.hashGet(key, k))
		}
	})
}
