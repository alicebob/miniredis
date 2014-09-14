// Commands from http://redis.io/commands#list

package miniredis

import (
	"strconv"

	"github.com/bsm/redeo"
)

// commandsList handles list commands (mostly L*)
func commandsList(m *Miniredis, srv *redeo.Server) {
	// BLPOP key [key ...] timeout
	// BRPOP key [key ...] timeout
	// BRPOPLPUSH source destination timeout
	// LINDEX key index
	// LINSERT key BEFORE|AFTER pivot value
	// LLEN key
	srv.HandleFunc("LPOP", m.cmdLpop)
	srv.HandleFunc("LPUSH", m.cmdLpush)
	// LPUSHX key value
	srv.HandleFunc("LRANGE", m.cmdLrange)
	// LREM key count value
	// LSET key index value
	// LTRIM key start stop
	srv.HandleFunc("RPOP", m.cmdRpop)
	// RPOPLPUSH source destination
	srv.HandleFunc("RPUSH", m.cmdRpush)
	// RPUSHX key value
}

// LPOP
func (m *Miniredis) cmdLpop(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'lpop' command")
		return nil
	}
	key := r.Args[0]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		elem, err := db.lpop(key)
		if err != nil {
			if err == ErrKeyNotFound {
				// Non-existing key is fine.
				out.WriteNil()
				return
			}
			out.WriteErrorString(err.Error())
			return
		}
		out.WriteString(elem)
	})
}

// LPUSH
func (m *Miniredis) cmdLpush(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) < 2 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'lpush' command")
		return nil
	}
	key := r.Args[0]
	args := r.Args[1:]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		var newLen int
		var err error
		for _, value := range args {
			newLen, err = db.lpush(key, value)
			if err != nil {
				out.WriteErrorString(err.Error())
				return
			}
		}
		out.WriteInt(newLen)
	})
}

// LRANGE
func (m *Miniredis) cmdLrange(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 3 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'lrange' command")
		return nil
	}
	key := r.Args[0]
	start, err := strconv.Atoi(r.Args[1])
	if err != nil {
		setDirty(r.Client())
		out.WriteErrorString(msgInvalidInt)
		return nil
	}
	end, err := strconv.Atoi(r.Args[2])
	if err != nil {
		setDirty(r.Client())
		out.WriteErrorString(msgInvalidInt)
		return nil
	}

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "list" {
			out.WriteErrorString(msgWrongType)
			return
		}

		l := db.listKeys[key]
		if len(l) == 0 {
			out.WriteBulkLen(0)
			return
		}

		rs, re := redisRange(len(l), start, end)
		out.WriteBulkLen(re - rs)
		for _, el := range l[rs:re] {
			out.WriteString(el)
		}
	})
}

// RPOP
func (m *Miniredis) cmdRpop(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'rpop' command")
		return nil
	}
	key := r.Args[0]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		elem, err := db.pop(key)
		if err != nil {
			if err == ErrKeyNotFound {
				// Non-existing key is fine.
				out.WriteNil()
				return
			}
			out.WriteErrorString(err.Error())
			return
		}
		out.WriteString(elem)
	})
}

// RPUSH
func (m *Miniredis) cmdRpush(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) < 2 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'rpush' command")
		return nil
	}
	key := r.Args[0]
	args := r.Args[1:]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		var newLen int
		var err error
		for _, value := range args {
			newLen, err = db.push(key, value)
			if err != nil {
				out.WriteErrorString(err.Error())
				return
			}
		}
		out.WriteInt(newLen)
	})
}
