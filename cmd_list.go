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
	srv.HandleFunc("LINDEX", m.cmdLindex)
	// LINSERT key BEFORE|AFTER pivot value
	srv.HandleFunc("LLEN", m.cmdLlen)
	srv.HandleFunc("LPOP", m.cmdLpop)
	srv.HandleFunc("LPUSH", m.cmdLpush)
	// LPUSHX key value
	srv.HandleFunc("LRANGE", m.cmdLrange)
	// LREM key count value
	// LSET key index value
	srv.HandleFunc("LTRIM", m.cmdLtrim)
	srv.HandleFunc("RPOP", m.cmdRpop)
	// RPOPLPUSH source destination
	srv.HandleFunc("RPUSH", m.cmdRpush)
	// RPUSHX key value
}

// LINDEX
func (m *Miniredis) cmdLindex(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 2 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'lindex' command")
		return nil
	}
	key := r.Args[0]
	offset, err := strconv.Atoi(r.Args[1])
	if err != nil {
		setDirty(r.Client())
		out.WriteErrorString(msgInvalidInt)
		return nil
	}

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			// No such key
			out.WriteNil()
			return
		}
		if t != "list" {
			out.WriteErrorString(msgWrongType)
			return
		}

		l := db.listKeys[key]
		if offset < 0 {
			offset = len(l) + offset
		}
		if offset < 0 || offset > len(l)-1 {
			out.WriteNil()
			return
		}
		out.WriteString(l[offset])
	})
}

// LLEN
func (m *Miniredis) cmdLlen(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'llen' command")
		return nil
	}
	key := r.Args[0]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			// No such key. That's zero length.
			out.WriteZero()
			return
		}
		if t != "list" {
			out.WriteErrorString(msgWrongType)
			return
		}

		out.WriteInt(len(db.listKeys[key]))
	})
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

// LTRIM
func (m *Miniredis) cmdLtrim(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 3 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'ltrim' command")
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

		t, ok := db.keys[key]
		if !ok {
			out.WriteOK()
			return
		}
		if t != "list" {
			out.WriteErrorString(msgWrongType)
			return
		}

		l := db.listKeys[key]
		rs, re := redisRange(len(l), start, end)
		db.listKeys[key] = l[rs:re]
		db.keyVersion[key]++
		out.WriteOK()
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
