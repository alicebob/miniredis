// Commands from http://redis.io/commands#list

package miniredis

import (
	"strconv"
	"strings"
	"time"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
)

type leftright int

const (
	left leftright = iota
	right
)

// commandsList handles list commands (mostly L*)
func commandsList(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("BLPOP", m.cmdBlpop)
	srv.HandleFunc("BRPOP", m.cmdBrpop)
	srv.HandleFunc("BRPOPLPUSH", m.cmdBrpoplpush)
	srv.HandleFunc("LINDEX", m.cmdLindex)
	srv.HandleFunc("LINSERT", m.cmdLinsert)
	srv.HandleFunc("LLEN", m.cmdLlen)
	srv.HandleFunc("LPOP", m.cmdLpop)
	srv.HandleFunc("LPUSH", m.cmdLpush)
	srv.HandleFunc("LPUSHX", m.cmdLpushx)
	srv.HandleFunc("LRANGE", m.cmdLrange)
	srv.HandleFunc("LREM", m.cmdLrem)
	srv.HandleFunc("LSET", m.cmdLset)
	srv.HandleFunc("LTRIM", m.cmdLtrim)
	srv.HandleFunc("RPOP", m.cmdRpop)
	srv.HandleFunc("RPOPLPUSH", m.cmdRpoplpush)
	srv.HandleFunc("RPUSH", m.cmdRpush)
	srv.HandleFunc("RPUSHX", m.cmdRpushx)
}

// BLPOP
func (m *Miniredis) cmdBlpop(out resp.ResponseWriter, r *resp.Command) {
	m.cmdBXpop(out, r, left)
}

// BRPOP
func (m *Miniredis) cmdBrpop(out resp.ResponseWriter, r *resp.Command) {
	m.cmdBXpop(out, r, right)
}

func (m *Miniredis) cmdBXpop(out resp.ResponseWriter, r *resp.Command, lr leftright) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	args := asString(r.Args())
	timeoutS := args[len(args)-1]
	keys := args[:len(args)-1]

	timeout, err := strconv.Atoi(timeoutS)
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidTimeout)
		return
	}
	if timeout < 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNegTimeout)
		return
	}

	blocking(
		m,
		out,
		r,
		time.Duration(timeout)*time.Second,
		func(out resp.ResponseWriter, ctx *connCtx) bool {
			db := m.db(ctx.selectedDB)
			for _, key := range keys {
				if !db.exists(key) {
					continue
				}
				if db.t(key) != "list" {
					out.AppendError(msgWrongType)
					return true
				}

				if len(db.listKeys[key]) == 0 {
					continue
				}
				out.AppendArrayLen(2)
				out.AppendBulkString(key)
				var v string
				switch lr {
				case left:
					v = db.listLpop(key)
				case right:
					v = db.listPop(key)
				}
				out.AppendBulkString(v)
				return true
			}
			return false
		},
		func(out resp.ResponseWriter) {
			// timeout
			out.AppendNil()
		},
	)
}

// LINDEX
func (m *Miniredis) cmdLindex(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()
	offset, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			// No such key
			out.AppendNil()
			return
		}
		if t != "list" {
			out.AppendError(msgWrongType)
			return
		}

		l := db.listKeys[key]
		if offset < 0 {
			offset = len(l) + offset
		}
		if offset < 0 || offset > len(l)-1 {
			out.AppendNil()
			return
		}
		out.AppendBulkString(l[offset])
	})
}

// LINSERT
func (m *Miniredis) cmdLinsert(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 4 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()
	where := 0
	switch strings.ToLower(r.Arg(1).String()) {
	case "before":
		where = -1
	case "after":
		where = +1
	default:
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgSyntaxError)
		return
	}
	pivot := r.Arg(2).String()
	value := r.Arg(3).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			// No such key
			out.AppendInt(0)
			return
		}
		if t != "list" {
			out.AppendError(msgWrongType)
			return
		}

		l := db.listKeys[key]
		for i, el := range l {
			if el != pivot {
				continue
			}

			if where < 0 {
				l = append(l[:i], append(listKey{value}, l[i:]...)...)
			} else {
				if i == len(l)-1 {
					l = append(l, value)
				} else {
					l = append(l[:i+1], append(listKey{value}, l[i+1:]...)...)
				}
			}
			db.listKeys[key] = l
			db.keyVersion[key]++
			out.AppendInt(int64(len(l)))
			return
		}
		out.AppendInt(-1)
	})
}

// LLEN
func (m *Miniredis) cmdLlen(out resp.ResponseWriter, r *resp.Command) {
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
			// No such key. That's zero length.
			out.AppendInt(0)
			return
		}
		if t != "list" {
			out.AppendError(msgWrongType)
			return
		}

		out.AppendInt(int64(len(db.listKeys[key])))
	})
}

// LPOP
func (m *Miniredis) cmdLpop(out resp.ResponseWriter, r *resp.Command) {
	m.cmdXpop(out, r, left)
}

// RPOP
func (m *Miniredis) cmdRpop(out resp.ResponseWriter, r *resp.Command) {
	m.cmdXpop(out, r, right)
}

func (m *Miniredis) cmdXpop(out resp.ResponseWriter, r *resp.Command, lr leftright) {
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
			// Non-existing key is fine.
			out.AppendNil()
			return
		}
		if db.t(key) != "list" {
			out.AppendError(msgWrongType)
			return
		}

		var elem string
		switch lr {
		case left:
			elem = db.listLpop(key)
		case right:
			elem = db.listPop(key)
		}
		out.AppendBulkString(elem)
	})
}

// LPUSH
func (m *Miniredis) cmdLpush(out resp.ResponseWriter, r *resp.Command) {
	m.cmdXpush(out, r, left)
}

// RPUSH
func (m *Miniredis) cmdRpush(out resp.ResponseWriter, r *resp.Command) {
	m.cmdXpush(out, r, right)
}

func (m *Miniredis) cmdXpush(out resp.ResponseWriter, r *resp.Command, lr leftright) {
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

		if db.exists(key) && db.t(key) != "list" {
			out.AppendError(msgWrongType)
			return
		}

		var newLen int
		for _, v := range args {
			switch lr {
			case left:
				newLen = db.listLpush(key, v)
			case right:
				newLen = db.listPush(key, v)
			}
		}
		out.AppendInt(int64(newLen))
	})
}

// LPUSHX
func (m *Miniredis) cmdLpushx(out resp.ResponseWriter, r *resp.Command) {
	m.cmdXpushx(out, r, left)
}

// RPUSHX
func (m *Miniredis) cmdRpushx(out resp.ResponseWriter, r *resp.Command) {
	m.cmdXpushx(out, r, right)
}

func (m *Miniredis) cmdXpushx(out resp.ResponseWriter, r *resp.Command, lr leftright) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()
	value := r.Arg(1).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendInt(0)
			return
		}
		if db.t(key) != "list" {
			out.AppendError(msgWrongType)
			return
		}

		var newLen int
		switch lr {
		case left:
			newLen = db.listLpush(key, value)
		case right:
			newLen = db.listPush(key, value)
		}
		out.AppendInt(int64(newLen))
	})
}

// LRANGE
func (m *Miniredis) cmdLrange(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()
	start, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}
	end, err := strconv.Atoi(r.Arg(2).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "list" {
			out.AppendError(msgWrongType)
			return
		}

		l := db.listKeys[key]
		if len(l) == 0 {
			out.AppendArrayLen(0)
			return
		}

		rs, re := redisRange(len(l), start, end, false)
		out.AppendArrayLen(re - rs)
		for _, el := range l[rs:re] {
			out.AppendBulkString(el)
		}
	})
}

// LREM
func (m *Miniredis) cmdLrem(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()
	count, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}
	value := r.Arg(2).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendInt(0)
			return
		}
		if db.t(key) != "list" {
			out.AppendError(msgWrongType)
			return
		}

		l := db.listKeys[key]
		if count < 0 {
			reverseSlice(l)
		}
		deleted := 0
		newL := []string{}
		toDelete := len(l)
		if count < 0 {
			toDelete = -count
		}
		if count > 0 {
			toDelete = count
		}
		for _, el := range l {
			if el == value {
				if toDelete > 0 {
					deleted++
					toDelete--
					continue
				}
			}
			newL = append(newL, el)
		}
		if count < 0 {
			reverseSlice(newL)
		}
		if len(newL) == 0 {
			db.del(key, true)
		} else {
			db.listKeys[key] = newL
			db.keyVersion[key]++
		}

		out.AppendInt(int64(deleted))
	})
}

// LSET
func (m *Miniredis) cmdLset(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()
	index, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}
	value := r.Arg(2).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendError(msgKeyNotFound)
			return
		}
		if db.t(key) != "list" {
			out.AppendError(msgWrongType)
			return
		}

		l := db.listKeys[key]
		if index < 0 {
			index = len(l) + index
		}
		if index < 0 || index > len(l)-1 {
			out.AppendError(msgOutOfRange)
			return
		}
		l[index] = value
		db.keyVersion[key]++

		out.AppendOK()
	})
}

// LTRIM
func (m *Miniredis) cmdLtrim(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()
	start, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}
	end, err := strconv.Atoi(r.Arg(2).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			out.AppendOK()
			return
		}
		if t != "list" {
			out.AppendError(msgWrongType)
			return
		}

		l := db.listKeys[key]
		rs, re := redisRange(len(l), start, end, false)
		l = l[rs:re]
		if len(l) == 0 {
			db.del(key, true)
		} else {
			db.listKeys[key] = l
			db.keyVersion[key]++
		}
		out.AppendOK()
	})
}

// RPOPLPUSH
func (m *Miniredis) cmdRpoplpush(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	src := r.Arg(0).String()
	dst := r.Arg(1).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(src) {
			out.AppendNil()
			return
		}
		if db.t(src) != "list" || (db.exists(dst) && db.t(dst) != "list") {
			out.AppendError(msgWrongType)
			return
		}
		elem := db.listPop(src)
		db.listLpush(dst, elem)
		out.AppendBulkString(elem)
	})
}

// BRPOPLPUSH
func (m *Miniredis) cmdBrpoplpush(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	src := r.Arg(0).String()
	dst := r.Arg(1).String()
	timeout, err := strconv.Atoi(r.Arg(2).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidTimeout)
		return
	}
	if timeout < 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNegTimeout)
		return
	}

	blocking(
		m,
		out,
		r,
		time.Duration(timeout)*time.Second,
		func(out resp.ResponseWriter, ctx *connCtx) bool {
			db := m.db(ctx.selectedDB)

			if !db.exists(src) {
				return false
			}
			if db.t(src) != "list" || (db.exists(dst) && db.t(dst) != "list") {
				out.AppendError(msgWrongType)
				return true
			}
			if len(db.listKeys[src]) == 0 {
				return false
			}
			elem := db.listPop(src)
			db.listLpush(dst, elem)
			out.AppendBulkString(elem)
			return true
		},
		func(out resp.ResponseWriter) {
			// timeout
			out.AppendNil()
		},
	)
}
