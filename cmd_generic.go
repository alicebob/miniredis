// Commands from http://redis.io/commands#generic

package miniredis

import (
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
)

// commandsGeneric handles EXPIRE, TTL, PERSIST, &c.
func commandsGeneric(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("DEL", m.cmdDel)
	// DUMP
	srv.HandleFunc("EXISTS", m.cmdExists)
	srv.HandleFunc("EXPIRE", makeCmdExpire(m, false, time.Second))
	srv.HandleFunc("EXPIREAT", makeCmdExpire(m, true, time.Second))
	srv.HandleFunc("KEYS", m.cmdKeys)
	// MIGRATE
	srv.HandleFunc("MOVE", m.cmdMove)
	// OBJECT
	srv.HandleFunc("PERSIST", m.cmdPersist)
	srv.HandleFunc("PEXPIRE", makeCmdExpire(m, false, time.Millisecond))
	srv.HandleFunc("PEXPIREAT", makeCmdExpire(m, true, time.Millisecond))
	srv.HandleFunc("PTTL", m.cmdPTTL)
	srv.HandleFunc("RANDOMKEY", m.cmdRandomkey)
	srv.HandleFunc("RENAME", m.cmdRename)
	srv.HandleFunc("RENAMENX", m.cmdRenamenx)
	// RESTORE
	// SORT
	srv.HandleFunc("TTL", m.cmdTTL)
	srv.HandleFunc("TYPE", m.cmdType)
	srv.HandleFunc("SCAN", m.cmdScan)
}

// generic expire command for EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT
// d is the time unit. If unix is set it'll be seen as a unixtimestamp and
// converted to a duration.
func makeCmdExpire(m *Miniredis, unix bool, d time.Duration) func(resp.ResponseWriter, *resp.Command) {
	return func(out resp.ResponseWriter, r *resp.Command) {
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
		i, err := strconv.Atoi(value)
		if err != nil {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgInvalidInt)
			return
		}

		withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			// Key must be present.
			if _, ok := db.keys[key]; !ok {
				out.AppendInt(0)
				return
			}
			if unix {
				var ts time.Time
				switch d {
				case time.Millisecond:
					ts = time.Unix(0, int64(i))
				case time.Second:
					ts = time.Unix(int64(i), 0)
				default:
					panic("invalid time unit (d). Fixme!")
				}
				now := m.now
				if now.IsZero() {
					now = time.Now().UTC()
				}
				db.ttl[key] = ts.Sub(now)
			} else {
				db.ttl[key] = time.Duration(i) * d
			}
			db.keyVersion[key]++
			db.checkTTL(key)
			out.AppendInt(1)
		})
	}
}

// TTL
func (m *Miniredis) cmdTTL(out resp.ResponseWriter, r *resp.Command) {
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

		if _, ok := db.keys[key]; !ok {
			// No such key
			out.AppendInt(-2)
			return
		}

		v, ok := db.ttl[key]
		if !ok {
			// No expire value
			out.AppendInt(-1)
			return
		}
		out.AppendInt(int64(v.Seconds()))
	})
}

// PTTL
func (m *Miniredis) cmdPTTL(out resp.ResponseWriter, r *resp.Command) {
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

		if _, ok := db.keys[key]; !ok {
			// no such key
			out.AppendInt(-2)
			return
		}

		v, ok := db.ttl[key]
		if !ok {
			// no expire value
			out.AppendInt(-1)
			return
		}
		out.AppendInt(int64(v.Nanoseconds() / 1000000))
	})
}

// PERSIST
func (m *Miniredis) cmdPersist(out resp.ResponseWriter, r *resp.Command) {
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

		if _, ok := db.keys[key]; !ok {
			// no such key
			out.AppendInt(0)
			return
		}

		if _, ok := db.ttl[key]; !ok {
			// no expire value
			out.AppendInt(0)
			return
		}
		delete(db.ttl, key)
		db.keyVersion[key]++
		out.AppendInt(1)
	})
}

// DEL
func (m *Miniredis) cmdDel(out resp.ResponseWriter, r *resp.Command) {
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	args := asString(r.Args())
	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		count := 0
		for _, k := range args {
			if db.exists(k) {
				count++
			}
			db.del(k, true) // delete expire
		}
		out.AppendInt(int64(count))
	})
}

// TYPE
func (m *Miniredis) cmdType(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError("usage error")
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
			out.AppendInlineString("none")
			return
		}

		out.AppendInlineString(t)
	})
}

// EXISTS
func (m *Miniredis) cmdExists(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	args := asString(r.Args())
	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		found := 0
		for _, k := range args {
			if db.exists(k) {
				found++
			}
		}
		out.AppendInt(int64(found))
	})
}

// MOVE
func (m *Miniredis) cmdMove(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	targetDB, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		targetDB = 0
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		if ctx.selectedDB == targetDB {
			out.AppendError("ERR source and destination objects are the same")
			return
		}
		db := m.db(ctx.selectedDB)
		targetDB := m.db(targetDB)

		if !db.move(key, targetDB) {
			out.AppendInt(0)
			return
		}
		out.AppendInt(1)
	})
}

// KEYS
func (m *Miniredis) cmdKeys(out resp.ResponseWriter, r *resp.Command) {
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

		keys := matchKeys(db.allKeys(), key)
		out.AppendArrayLen(len(keys))
		for _, s := range keys {
			out.AppendBulkString(s)
		}
	})
}

// RANDOMKEY
func (m *Miniredis) cmdRandomkey(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if len(db.keys) == 0 {
			out.AppendNil()
			return
		}
		nr := rand.Intn(len(db.keys))
		for k := range db.keys {
			if nr == 0 {
				out.AppendBulkString(k)
				return
			}
			nr--
		}
	})
}

// RENAME
func (m *Miniredis) cmdRename(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	from := r.Arg(0).String()
	to := r.Arg(1).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(from) {
			out.AppendError(msgKeyNotFound)
			return
		}

		db.rename(from, to)
		out.AppendOK()
	})
}

// RENAMENX
func (m *Miniredis) cmdRenamenx(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	from := r.Arg(0).String()
	to := r.Arg(1).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(from) {
			out.AppendError(msgKeyNotFound)
			return
		}

		if db.exists(to) {
			out.AppendInt(0)
			return
		}

		db.rename(from, to)
		out.AppendInt(1)
	})
}

// SCAN
func (m *Miniredis) cmdScan(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	cursor, err := strconv.Atoi(r.Arg(0).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidCursor)
		return
	}
	// MATCH and COUNT options
	var withMatch bool
	var match string
	args := asString(r.Args()[1:])
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

		keys := db.allKeys()
		if withMatch {
			keys = matchKeys(keys, match)
		}

		out.AppendArrayLen(2)
		out.AppendBulkString("0") // no next cursor
		out.AppendArrayLen(len(keys))
		for _, k := range keys {
			out.AppendBulkString(k)
		}
	})
}
