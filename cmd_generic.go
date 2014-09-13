// Commands from http://redis.io/commands#generic

package miniredis

import (
	"math/rand"
	"sort"
	"strconv"

	"github.com/bsm/redeo"
)

// Del deletes a key and any expiration value. Returns whether there was a key.
func (m *Miniredis) Del(k string) bool {
	return m.DB(m.selectedDB).Del(k)
}

// Del deletes a key and any expiration value. Returns whether there was a key.
func (db *RedisDB) Del(k string) bool {
	db.master.Lock()
	defer db.master.Unlock()
	return db.del(k, false)
}

// internal, non-locked delete.
func (db *RedisDB) del(k string, delTTL bool) bool {
	if _, ok := db.keys[k]; !ok {
		return false
	}
	delete(db.keys, k)
	if delTTL {
		delete(db.expire, k)
	}
	// These are not strictly needed:
	delete(db.stringKeys, k)
	delete(db.hashKeys, k)
	return true
}

// Expire value. As set by the client (via EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT and
// similar commands). 0 if not set.
func (m *Miniredis) Expire(k string) int {
	return m.DB(m.selectedDB).Expire(k)
}

// Expire value. As set by the client (via EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT and
// similar commands). 0 if not set.
func (db *RedisDB) Expire(k string) int {
	db.master.Lock()
	defer db.master.Unlock()
	return db.expire[k]
}

// SetExpire sets expiration of a key.
func (m *Miniredis) SetExpire(k string, ex int) {
	m.DB(m.selectedDB).SetExpire(k, ex)
}

// SetExpire sets expiration of a key.
func (db *RedisDB) SetExpire(k string, ex int) {
	db.master.Lock()
	defer db.master.Unlock()
	db.expire[k] = ex
	db.keyVersion[k]++
}

// Type gives the type of a key, or ""
func (m *Miniredis) Type(k string) string {
	return m.DB(m.selectedDB).Type(k)
}

// Type gives the type of a key, or ""
func (db *RedisDB) Type(k string) string {
	db.master.Lock()
	defer db.master.Unlock()
	return db.keys[k]
}

// Exists tells whether a key exists.
func (m *Miniredis) Exists(k string) bool {
	return m.DB(m.selectedDB).Exists(k)
}

// Exists tells whether a key exists.
func (db *RedisDB) Exists(k string) bool {
	db.master.Lock()
	defer db.master.Unlock()
	_, ok := db.keys[k]
	return ok
}

// commandsGeneric handles EXPIRE, TTL, PERSIST, &c.
func commandsGeneric(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("DEL", m.cmdDel)
	srv.HandleFunc("EXISTS", m.cmdExists)
	srv.HandleFunc("EXPIREAT", makeCmdExpire(m, "expireat"))
	srv.HandleFunc("EXPIRE", makeCmdExpire(m, "expire"))
	srv.HandleFunc("KEYS", m.cmdKeys)
	srv.HandleFunc("MOVE", m.cmdMove)
	srv.HandleFunc("PERSIST", m.cmdPersist)
	srv.HandleFunc("PEXPIREAT", makeCmdExpire(m, "pexpireat"))
	srv.HandleFunc("PEXPIRE", makeCmdExpire(m, "pexpire"))
	srv.HandleFunc("PTTL", m.cmdPTTL)
	srv.HandleFunc("RANDOMKEY", m.cmdRandomkey)
	srv.HandleFunc("TTL", m.cmdTTL)
	srv.HandleFunc("TYPE", m.cmdType)
}

// generic expire command for EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT
func makeCmdExpire(m *Miniredis, cmd string) func(*redeo.Responder, *redeo.Request) error {
	return func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for '" + cmd + "' command")
			return nil
		}
		key := r.Args[0]
		value := r.Args[1]
		i, err := strconv.Atoi(value)
		if err != nil {
			setDirty(r.Client())
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			// Key must be present.
			if _, ok := db.keys[key]; !ok {
				out.WriteZero()
				return
			}
			db.expire[key] = i
			db.keyVersion[key]++
			out.WriteOne()
		})
	}
}

// TTL
func (m *Miniredis) cmdTTL(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'ttl' command")
		return nil
	}
	key := r.Args[0]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if _, ok := db.keys[key]; !ok {
			// No such key
			out.WriteInt(-2)
			return
		}

		value, ok := db.expire[key]
		if !ok {
			// No expire value
			out.WriteInt(-1)
			return
		}
		out.WriteInt(value)
	})
}

// PTTL
func (m *Miniredis) cmdPTTL(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'pttl' command")
		return nil
	}
	key := r.Args[0]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if _, ok := db.keys[key]; !ok {
			// No such key
			out.WriteInt(-2)
			return
		}

		value, ok := db.expire[key]
		if !ok {
			// No expire value
			out.WriteInt(-1)
			return
		}
		out.WriteInt(value)
	})
}

// PERSIST
func (m *Miniredis) cmdPersist(out *redeo.Responder, r *redeo.Request) error {
	key := r.Args[0]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if _, ok := db.keys[key]; !ok {
			// No such key
			out.WriteInt(0)
			return
		}

		_, ok := db.expire[key]
		if !ok {
			// No expire value
			out.WriteInt(0)
			return
		}
		delete(db.expire, key)
		db.keyVersion[key]++
		out.WriteInt(1)
	})
}

// DEL
func (m *Miniredis) cmdDel(out *redeo.Responder, r *redeo.Request) error {
	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		count := 0
		for _, key := range r.Args {
			if db.del(key, true) {
				count++
			}
		}
		out.WriteInt(count)
	})
}

// TYPE
func (m *Miniredis) cmdType(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 1 {
		setDirty(r.Client())
		out.WriteErrorString("usage error")
		return nil
	}

	key := r.Args[0]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			out.WriteInlineString("none")
			return
		}

		out.WriteString(t)
	})
}

// EXISTS
func (m *Miniredis) cmdExists(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'exists' command")
		return nil
	}

	key := r.Args[0]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if _, ok := db.keys[key]; !ok {
			out.WriteZero()
			return
		}
		out.WriteOne()
	})
}

// MOVE
func (m *Miniredis) cmdMove(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 2 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'move' command")
		return nil
	}

	key := r.Args[0]
	targetDB, err := strconv.Atoi(r.Args[1])
	if err != nil {
		targetDB = 0
	}

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		if ctx.selectedDB == targetDB {
			out.WriteErrorString("ERR source and destination objects are the same")
			return
		}
		db := m.db(ctx.selectedDB)
		targetDB := m.db(targetDB)

		if _, ok := db.keys[key]; !ok {
			out.WriteZero()
			return
		}
		if _, ok := targetDB.keys[key]; ok {
			out.WriteZero()
			return
		}
		targetDB.keys[key] = db.keys[key]
		targetDB.stringKeys[key] = db.stringKeys[key]
		targetDB.hashKeys[key] = db.hashKeys[key]
		targetDB.keyVersion[key]++
		db.del(key, true)
		out.WriteOne()
	})
}

// KEYS
func (m *Miniredis) cmdKeys(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'keys' command")
		return nil
	}

	key := r.Args[0]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		var res []string
		re := patternRE(key)
		if re == nil {
			// Special case, the given pattern won't match anything / is
			// invalid.
			out.WriteBulkLen(0)
			return
		}
		for k := range db.keys {
			if !re.MatchString(k) {
				continue
			}
			res = append(res, k)
		}

		out.WriteBulkLen(len(res))
		sort.Strings(res) // To make things deterministic.
		for _, s := range res {
			out.WriteString(s)
		}
	})
}

// RANDOMKEY
func (m *Miniredis) cmdRandomkey(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 0 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'randomkey' command")
		return nil
	}

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if len(db.keys) == 0 {
			out.WriteNil()
			return
		}
		nr := rand.Intn(len(db.keys))
		for k := range db.keys {
			if nr == 0 {
				out.WriteString(k)
				return
			}
			nr--
		}
	})
}
