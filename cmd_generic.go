// Commands from http://redis.io/commands#generic

package miniredis

import (
	"strconv"

	"github.com/bsm/redeo"
)

// Del deletes a key and any expiration value. Returns whether there was a key.
func (m *Miniredis) Del(k string) bool {
	return m.DB(m.selectedDB).Del(k)
}

func (db *redisDB) Del(k string) bool {
	db.master.Lock()
	defer db.master.Unlock()
	return db.del(k, false)
}

// internal, non-locked delete.
func (db *redisDB) del(k string, delTTL bool) bool {
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

// Expire value. As set by the client. 0 if not set.
func (m *Miniredis) Expire(k string) int {
	return m.DB(m.selectedDB).Expire(k)
}

func (db *redisDB) Expire(k string) int {
	db.master.Lock()
	defer db.master.Unlock()
	return db.expire[k]
}

// SetExpire sets expiration of a key.
func (m *Miniredis) SetExpire(k string, ex int) {
	m.DB(m.selectedDB).SetExpire(k, ex)
}

func (db *redisDB) SetExpire(k string, ex int) {
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
func (db *redisDB) Type(k string) string {
	db.master.Lock()
	defer db.master.Unlock()
	return db.keys[k]
}

// Exists tells if a key exists.
func (m *Miniredis) Exists(k string) bool {
	return m.DB(m.selectedDB).Exists(k)
}

// Exists tells if a key exists.
func (db *redisDB) Exists(k string) bool {
	db.master.Lock()
	defer db.master.Unlock()
	_, ok := db.keys[k]
	return ok
}

// generic expire command for EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT
func makeExpirefunc(m *Miniredis, cmd string) func(*redeo.Responder, *redeo.Request) error {
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

// commandsGeneric handles EXPIRE, TTL, PERSIST, &c.
func commandsGeneric(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("EXPIRE", makeExpirefunc(m, "expire"))
	srv.HandleFunc("PEXPIRE", makeExpirefunc(m, "pexpire"))
	srv.HandleFunc("EXPIREAT", makeExpirefunc(m, "expireat"))
	srv.HandleFunc("PEXPIREAT", makeExpirefunc(m, "pexpireat"))

	srv.HandleFunc("TTL", func(out *redeo.Responder, r *redeo.Request) error {
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
	})

	// Same at `TTL'
	srv.HandleFunc("PTTL", func(out *redeo.Responder, r *redeo.Request) error {
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
	})

	srv.HandleFunc("PERSIST", func(out *redeo.Responder, r *redeo.Request) error {
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
	})

	srv.HandleFunc("DEL", func(out *redeo.Responder, r *redeo.Request) error {
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
	})

	srv.HandleFunc("TYPE", func(out *redeo.Responder, r *redeo.Request) error {
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
	})

	srv.HandleFunc("EXISTS", func(out *redeo.Responder, r *redeo.Request) error {
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
	})
}
