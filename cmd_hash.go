// Commands from http://redis.io/commands#hash

package miniredis

import (
	"github.com/bsm/redeo"
)

// HKeys returns all keys ('fields') for a hash key.
func (m *Miniredis) HKeys(k string) []string {
	return m.DB(m.selectedDB).HKeys(k)
}

// HKeys returns all keys ('fields') for a hash key.
func (db *RedisDB) HKeys(k string) []string {
	db.master.Lock()
	defer db.master.Unlock()
	v, ok := db.hashKeys[k]
	if !ok {
		return []string{}
	}
	r := []string{}
	for k := range v {
		r = append(r, k)
	}
	return r
}

// HGet returns hash keys added with HSET.
// This will return an empty string if the key is not set. Redis would return
// a nil.
// Returns empty string when the key is of a different type.
func (m *Miniredis) HGet(k, f string) string {
	return m.DB(m.selectedDB).HGet(k, f)
}

// HGet returns hash keys added with HSET.
// Returns empty string when the key is of a different type.
func (db *RedisDB) HGet(k, f string) string {
	db.master.Lock()
	defer db.master.Unlock()
	h, ok := db.hashKeys[k]
	if !ok {
		return ""
	}
	return h[f]
}

// HSet sets a hash key.
// If there is another key by the same name it will be gone.
func (m *Miniredis) HSet(k, f, v string) {
	m.DB(m.selectedDB).HSet(k, f, v)
}

// HSet sets a hash key.
// If there is another key by the same name it will be gone.
func (db *RedisDB) HSet(k, f, v string) {
	db.master.Lock()
	defer db.master.Unlock()
	db.hset(k, f, v)
}

// hset returns whether the key already existed
func (db *RedisDB) hset(k, f, v string) bool {
	if t, ok := db.keys[k]; ok && t != "hash" {
		db.del(k, true)
	}
	db.keys[k] = "hash"
	if _, ok := db.hashKeys[k]; !ok {
		db.hashKeys[k] = map[string]string{}
	}
	_, ok := db.hashKeys[k][f]
	db.hashKeys[k][f] = v
	db.keyVersion[k]++
	return ok
}

// HDel deletes a hash key.
func (m *Miniredis) HDel(k, f string) {
	m.DB(m.selectedDB).HDel(k, f)
}

// HDel deletes a hash key.
func (db *RedisDB) HDel(k, f string) {
	db.master.Lock()
	defer db.master.Unlock()
	db.hdel(k, f)
}

func (db *RedisDB) hdel(k, f string) {
	if _, ok := db.hashKeys[k]; !ok {
		return
	}
	delete(db.hashKeys[k], f)
	db.keyVersion[k]++
}

// commandsHash handles all hash value operations.
func commandsHash(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("HSET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'hset' command")
			return nil
		}
		key := r.Args[0]
		field := r.Args[1]
		value := r.Args[2]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "hash" {
				out.WriteErrorString(msgWrongType)
				return
			}

			if db.hset(key, field, value) {
				out.WriteZero()
			} else {
				out.WriteOne()
			}
		})
	})

	srv.HandleFunc("HSETNX", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			setDirty(r.Client())
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		field := r.Args[1]
		value := r.Args[2]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "hash" {
				out.WriteErrorString(msgWrongType)
				return
			}

			if _, ok := db.hashKeys[key]; !ok {
				db.hashKeys[key] = map[string]string{}
				db.keys[key] = "hash"
			}
			_, ok := db.hashKeys[key][field]
			if ok {
				out.WriteZero()
				return
			}
			db.hashKeys[key][field] = value
			db.keyVersion[key]++
			out.WriteOne()
		})
	})

	srv.HandleFunc("HGET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'hget' command")
			return nil
		}
		key := r.Args[0]
		field := r.Args[1]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			t, ok := db.keys[key]
			if !ok {
				out.WriteNil()
				return
			}
			if t != "hash" {
				out.WriteErrorString(msgWrongType)
				return
			}
			value, ok := db.hashKeys[key][field]
			if !ok {
				out.WriteNil()
				return
			}
			out.WriteString(value)
		})
	})

	srv.HandleFunc("HDEL", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			setDirty(r.Client())
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			t, ok := db.keys[key]
			if !ok {
				// No key is zero deleted
				out.WriteInt(0)
				return
			}
			if t != "hash" {
				out.WriteErrorString(msgWrongType)
				return
			}

			deleted := 0
			for _, f := range r.Args[1:] {
				_, ok := db.hashKeys[key][f]
				if !ok {
					continue
				}
				delete(db.hashKeys[key], f)
				deleted++
			}
			out.WriteInt(deleted)

			// Nothing left. Remove the whole key.
			if len(db.hashKeys[key]) == 0 {
				db.del(key, true)
			}
		})
	})

	srv.HandleFunc("HEXISTS", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			setDirty(r.Client())
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		field := r.Args[1]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			t, ok := db.keys[key]
			if !ok {
				out.WriteInt(0)
				return
			}
			if t != "hash" {
				out.WriteErrorString(msgWrongType)
				return
			}

			if _, ok := db.hashKeys[key][field]; !ok {
				out.WriteInt(0)
				return
			}
			out.WriteInt(1)
		})
	})

	srv.HandleFunc("HGETALL", func(out *redeo.Responder, r *redeo.Request) error {
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
				out.WriteBulkLen(0)
				return
			}
			if t != "hash" {
				out.WriteErrorString(msgWrongType)
				return
			}

			out.WriteBulkLen(len(db.hashKeys[key]) * 2)
			for f, v := range db.hashKeys[key] {
				out.WriteString(f)
				out.WriteString(v)
			}
		})
	})

	srv.HandleFunc("HKEYS", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'hkeys' command")
			return nil
		}
		key := r.Args[0]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			t, ok := db.keys[key]
			if !ok {
				out.WriteBulkLen(0)
				return
			}
			if t != "hash" {
				out.WriteErrorString(msgWrongType)
				return
			}

			out.WriteBulkLen(len(db.hashKeys[key]))
			for f := range db.hashKeys[key] {
				out.WriteString(f)
			}
		})
	})

	srv.HandleFunc("HVALS", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'hvals' command")
			return nil
		}
		key := r.Args[0]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			t, ok := db.keys[key]
			if !ok {
				out.WriteBulkLen(0)
				return
			}
			if t != "hash" {
				out.WriteErrorString(msgWrongType)
				return
			}

			out.WriteBulkLen(len(db.hashKeys[key]))
			for _, v := range db.hashKeys[key] {
				out.WriteString(v)
			}
		})
	})

	srv.HandleFunc("HLEN", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'hlen' command")
			return nil
		}
		key := r.Args[0]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			t, ok := db.keys[key]
			if !ok {
				out.WriteInt(0)
				return
			}
			if t != "hash" {
				out.WriteErrorString(msgWrongType)
				return
			}

			out.WriteInt(len(db.hashKeys[key]))
		})
	})

	srv.HandleFunc("HMGET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'hmget' command")
			return nil
		}
		key := r.Args[0]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "hash" {
				out.WriteErrorString(msgWrongType)
				return
			}

			f, ok := db.hashKeys[key]
			if !ok {
				f = map[string]string{}
			}

			out.WriteBulkLen(len(r.Args) - 1)
			for _, k := range r.Args[1:] {
				v, ok := f[k]
				if !ok {
					out.WriteNil()
					continue
				}
				out.WriteString(v)
			}
		})
	})
}
