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
	db.Lock()
	defer db.Unlock()
	return db.del(k)
}

// internal, non-locked delete.
func (db *redisDB) del(k string) bool {
	if _, ok := db.keys[k]; !ok {
		return false
	}
	delete(db.keys, k)
	delete(db.expire, k)
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
	db.Lock()
	defer db.Unlock()
	return db.expire[k]
}

// SetExpire sets expiration of a key.
func (m *Miniredis) SetExpire(k string, ex int) {
	m.DB(m.selectedDB).SetExpire(k, ex)
}

func (db *redisDB) SetExpire(k string, ex int) {
	db.Lock()
	defer db.Unlock()
	db.expire[k] = ex
}

// Type gives the type of a key, or ""
func (m *Miniredis) Type(k string) string {
	return m.DB(m.selectedDB).Type(k)
}

// Type gives the type of a key, or ""
func (db *redisDB) Type(k string) string {
	db.Lock()
	defer db.Unlock()
	return db.keys[k]
}

// commandsGeneric handles EXPIRE, TTL, PERSIST, &c.
func commandsGeneric(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("EXPIRE", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			out.WriteErrorString("ERR wrong number of arguments for 'expire' command")
			return nil
		}
		key := r.Args[0]
		value := r.Args[1]
		i, err := strconv.Atoi(value)
		if err != nil {
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}
		db := m.dbFor(r.Client().Ctx)
		db.Lock()
		defer db.Unlock()

		// Key must be present.
		if _, ok := db.keys[key]; !ok {
			out.WriteZero()
			return nil
		}
		db.expire[key] = i
		out.WriteOne()
		return nil
	})

	srv.HandleFunc("PEXPIRE", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			out.WriteErrorString("ERR wrong number of arguments for 'pexpire' command")
			return nil
		}
		key := r.Args[0]
		value := r.Args[1]
		i, err := strconv.Atoi(value)
		if err != nil {
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}
		db := m.dbFor(r.Client().Ctx)
		db.Lock()
		defer db.Unlock()

		// Key must be present.
		if _, ok := db.keys[key]; !ok {
			out.WriteZero()
			return nil
		}
		db.expire[key] = i // We put pexires in expire.
		out.WriteOne()
		return nil
	})

	srv.HandleFunc("TTL", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("ERR wrong number of arguments for 'ttl' command")
			return nil
		}
		key := r.Args[0]
		db := m.dbFor(r.Client().Ctx)
		db.Lock()
		defer db.Unlock()

		if _, ok := db.keys[key]; !ok {
			// No such key
			out.WriteInt(-2)
			return nil
		}

		value, ok := db.expire[key]
		if !ok {
			// No expire value
			out.WriteInt(-1)
			return nil
		}
		out.WriteInt(value)
		return nil
	})

	// Same at `TTL'
	srv.HandleFunc("PTTL", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("ERR wrong number of arguments for 'pttl' command")
			return nil
		}
		key := r.Args[0]
		db := m.dbFor(r.Client().Ctx)
		db.Lock()
		defer db.Unlock()

		if _, ok := db.keys[key]; !ok {
			// No such key
			out.WriteInt(-2)
			return nil
		}

		value, ok := db.expire[key]
		if !ok {
			// No expire value
			out.WriteInt(-1)
			return nil
		}
		out.WriteInt(value)
		return nil
	})

	srv.HandleFunc("PERSIST", func(out *redeo.Responder, r *redeo.Request) error {
		key := r.Args[0]

		db := m.dbFor(r.Client().Ctx)
		db.Lock()
		defer db.Unlock()

		if _, ok := db.keys[key]; !ok {
			// No such key
			out.WriteInt(0)
			return nil
		}

		_, ok := db.expire[key]
		if !ok {
			// No expire value
			out.WriteInt(0)
			return nil
		}
		delete(db.expire, key)
		out.WriteInt(1)
		return nil
	})

	// MULTI is a no-op
	srv.HandleFunc("MULTI", func(out *redeo.Responder, r *redeo.Request) error {
		out.WriteOK()
		return nil
	})

	// EXEC is a no-op
	srv.HandleFunc("EXEC", func(out *redeo.Responder, r *redeo.Request) error {
		out.WriteNil()
		return nil
	})

	srv.HandleFunc("DEL", func(out *redeo.Responder, r *redeo.Request) error {
		db := m.dbFor(r.Client().Ctx)
		db.Lock()
		defer db.Unlock()

		count := 0
		for _, key := range r.Args {
			if db.del(key) {
				count++
			}
		}
		out.WriteInt(count)
		return nil
	})

	srv.HandleFunc("TYPE", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("usage error")
			return nil
		}

		key := r.Args[0]

		db := m.dbFor(r.Client().Ctx)
		db.Lock()
		defer db.Unlock()

		t, ok := db.keys[key]
		if !ok {
			out.WriteString("none")
			return nil
		}

		out.WriteString(t)
		return nil
	})
}
