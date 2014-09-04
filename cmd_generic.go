// Commands from http://redis.io/commands#generic

package miniredis

import (
	"strconv"

	"github.com/bsm/redeo"
)

// Expire value. As set by the client. 0 if not set.
func (m *Miniredis) Expire(k string) int {
	return m.DB(m.clientDB).Expire(k)
}

func (db *redisDB) Expire(k string) int {
	db.Lock()
	defer db.Unlock()
	return db.expire[k]
}

// SetExpire sets expiration of a key.
func (m *Miniredis) SetExpire(k string, ex int) {
	m.DB(m.clientDB).SetExpire(k, ex)
}

func (db *redisDB) SetExpire(k string, ex int) {
	db.Lock()
	defer db.Unlock()
	db.expire[k] = ex
}

// commandsGeneric handles EXPIRE, TTL, PERSIST
func commandsGeneric(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("EXPIRE", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		value := r.Args[1]
		i, err := strconv.Atoi(value)
		if err != nil {
			out.WriteErrorString("value error")
			return nil
		}
		db := m.dbFor(r.Client().ID)
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

	srv.HandleFunc("TTL", func(out *redeo.Responder, r *redeo.Request) error {
		key := r.Args[0]
		db := m.dbFor(r.Client().ID)
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

		db := m.dbFor(r.Client().ID)
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
		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		count := 0
		for _, key := range r.Args {
			if _, ok := db.keys[key]; !ok {
				continue
			}
			delete(db.keys, key)
			delete(db.expire, key)
			// These are not strictly needed:
			delete(db.stringKeys, key)
			delete(db.hashKeys, key)
			count++
		}
		out.WriteInt(count)
		return nil
	})
}
