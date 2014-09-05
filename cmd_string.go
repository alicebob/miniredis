// Commands from http://redis.io/commands#string

package miniredis

import (
	"github.com/bsm/redeo"
)

// Get returns string keys added with SET.
// This will return an empty string if the key is not set. Redis would return
// a nil.
// Returns empty string when the key is of a different type.
func (m *Miniredis) Get(k string) string {
	return m.DB(m.clientDB).Get(k)
}

// Get returns a string key
func (db *redisDB) Get(k string) string {
	db.Lock()
	defer db.Unlock()
	return db.stringKeys[k]
}

// Set sets a string key.
func (m *Miniredis) Set(k, v string) {
	m.DB(m.clientDB).Set(k, v)
}

// Set sets a string key.
func (db *redisDB) Set(k, v string) {
	db.Lock()
	defer db.Unlock()
	db.keys[k] = "string"
	db.stringKeys[k] = v
}

// commandsString handles all string value operations.
func commandsString(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("SET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			out.WriteErrorString("Usage error")
			return nil
		}
		if len(r.Args) > 2 {
			// EX/PX/NX/XX options.
			return errUnimplemented
		}
		key := r.Args[0]
		value := r.Args[1]
		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		db.keys[key] = "string"
		db.stringKeys[key] = value
		// a SET clears the expire
		delete(db.expire, key)
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("MSET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args)%2 != 0 {
			out.WriteErrorString("wrong number of arguments for 'mset' command")
			return nil
		}
		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		for len(r.Args) > 0 {
			key := r.Args[0]
			value := r.Args[1]
			r.Args = r.Args[2:]

			// The TTL is always cleared.
			db.del(key)
			db.keys[key] = "string"
			db.stringKeys[key] = value
		}
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("GET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("Usage error")
			return nil
		}
		key := r.Args[0]
		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		value, ok := db.stringKeys[key]
		if !ok {
			out.WriteNil()
			return nil
		}
		out.WriteString(value)
		return nil
	})

	srv.HandleFunc("MGET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 1 {
			out.WriteErrorString("Usage error")
			return nil
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		out.WriteBulkLen(len(r.Args))
		for _, k := range r.Args {
			if t, ok := db.keys[k]; !ok || t != "string" {
				out.WriteNil()
				continue
			}
			v, ok := db.stringKeys[k]
			if !ok {
				// Should not happen, we just check keys[]
				out.WriteNil()
				continue
			}
			out.WriteString(v)
		}
		return nil
	})

	// TODO: GETSET (clears expire!)
}
