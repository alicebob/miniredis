// Commands from http://redis.io/commands#string

package miniredis

import (
	"errors"
	"strconv"
	"strings"

	"github.com/bsm/redeo"
)

var (
	errValueError = errors.New("key value error")
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

// Set sets a string key. Doesn't touch expire.
func (db *redisDB) Set(k, v string) {
	db.Lock()
	defer db.Unlock()
	db.set(k, v)
}

// internal not-locked version. Doesn't touch expire.
func (db *redisDB) set(k, v string) {
	db.keys[k] = "string"
	db.stringKeys[k] = v
}

// Incr changes a int string value by delta.
func (m *Miniredis) Incr(k string, delta int) (int, error) {
	return m.DB(m.clientDB).Incr(k, delta)
}

// Incr changes a int string value by delta.
func (db *redisDB) Incr(k string, delta int) (int, error) {
	db.Lock()
	defer db.Unlock()
	return db.incr(k, delta)
}

// change int key value
func (db *redisDB) incr(k string, delta int) (int, error) {
	v := 0
	if sv, ok := db.stringKeys[k]; ok {
		var err error
		v, err = strconv.Atoi(sv)
		if err != nil {
			return 0, errValueError
		}
	}
	v += delta
	db.stringKeys[k] = strconv.Itoa(v)
	return v, nil
}

// commandsString handles all string value operations.
func commandsString(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("SET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			out.WriteErrorString("usage error")
			return nil
		}
		nx := false // set iff not exists
		xx := false // set iff exists
		expire := 0 // For seconds and milliseconds.
		key := r.Args[0]
		value := r.Args[1]
		r.Args = r.Args[2:]
		for len(r.Args) > 0 {
			switch strings.ToUpper(r.Args[0]) {
			case "NX":
				nx = true
				r.Args = r.Args[1:]
				continue
			case "XX":
				xx = true
				r.Args = r.Args[1:]
				continue
			case "EX", "PX":
				if len(r.Args) < 2 {
					out.WriteErrorString("Expire value error")
					return nil
				}
				var err error
				expire, err = strconv.Atoi(r.Args[1])
				if err != nil {
					out.WriteErrorString("Expire value error")
					return nil
				}
				r.Args = r.Args[2:]
				continue
			default:
				out.WriteErrorString("invalid SET flag")
				return nil
			}
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if nx {
			if _, ok := db.keys[key]; ok {
				out.WriteNil()
				return nil
			}
		}
		if xx {
			if _, ok := db.keys[key]; !ok {
				out.WriteNil()
				return nil
			}
		}

		db.del(key) // be sure to remove existing values of other type keys.
		// a SET clears the expire
		delete(db.expire, key)
		db.set(key, value)
		if expire != 0 {
			db.expire[key] = expire
		}
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("SETEX", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		ttl, err := strconv.Atoi(r.Args[1])
		if err != nil {
			out.WriteErrorString("expire value error")
			return nil
		}
		value := r.Args[2]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		db.del(key) // Clear any existing keys.
		db.keys[key] = "string"
		db.stringKeys[key] = value
		db.expire[key] = ttl
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("SETNX", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		value := r.Args[1]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if _, ok := db.keys[key]; ok {
			out.WriteZero()
			return nil
		}

		db.set(key, value)
		out.WriteOne()
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
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("wrong type of key")
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

	srv.HandleFunc("GETSET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		value := r.Args[1]
		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("wrong type of key")
			return nil
		}

		old, ok := db.stringKeys[key]
		db.stringKeys[key] = value
		// a GETSET clears the expire
		delete(db.expire, key)

		if !ok {
			out.WriteNil()
			return nil
		}
		out.WriteString(old)
		return nil
	})

	srv.HandleFunc("MGET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 1 {
			out.WriteErrorString("usage error")
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

	srv.HandleFunc("INCR", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("usage error")
			return nil
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		key := r.Args[0]
		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("wrong type of key")
			return nil
		}
		v, err := db.incr(key, +1)
		if err != nil {
			out.WriteErrorString(err.Error())
			return nil
		}
		// Don't touch TTL
		out.WriteInt(v)
		return nil
	})

	srv.HandleFunc("INCRBY", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			out.WriteErrorString("usage error")
			return nil
		}

		key := r.Args[0]
		delta, err := strconv.Atoi(r.Args[1])
		if err != nil {
			out.WriteErrorString("value error")
			return nil
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("wrong type of key")
			return nil
		}

		v, err := db.incr(key, delta)
		if err != nil {
			out.WriteErrorString(err.Error())
			return nil
		}
		// Don't touch TTL
		out.WriteInt(v)
		return nil
	})

	srv.HandleFunc("DECR", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("usage error")
			return nil
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		key := r.Args[0]
		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("wrong type of key")
			return nil
		}
		v, err := db.incr(key, -1)
		if err != nil {
			out.WriteErrorString(err.Error())
			return nil
		}
		// Don't touch TTL
		out.WriteInt(v)
		return nil
	})

	srv.HandleFunc("DECRBY", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			out.WriteErrorString("usage error")
			return nil
		}

		key := r.Args[0]
		delta, err := strconv.Atoi(r.Args[1])
		if err != nil {
			out.WriteErrorString("value error")
			return nil
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("wrong type of key")
			return nil
		}

		v, err := db.incr(key, -delta)
		if err != nil {
			out.WriteErrorString(err.Error())
			return nil
		}
		// Don't touch TTL
		out.WriteInt(v)
		return nil
	})

	srv.HandleFunc("STRLEN", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("usage error")
			return nil
		}

		key := r.Args[0]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("wrong type of key")
			return nil
		}

		out.WriteInt(len(db.stringKeys[key]))
		return nil
	})
}
