// Commands from http://redis.io/commands#hash

package miniredis

import (
	"github.com/bsm/redeo"
)

// HKeys returns all keys ('fields') for a hash key.
func (m *Miniredis) HKeys(k string) []string {
	return m.DB(m.clientDB).HKeys(k)
}

func (db *redisDB) HKeys(k string) []string {
	db.Lock()
	defer db.Unlock()
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
	return m.DB(m.clientDB).HGet(k, f)
}

func (db *redisDB) HGet(k, f string) string {
	db.Lock()
	defer db.Unlock()
	h, ok := db.hashKeys[k]
	if !ok {
		return ""
	}
	return h[f]
}

// HSet sets a hash key.
// If there is another key by the same name it will be gone.
func (m *Miniredis) HSet(k, f, v string) {
	m.DB(m.clientDB).HSet(k, f, v)
}

func (db *redisDB) HSet(k, f, v string) {
	db.Lock()
	defer db.Unlock()

	db.keys[k] = "hash"
	_, ok := db.hashKeys[k]
	if !ok {
		db.hashKeys[k] = map[string]string{}
	}
	db.hashKeys[k][f] = v
}

// HDel deletes a hash key.
func (m *Miniredis) HDel(k, f string) {
	m.DB(m.clientDB).HDel(k, f)
}

func (db *redisDB) HDel(k, f string) {
	db.Lock()
	defer db.Unlock()

	if _, ok := db.hashKeys[k]; !ok {
		return
	}
	delete(db.hashKeys[k], f)
}

// commandsHash handles all hash value operations.
func commandsHash(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("HSET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		field := r.Args[1]
		value := r.Args[2]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "hash" {
			out.WriteErrorString("wrong type of key")
			return nil
		}

		if _, ok := db.hashKeys[key]; !ok {
			db.hashKeys[key] = map[string]string{}
			db.keys[key] = "hash"
		}
		_, ok := db.hashKeys[key][field]
		db.hashKeys[key][field] = value
		if ok {
			out.WriteZero()
		} else {
			out.WriteOne()
		}
		return nil
	})

	srv.HandleFunc("HSETNX", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		field := r.Args[1]
		value := r.Args[2]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "hash" {
			out.WriteErrorString("wrong type of key")
			return nil
		}

		if _, ok := db.hashKeys[key]; !ok {
			db.hashKeys[key] = map[string]string{}
			db.keys[key] = "hash"
		}
		_, ok := db.hashKeys[key][field]
		if ok {
			out.WriteZero()
			return nil
		}
		db.hashKeys[key][field] = value
		out.WriteOne()
		return nil
	})

	srv.HandleFunc("HGET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		field := r.Args[1]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		t, ok := db.keys[key]
		if !ok {
			out.WriteNil()
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("wrong type of key")
			return nil
		}
		value, ok := db.hashKeys[key][field]
		if !ok {
			out.WriteNil()
			return nil
		}
		out.WriteString(value)
		return nil
	})

	srv.HandleFunc("HDEL", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		t, ok := db.keys[key]
		if !ok {
			// No key is zero deleted
			out.WriteInt(0)
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("wrong type of key")
			return nil
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
		return nil
	})

	srv.HandleFunc("HEXISTS", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		field := r.Args[1]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		t, ok := db.keys[key]
		if !ok {
			out.WriteInt(0)
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("wrong type of key")
			return nil
		}

		if _, ok := db.hashKeys[key][field]; !ok {
			out.WriteInt(0)
			return nil
		}
		out.WriteInt(1)
		return nil
	})

	srv.HandleFunc("HGETALL", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		t, ok := db.keys[key]
		if !ok {
			out.WriteBulkLen(0)
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("wrong type of key")
			return nil
		}

		out.WriteBulkLen(len(db.hashKeys[key]) * 2)
		for f, v := range db.hashKeys[key] {
			out.WriteString(f)
			out.WriteString(v)
		}
		return nil
	})

	srv.HandleFunc("HKEYS", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		t, ok := db.keys[key]
		if !ok {
			out.WriteBulkLen(0)
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("wrong type of key")
			return nil
		}

		out.WriteBulkLen(len(db.hashKeys[key]))
		for f := range db.hashKeys[key] {
			out.WriteString(f)
		}
		return nil
	})

	srv.HandleFunc("HVALS", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		t, ok := db.keys[key]
		if !ok {
			out.WriteBulkLen(0)
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("wrong type of key")
			return nil
		}

		out.WriteBulkLen(len(db.hashKeys[key]))
		for _, v := range db.hashKeys[key] {
			out.WriteString(v)
		}
		return nil
	})

	srv.HandleFunc("HLEN", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		t, ok := db.keys[key]
		if !ok {
			out.WriteInt(0)
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("wrong type of key")
			return nil
		}

		out.WriteInt(len(db.hashKeys[key]))
		return nil
	})

	srv.HandleFunc("HMGET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 1 {
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "hash" {
			out.WriteErrorString("wrong type of key")
			return nil
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
		return nil
	})
}
