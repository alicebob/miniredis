// Commands from http://redis.io/commands#hash

package miniredis

import (
	"github.com/bsm/redeo"
)

// HKeys returns all keys ('fields') for a hash key.
func (m *Miniredis) HKeys(k string) []string {
	m.Lock()
	defer m.Unlock()
	v, ok := m.hashKeys[k]
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
	m.Lock()
	defer m.Unlock()
	h, ok := m.hashKeys[k]
	if !ok {
		return ""
	}
	return h[f]
}

// HSet sets a hash key.
// If there is another key by the same name it will be gone.
func (m *Miniredis) HSet(k, f, v string) {
	m.Lock()
	defer m.Unlock()

	m.keys[k] = "hash"
	_, ok := m.hashKeys[k]
	if !ok {
		m.hashKeys[k] = map[string]string{}
	}
	m.hashKeys[k][f] = v
}

// HDel deletes a hash key.
func (m *Miniredis) HDel(k, f string) {
	m.Lock()
	defer m.Unlock()

	if _, ok := m.hashKeys[k]; !ok {
		return
	}
	delete(m.hashKeys[k], f)
}

// commandsHash handles all hash value operations.
func commandsHash(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("HSET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			out.WriteErrorString("Usage error")
			return nil
		}
		key := r.Args[0]
		field := r.Args[1]
		value := r.Args[2]
		m.Lock()
		defer m.Unlock()

		if t, ok := m.keys[key]; ok && t != "hash" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		if _, ok := m.hashKeys[key]; !ok {
			m.hashKeys[key] = map[string]string{}
			m.keys[key] = "hash"
		}
		_, ok := m.hashKeys[key][field]
		m.hashKeys[key][field] = value
		if ok {
			out.WriteZero()
		} else {
			out.WriteOne()
		}
		return nil
	})

	srv.HandleFunc("HSETNX", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			out.WriteErrorString("Usage error")
			return nil
		}
		key := r.Args[0]
		field := r.Args[1]
		value := r.Args[2]
		m.Lock()
		defer m.Unlock()

		if t, ok := m.keys[key]; ok && t != "hash" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		if _, ok := m.hashKeys[key]; !ok {
			m.hashKeys[key] = map[string]string{}
			m.keys[key] = "hash"
		}
		_, ok := m.hashKeys[key][field]
		if ok {
			out.WriteZero()
			return nil
		}
		m.hashKeys[key][field] = value
		out.WriteOne()
		return nil
	})

	srv.HandleFunc("HGET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			out.WriteErrorString("Usage error")
			return nil
		}
		key := r.Args[0]
		field := r.Args[1]
		m.Lock()
		defer m.Unlock()

		t, ok := m.keys[key]
		if !ok {
			out.WriteNil()
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}
		value, ok := m.hashKeys[key][field]
		if !ok {
			out.WriteNil()
			return nil
		}
		out.WriteString(value)
		return nil
	})

	srv.HandleFunc("HDEL", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			out.WriteErrorString("Usage error")
			return nil
		}
		key := r.Args[0]
		m.Lock()
		defer m.Unlock()

		t, ok := m.keys[key]
		if !ok {
			// No key is zero deleted
			out.WriteInt(0)
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		deleted := 0
		for _, f := range r.Args[1:] {
			_, ok := m.hashKeys[key][f]
			if !ok {
				continue
			}
			delete(m.hashKeys[key], f)
			deleted++
		}
		out.WriteInt(deleted)
		return nil
	})

	srv.HandleFunc("HEXISTS", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			out.WriteErrorString("Usage error")
			return nil
		}
		key := r.Args[0]
		field := r.Args[1]
		m.Lock()
		defer m.Unlock()

		t, ok := m.keys[key]
		if !ok {
			out.WriteInt(0)
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		if _, ok := m.hashKeys[key][field]; !ok {
			out.WriteInt(0)
			return nil
		}
		out.WriteInt(1)
		return nil
	})

	srv.HandleFunc("HGETALL", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("Usage error")
			return nil
		}
		key := r.Args[0]
		m.Lock()
		defer m.Unlock()

		t, ok := m.keys[key]
		if !ok {
			out.WriteBulkLen(0)
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		out.WriteBulkLen(len(m.hashKeys[key]) * 2)
		for f, v := range m.hashKeys[key] {
			out.WriteString(f)
			out.WriteString(v)
		}
		return nil
	})

	srv.HandleFunc("HKEYS", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("Usage error")
			return nil
		}
		key := r.Args[0]
		m.Lock()
		defer m.Unlock()

		t, ok := m.keys[key]
		if !ok {
			out.WriteBulkLen(0)
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		out.WriteBulkLen(len(m.hashKeys[key]))
		for f := range m.hashKeys[key] {
			out.WriteString(f)
		}
		return nil
	})

	srv.HandleFunc("HVALS", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("Usage error")
			return nil
		}
		key := r.Args[0]
		m.Lock()
		defer m.Unlock()

		t, ok := m.keys[key]
		if !ok {
			out.WriteBulkLen(0)
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		out.WriteBulkLen(len(m.hashKeys[key]))
		for _, v := range m.hashKeys[key] {
			out.WriteString(v)
		}
		return nil
	})

	srv.HandleFunc("HLEN", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("Usage error")
			return nil
		}
		key := r.Args[0]
		m.Lock()
		defer m.Unlock()

		t, ok := m.keys[key]
		if !ok {
			out.WriteInt(0)
			return nil
		}
		if t != "hash" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		out.WriteInt(len(m.hashKeys[key]))
		return nil
	})

	srv.HandleFunc("HMGET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 1 {
			out.WriteErrorString("Usage error")
			return nil
		}
		key := r.Args[0]
		m.Lock()
		defer m.Unlock()

		if t, ok := m.keys[key]; ok && t != "hash" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		f, ok := m.hashKeys[key]
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
