// Commands from http://redis.io/commands#hash

package miniredis

import (
	"github.com/bsm/redeo"
)

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
		}
		m.keys[key] = "hash"
		_, ok := m.hashKeys[key][field]
		m.hashKeys[key][field] = value
		if ok {
			out.WriteZero()
		} else {
			out.WriteOne()
		}
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
}
