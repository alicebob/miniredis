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
	m.Lock()
	defer m.Unlock()
	return m.stringKeys[k]
}

// Set sets a string key.
func (m *Miniredis) Set(k, v string) {
	m.Lock()
	defer m.Unlock()
	m.keys[k] = "string"
	m.stringKeys[k] = v
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
		m.Lock()
		defer m.Unlock()

		if t, ok := m.keys[key]; ok && t != "string" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		m.keys[key] = "string"
		m.stringKeys[key] = value
		// a SET clears the expire
		delete(m.expire, key)
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("GET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("Usage error")
			return nil
		}
		key := r.Args[0]
		m.Lock()
		defer m.Unlock()

		if t, ok := m.keys[key]; ok && t != "string" {
			out.WriteErrorString("Wrong type of key")
			return nil
		}

		value, ok := m.stringKeys[key]
		if !ok {
			out.WriteNil()
			return nil
		}
		out.WriteString(value)
		return nil
	})

	// TODO: GETSET (clears expire!)
}
