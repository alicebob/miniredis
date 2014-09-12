// Commands from http://redis.io/commands#string

package miniredis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/bsm/redeo"
)

var (
	errIntValueError   = errors.New("ERR value is not an integer or out of range")
	errFloatValueError = errors.New("ERR value is not a valid float")
)

// Get returns string keys added with SET.
// This will return an empty string if the key is not set. Redis would return
// a nil.
// Returns empty string when the key is of a different type.
func (m *Miniredis) Get(k string) string {
	return m.DB(m.selectedDB).Get(k)
}

// Get returns a string key
func (db *RedisDB) Get(k string) string {
	db.master.Lock()
	defer db.master.Unlock()
	return db.stringKeys[k]
}

// Set sets a string key.
func (m *Miniredis) Set(k, v string) {
	m.DB(m.selectedDB).Set(k, v)
}

// Set sets a string key. Doesn't touch expire.
func (db *RedisDB) Set(k, v string) {
	db.master.Lock()
	defer db.master.Unlock()
	db.set(k, v)
}

// internal not-locked version. Doesn't touch expire.
func (db *RedisDB) set(k, v string) {
	db.keys[k] = "string"
	db.stringKeys[k] = v
	db.keyVersion[k]++
}

// Incr changes a int string value by delta.
func (m *Miniredis) Incr(k string, delta int) (int, error) {
	return m.DB(m.selectedDB).Incr(k, delta)
}

// Incr changes a int string value by delta.
func (db *RedisDB) Incr(k string, delta int) (int, error) {
	db.master.Lock()
	defer db.master.Unlock()
	return db.incr(k, delta)
}

// change int key value
func (db *RedisDB) incr(k string, delta int) (int, error) {
	v := 0
	if sv, ok := db.stringKeys[k]; ok {
		var err error
		v, err = strconv.Atoi(sv)
		if err != nil {
			return 0, errIntValueError
		}
	}
	v += delta
	db.set(k, strconv.Itoa(v))
	return v, nil
}

// change float key value
func (db *RedisDB) incrfloat(k string, delta float64) (string, error) {
	v := 0.0
	if sv, ok := db.stringKeys[k]; ok {
		var err error
		v, err = strconv.ParseFloat(sv, 64)
		if err != nil {
			return "0", errFloatValueError
		}
	}
	v += delta
	// Format with %f and strip trailing 0s. This is the most like Redis does
	// it :(
	sv := fmt.Sprintf("%.12f", v)
	for strings.Contains(sv, ".") {
		if sv[len(sv)-1] != '0' {
			break
		}
		// Remove trailing 0s.
		sv = sv[:len(sv)-1]
		// Ends with a '.'.
		if sv[len(sv)-1] == '.' {
			sv = sv[:len(sv)-1]
			break
		}
	}
	db.set(k, sv)
	return sv, nil
}

// commandsString handles all string value operations.
func commandsString(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("SET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'set' command")
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
					setDirty(r.Client())
					out.WriteErrorString("ERR value is not an integer or out of range")
					return nil
				}
				var err error
				expire, err = strconv.Atoi(r.Args[1])
				if err != nil {
					setDirty(r.Client())
					out.WriteErrorString("ERR value is not an integer or out of range")
					return nil
				}
				r.Args = r.Args[2:]
				continue
			default:
				setDirty(r.Client())
				out.WriteErrorString("ERR syntax error")
				return nil
			}
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if nx {
				if _, ok := db.keys[key]; ok {
					out.WriteNil()
					return
				}
			}
			if xx {
				if _, ok := db.keys[key]; !ok {
					out.WriteNil()
					return
				}
			}

			db.del(key, true) // be sure to remove existing values of other type keys.
			// a vanilla SET clears the expire
			db.set(key, value)
			if expire != 0 {
				db.expire[key] = expire
			}
			out.WriteOK()
		})
	})

	srv.HandleFunc("SETEX", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'setex' command")
			return nil
		}
		key := r.Args[0]
		ttl, err := strconv.Atoi(r.Args[1])
		if err != nil {
			setDirty(r.Client())
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}
		value := r.Args[2]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			db.del(key, true) // Clear any existing keys.
			db.set(key, value)
			db.expire[key] = ttl
			out.WriteOK()
		})
	})

	srv.HandleFunc("PSETEX", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'psetex' command")
			return nil
		}
		key := r.Args[0]
		ttl, err := strconv.Atoi(r.Args[1])
		if err != nil {
			setDirty(r.Client())
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}
		value := r.Args[2]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			db.del(key, true) // Clear any existing keys.
			db.set(key, value)
			db.expire[key] = ttl // We put millisecond keys in with the second keys.
			out.WriteOK()
		})
	})

	srv.HandleFunc("SETNX", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			setDirty(r.Client())
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		value := r.Args[1]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if _, ok := db.keys[key]; ok {
				out.WriteZero()
				return
			}

			db.set(key, value)
			out.WriteOne()
		})
	})

	srv.HandleFunc("MSET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'mset' command")
			return nil
		}
		if len(r.Args)%2 != 0 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for MSET")
			return nil
		}
		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			for len(r.Args) > 0 {
				key := r.Args[0]
				value := r.Args[1]
				r.Args = r.Args[2:]

				db.del(key, true) // clear TTL
				db.set(key, value)
			}
			out.WriteOK()
		})
	})

	srv.HandleFunc("MSETNX", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'msetnx' command")
			return nil
		}
		if len(r.Args)%2 != 0 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for MSET")
			return nil
		}
		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			keys := map[string]string{}
			existing := false
			for len(r.Args) > 0 {
				key := r.Args[0]
				value := r.Args[1]
				r.Args = r.Args[2:]
				keys[key] = value
				if _, ok := db.keys[key]; ok {
					existing = true
				}
			}

			res := 0
			if !existing {
				res = 1
				for k, v := range keys {
					// Nothing to delete. That's the whole point.
					db.set(k, v)
				}
			}
			out.WriteInt(res)
		})
	})

	srv.HandleFunc("GET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			setDirty(r.Client())
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			{
				t, ok := db.keys[key]
				if !ok {
					out.WriteNil()
					return
				}
				if t != "string" {
					setDirty(r.Client())
					out.WriteErrorString(msgWrongType)
					return
				}
			}

			value, ok := db.stringKeys[key]
			if !ok {
				out.WriteNil()
				return
			}
			out.WriteString(value)
		})
	})

	srv.HandleFunc("GETSET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			setDirty(r.Client())
			out.WriteErrorString("usage error")
			return nil
		}
		key := r.Args[0]
		value := r.Args[1]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}

			old, ok := db.stringKeys[key]
			db.set(key, value)
			// a GETSET clears the expire
			delete(db.expire, key)

			if !ok {
				out.WriteNil()
				return
			}
			out.WriteString(old)
			return
		})
	})

	srv.HandleFunc("MGET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 1 {
			setDirty(r.Client())
			out.WriteErrorString("usage error")
			return nil
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

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
		})
	})

	srv.HandleFunc("INCR", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'incr' command")
			return nil
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			key := r.Args[0]
			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}
			v, err := db.incr(key, +1)
			if err != nil {
				out.WriteErrorString(err.Error())
				return
			}
			// Don't touch TTL
			out.WriteInt(v)
		})
	})

	srv.HandleFunc("INCRBY", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'incrby' command")
			return nil
		}

		key := r.Args[0]
		delta, err := strconv.Atoi(r.Args[1])
		if err != nil {
			setDirty(r.Client())
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}

			v, err := db.incr(key, delta)
			if err != nil {
				out.WriteErrorString(err.Error())
				return
			}
			// Don't touch TTL
			out.WriteInt(v)
		})
	})

	srv.HandleFunc("INCRBYFLOAT", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'incrbyfloat' command")
			return nil
		}

		key := r.Args[0]
		delta, err := strconv.ParseFloat(r.Args[1], 64)
		if err != nil {
			setDirty(r.Client())
			out.WriteErrorString("ERR value is not a valid float")
			return nil
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}

			v, err := db.incrfloat(key, delta)
			if err != nil {
				out.WriteErrorString(err.Error())
				return
			}
			// Don't touch TTL
			out.WriteString(v)
		})
	})

	srv.HandleFunc("DECR", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'decr' command")
			return nil
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			key := r.Args[0]
			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}
			v, err := db.incr(key, -1)
			if err != nil {
				out.WriteErrorString(err.Error())
				return
			}
			// Don't touch TTL
			out.WriteInt(v)
		})
	})

	srv.HandleFunc("DECRBY", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'decrby' command")
			return nil
		}

		key := r.Args[0]
		delta, err := strconv.Atoi(r.Args[1])
		if err != nil {
			setDirty(r.Client())
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}

			v, err := db.incr(key, -delta)
			if err != nil {
				out.WriteErrorString(err.Error())
				return
			}
			// Don't touch TTL
			out.WriteInt(v)
		})
	})

	srv.HandleFunc("STRLEN", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'strlen' command")
			return nil
		}

		key := r.Args[0]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}

			out.WriteInt(len(db.stringKeys[key]))
		})
	})

	srv.HandleFunc("APPEND", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'append' command")
			return nil
		}

		key := r.Args[0]
		value := r.Args[1]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}

			newValue := db.stringKeys[key] + value
			db.set(key, newValue)

			out.WriteInt(len(newValue))
		})
	})

	srv.HandleFunc("GETRANGE", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'getrange' command")
			return nil
		}

		key := r.Args[0]
		start, err := strconv.Atoi(r.Args[1])
		if err != nil {
			setDirty(r.Client())
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}
		end, err := strconv.Atoi(r.Args[2])
		if err != nil {
			setDirty(r.Client())
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}

			v := db.stringKeys[key]
			out.WriteString(withRange(v, start, end))
		})
	})

	srv.HandleFunc("SETRANGE", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'setrange' command")
			return nil
		}

		key := r.Args[0]
		pos, err := strconv.Atoi(r.Args[1])
		if err != nil {
			setDirty(r.Client())
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}
		if pos < 0 {
			setDirty(r.Client())
			out.WriteErrorString("ERR offset is out of range")
			return nil
		}
		subst := r.Args[2]

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}

			v := []byte(db.stringKeys[key])
			if len(v) < pos+len(subst) {
				newV := make([]byte, pos+len(subst))
				copy(newV, v)
				v = newV
			}
			copy(v[pos:pos+len(subst)], subst)
			db.set(key, string(v))
			out.WriteInt(len(v))
		})
	})

	srv.HandleFunc("BITCOUNT", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 && len(r.Args) != 3 {
			setDirty(r.Client())
			out.WriteErrorString("ERR syntax error")
			return nil
		}

		key := r.Args[0]
		useRange := false
		start, end := 0, 0
		if len(r.Args) == 3 {
			useRange = true
			var err error
			start, err = strconv.Atoi(r.Args[1])
			if err != nil {
				setDirty(r.Client())
				out.WriteErrorString("ERR value is not an integer or out of range")
				return nil
			}
			end, err = strconv.Atoi(r.Args[2])
			if err != nil {
				setDirty(r.Client())
				out.WriteErrorString("ERR value is not an integer or out of range")
				return nil
			}
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}

			v := db.stringKeys[key]
			if useRange {
				v = withRange(v, start, end)
			}

			out.WriteInt(countBits([]byte(v)))
		})
	})

	srv.HandleFunc("BITOP", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 3 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'bitop' command")
			return nil
		}

		op := strings.ToUpper(r.Args[0])
		target := r.Args[1]
		input := r.Args[2:]

		// 'op' is tested when the transaction is executed.
		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			switch op {
			case "AND", "OR", "XOR":
				first := input[0]
				if t, ok := db.keys[first]; ok && t != "string" {
					out.WriteErrorString(msgWrongType)
					return
				}
				res := []byte(db.stringKeys[first])
				for _, vk := range input[1:] {
					if t, ok := db.keys[vk]; ok && t != "string" {
						out.WriteErrorString(msgWrongType)
						return
					}
					v := db.stringKeys[vk]
					cb := map[string]func(byte, byte) byte{
						"AND": func(a, b byte) byte { return a & b },
						"OR":  func(a, b byte) byte { return a | b },
						"XOR": func(a, b byte) byte { return a ^ b },
					}[op]
					res = sliceBinOp(cb, res, []byte(v))
				}
				db.del(target, false) // Keep TTL
				if len(res) == 0 {
					db.del(target, true)
				} else {
					db.set(target, string(res))
				}
				out.WriteInt(len(res))
			case "NOT":
				// NOT only takes a single argument.
				if len(input) != 1 {
					out.WriteErrorString("ERR BITOP NOT must be called with a single source key.")
					return
				}
				key := input[0]
				if t, ok := db.keys[key]; ok && t != "string" {
					out.WriteErrorString(msgWrongType)
					return
				}
				value := []byte(db.stringKeys[key])
				for i := range value {
					value[i] = ^value[i]
				}
				db.del(target, false) // Keep TTL
				if len(value) == 0 {
					db.del(target, true)
				} else {
					db.set(target, string(value))
				}
				out.WriteInt(len(value))
			default:
				out.WriteErrorString("ERR syntax error")
			}
		})
	})

	srv.HandleFunc("BITPOS", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 || len(r.Args) > 4 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'bitpos' command")
			return nil
		}

		key := r.Args[0]
		bit, err := strconv.Atoi(r.Args[1])
		if err != nil {
			setDirty(r.Client())
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}
		var start, end int
		withEnd := false
		if len(r.Args) > 2 {
			start, err = strconv.Atoi(r.Args[2])
			if err != nil {
				setDirty(r.Client())
				out.WriteErrorString("ERR value is not an integer or out of range")
				return nil
			}
		}
		if len(r.Args) > 3 {
			end, err = strconv.Atoi(r.Args[3])
			if err != nil {
				setDirty(r.Client())
				out.WriteErrorString("ERR value is not an integer or out of range")
				return nil
			}
			withEnd = true
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}
			value := db.stringKeys[key]
			if start != 0 {
				if start > len(value) {
					start = len(value)
				}
			}
			if withEnd {
				end++ // redis end semantics.
				if end < 0 {
					end = len(value) + end
				}
				if end > len(value) {
					end = len(value)
				}
			} else {
				end = len(value)
			}
			if start != 0 || withEnd {
				if end < start {
					value = ""
				} else {
					value = value[start:end]
				}
			}
			pos := bitPos([]byte(value), bit == 1)
			if pos >= 0 {
				pos += start * 8
			}
			// Special case when looking for 0, but not when start and end are
			// given.
			if bit == 0 && pos == -1 && !withEnd {
				pos = start*8 + len(value)*8
			}
			out.WriteInt(pos)
		})
	})

	srv.HandleFunc("GETBIT", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'getbit' command")
			return nil
		}

		key := r.Args[0]
		bit, err := strconv.Atoi(r.Args[1])
		if err != nil {
			setDirty(r.Client())
			out.WriteErrorString("ERR bit offset is not an integer or out of range")
			return nil
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}
			value := db.stringKeys[key]

			ourByteNr := bit / 8
			var ourByte byte
			if ourByteNr > len(value)-1 {
				ourByte = '\x00'
			} else {
				ourByte = value[ourByteNr]
			}
			res := 0
			if toBits(ourByte)[bit%8] {
				res = 1
			}
			out.WriteInt(res)
		})
	})

	srv.HandleFunc("SETBIT", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			setDirty(r.Client())
			out.WriteErrorString("ERR wrong number of arguments for 'setbit' command")
			return nil
		}

		key := r.Args[0]
		bit, err := strconv.Atoi(r.Args[1])
		if err != nil || bit < 0 {
			setDirty(r.Client())
			out.WriteErrorString("ERR bit offset is not an integer or out of range")
			return nil
		}
		newBit, err := strconv.Atoi(r.Args[2])
		if err != nil || (newBit != 0 && newBit != 1) {
			setDirty(r.Client())
			out.WriteErrorString("ERR bit is not an integer or out of range")
			return nil
		}

		return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString(msgWrongType)
				return
			}
			value := []byte(db.stringKeys[key])

			ourByteNr := bit / 8
			ourBitNr := bit % 8
			if ourByteNr > len(value)-1 {
				// Too short. Expand.
				newValue := make([]byte, ourByteNr+1)
				copy(newValue, value)
				value = newValue
			}
			old := 0
			if toBits(value[ourByteNr])[ourBitNr] {
				old = 1
			}
			if newBit == 0 {
				value[ourByteNr] &^= 1 << uint8(7-ourBitNr)
			} else {
				value[ourByteNr] |= 1 << uint8(7-ourBitNr)
			}
			db.set(key, string(value))

			out.WriteInt(old)
		})
	})
}

// Redis range. both start and end can be negative.
func withRange(v string, start, end int) string {
	if start < 0 {
		start = len(v) + start
		if start < 0 {
			start = 0
		}
	}
	if start > len(v) {
		start = len(v)
	}

	if end < 0 {
		end = len(v) + end
		if end < 0 {
			end = 0
		}
	}
	end++ // end argument is inclusive in Redis.
	if end > len(v) {
		end = len(v)
	}

	if end < start {
		return ""
	}
	return v[start:end]
}

func countBits(v []byte) int {
	count := 0
	for _, b := range []byte(v) {
		for b > 0 {
			count += int((b % uint8(2)))
			b = b >> 1
		}
	}
	return count
}

// sliceBinOp applies an operator to all slice elements, with Redis string
// padding logic.
func sliceBinOp(f func(a, b byte) byte, a, b []byte) []byte {
	maxl := len(a)
	if len(b) > maxl {
		maxl = len(b)
	}
	lA := make([]byte, maxl)
	copy(lA, a)
	lB := make([]byte, maxl)
	copy(lB, b)
	res := make([]byte, maxl)
	for i := range res {
		res[i] = f(lA[i], lB[i])
	}
	return res
}

// Return the number of the first bit set/unset.
func bitPos(s []byte, bit bool) int {
	for i, b := range s {
		for j, set := range toBits(b) {
			if set == bit {
				return i*8 + j
			}
		}
	}
	return -1
}

// toBits changes a byte in 8 bools.
func toBits(s byte) [8]bool {
	r := [8]bool{}
	for i := range r {
		if s&(uint8(1)<<uint8(7-i)) != 0 {
			r[i] = true
		}
	}
	return r
}
