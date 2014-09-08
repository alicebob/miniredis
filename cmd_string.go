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

// change float key value
func (db *redisDB) incrfloat(k string, delta float64) (string, error) {
	v := 0.0
	if sv, ok := db.stringKeys[k]; ok {
		var err error
		v, err = strconv.ParseFloat(sv, 64)
		if err != nil {
			return "0", errValueError
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
	db.stringKeys[k] = sv
	return sv, nil
}

// commandsString handles all string value operations.
func commandsString(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("SET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
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
					out.WriteErrorString("ERR value is not an integer or out of range")
					return nil
				}
				var err error
				expire, err = strconv.Atoi(r.Args[1])
				if err != nil {
					out.WriteErrorString("ERR value is not an integer or out of range")
					return nil
				}
				r.Args = r.Args[2:]
				continue
			default:
				out.WriteErrorString("ERR syntax error")
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
		if len(r.Args) < 2 {
			out.WriteErrorString("ERR wrong number of arguments for 'mset' command")
			return nil
		}
		if len(r.Args)%2 != 0 {
			out.WriteErrorString("ERR wrong number of arguments for MSET")
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

	srv.HandleFunc("MSETNX", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			out.WriteErrorString("ERR wrong number of arguments for 'msetnx' command")
			return nil
		}
		if len(r.Args)%2 != 0 {
			out.WriteErrorString("ERR wrong number of arguments for MSET")
			return nil
		}
		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

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
			out.WriteErrorString("ERR wrong number of arguments for 'incr' command")
			return nil
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		key := r.Args[0]
		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")
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
			out.WriteErrorString("ERR wrong number of arguments for 'incrby' command")
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
			out.WriteErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")
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

	srv.HandleFunc("INCRBYFLOAT", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			out.WriteErrorString("ERR wrong number of arguments for 'incrbyfloat' command")
			return nil
		}

		key := r.Args[0]
		delta, err := strconv.ParseFloat(r.Args[1], 64)
		if err != nil {
			out.WriteErrorString("value error")
			return nil
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}

		v, err := db.incrfloat(key, delta)
		if err != nil {
			out.WriteErrorString(err.Error())
			return nil
		}
		// Don't touch TTL
		out.WriteString(v)
		return nil
	})

	srv.HandleFunc("DECR", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 {
			out.WriteErrorString("ERR wrong number of arguments for 'decr' command")
			return nil
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		key := r.Args[0]
		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")
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
			out.WriteErrorString("ERR wrong number of arguments for 'decrby' command")
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

	srv.HandleFunc("APPEND", func(out *redeo.Responder, r *redeo.Request) error {
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
			out.WriteErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}

		newValue := db.stringKeys[key] + value
		db.stringKeys[key] = newValue

		out.WriteInt(len(newValue))
		return nil
	})

	srv.HandleFunc("GETRANGE", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 3 {
			out.WriteErrorString("ERR wrong number of arguments for 'getrange' command")
			return nil
		}

		key := r.Args[0]
		start, err := strconv.Atoi(r.Args[1])
		if err != nil {
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}
		end, err := strconv.Atoi(r.Args[2])
		if err != nil {
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}

		v := db.stringKeys[key]
		out.WriteString(withRange(v, start, end))
		return nil
	})

	srv.HandleFunc("BITCOUNT", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 1 && len(r.Args) != 3 {
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
				out.WriteErrorString("ERR value is not an integer or out of range")
				return nil
			}
			end, err = strconv.Atoi(r.Args[2])
			if err != nil {
				out.WriteErrorString("ERR value is not an integer or out of range")
				return nil
			}
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
		}

		v := db.stringKeys[key]
		if useRange {
			v = withRange(v, start, end)
		}

		out.WriteInt(countBits([]byte(v)))
		return nil
	})

	srv.HandleFunc("BITOP", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 3 {
			out.WriteErrorString("ERR wrong number of arguments for 'bitop' command")
			return nil
		}

		op := strings.ToUpper(r.Args[0])
		target := r.Args[1]
		input := r.Args[2:]

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		switch op {
		case "AND", "OR", "XOR":
			first := input[0]
			if t, ok := db.keys[first]; ok && t != "string" {
				out.WriteErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")
				return nil
			}
			res := []byte(db.stringKeys[first])
			for _, vk := range input[1:] {
				if t, ok := db.keys[vk]; ok && t != "string" {
					out.WriteErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")
					return nil
				}
				v := db.stringKeys[vk]
				cb := map[string]func(byte, byte) byte{
					"AND": func(a, b byte) byte { return a & b },
					"OR":  func(a, b byte) byte { return a | b },
					"XOR": func(a, b byte) byte { return a ^ b },
				}[op]
				res = sliceBinOp(cb, res, []byte(v))
			}
			db.del(target) // Keep TTL
			db.set(target, string(res))
			out.WriteInt(len(res))
			return nil
		case "NOT":
			// NOT only takes a single argument.
			if len(input) != 1 {
				out.WriteErrorString("ERR BITOP NOT must be called with a single source key.")
				return nil
			}
			key := input[0]
			if t, ok := db.keys[key]; ok && t != "string" {
				out.WriteErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")
				return nil
			}
			value := []byte(db.stringKeys[key])
			for i := range value {
				value[i] = ^value[i]
			}
			db.del(target) // Keep TTL
			db.set(target, string(value))
			out.WriteInt(len(value))
			return nil
		default:
			out.WriteErrorString("ERR syntax error")
			return nil
		}

	})

	srv.HandleFunc("BITPOS", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 || len(r.Args) > 4 {
			out.WriteErrorString("ERR wrong number of arguments for 'bitpos' command")
			return nil
		}

		key := r.Args[0]
		bit, err := strconv.Atoi(r.Args[1])
		if err != nil {
			out.WriteErrorString("ERR value is not an integer or out of range")
			return nil
		}
		var start, end int
		withEnd := false
		if len(r.Args) > 2 {
			start, err = strconv.Atoi(r.Args[2])
			if err != nil {
				out.WriteErrorString("ERR value is not an integer or out of range")
				return nil
			}
		}
		if len(r.Args) > 3 {
			end, err = strconv.Atoi(r.Args[3])
			if err != nil {
				out.WriteErrorString("ERR value is not an integer or out of range")
				return nil
			}
			withEnd = true
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
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
		return nil
	})

	srv.HandleFunc("GETBIT", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) != 2 {
			out.WriteErrorString("ERR wrong number of arguments for 'getbit' command")
			return nil
		}

		key := r.Args[0]
		bit, err := strconv.Atoi(r.Args[1])
		if err != nil {
			out.WriteErrorString("ERR bit offset is not an integer or out of range")
			return nil
		}

		db := m.dbFor(r.Client().ID)
		db.Lock()
		defer db.Unlock()

		if t, ok := db.keys[key]; ok && t != "string" {
			out.WriteErrorString("WRONGTYPE Operation against a key holding the wrong kind of value")
			return nil
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
		return nil
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
