// Commands from http://redis.io/commands#string

package miniredis

import (
	"strconv"
	"strings"
	"time"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
)

// commandsString handles all string value operations.
func commandsString(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("APPEND", m.cmdAppend)
	srv.HandleFunc("BITCOUNT", m.cmdBitcount)
	srv.HandleFunc("BITOP", m.cmdBitop)
	srv.HandleFunc("BITPOS", m.cmdBitpos)
	srv.HandleFunc("DECRBY", m.cmdDecrby)
	srv.HandleFunc("DECR", m.cmdDecr)
	srv.HandleFunc("GETBIT", m.cmdGetbit)
	srv.HandleFunc("GET", m.cmdGet)
	srv.HandleFunc("GETRANGE", m.cmdGetrange)
	srv.HandleFunc("GETSET", m.cmdGetset)
	srv.HandleFunc("INCRBYFLOAT", m.cmdIncrbyfloat)
	srv.HandleFunc("INCRBY", m.cmdIncrby)
	srv.HandleFunc("INCR", m.cmdIncr)
	srv.HandleFunc("MGET", m.cmdMget)
	srv.HandleFunc("MSET", m.cmdMset)
	srv.HandleFunc("MSETNX", m.cmdMsetnx)
	srv.HandleFunc("PSETEX", m.cmdPsetex)
	srv.HandleFunc("SETBIT", m.cmdSetbit)
	srv.HandleFunc("SETEX", m.cmdSetex)
	srv.HandleFunc("SET", m.cmdSet)
	srv.HandleFunc("SETNX", m.cmdSetnx)
	srv.HandleFunc("SETRANGE", m.cmdSetrange)
	srv.HandleFunc("STRLEN", m.cmdStrlen)
}

// SET
func (m *Miniredis) cmdSet(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	var (
		nx  = false // set iff not exists
		xx  = false // set iff exists
		ttl time.Duration
	)

	args := asString(r.Args())
	key := args[0]
	value := args[1]
	args = args[2:]
	for len(args) > 0 {
		timeUnit := time.Second
		switch strings.ToUpper(args[0]) {
		case "NX":
			nx = true
			args = args[1:]
			continue
		case "XX":
			xx = true
			args = args[1:]
			continue
		case "PX":
			timeUnit = time.Millisecond
			fallthrough
		case "EX":
			if len(args) < 2 {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgInvalidInt)
				return
			}
			expire, err := strconv.Atoi(args[1])
			if err != nil {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgInvalidInt)
				return
			}
			ttl = time.Duration(expire) * timeUnit
			if ttl <= 0 {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgInvalidSETime)
				return
			}

			args = args[2:]
			continue
		default:
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgSyntaxError)
			return
		}
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if nx {
			if db.exists(key) {
				out.AppendNil()
				return
			}
		}
		if xx {
			if !db.exists(key) {
				out.AppendNil()
				return
			}
		}

		db.del(key, true) // be sure to remove existing values of other type keys.
		// a vanilla SET clears the expire
		db.stringSet(key, value)
		if ttl != 0 {
			db.ttl[key] = ttl
		}
		out.AppendOK()
	})
}

// SETEX
func (m *Miniredis) cmdSetex(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()
	ttl, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}
	if ttl <= 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidSETEXTime)
		return
	}
	value := r.Arg(2).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		db.del(key, true) // Clear any existing keys.
		db.stringSet(key, value)
		db.ttl[key] = time.Duration(ttl) * time.Second
		out.AppendOK()
	})
}

// PSETEX
func (m *Miniredis) cmdPsetex(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()
	ttl, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}
	if ttl <= 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidPSETEXTime)
		return
	}
	value := r.Arg(2).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		db.del(key, true) // Clear any existing keys.
		db.stringSet(key, value)
		db.ttl[key] = time.Duration(ttl) * time.Millisecond
		out.AppendOK()
	})
}

// SETNX
func (m *Miniredis) cmdSetnx(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()
	value := r.Arg(1).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if _, ok := db.keys[key]; ok {
			out.AppendInt(0)
			return
		}

		db.stringSet(key, value)
		out.AppendInt(1)
	})
}

// MSET
func (m *Miniredis) cmdMset(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	if r.ArgN()%2 != 0 {
		setDirty(redeo.GetClient(r.Context()))
		// non-default error message
		out.AppendError("ERR wrong number of arguments for MSET")
		return
	}
	args := asString(r.Args())
	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		for len(args) > 0 {
			key := args[0]
			value := args[1]
			args = args[2:]

			db.del(key, true) // clear TTL
			db.stringSet(key, value)
		}
		out.AppendOK()
	})
}

// MSETNX
func (m *Miniredis) cmdMsetnx(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	if r.ArgN()%2 != 0 {
		setDirty(redeo.GetClient(r.Context()))
		// non-default error message (yes, with 'MSET').
		out.AppendError("ERR wrong number of arguments for MSET")
		return
	}
	args := asString(r.Args())
	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		keys := map[string]string{}
		existing := false
		for len(args) > 0 {
			key := args[0]
			value := args[1]
			args = args[2:]
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
				db.stringSet(k, v)
			}
		}
		out.AppendInt(int64(res))
	})
}

// GET
func (m *Miniredis) cmdGet(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendNil()
			return
		}
		if db.t(key) != "string" {
			out.AppendError(msgWrongType)
			return
		}

		out.AppendBulkString(db.stringGet(key))
	})
}

// GETSET
func (m *Miniredis) cmdGetset(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()
	value := r.Arg(1).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
			return
		}

		old, ok := db.stringKeys[key]
		db.stringSet(key, value)
		// a GETSET clears the ttl
		delete(db.ttl, key)

		if !ok {
			out.AppendNil()
			return
		}
		out.AppendBulkString(old)
		return
	})
}

// MGET
func (m *Miniredis) cmdMget(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	args := asString(r.Args())

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		out.AppendArrayLen(len(args))
		for _, k := range args {
			if t, ok := db.keys[k]; !ok || t != "string" {
				out.AppendNil()
				continue
			}
			v, ok := db.stringKeys[k]
			if !ok {
				// Should not happen, we just checked keys[]
				out.AppendNil()
				continue
			}
			out.AppendBulkString(v)
		}
	})
}

// INCR
func (m *Miniredis) cmdIncr(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}
	key := r.Arg(0).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
			return
		}
		v, err := db.stringIncr(key, +1)
		if err != nil {
			out.AppendError(err.Error())
			return
		}
		// Don't touch TTL
		out.AppendInt(int64(v))
	})
}

// INCRBY
func (m *Miniredis) cmdIncrby(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	delta, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
			return
		}

		v, err := db.stringIncr(key, delta)
		if err != nil {
			out.AppendError(err.Error())
			return
		}
		// Don't touch TTL
		out.AppendInt(int64(v))
	})
}

// INCRBYFLOAT
func (m *Miniredis) cmdIncrbyfloat(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	delta, err := strconv.ParseFloat(r.Arg(1).String(), 64)
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidFloat)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
			return
		}

		v, err := db.stringIncrfloat(key, delta)
		if err != nil {
			out.AppendError(err.Error())
			return
		}
		// Don't touch TTL
		out.AppendBulkString(formatFloat(v))
	})
}

// DECR
func (m *Miniredis) cmdDecr(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
			return
		}
		v, err := db.stringIncr(key, -1)
		if err != nil {
			out.AppendError(err.Error())
			return
		}
		// Don't touch TTL
		out.AppendInt(int64(v))
	})
}

// DECRBY
func (m *Miniredis) cmdDecrby(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	delta, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
			return
		}

		v, err := db.stringIncr(key, -delta)
		if err != nil {
			out.AppendError(err.Error())
			return
		}
		// Don't touch TTL
		out.AppendInt(int64(v))
	})
}

// STRLEN
func (m *Miniredis) cmdStrlen(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
			return
		}

		out.AppendInt(int64(len(db.stringKeys[key])))
	})
}

// APPEND
func (m *Miniredis) cmdAppend(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	value := r.Arg(1).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
			return
		}

		newValue := db.stringKeys[key] + value
		db.stringSet(key, newValue)

		out.AppendInt(int64(len(newValue)))
	})
}

// GETRANGE
func (m *Miniredis) cmdGetrange(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	start, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}
	end, err := strconv.Atoi(r.Arg(2).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
			return
		}

		v := db.stringKeys[key]
		out.AppendBulkString(withRange(v, start, end))
	})
}

// SETRANGE
func (m *Miniredis) cmdSetrange(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	pos, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}
	if pos < 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError("ERR offset is out of range")
		return
	}
	subst := r.Arg(2)

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
			return
		}

		v := []byte(db.stringKeys[key])
		if len(v) < pos+len(subst) {
			newV := make([]byte, pos+len(subst))
			copy(newV, v)
			v = newV
		}
		copy(v[pos:pos+len(subst)], subst)
		db.stringSet(key, string(v))
		out.AppendInt(int64(len(v)))
	})
}

// BITCOUNT
func (m *Miniredis) cmdBitcount(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	var (
		key        = r.Arg(0).String()
		useRange   = false
		start, end = 0, 0
		args       = asString(r.Args()[1:])
	)
	if len(args) >= 2 {
		useRange = true
		var err error
		start, err = strconv.Atoi(args[0])
		if err != nil {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgInvalidInt)
			return
		}
		end, err = strconv.Atoi(args[1])
		if err != nil {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgInvalidInt)
			return
		}
		args = args[2:]
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendInt(0)
			return
		}
		if db.t(key) != "string" {
			out.AppendError(msgWrongType)
			return
		}

		// Real redis only checks after it knows the key is there and a string.
		if len(args) != 0 {
			out.AppendError(msgSyntaxError)
			return
		}

		v := db.stringKeys[key]
		if useRange {
			v = withRange(v, start, end)
		}

		out.AppendInt(int64(countBits([]byte(v))))
	})
}

// BITOP
func (m *Miniredis) cmdBitop(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	var (
		op     = strings.ToUpper(r.Arg(0).String())
		target = r.Arg(1).String()
		input  = asString(r.Args()[2:])
	)

	// 'op' is tested when the transaction is executed.
	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		switch op {
		case "AND", "OR", "XOR":
			first := input[0]
			if t, ok := db.keys[first]; ok && t != "string" {
				out.AppendError(msgWrongType)
				return
			}
			res := []byte(db.stringKeys[first])
			for _, vk := range input[1:] {
				if t, ok := db.keys[vk]; ok && t != "string" {
					out.AppendError(msgWrongType)
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
				db.stringSet(target, string(res))
			}
			out.AppendInt(int64(len(res)))
		case "NOT":
			// NOT only takes a single argument.
			if len(input) != 1 {
				out.AppendError("ERR BITOP NOT must be called with a single source key.")
				return
			}
			key := input[0]
			if t, ok := db.keys[key]; ok && t != "string" {
				out.AppendError(msgWrongType)
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
				db.stringSet(target, string(value))
			}
			out.AppendInt(int64(len(value)))
		default:
			out.AppendError(msgSyntaxError)
		}
	})
}

// BITPOS
func (m *Miniredis) cmdBitpos(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 || r.ArgN() > 4 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	bit, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}
	var start, end int
	withEnd := false
	if r.ArgN() > 2 {
		start, err = strconv.Atoi(r.Arg(2).String())
		if err != nil {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgInvalidInt)
			return
		}
	}
	if r.ArgN() > 3 {
		end, err = strconv.Atoi(r.Arg(3).String())
		if err != nil {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgInvalidInt)
			return
		}
		withEnd = true
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
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
		out.AppendInt(int64(pos))
	})
}

// GETBIT
func (m *Miniredis) cmdGetbit(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	bit, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError("ERR bit offset is not an integer or out of range")
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
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
		out.AppendInt(int64(res))
	})
}

// SETBIT
func (m *Miniredis) cmdSetbit(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	bit, err := strconv.Atoi(r.Arg(1).String())
	if err != nil || bit < 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError("ERR bit offset is not an integer or out of range")
		return
	}
	newBit, err := strconv.Atoi(r.Arg(2).String())
	if err != nil || (newBit != 0 && newBit != 1) {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError("ERR bit is not an integer or out of range")
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[key]; ok && t != "string" {
			out.AppendError(msgWrongType)
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
		db.stringSet(key, string(value))

		out.AppendInt(int64(old))
	})
}

// Redis range. both start and end can be negative.
func withRange(v string, start, end int) string {
	s, e := redisRange(len(v), start, end, true /* string getrange symantics */)
	return v[s:e]
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
